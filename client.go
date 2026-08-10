package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxErrorBody = 64 << 10

// Client is a minimal FMSG-003 Web API client (API key auth, inbox, watch, send).
type Client struct {
	baseURL    *url.URL
	apiKey     string
	httpClient *http.Client
	dialer     *websocket.Dialer

	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
	address     string
}

// Attachment is unused by the bot but present on list payloads.
type Attachment struct {
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

// RecipientDelivery is delivery state for one recipient.
type RecipientDelivery struct {
	Addr string `json:"addr"`
}

// AddToBatch is one batch of recipients added to a message.
type AddToBatch struct {
	BatchID   int64    `json:"batch_id"`
	AddToFrom string   `json:"add_to_from"`
	To        []string `json:"to"`
}

// Message is inbox / event message metadata.
type Message struct {
	ID        int64       `json:"id"`
	Version   int         `json:"version"`
	HasPID    bool        `json:"has_pid"`
	HasAddTo  bool        `json:"has_add_to"`
	Important bool        `json:"important"`
	NoReply   bool        `json:"no_reply"`
	PID       *int64      `json:"pid"`
	From      string      `json:"from"`
	To        []string    `json:"to"`
	AddTo     []AddToBatch `json:"add_to"`
	Time      *float64    `json:"time"`
	Topic     string      `json:"topic"`
	Type      string      `json:"type"`
	Size      int64       `json:"size"`
}

// APIError is a non-2xx Web API response.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("fmsg Web API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("fmsg Web API returned HTTP %d: %s", e.StatusCode, e.Message)
}

// NewClient builds a client for baseURL authenticated with apiKey.
func NewClient(baseURL, apiKey string) (*Client, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid Web API base URL %q", baseURL)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, errors.New("Web API base URL must use http or https")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("Web API base URL must not contain a query or fragment")
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, errors.New("API key is required")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	return &Client{
		baseURL:    base,
		apiKey:     strings.TrimSpace(apiKey),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		dialer:     websocket.DefaultDialer,
	}, nil
}

// Address returns the effective fmsg address (JWT sub) after a successful token exchange.
func (c *Client) Address() string {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.address
}

// EnsureToken exchanges the API key if needed and returns the address.
func (c *Client) EnsureToken(ctx context.Context) (string, error) {
	if _, err := c.token(ctx); err != nil {
		return "", err
	}
	return c.Address(), nil
}

// ListInbox returns a page of inbox messages.
func (c *Client) ListInbox(ctx context.Context, limit, offset int) ([]Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	path := "/fmsg?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	var messages []Message
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// MaxInboxID returns the highest local message ID currently in the inbox, or 0 if empty.
func (c *Client) MaxInboxID(ctx context.Context) (int64, error) {
	// Inbox list is newest-first; first page[0] is the latest.
	page, err := c.ListInbox(ctx, 1, 0)
	if err != nil {
		return 0, err
	}
	if len(page) == 0 {
		return 0, nil
	}
	return page[0].ID, nil
}

// Data returns the raw message body.
func (c *Client) Data(ctx context.Context, id int64) ([]byte, error) {
	if id <= 0 {
		return nil, errors.New("message ID must be positive")
	}
	resp, err := c.do(ctx, http.MethodGet, "/fmsg/"+strconv.FormatInt(id, 10)+"/data", nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read message data: %w", err)
	}
	return data, nil
}

// CreateAndSend posts a draft and sends it. Returns the draft id.
func (c *Client) CreateAndSend(ctx context.Context, from string, to []string, pid int64, body string) (int64, error) {
	if from == "" {
		from = c.Address()
	}
	if len(to) == 0 {
		return 0, errors.New("at least one recipient is required")
	}
	draft := map[string]any{
		"version": 1,
		"from":    from,
		"to":      to,
		"type":    "text/plain; charset=utf-8",
		"size":    len([]byte(body)),
		"data":    body,
	}
	if pid > 0 {
		draft["pid"] = pid
	}
	var response struct {
		ID int64 `json:"id"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/fmsg", draft, &response); err != nil {
		return 0, err
	}
	if response.ID <= 0 {
		return 0, errors.New("fmsg Web API returned an invalid draft ID")
	}
	if err := c.doJSON(ctx, http.MethodPost, "/fmsg/"+strconv.FormatInt(response.ID, 10)+"/send", nil, nil); err != nil {
		return response.ID, err
	}
	return response.ID, nil
}

// Watch delivers new inbox messages in increasing local ID order.
// It connects WebSocket first, synchronizes the HTTP inbox, and reconnects until ctx is canceled.
// handler errors leave the cursor unchanged so the message is retried after reconnection.
// When handler returns nil, lastID is advanced (caller should persist it).
func (c *Client) Watch(ctx context.Context, afterID int64, handler func(context.Context, Message) error) (int64, error) {
	if handler == nil {
		return afterID, errors.New("nil message handler")
	}
	lastID := afterID
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		conn, err := c.connect(ctx)
		if err != nil {
			if !waitContext(ctx, backoff) {
				break
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		if err := c.synchronize(ctx, &lastID, handler); err != nil {
			conn.Close()
			if !waitContext(ctx, backoff) {
				break
			}
			continue
		}
		err = c.readEvents(ctx, conn, &lastID, handler)
		conn.Close()
		if ctx.Err() != nil {
			break
		}
		if err == nil {
			return lastID, nil
		}
		if !waitContext(ctx, backoff) {
			break
		}
		backoff = min(backoff*2, 5*time.Second)
	}
	return lastID, ctx.Err()
}

func (c *Client) synchronize(ctx context.Context, lastID *int64, handler func(context.Context, Message) error) error {
	var pending []Message
	for offset := 0; ; offset += 100 {
		page, err := c.ListInbox(ctx, 100, offset)
		if err != nil {
			return err
		}
		stop := false
		for _, message := range page {
			if message.ID <= *lastID {
				stop = true
				continue
			}
			pending = append(pending, message)
		}
		if stop || len(page) < 100 {
			break
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })
	for _, message := range pending {
		if message.ID <= *lastID {
			continue
		}
		if err := handler(ctx, message); err != nil {
			return err
		}
		*lastID = message.ID
	}
	return nil
}

func (c *Client) connect(ctx context.Context) (*websocket.Conn, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}
	endpoint := *c.baseURL
	if endpoint.Scheme == "https" {
		endpoint.Scheme = "wss"
	} else {
		endpoint.Scheme = "ws"
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/fmsg/ws"
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, response, err := c.dialer.DialContext(ctx, endpoint.String(), header)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		if response != nil && response.StatusCode == http.StatusUnauthorized {
			c.invalidateToken()
		}
		return nil, fmt.Errorf("connect fmsg WebSocket: %w", err)
	}
	return conn, nil
}

func (c *Client) readEvents(ctx context.Context, conn *websocket.Conn, lastID *int64, handler func(context.Context, Message) error) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	defer close(done)
	for {
		var event struct {
			Type string  `json:"type"`
			Data Message `json:"data"`
		}
		if err := conn.ReadJSON(&event); err != nil {
			return err
		}
		if event.Type != "new_msg" || event.Data.ID <= *lastID {
			continue
		}
		if err := handler(ctx, event.Data); err != nil {
			return err
		}
		*lastID = event.Data.ID
	}
}

func (c *Client) doJSON(ctx context.Context, method, path string, requestBody, responseBody any) error {
	var body []byte
	var err error
	if requestBody != nil {
		body, err = json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
	}
	response, err := c.do(ctx, method, path, body, "application/json")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if responseBody == nil || response.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(responseBody); err != nil {
		return fmt.Errorf("decode Web API response: %w", err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	for attempt := 0; attempt < 2; attempt++ {
		token, err := c.token(ctx)
		if err != nil {
			return nil, err
		}
		request, err := c.newRequest(ctx, method, path, body)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		response, err := c.httpClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("call fmsg Web API: %w", err)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return response, nil
		}
		if response.StatusCode == http.StatusUnauthorized && attempt == 0 {
			response.Body.Close()
			c.invalidateToken()
			continue
		}
		apiErr := decodeAPIError(response)
		response.Body.Close()
		return nil, apiErr
	}
	return nil, errors.New("fmsg Web API authentication retry exhausted")
}

func (c *Client) newRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + strings.SplitN(path, "?", 2)[0]
	if index := strings.IndexByte(path, '?'); index >= 0 {
		endpoint.RawQuery = path[index+1:]
	}
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), r)
	if err != nil {
		return nil, fmt.Errorf("create Web API request: %w", err)
	}
	return request, nil
}

func (c *Client) token(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.accessToken != "" && time.Until(c.tokenExpiry) > time.Minute {
		return c.accessToken, nil
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/fmsg/token"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("exchange fmsg API key: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", decodeAPIError(response)
	}
	var value struct {
		AccessToken string    `json:"access_token"`
		ExpiresIn   int       `json:"expires_in"`
		ExpiresAt   time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if value.AccessToken == "" {
		return "", errors.New("token response omitted access_token")
	}
	expiry := value.ExpiresAt
	if expiry.IsZero() && value.ExpiresIn > 0 {
		expiry = time.Now().Add(time.Duration(value.ExpiresIn) * time.Second)
	}
	if expiry.IsZero() {
		return "", errors.New("token response omitted expiry")
	}
	sub, err := jwtSub(value.AccessToken)
	if err != nil {
		return "", err
	}
	c.accessToken, c.tokenExpiry, c.address = value.AccessToken, expiry, sub
	return c.accessToken, nil
}

func (c *Client) invalidateToken() {
	c.tokenMu.Lock()
	c.accessToken, c.tokenExpiry = "", time.Time{}
	c.tokenMu.Unlock()
}

func jwtSub(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return "", errors.New("malformed JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// Some tokens may use padded encoding.
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return "", fmt.Errorf("decode JWT payload: %w", err)
		}
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("parse JWT claims: %w", err)
	}
	if claims.Sub == "" {
		return "", errors.New("JWT missing sub claim")
	}
	return claims.Sub, nil
}

func decodeAPIError(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
	var value struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(data, &value)
	if value.Error == "" {
		value.Error = strings.TrimSpace(string(data))
	}
	return &APIError{StatusCode: response.StatusCode, Message: value.Error}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
