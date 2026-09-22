package a2afmsg

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxErrorBody = 64 << 10

type apiError struct {
	StatusCode int
	Message    string
}

func (e *apiError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("fmsg Web API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("fmsg Web API returned HTTP %d: %s", e.StatusCode, e.Message)
}

type attachment struct {
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

type recipientDelivery struct {
	Addr          string  `json:"addr"`
	TimeDelivered *string `json:"time_delivered"`
	ResponseCode  *int    `json:"response_code"`
}

type message struct {
	ID          int64               `json:"id"`
	Version     int                 `json:"version"`
	HasPID      bool                `json:"has_pid"`
	HasAddTo    bool                `json:"has_add_to"`
	NoReply     bool                `json:"no_reply"`
	PID         *int64              `json:"pid"`
	From        string              `json:"from"`
	To          []string            `json:"to"`
	ToDelivery  []recipientDelivery `json:"to_delivery"`
	Topic       string              `json:"topic"`
	Type        string              `json:"type"`
	Attachments []attachment        `json:"attachments"`
}

type outgoingAttachment struct {
	Filename  string
	MediaType string
	Data      []byte
}

type outgoingMessage struct {
	To          string
	PID         int64
	Topic       string
	Data        []byte
	Attachments []outgoingAttachment
}

type webAPIClient struct {
	baseURL   *url.URL
	apiKey    string
	http      *http.Client
	tokenMu   sync.Mutex
	token     string
	expiresAt time.Time
	address   string
}

func newWebAPIClient(config Config) (*webAPIClient, error) {
	base, err := url.Parse(strings.TrimSpace(config.WebAPIURL))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid fmsg Web API URL %q", config.WebAPIURL)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, errors.New("fmsg Web API URL must use http or https")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("fmsg Web API URL must not contain a query or fragment")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	return &webAPIClient{
		baseURL: base,
		apiKey:  strings.TrimSpace(config.APIKey),
		http:    config.HTTPClient,
	}, nil
}

func (c *webAPIClient) ensureToken(ctx context.Context) (string, error) {
	if _, err := c.accessToken(ctx); err != nil {
		return "", err
	}
	return c.address, nil
}

func (c *webAPIClient) accessToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Until(c.expiresAt) > time.Minute {
		return c.token, nil
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/fmsg/token"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.http.Do(request)
	if err != nil {
		return "", fmt.Errorf("exchange fmsg API key: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", decodeAPIError(response)
	}
	var tokenResponse struct {
		AccessToken string    `json:"access_token"`
		ExpiresIn   int       `json:"expires_in"`
		ExpiresAt   time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(response.Body).Decode(&tokenResponse); err != nil {
		return "", fmt.Errorf("decode fmsg token response: %w", err)
	}
	if tokenResponse.AccessToken == "" {
		return "", errors.New("fmsg token response omitted access_token")
	}
	expiresAt := tokenResponse.ExpiresAt
	if expiresAt.IsZero() && tokenResponse.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)
	}
	if expiresAt.IsZero() {
		return "", errors.New("fmsg token response omitted expiry")
	}
	address, err := jwtSubject(tokenResponse.AccessToken)
	if err != nil {
		return "", err
	}
	if !validAddress(address) {
		return "", errors.New("fmsg access token contains an invalid subject address")
	}
	c.token = tokenResponse.AccessToken
	c.expiresAt = expiresAt
	c.address = address
	return c.token, nil
}

func (c *webAPIClient) invalidateToken() {
	c.tokenMu.Lock()
	c.token = ""
	c.expiresAt = time.Time{}
	c.tokenMu.Unlock()
}

func jwtSubject(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("fmsg access token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode fmsg access token: %w", err)
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("decode fmsg access token claims: %w", err)
	}
	if claims.Subject == "" {
		return "", errors.New("fmsg access token omitted subject")
	}
	return claims.Subject, nil
}

func (c *webAPIClient) listInbox(ctx context.Context, limit, offset int) ([]message, error) {
	path := "/fmsg?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	var messages []message
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (c *webAPIClient) maxInboxID(ctx context.Context) (int64, error) {
	messages, err := c.listInbox(ctx, 1, 0)
	if err != nil || len(messages) == 0 {
		return 0, err
	}
	return messages[0].ID, nil
}

func (c *webAPIClient) inboxAfter(ctx context.Context, after int64) ([]message, error) {
	var result []message
	for offset := 0; ; offset += 100 {
		page, err := c.listInbox(ctx, 100, offset)
		if err != nil {
			return nil, err
		}
		stop := false
		for _, item := range page {
			if item.ID <= after {
				stop = true
				continue
			}
			result = append(result, item)
		}
		if stop || len(page) < 100 {
			break
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (c *webAPIClient) getMessage(ctx context.Context, id int64) (message, error) {
	var result message
	if err := c.doJSON(ctx, http.MethodGet, "/fmsg/"+strconv.FormatInt(id, 10), nil, &result); err != nil {
		return message{}, err
	}
	result.ID = id
	return result, nil
}

func (c *webAPIClient) data(ctx context.Context, id, limit int64) ([]byte, error) {
	response, err := c.do(ctx, http.MethodGet, "/fmsg/"+strconv.FormatInt(id, 10)+"/data", nil, "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return io.ReadAll(io.LimitReader(response.Body, limit+1))
}

func (c *webAPIClient) attachmentData(ctx context.Context, id int64, filename string, limit int64) ([]byte, error) {
	// FMSG filenames cannot contain path separators, queries, fragments, or
	// percent signs. Let url.URL.String escape the permitted Unicode and spaces
	// exactly once when do constructs the request URL.
	path := "/fmsg/" + strconv.FormatInt(id, 10) + "/attach/" + filename
	response, err := c.do(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return io.ReadAll(io.LimitReader(response.Body, limit+1))
}

func (c *webAPIClient) attachmentTypes(ctx context.Context, id int64) (map[string]string, error) {
	var thread struct {
		Messages []struct {
			ID          int64 `json:"id"`
			Attachments []struct {
				Filename string `json:"filename"`
				Type     string `json:"type"`
			} `json:"attachments"`
		} `json:"messages"`
	}
	path := "/fmsg/" + strconv.FormatInt(id, 10) + "/thread/messages"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &thread); err != nil {
		return nil, err
	}
	for _, item := range thread.Messages {
		if item.ID != id {
			continue
		}
		result := make(map[string]string, len(item.Attachments))
		for _, item := range item.Attachments {
			result[strings.ToLower(item.Filename)] = item.Type
		}
		return result, nil
	}
	return nil, errors.New("message missing from fmsg thread response")
}

func (c *webAPIClient) createAndSend(ctx context.Context, outgoing outgoingMessage) (int64, error) {
	draft := map[string]any{
		"version":  1,
		"from":     c.address,
		"to":       []string{outgoing.To},
		"type":     MediaType,
		"size":     len(outgoing.Data),
		"data":     string(outgoing.Data),
		"no_reply": false,
	}
	if outgoing.PID > 0 {
		draft["pid"] = outgoing.PID
	} else {
		draft["topic"] = outgoing.Topic
	}
	var created struct {
		ID int64 `json:"id"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/fmsg", draft, &created); err != nil {
		return 0, err
	}
	if created.ID <= 0 {
		return 0, errors.New("fmsg Web API returned an invalid draft ID")
	}
	for _, item := range outgoing.Attachments {
		if err := c.uploadAttachment(ctx, created.ID, item); err != nil {
			_ = c.deleteDraft(ctx, created.ID)
			return created.ID, err
		}
	}
	path := "/fmsg/" + strconv.FormatInt(created.ID, 10) + "/send"
	if err := c.doJSON(ctx, http.MethodPost, path, nil, nil); err != nil {
		return created.ID, err
	}
	return created.ID, nil
}

func (c *webAPIClient) uploadAttachment(ctx context.Context, id int64, item outgoingAttachment) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, item.Filename))
	header.Set("Content-Type", item.MediaType)
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := part.Write(item.Data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	path := "/fmsg/" + strconv.FormatInt(id, 10) + "/attach"
	response, err := c.do(ctx, http.MethodPost, path, body.Bytes(), writer.FormDataContentType())
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var uploaded attachment
	if err := json.NewDecoder(response.Body).Decode(&uploaded); err != nil {
		return err
	}
	if uploaded.Filename != item.Filename {
		return fmt.Errorf("attachment stored as %q, expected %q", uploaded.Filename, item.Filename)
	}
	return nil
}

func (c *webAPIClient) deleteDraft(ctx context.Context, id int64) error {
	response, err := c.do(ctx, http.MethodDelete, "/fmsg/"+strconv.FormatInt(id, 10), nil, "")
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}

func (c *webAPIClient) doJSON(ctx context.Context, method, path string, requestBody, responseBody any) error {
	var body []byte
	var err error
	if requestBody != nil {
		body, err = json.Marshal(requestBody)
		if err != nil {
			return err
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
	return json.NewDecoder(response.Body).Decode(responseBody)
}

func (c *webAPIClient) do(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	for attempt := 0; attempt < 2; attempt++ {
		token, err := c.accessToken(ctx)
		if err != nil {
			return nil, err
		}
		endpoint := *c.baseURL
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + strings.SplitN(path, "?", 2)[0]
		if index := strings.IndexByte(path, '?'); index >= 0 {
			endpoint.RawQuery = path[index+1:]
		}
		request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		response, err := c.http.Do(request)
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
		err = decodeAPIError(response)
		response.Body.Close()
		return nil, err
	}
	return nil, errors.New("fmsg Web API authentication retry exhausted")
}

func decodeAPIError(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(data, &payload)
	message := strings.TrimSpace(payload.Error)
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	return &apiError{StatusCode: response.StatusCode, Message: message}
}
