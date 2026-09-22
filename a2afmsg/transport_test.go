package a2afmsg

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

const (
	testClient = "@rocket@example.net"
	testServer = "@groot@fmsg.io"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"fmsg:@groot@fmsg.io", testServer},
		{"fmsg:@gr%C3%B6%C3%9Ft@example.com", "@größt@example.com"},
		{"fmsg:@größt@example.com", ""},
		{"FMSG:@groot@fmsg.io", ""},
		{"fmsg://groot@fmsg.io", ""},
		{"fmsg:@groot@fmsg.io?x=1", ""},
		{"fmsg:@groot@fmsg.io#x", ""},
		{"fmsg:@gr%c3%b6%c3%9ft@example.com", ""},
		{"fmsg:@bad..user@example.com", ""},
		{"fmsg:@groot@localhost", "@groot@localhost"},
	}
	for _, test := range tests {
		t.Run(test.endpoint, func(t *testing.T) {
			got, err := ParseEndpoint(test.endpoint)
			if test.want == "" {
				if err == nil {
					t.Fatalf("ParseEndpoint()=%q, expected an error", got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("ParseEndpoint()=%q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestOfficialClientSendMessageOverFMSG(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		fake.addResponse(id, request, false, map[string]any{
			"message": map[string]any{
				"messageId": "reply-1",
				"contextId": "context-1",
				"role":      "ROLE_AGENT",
				"parts": []any{map[string]any{
					"text": "I am Groot!", "mediaType": "text/plain",
				}},
			},
		})
	}
	client := newOfficialClient(t, fake)
	defer client.Destroy()

	result, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("Hello Groot")),
	})
	if err != nil {
		t.Fatal(err)
	}
	message, ok := result.(*a2a.Message)
	if !ok || message.ContextID != "context-1" || len(message.Parts) != 1 || message.Parts[0].Text() != "I am Groot!" {
		t.Fatalf("unexpected A2A result: %#v", result)
	}

	drafts := fake.capturedDrafts()
	if len(drafts) != 1 || drafts[0].PID != nil || drafts[0].Type != MediaType ||
		!strings.HasPrefix(drafts[0].Topic, "A2A ") || drafts[0].To[0] != testServer {
		t.Fatalf("unexpected fmsg request: %#v", drafts)
	}
	request := decodeCapturedRequest(t, drafts[0])
	if request.BindingVersion != bindingVersion || request.A2AVersion != a2aVersion ||
		request.ServiceParameters["a2a-version"] != a2aVersion {
		t.Fatalf("unexpected request envelope: %#v", request)
	}
}

func TestRemoteErrorMapsToOfficialA2AError(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		fake.addError(id, request, "A2A_TASK_NOT_FOUND", "missing")
	}
	client := newOfficialClient(t, fake)
	defer client.Destroy()

	_, err := client.GetTask(context.Background(), &a2a.GetTaskRequest{ID: "task-1"})
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("GetTask error=%v, want ErrTaskNotFound", err)
	}
	var bindingErr *BindingError
	if !errors.As(err, &bindingErr) || bindingErr.Code != "A2A_TASK_NOT_FOUND" {
		t.Fatalf("GetTask error did not retain binding detail: %v", err)
	}
}

func TestRawPartUsesNativeFMSGAttachment(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		fake.addResponse(id, request, false, map[string]any{
			"message": map[string]any{
				"messageId": "reply-raw", "role": "ROLE_AGENT",
				"parts": []any{map[string]any{"text": "I am Groot", "mediaType": "text/plain"}},
			},
		})
	}
	client := newOfficialClient(t, fake)
	defer client.Destroy()

	part := a2a.NewRawPart([]byte("raw-input"))
	part.MediaType = "text/plain"
	_, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, part),
	})
	if err != nil {
		t.Fatal(err)
	}
	drafts := fake.capturedDrafts()
	request := decodeCapturedRequest(t, drafts[0])
	payload := request.Payload.(map[string]any)
	message := payload["message"].(map[string]any)
	mapped := message["parts"].([]any)[0].(map[string]any)
	if mapped["url"] != "fmsg-attachment:a2a-part-0" {
		t.Fatalf("raw part was not mapped: %#v", mapped)
	}
	if _, exists := mapped["raw"]; exists {
		t.Fatalf("mapped part retained raw: %#v", mapped)
	}
	if got := string(fake.uploaded(drafts[0].ID, "a2a-part-0")); got != "raw-input" {
		t.Fatalf("uploaded attachment=%q", got)
	}
}

func TestReservedAttachmentURLIsRejectedAtClientBoundary(t *testing.T) {
	fake := newFakeAPI(t)
	transport := newTransport(t, fake)
	_, err := transport.SendMessage(context.Background(), nil, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser,
			a2a.NewFileURLPart(a2a.URL("FMSG-ATTACHMENT:a2a-part-0"), "application/octet-stream")),
	})
	if err == nil || !strings.Contains(err.Error(), "reserved fmsg-attachment scheme") {
		t.Fatalf("SendMessage error=%v, want reserved-scheme rejection", err)
	}
	if drafts := fake.capturedDrafts(); len(drafts) != 0 {
		t.Fatalf("reserved URL produced an fmsg draft: %#v", drafts)
	}
}

func TestResponseNativeAttachmentIsRestoredToRawPart(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		data := []byte("raw-response")
		responseID := fake.addResult(message{
			Version: 1, HasPID: true, PID: &id, From: testServer, To: []string{testClient}, Type: MediaType,
			Attachments: []attachment{{Filename: "a2a-part-0", Size: int64(len(data))}},
		}, map[string]any{
			"bindingVersion": bindingVersion, "a2aVersion": a2aVersion, "kind": "response",
			"requestId": request.RequestID, "operation": request.Operation,
			"payload": map[string]any{"message": map[string]any{
				"messageId": "reply-attachment", "role": "ROLE_AGENT",
				"parts": []any{map[string]any{
					"url": "fmsg-attachment:a2a-part-0", "mediaType": "application/octet-stream",
				}},
			}},
		})
		fake.storeAttachment(responseID, "a2a-part-0", data)
	}
	client := newOfficialClient(t, fake)
	defer client.Destroy()

	result, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("raw please")),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := result.(*a2a.Message)
	if got := string(response.Parts[0].Raw()); got != "raw-response" {
		t.Fatalf("restored response raw=%q", got)
	}
}

func TestCode6ResendsThreadedRequestAsDetachedRoot(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		if draft.PID != nil {
			code := 6
			fake.setDelivery(id, recipientDelivery{Addr: testServer, ResponseCode: &code})
			return
		}
		fake.addResponse(id, request, false, map[string]any{
			"message": map[string]any{
				"messageId": "detached-reply", "contextId": "context-1", "role": "ROLE_AGENT",
				"parts": []any{map[string]any{"text": "I am Groot", "mediaType": "text/plain"}},
			},
		})
	}
	transport := newTransport(t, fake)
	transport.contextAnchors["context-1"] = 99
	request := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("Again"))}
	request.Message.ContextID = "context-1"
	if _, err := transport.SendMessage(context.Background(), nil, request); err != nil {
		t.Fatal(err)
	}
	drafts := fake.capturedDrafts()
	if len(drafts) != 2 || drafts[0].PID == nil || *drafts[0].PID != 99 ||
		drafts[1].PID != nil || !strings.HasPrefix(drafts[1].Topic, "A2A ") {
		t.Fatalf("unexpected threaded/detached requests: %#v", drafts)
	}
	first := decodeCapturedRequest(t, drafts[0])
	second := decodeCapturedRequest(t, drafts[1])
	if first.RequestID != second.RequestID || drafts[0].Data != drafts[1].Data {
		t.Fatal("detached retry did not preserve request ID and content")
	}
	if parent := transport.contextAnchors["context-1"]; parent == 99 {
		t.Fatal("lost parent remained a usable context anchor")
	}
}

func TestStreamingEventsAreReorderedBySequence(t *testing.T) {
	fake := newFakeAPI(t)
	fake.onSend = func(id int64, draft capturedDraft) {
		request := decodeCapturedRequest(t, draft)
		fake.addEvent(id, request, 1, true, "second")
		fake.addEvent(id, request, 0, false, "first")
	}
	transport := newTransport(t, fake)
	request := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("stream"))}
	var texts []string
	for event, err := range transport.SendStreamingMessage(context.Background(), nil, request) {
		if err != nil {
			t.Fatal(err)
		}
		message, ok := event.(*a2a.Message)
		if !ok {
			t.Fatalf("event is %T", event)
		}
		texts = append(texts, message.Parts[0].Text())
	}
	if strings.Join(texts, ",") != "first,second" {
		t.Fatalf("event order=%v", texts)
	}
}

func newOfficialClient(t *testing.T, fake *fakeAPI) *a2aclient.Client {
	t.Helper()
	card := &a2a.AgentCard{
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL: "fmsg:" + testServer, ProtocolBinding: ProtocolBinding, ProtocolVersion: a2a.Version,
		}},
	}
	client, err := a2aclient.NewFromCard(context.Background(), card, WithTransport(fake.config()))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newTransport(t *testing.T, fake *fakeAPI) *Transport {
	t.Helper()
	transport, err := NewTransport(context.Background(), "fmsg:"+testServer, fake.config())
	if err != nil {
		t.Fatal(err)
	}
	return transport
}

type capturedDraft struct {
	ID      int64
	PID     *int64   `json:"pid"`
	Topic   string   `json:"topic"`
	To      []string `json:"to"`
	Type    string   `json:"type"`
	Data    string   `json:"data"`
	NoReply bool     `json:"no_reply"`
}

type fakeStoredMessage struct {
	meta message
	body []byte
}

type fakeAPI struct {
	t           *testing.T
	server      *httptest.Server
	mu          sync.Mutex
	nextID      int64
	drafts      []capturedDraft
	inbox       []fakeStoredMessage
	deliveries  map[int64][]recipientDelivery
	attachments map[int64]map[string][]byte
	onSend      func(int64, capturedDraft)
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	fake := &fakeAPI{
		t: t, nextID: 10,
		deliveries:  make(map[int64][]recipientDelivery),
		attachments: make(map[int64]map[string][]byte),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeAPI) config() Config {
	return Config{
		WebAPIURL: f.server.URL, APIKey: "fmsgk_test", HTTPClient: f.server.Client(),
		PollInterval: time.Millisecond, RequestTimeout: time.Second,
	}
}

func (f *fakeAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/fmsg/token":
		payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + testClient + `"}`))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "e30." + payload + ".sig", "expires_in": 3600,
		})
	case r.Method == http.MethodGet && r.URL.Path == "/fmsg":
		f.serveInbox(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/fmsg":
		f.createDraft(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/attach"):
		f.uploadAttachment(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/send"):
		f.sendDraft(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/thread/messages"):
		f.serveThread(w, r)
	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/attach/"):
		f.serveAttachment(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/data"):
		f.serveData(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/fmsg/"):
		f.serveMessage(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/fmsg/"):
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeAPI) serveInbox(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]message, len(f.inbox))
	for i, stored := range f.inbox {
		items[len(items)-1-i] = stored.meta
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if offset > len(items) {
		offset = len(items)
	}
	end := min(offset+limit, len(items))
	_ = json.NewEncoder(w).Encode(items[offset:end])
}

func (f *fakeAPI) createDraft(w http.ResponseWriter, r *http.Request) {
	var draft capturedDraft
	if err := json.NewDecoder(r.Body).Decode(&draft); err != nil {
		f.t.Errorf("decode draft: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	draft.ID = f.nextID
	f.nextID++
	f.drafts = append(f.drafts, draft)
	f.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": draft.ID})
}

func (f *fakeAPI) uploadAttachment(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	data, _ := io.ReadAll(file)
	f.mu.Lock()
	if f.attachments[id] == nil {
		f.attachments[id] = make(map[string][]byte)
	}
	f.attachments[id][header.Filename] = data
	f.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"filename": header.Filename, "size": len(data)})
}

func (f *fakeAPI) sendDraft(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	f.mu.Lock()
	var draft capturedDraft
	for _, candidate := range f.drafts {
		if candidate.ID == id {
			draft = candidate
			break
		}
	}
	onSend := f.onSend
	f.mu.Unlock()
	if onSend != nil {
		onSend(id, draft)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (f *fakeAPI) serveMessage(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	f.mu.Lock()
	deliveries := append([]recipientDelivery(nil), f.deliveries[id]...)
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(message{ID: id, ToDelivery: deliveries})
}

func (f *fakeAPI) serveData(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, stored := range f.inbox {
		if stored.meta.ID == id {
			_, _ = w.Write(stored.body)
			return
		}
	}
	http.NotFound(w, r)
}

func (f *fakeAPI) serveThread(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	f.mu.Lock()
	defer f.mu.Unlock()
	var attachments []map[string]any
	for filename, data := range f.attachments[id] {
		attachments = append(attachments, map[string]any{
			"filename": filename, "size": len(data), "type": "application/octet-stream",
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"messages": []any{map[string]any{"id": id, "attachments": attachments}},
	})
}

func (f *fakeAPI) serveAttachment(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path)
	filename := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
	f.mu.Lock()
	data := append([]byte(nil), f.attachments[id][filename]...)
	f.mu.Unlock()
	_, _ = w.Write(data)
}

func (f *fakeAPI) addResponse(parentID int64, request requestEnvelope, noReply bool, payload any) {
	f.addResult(message{
		Version: 1, HasPID: true, PID: &parentID, From: testServer, To: []string{testClient},
		Type: MediaType, NoReply: noReply,
	}, map[string]any{
		"bindingVersion": bindingVersion, "a2aVersion": a2aVersion, "kind": "response",
		"requestId": request.RequestID, "operation": request.Operation, "payload": payload,
	})
}

func (f *fakeAPI) addError(parentID int64, request requestEnvelope, code, errorMessage string) {
	f.addResult(message{
		Version: 1, HasPID: true, PID: &parentID, From: testServer, To: []string{testClient}, Type: MediaType,
	}, map[string]any{
		"bindingVersion": bindingVersion, "a2aVersion": a2aVersion, "kind": "response",
		"requestId": request.RequestID, "operation": request.Operation,
		"error": map[string]any{"code": code, "message": errorMessage, "details": []any{}},
	})
}

func (f *fakeAPI) addEvent(parentID int64, request requestEnvelope, sequence uint64, final bool, text string) {
	f.addResult(message{
		Version: 1, HasPID: true, PID: &parentID, From: testServer, To: []string{testClient}, Type: MediaType,
	}, map[string]any{
		"bindingVersion": bindingVersion, "a2aVersion": a2aVersion, "kind": "event",
		"requestId": request.RequestID, "operation": request.Operation, "sequence": sequence, "final": final,
		"payload": map[string]any{"message": map[string]any{
			"messageId": fmt.Sprintf("event-%d", sequence), "role": "ROLE_AGENT",
			"parts": []any{map[string]any{"text": text, "mediaType": "text/plain"}},
		}},
	})
}

func (f *fakeAPI) addResult(meta message, envelope any) int64 {
	body, err := json.Marshal(envelope)
	if err != nil {
		f.t.Fatal(err)
	}
	f.mu.Lock()
	meta.ID = f.nextID
	f.nextID++
	f.inbox = append(f.inbox, fakeStoredMessage{meta: meta, body: body})
	f.mu.Unlock()
	return meta.ID
}

func (f *fakeAPI) setDelivery(id int64, delivery recipientDelivery) {
	f.mu.Lock()
	f.deliveries[id] = []recipientDelivery{delivery}
	f.mu.Unlock()
}

func (f *fakeAPI) capturedDrafts() []capturedDraft {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedDraft(nil), f.drafts...)
}

func (f *fakeAPI) uploaded(id int64, filename string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.attachments[id][filename]...)
}

func (f *fakeAPI) storeAttachment(id int64, filename string, data []byte) {
	f.mu.Lock()
	if f.attachments[id] == nil {
		f.attachments[id] = make(map[string][]byte)
	}
	f.attachments[id][filename] = append([]byte(nil), data...)
	f.mu.Unlock()
}

func decodeCapturedRequest(t *testing.T, draft capturedDraft) requestEnvelope {
	t.Helper()
	var request requestEnvelope
	if err := json.Unmarshal([]byte(draft.Data), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func pathID(path string) int64 {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return 0
	}
	id, _ := strconv.ParseInt(parts[1], 10, 64)
	return id
}
