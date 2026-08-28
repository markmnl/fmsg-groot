package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const (
	testSelf   = "@groot@fmsg.io"
	testClient = "@rocket@example.com"
	testReqID  = "018f3f6e-7c1a-7e95-8f23-6ed8b985a781"
)

func TestDecodeA2ARequestJSONRejectsDuplicateKeys(t *testing.T) {
	body := `{"requestId":"` + testReqID + `","requestId":"` + testReqID + `"}`
	if _, _, err := decodeA2ARequestJSON([]byte(body)); err == nil {
		t.Fatal("expected duplicate requestId to be rejected")
	}
}

func TestDecodeA2ARequestJSONRejectsInvalidUTF8AndBOM(t *testing.T) {
	for _, body := range [][]byte{{0xff}, append([]byte{0xef, 0xbb, 0xbf}, []byte(`{"requestId":"`+testReqID+`"}`)...)} {
		if _, _, err := decodeA2ARequestJSON(body); err == nil {
			t.Fatalf("expected %x to be rejected", body)
		}
	}
}

func TestA2ASendMessageAndMessageIDIdempotency(t *testing.T) {
	server := newTestA2AServer(t)
	request := &a2a.SendMessageRequest{Message: &a2a.Message{
		ID:   "message-1",
		Role: a2a.MessageRoleUser,
		Parts: a2a.ContentParts{
			&a2a.Part{Content: a2a.Text("Hello there"), MediaType: "text/plain"},
		},
	}}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	first, fail := server.sendMessage(testSelf, testClient, request)
	if fail != nil {
		t.Fatal(fail)
	}

	var result a2a.StreamResponse
	if err := json.Unmarshal(first, &result); err != nil {
		t.Fatal(err)
	}
	message, ok := result.Event.(*a2a.Message)
	if !ok {
		t.Fatalf("result event is %T, want *a2a.Message", result.Event)
	}
	if message.Role != a2a.MessageRoleAgent || message.ContextID == "" || len(message.Parts) != 1 {
		t.Fatalf("invalid response message: %#v", message)
	}
	if message.Parts[0].MediaType != "text/plain" || !strings.Contains(strings.ToLower(message.Parts[0].Text()), "groot") {
		t.Fatalf("unexpected response part: %#v", message.Parts[0])
	}

	second, fail := server.sendMessage(testSelf, testClient, request)
	if fail != nil || string(second) != string(first) {
		t.Fatalf("same messageId was not replayed: fail=%v\nfirst=%s\nsecond=%s", fail, first, second)
	}

	var changed a2a.SendMessageRequest
	if err := json.Unmarshal(payload, &changed); err != nil {
		t.Fatal(err)
	}
	changed.Message.Parts[0] = &a2a.Part{Content: a2a.Text("Different")}
	if _, fail := server.sendMessage(testSelf, testClient, &changed); fail == nil || fail.Code != "INVALID_ARGUMENT" {
		t.Fatalf("changed messageId got failure %v", fail)
	}
}

func TestA2ADispatchRecognizesAllOperations(t *testing.T) {
	server := newTestA2AServer(t)
	tests := []struct {
		operation string
		payload   string
		code      string
	}{
		{"SendStreamingMessage", `{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hi"}]}}`, "A2A_UNSUPPORTED_OPERATION"},
		{"GetTask", `{"id":"task"}`, "A2A_TASK_NOT_FOUND"},
		{"CancelTask", `{"id":"task"}`, "A2A_TASK_NOT_FOUND"},
		{"SubscribeToTask", `{"id":"task"}`, "A2A_UNSUPPORTED_OPERATION"},
		{"CreateTaskPushNotificationConfig", `{"taskId":"task","url":"https://example.com"}`, "A2A_PUSH_NOTIFICATION_NOT_SUPPORTED"},
		{"GetTaskPushNotificationConfig", `{"taskId":"task","id":"config"}`, "A2A_PUSH_NOTIFICATION_NOT_SUPPORTED"},
		{"ListTaskPushNotificationConfigs", `{"taskId":"task"}`, "A2A_PUSH_NOTIFICATION_NOT_SUPPORTED"},
		{"DeleteTaskPushNotificationConfig", `{"taskId":"task","id":"config"}`, "A2A_PUSH_NOTIFICATION_NOT_SUPPORTED"},
		{"GetExtendedAgentCard", `{}`, "A2A_UNSUPPORTED_OPERATION"},
	}
	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			_, fail := server.dispatch(testSelf, testClient, a2aRequestEnvelope{
				Operation: test.operation,
				Payload:   json.RawMessage(test.payload),
			})
			if fail == nil || fail.Code != test.code {
				t.Fatalf("got %v, want code %s", fail, test.code)
			}
		})
	}

	payload, fail := server.dispatch(testSelf, testClient, a2aRequestEnvelope{
		Operation: "ListTasks",
		Payload:   json.RawMessage(`{}`),
	})
	if fail != nil {
		t.Fatal(fail)
	}
	var list a2a.ListTasksResponse
	if err := json.Unmarshal(payload, &list); err != nil {
		t.Fatal(err)
	}
	if list.Tasks == nil || len(list.Tasks) != 0 || list.TotalSize != 0 || list.PageSize != 50 {
		t.Fatalf("unexpected empty task list: %#v", list)
	}
}

func TestA2ARestoresNativeAttachment(t *testing.T) {
	data := []byte("hello from an attachment")
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fmsg/10/thread/messages":
			_, _ = io.WriteString(w, `{"messages":[{"id":10,"attachments":[{"filename":"a2a-part-0","type":"text/plain"}]}]}`)
		case "/fmsg/10/attach/a2a-part-0":
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer httpServer.Close()
	client := authenticatedTestClient(t, httpServer)
	root := map[string]any{
		"payload": map[string]any{
			"message": map[string]any{
				"parts": []any{map[string]any{
					"url":       "fmsg-attachment:a2a-part-0",
					"mediaType": "text/plain",
				}},
			},
		},
	}
	message := Message{ID: 10, Attachments: []Attachment{{Filename: "a2a-part-0", Size: int64(len(data))}}}
	if fail := newTestA2AServer(t).restoreAttachments(context.Background(), client, message, root); fail != nil {
		t.Fatal(fail)
	}
	part := root["payload"].(map[string]any)["message"].(map[string]any)["parts"].([]any)[0].(map[string]any)
	if _, exists := part["url"]; exists {
		t.Fatal("attachment URL was not removed")
	}
	decoded, err := base64.StdEncoding.DecodeString(part["raw"].(string))
	if err != nil || string(decoded) != string(data) {
		t.Fatalf("restored raw=%q err=%v", decoded, err)
	}
}

func TestA2ADoesNotTreatMetadataURLAsAttachmentReference(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fmsg/10/thread/messages" {
			t.Errorf("unexpected attachment fetch: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"messages":[{"id":10,"attachments":[{"filename":"a2a-part-0","type":"text/plain"}]}]}`)
	}))
	defer httpServer.Close()
	root := map[string]any{
		"payload": map[string]any{
			"message": map[string]any{
				"parts": []any{map[string]any{
					"text":     "hello",
					"metadata": map[string]any{"url": "fmsg-attachment:a2a-part-0"},
				}},
			},
		},
	}
	message := Message{ID: 10, Attachments: []Attachment{{Filename: "a2a-part-0", Size: 1}}}
	fail := newTestA2AServer(t).restoreAttachments(context.Background(), authenticatedTestClient(t, httpServer), message, root)
	if fail == nil || fail.Code != "FMSG_A2A_ATTACHMENT_INVALID" {
		t.Fatalf("metadata URL got failure %v", fail)
	}
}

func TestA2AHandleRepliesAndReplaysRequest(t *testing.T) {
	body := validA2AEnvelope(t, testReqID, "request-message", "Hello")
	var mu sync.Mutex
	var drafts []capturedDraft
	nextID := int64(20)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/data"):
			_, _ = w.Write(body)
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg":
			var draft capturedDraft
			if err := json.NewDecoder(r.Body).Decode(&draft); err != nil {
				t.Errorf("decode draft: %v", err)
			}
			mu.Lock()
			drafts = append(drafts, draft)
			id := nextID
			nextID++
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":`+jsonInt(id)+`}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/send"):
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer httpServer.Close()
	client := authenticatedTestClient(t, httpServer)
	server := newTestA2AServer(t)

	for _, id := range []int64{10, 11} {
		message := validRootMessage(id, testReqID)
		if err := server.Handle(context.Background(), client, testSelf, message); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(drafts) != 2 {
		t.Fatalf("got %d drafts, want 2", len(drafts))
	}
	if drafts[0].PID == nil || *drafts[0].PID != 10 || drafts[1].PID == nil || *drafts[1].PID != 11 {
		t.Fatalf("responses did not reply to each request: %#v", drafts)
	}
	if drafts[0].Data != drafts[1].Data {
		t.Fatalf("duplicate request was not replayed exactly:\n%s\n%s", drafts[0].Data, drafts[1].Data)
	}
	var response a2aResponseEnvelope
	if err := json.Unmarshal([]byte(drafts[0].Data), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID != testReqID || response.Operation != "SendMessage" || response.Error != nil || response.Payload == nil {
		t.Fatalf("unexpected response envelope: %#v", response)
	}
}

func TestA2AResponseFallsBackToDetachedRootOnParentFailure(t *testing.T) {
	var drafts []capturedDraft
	nextID := int64(20)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg":
			var draft capturedDraft
			_ = json.NewDecoder(r.Body).Decode(&draft)
			drafts = append(drafts, draft)
			id := nextID
			nextID++
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":`+jsonInt(id)+`}`)
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg/20/send":
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"error":"parent unavailable"}`)
		case r.Method == http.MethodDelete && r.URL.Path == "/fmsg/20":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg/21/send":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer httpServer.Close()
	client := authenticatedTestClient(t, httpServer)
	server := newTestA2AServer(t)
	if err := server.sendResult(context.Background(), client, testSelf, validRootMessage(10, testReqID), testReqID, json.RawMessage(`{"ok":true}`), false); err != nil {
		t.Fatal(err)
	}
	if len(drafts) != 2 || drafts[0].PID == nil || drafts[1].PID != nil || drafts[1].Topic != "A2A "+testReqID {
		t.Fatalf("unexpected reply/detached drafts: %#v", drafts)
	}
}

func TestA2APendingCode6FallsBackToDetachedRoot(t *testing.T) {
	code := 6
	var drafts []capturedDraft
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/fmsg/30":
			_ = json.NewEncoder(w).Encode(Message{
				ToDelivery: []RecipientDelivery{{Addr: testClient, ResponseCode: &code}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg":
			var draft capturedDraft
			_ = json.NewDecoder(r.Body).Decode(&draft)
			drafts = append(drafts, draft)
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":31}`)
		case r.Method == http.MethodPost && r.URL.Path == "/fmsg/31/send":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer httpServer.Close()
	client := authenticatedTestClient(t, httpServer)
	server := newTestA2AServer(t)
	if err := server.state.AddPending(30, pendingA2AResult{
		Client: testClient, RequestID: testReqID, Body: json.RawMessage(`{"ok":true}`),
	}); err != nil {
		t.Fatal(err)
	}

	server.reconcilePending(context.Background(), client, testSelf)

	if len(drafts) != 1 || drafts[0].PID != nil || drafts[0].Topic != "A2A "+testReqID {
		t.Fatalf("unexpected detached result: %#v", drafts)
	}
	if len(server.state.Pending()) != 0 {
		t.Fatal("pending reply was not cleared after detached delivery")
	}
}

func TestA2AThreadParentCorrelation(t *testing.T) {
	parentBody := `{"kind":"response","operation":"SendMessage","payload":{"message":{"contextId":"context-1"}}}`
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fmsg/99":
			_ = json.NewEncoder(w).Encode(Message{
				Version: 1, From: testSelf, To: []string{testClient}, Type: a2aMediaType,
			})
		case "/fmsg/99/data":
			_, _ = io.WriteString(w, parentBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer httpServer.Close()
	client := authenticatedTestClient(t, httpServer)
	server := newTestA2AServer(t)
	parentID := int64(99)
	message := Message{PID: &parentID, From: testClient}
	envelope := a2aRequestEnvelope{Operation: "SendMessage"}

	root := map[string]any{"payload": map[string]any{"message": map[string]any{"contextId": "context-1"}}}
	if fail := server.validateThreadParent(context.Background(), client, testSelf, message, root, envelope); fail != nil {
		t.Fatalf("valid parent was rejected: %v", fail)
	}
	root = map[string]any{"payload": map[string]any{"message": map[string]any{"contextId": "other-context"}}}
	if fail := server.validateThreadParent(context.Background(), client, testSelf, message, root, envelope); fail == nil || fail.Code != "FMSG_A2A_CORRELATION_FAILED" {
		t.Fatalf("mismatched parent got failure %v", fail)
	}
}

func TestA2AStatePersistsReplayRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadA2AState(path)
	if err != nil {
		t.Fatal(err)
	}
	record := storedA2AResponse{Digest: "digest", Body: json.RawMessage(`{"ok":true}`), CreatedAt: time.Now().UTC()}
	if err := state.SaveRequest("request", record); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadA2AState(path)
	if err != nil {
		t.Fatal(err)
	}
	got, found, conflict := reloaded.LookupRequest("request", "digest")
	var gotCompact, wantCompact bytes.Buffer
	_ = json.Compact(&gotCompact, got.Body)
	_ = json.Compact(&wantCompact, record.Body)
	if !found || conflict || gotCompact.String() != wantCompact.String() {
		t.Fatalf("reloaded record=%#v found=%v conflict=%v", got, found, conflict)
	}
}

type capturedDraft struct {
	PID     *int64   `json:"pid"`
	Topic   string   `json:"topic"`
	To      []string `json:"to"`
	Type    string   `json:"type"`
	NoReply bool     `json:"no_reply"`
	Data    string   `json:"data"`
}

func newTestA2AServer(t *testing.T) *A2AServer {
	t.Helper()
	state, err := LoadA2AState(filepath.Join(t.TempDir(), "a2a.json"))
	if err != nil {
		t.Fatal(err)
	}
	return NewA2AServer(state)
}

func authenticatedTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &Client{
		baseURL:     base,
		httpClient:  server.Client(),
		accessToken: "token",
		tokenExpiry: time.Now().Add(time.Hour),
		address:     testSelf,
	}
}

func validRootMessage(id int64, requestID string) Message {
	return Message{
		ID:      id,
		Version: 1,
		From:    testClient,
		To:      []string{testSelf},
		Topic:   "A2A " + requestID,
		Type:    a2aMediaType,
	}
}

func validA2AEnvelope(t *testing.T, requestID, messageID, text string) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"bindingVersion": a2aBindingVersion,
		"a2aVersion":     a2aProtocolVersion,
		"kind":           "request",
		"requestId":      requestID,
		"operation":      "SendMessage",
		"serviceParameters": map[string]string{
			"a2a-version": a2aProtocolVersion,
		},
		"payload": map[string]any{
			"message": map[string]any{
				"messageId": messageID,
				"role":      "ROLE_USER",
				"parts":     []any{map[string]any{"text": text, "mediaType": "text/plain"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func jsonInt(value int64) string {
	data, _ := json.Marshal(value)
	return string(data)
}
