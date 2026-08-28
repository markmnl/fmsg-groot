package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
)

const (
	a2aMediaType       = "application/vnd.fmsg.a2a+json"
	a2aBindingVersion  = "0.2"
	a2aProtocolVersion = "1.0"
	maxA2AJSONBytes    = 4 << 20
	maxA2AJSONDepth    = 64
	maxA2AJSONValues   = 100_000
	maxA2AAttachBytes  = 32 << 20
)

var a2aOperations = map[string]struct{}{
	"SendMessage":                      {},
	"SendStreamingMessage":             {},
	"GetTask":                          {},
	"ListTasks":                        {},
	"CancelTask":                       {},
	"SubscribeToTask":                  {},
	"CreateTaskPushNotificationConfig": {},
	"GetTaskPushNotificationConfig":    {},
	"ListTaskPushNotificationConfigs":  {},
	"DeleteTaskPushNotificationConfig": {},
	"GetExtendedAgentCard":             {},
}

type a2aRequestEnvelope struct {
	BindingVersion    string            `json:"bindingVersion"`
	A2AVersion        string            `json:"a2aVersion"`
	Kind              string            `json:"kind"`
	RequestID         string            `json:"requestId"`
	Operation         string            `json:"operation"`
	ServiceParameters map[string]string `json:"serviceParameters"`
	Payload           json.RawMessage   `json:"payload"`
	Credentials       json.RawMessage   `json:"credentials,omitempty"`
}

type a2aResponseEnvelope struct {
	BindingVersion string          `json:"bindingVersion"`
	A2AVersion     string          `json:"a2aVersion"`
	Kind           string          `json:"kind"`
	RequestID      string          `json:"requestId"`
	Operation      string          `json:"operation"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Error          *a2aError       `json:"error,omitempty"`
}

type a2aError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details []any  `json:"details,omitempty"`
}

type a2aFailure struct {
	Code    string
	Message string
}

func (e *a2aFailure) Error() string { return e.Code + ": " + e.Message }

type A2AServer struct {
	state *A2AState
	now   func() time.Time
}

func NewA2AServer(state *A2AState) *A2AServer {
	return &A2AServer{state: state, now: time.Now}
}

func isA2AMediaType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, a2aMediaType)
}

// Handle validates and executes one FMSG-004 request message.
func (s *A2AServer) Handle(ctx context.Context, client *Client, self string, message Message) error {
	body, err := client.Data(ctx, message.ID)
	if err != nil {
		return fmt.Errorf("fetch A2A data for %d: %w", message.ID, err)
	}

	root, requestID, err := decodeA2ARequestJSON(body)
	if err != nil {
		log.Printf("reject A2A id=%d without response: %v", message.ID, err)
		return nil
	}

	fail := s.restoreAttachments(ctx, client, message, root)
	canonical, canonicalErr := json.Marshal(root)
	if canonicalErr != nil {
		return fmt.Errorf("canonicalize A2A request %s: %w", requestID, canonicalErr)
	}

	envelope := a2aRequestEnvelope{
		RequestID: requestID,
		Operation: stringField(root, "operation"),
	}
	if err := json.Unmarshal(canonical, &envelope); err != nil && fail == nil {
		fail = failure("FMSG_A2A_INVALID_ENVELOPE", "The request envelope has invalid field types")
	}
	if fail == nil {
		fail = validateA2AEnvelope(envelope, root)
	}
	if fail == nil {
		fail = validateA2AMessageProfile(message, self, envelope)
	}
	if fail == nil && message.PID != nil {
		fail = s.validateThreadParent(ctx, client, self, message, root, envelope)
	}

	if fail != nil {
		return s.sendFailure(ctx, client, self, message, envelope, fail)
	}

	digest := sha256Hex(canonical)
	requestKey := replayKey(self, message.From, envelope.RequestID)
	if stored, found, conflict := s.state.LookupRequest(requestKey, digest); found {
		if conflict {
			return s.sendFailure(ctx, client, self, message, envelope,
				failure("FMSG_A2A_REQUEST_ID_CONFLICT", "The request ID was reused with different content"))
		}
		return s.sendStored(ctx, client, self, message, envelope.RequestID, stored)
	}

	payload, operationFailure := s.dispatch(self, message.From, envelope)
	responseBody, noReply, err := marshalA2AResponse(envelope, payload, operationFailure)
	if err != nil {
		return err
	}
	record := storedA2AResponse{
		Digest:    digest,
		Body:      responseBody,
		NoReply:   noReply,
		CreatedAt: s.now().UTC(),
	}
	if err := s.state.SaveRequest(requestKey, record); err != nil {
		resource := failure("RESOURCE_EXHAUSTED", "The replay store cannot accept another request")
		return s.sendFailure(ctx, client, self, message, envelope, resource)
	}
	return s.sendStored(ctx, client, self, message, envelope.RequestID, record)
}

func decodeA2ARequestJSON(body []byte) (map[string]any, string, error) {
	if len(body) > maxA2AJSONBytes {
		return nil, "", errors.New("A2A envelope exceeds the JSON size limit")
	}
	if !utf8.Valid(body) || (len(body) >= 3 && body[0] == 0xef && body[1] == 0xbb && body[2] == 0xbf) {
		return nil, "", errors.New("A2A envelope must be BOM-free UTF-8")
	}
	value, err := decodeUniqueJSON(body)
	if err != nil {
		return nil, "", err
	}
	root, ok := value.(map[string]any)
	if !ok {
		return nil, "", errors.New("A2A envelope must be a JSON object")
	}
	requestID, ok := root["requestId"].(string)
	if !ok || !validCanonicalUUID(requestID) {
		return nil, "", errors.New("A2A envelope has no trustworthy requestId")
	}
	return root, requestID, nil
}

func validateA2AEnvelope(envelope a2aRequestEnvelope, root map[string]any) *a2aFailure {
	if envelope.BindingVersion != a2aBindingVersion {
		return failure("FMSG_A2A_UNSUPPORTED_BINDING_VERSION", "Only FMSG-004 binding version 0.2 is supported")
	}
	if envelope.A2AVersion != a2aProtocolVersion {
		return failure("A2A_VERSION_NOT_SUPPORTED", "Only A2A version 1.0 is supported")
	}
	if envelope.Kind != "request" {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "Envelope kind must be request")
	}
	if !validCanonicalUUID(envelope.RequestID) {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "requestId must be a lowercase canonical UUID")
	}
	if _, ok := a2aOperations[envelope.Operation]; !ok {
		return failure("FMSG_A2A_UNKNOWN_OPERATION", "The requested operation is not recognized")
	}
	paramsValue, ok := root["serviceParameters"].(map[string]any)
	if !ok {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "serviceParameters must be an object")
	}
	seen := make(map[string]struct{}, len(paramsValue))
	for key, value := range paramsValue {
		folded := strings.ToLower(key)
		if key != folded {
			return failure("FMSG_A2A_INVALID_ENVELOPE", "Service parameter names must be lowercase")
		}
		if _, exists := seen[folded]; exists {
			return failure("FMSG_A2A_INVALID_ENVELOPE", "Service parameter names must be unique case-insensitively")
		}
		seen[folded] = struct{}{}
		if _, ok := value.(string); !ok {
			return failure("FMSG_A2A_INVALID_ENVELOPE", "Service parameter values must be strings")
		}
	}
	if envelope.ServiceParameters["a2a-version"] != envelope.A2AVersion {
		return failure("A2A_VERSION_NOT_SUPPORTED", "a2a-version must equal a2aVersion")
	}
	if _, ok := root["payload"].(map[string]any); !ok {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "payload must be an object")
	}
	if _, hasCredentials := root["credentials"]; hasCredentials {
		return failure("UNAUTHENTICATED", "This interface does not accept application credentials")
	}
	return nil
}

func validateA2AMessageProfile(message Message, self string, envelope a2aRequestEnvelope) *a2aFailure {
	if message.Version != 1 || !isA2AMediaType(message.Type) {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "The fmsg message profile is invalid")
	}
	if message.HasAddTo || len(message.AddTo) != 0 {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "A2A messages cannot use add-to")
	}
	if message.NoReply {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "A2A requests must allow replies")
	}
	if len(message.To) != 1 || normalizeAddr(message.To[0]) != normalizeAddr(self) {
		return failure("FMSG_A2A_INVALID_ENVELOPE", "An A2A request must have exactly one server recipient")
	}
	if message.PID == nil {
		if message.HasPID || message.Topic != "A2A "+envelope.RequestID {
			return failure("FMSG_A2A_CORRELATION_FAILED", "A root request topic must match its request ID")
		}
	} else if !message.HasPID || message.Topic != "" {
		return failure("FMSG_A2A_CORRELATION_FAILED", "A threaded request must have a parent and no topic")
	}
	return nil
}

func (s *A2AServer) dispatch(self, client string, envelope a2aRequestEnvelope) (json.RawMessage, *a2aFailure) {
	switch envelope.Operation {
	case "SendMessage":
		var request a2a.SendMessageRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return s.sendMessage(self, client, &request)
	case "SendStreamingMessage":
		var request a2a.SendMessageRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_UNSUPPORTED_OPERATION", "Streaming is not supported")
	case "GetTask":
		var request a2a.GetTaskRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		if request.Tenant != "" || request.ID == "" {
			return nil, failure("INVALID_ARGUMENT", "A task ID is required and tenant must be omitted")
		}
		return nil, failure("A2A_TASK_NOT_FOUND", "The task does not exist or is not accessible")
	case "ListTasks":
		var request a2a.ListTasksRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		if request.Tenant != "" || request.PageToken != "" || request.PageSize < 0 || request.PageSize > 100 ||
			!validTaskState(request.Status) {
			return nil, failure("INVALID_ARGUMENT", "The task list parameters are invalid")
		}
		pageSize := request.PageSize
		if pageSize == 0 {
			pageSize = 50
		}
		return marshalPayload(&a2a.ListTasksResponse{Tasks: []*a2a.Task{}, PageSize: pageSize})
	case "CancelTask":
		var request a2a.CancelTaskRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		if request.Tenant != "" || request.ID == "" {
			return nil, failure("INVALID_ARGUMENT", "A task ID is required and tenant must be omitted")
		}
		return nil, failure("A2A_TASK_NOT_FOUND", "The task does not exist or is not accessible")
	case "SubscribeToTask":
		var request a2a.SubscribeToTaskRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_UNSUPPORTED_OPERATION", "Streaming is not supported")
	case "CreateTaskPushNotificationConfig":
		var request a2a.PushConfig
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_PUSH_NOTIFICATION_NOT_SUPPORTED", "Push notifications are not supported")
	case "GetTaskPushNotificationConfig":
		var request a2a.GetTaskPushConfigRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_PUSH_NOTIFICATION_NOT_SUPPORTED", "Push notifications are not supported")
	case "ListTaskPushNotificationConfigs":
		var request a2a.ListTaskPushConfigRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_PUSH_NOTIFICATION_NOT_SUPPORTED", "Push notifications are not supported")
	case "DeleteTaskPushNotificationConfig":
		var request a2a.DeleteTaskPushConfigRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_PUSH_NOTIFICATION_NOT_SUPPORTED", "Push notifications are not supported")
	case "GetExtendedAgentCard":
		var request a2a.GetExtendedAgentCardRequest
		if fail := decodePayload(envelope.Payload, &request); fail != nil {
			return nil, fail
		}
		return nil, failure("A2A_UNSUPPORTED_OPERATION", "An extended Agent Card is not supported")
	default:
		return nil, failure("FMSG_A2A_UNKNOWN_OPERATION", "The requested operation is not recognized")
	}
}

func (s *A2AServer) sendMessage(self, client string, request *a2a.SendMessageRequest) (json.RawMessage, *a2aFailure) {
	if request.Tenant != "" || request.Message == nil || request.Message.ID == "" {
		return nil, failure("INVALID_ARGUMENT", "A message with a messageId is required and tenant must be omitted")
	}
	if request.Message.Role != a2a.MessageRoleUser || len(request.Message.Parts) == 0 {
		return nil, failure("INVALID_ARGUMENT", "The request must contain a user message with at least one part")
	}
	if request.Message.TaskID != "" {
		return nil, failure("A2A_TASK_NOT_FOUND", "The task does not exist or is not accessible")
	}
	if request.Config != nil {
		if request.Config.PushConfig != nil {
			return nil, failure("A2A_PUSH_NOTIFICATION_NOT_SUPPORTED", "Push notifications are not supported")
		}
		if len(request.Config.AcceptedOutputModes) > 0 && !containsTextPlain(request.Config.AcceptedOutputModes) {
			return nil, failure("A2A_CONTENT_TYPE_NOT_SUPPORTED", "Groot produces text/plain responses")
		}
	}

	text, fail := textFromParts(request.Message.Parts)
	if fail != nil {
		return nil, fail
	}
	messageDigest, err := json.Marshal(request.Message)
	if err != nil {
		return nil, failure("INVALID_ARGUMENT", "The A2A message cannot be encoded")
	}
	messageKey := replayKey(self, client, request.Message.ID)
	if stored, found, conflict := s.state.LookupMessage(messageKey, sha256Hex(messageDigest)); found {
		if conflict {
			return nil, failure("INVALID_ARGUMENT", "messageId was reused with different content")
		}
		return stored.Payload, nil
	}

	reply, skip := ChooseReply(text)
	if skip {
		reply = GrootToo
	}
	contextID := request.Message.ContextID
	if contextID == "" {
		contextID = a2a.NewContextID()
	}
	response := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(reply))
	response.ContextID = contextID
	response.Parts[0].MediaType = "text/plain"
	payload, marshalFailure := marshalPayload(a2a.StreamResponse{Event: response})
	if marshalFailure != nil {
		return nil, marshalFailure
	}
	if err := s.state.SaveMessage(messageKey, storedA2AMessage{
		Digest:    sha256Hex(messageDigest),
		Payload:   payload,
		CreatedAt: s.now().UTC(),
	}); err != nil {
		return nil, failure("RESOURCE_EXHAUSTED", "The message replay store cannot accept another message")
	}
	return payload, nil
}

func textFromParts(parts a2a.ContentParts) (string, *a2aFailure) {
	texts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			return "", failure("INVALID_ARGUMENT", "Message parts cannot be null")
		}
		if part.MediaType != "" && !isTextPlain(part.MediaType) {
			return "", failure("A2A_CONTENT_TYPE_NOT_SUPPORTED", "Groot accepts only text/plain input")
		}
		switch content := part.Content.(type) {
		case a2a.Text:
			texts = append(texts, string(content))
		case a2a.Raw:
			if !utf8.Valid(content) {
				return "", failure("A2A_CONTENT_TYPE_NOT_SUPPORTED", "Raw text/plain input must be UTF-8")
			}
			texts = append(texts, string(content))
		default:
			return "", failure("A2A_CONTENT_TYPE_NOT_SUPPORTED", "Groot accepts only text or raw text/plain parts")
		}
	}
	return strings.Join(texts, "\n"), nil
}

func containsTextPlain(values []string) bool {
	for _, value := range values {
		if isTextPlain(value) {
			return true
		}
	}
	return false
}

func validTaskState(state a2a.TaskState) bool {
	switch state {
	case a2a.TaskStateUnspecified,
		a2a.TaskStateAuthRequired,
		a2a.TaskStateCanceled,
		a2a.TaskStateCompleted,
		a2a.TaskStateFailed,
		a2a.TaskStateInputRequired,
		a2a.TaskStateRejected,
		a2a.TaskStateSubmitted,
		a2a.TaskStateWorking:
		return true
	default:
		return false
	}
}

func isTextPlain(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "text/plain")
}

func decodePayload(payload json.RawMessage, target any) *a2aFailure {
	if err := json.Unmarshal(payload, target); err != nil {
		return failure("INVALID_ARGUMENT", "The operation payload is invalid")
	}
	return nil
}

func marshalPayload(value any) (json.RawMessage, *a2aFailure) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, failure("INTERNAL", "The operation result could not be encoded")
	}
	return data, nil
}

func marshalA2AResponse(request a2aRequestEnvelope, payload json.RawMessage, fail *a2aFailure) (json.RawMessage, bool, error) {
	response := a2aResponseEnvelope{
		BindingVersion: a2aBindingVersion,
		A2AVersion:     a2aProtocolVersion,
		Kind:           "response",
		RequestID:      request.RequestID,
		Operation:      request.Operation,
		Payload:        payload,
	}
	noReply := request.Operation != "SendMessage" || fail != nil
	if fail != nil {
		response.Payload = nil
		response.Error = &a2aError{Code: fail.Code, Message: fail.Message, Details: []any{}}
	}
	data, err := json.Marshal(response)
	if err != nil {
		return nil, false, fmt.Errorf("marshal A2A response: %w", err)
	}
	return data, noReply, nil
}

func (s *A2AServer) sendFailure(ctx context.Context, client *Client, self string, message Message, envelope a2aRequestEnvelope, fail *a2aFailure) error {
	if envelope.RequestID == "" {
		return nil
	}
	body, noReply, err := marshalA2AResponse(envelope, nil, fail)
	if err != nil {
		return err
	}
	return s.sendResult(ctx, client, self, message, envelope.RequestID, body, noReply)
}

func (s *A2AServer) sendStored(ctx context.Context, client *Client, self string, message Message, requestID string, stored storedA2AResponse) error {
	return s.sendResult(ctx, client, self, message, requestID, stored.Body, stored.NoReply)
}

func (s *A2AServer) sendResult(ctx context.Context, client *Client, self string, request Message, requestID string, body json.RawMessage, noReply bool) error {
	outgoing := OutgoingMessage{
		From:    self,
		To:      []string{request.From},
		PID:     request.ID,
		Type:    a2aMediaType,
		Data:    body,
		NoReply: noReply,
	}
	sentID, err := client.CreateAndSendMessage(ctx, outgoing)
	if err != nil {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 409 {
			return fmt.Errorf("send A2A response to %d: %w", request.ID, err)
		}
		_ = client.DeleteDraft(ctx, sentID)
		outgoing.PID = 0
		outgoing.Topic = "A2A " + requestID
		if _, err := client.CreateAndSendMessage(ctx, outgoing); err != nil {
			return fmt.Errorf("send detached A2A response to %d: %w", request.ID, err)
		}
		return nil
	}
	return s.state.AddPending(sentID, pendingA2AResult{
		Client:    request.From,
		RequestID: requestID,
		Body:      body,
		NoReply:   noReply,
		CreatedAt: s.now().UTC(),
	})
}

// MonitorPending polls authoritative delivery state so code 6 can be recovered
// even if a live WebSocket delivery event was missed during a reconnect.
func (s *A2AServer) MonitorPending(ctx context.Context, client *Client, self string) {
	s.reconcilePending(ctx, client, self)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcilePending(ctx, client, self)
		}
	}
}

func (s *A2AServer) reconcilePending(ctx context.Context, client *Client, self string) {
	for id, pending := range s.state.Pending() {
		message, err := client.GetMessage(ctx, id)
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
				if s.sendDetached(ctx, client, self, pending) == nil {
					_ = s.state.RemovePending(id)
				}
			}
			continue
		}
		for _, delivery := range message.ToDelivery {
			if normalizeAddr(delivery.Addr) != normalizeAddr(pending.Client) {
				continue
			}
			switch {
			case delivery.TimeDelivered != nil:
				_ = s.state.RemovePending(id)
			case delivery.ResponseCode != nil && *delivery.ResponseCode == 6:
				if err := s.sendDetached(ctx, client, self, pending); err != nil {
					log.Printf("retry detached A2A result id=%d: %v", id, err)
				} else {
					_ = s.state.RemovePending(id)
				}
			case delivery.ResponseCode != nil:
				log.Printf("A2A result id=%d delivery failed with fmsg code %d", id, *delivery.ResponseCode)
				_ = s.state.RemovePending(id)
			}
			break
		}
	}
}

func (s *A2AServer) sendDetached(ctx context.Context, client *Client, self string, pending pendingA2AResult) error {
	_, err := client.CreateAndSendMessage(ctx, OutgoingMessage{
		From:    self,
		To:      []string{pending.Client},
		Topic:   "A2A " + pending.RequestID,
		Type:    a2aMediaType,
		Data:    pending.Body,
		NoReply: pending.NoReply,
	})
	return err
}

func (s *A2AServer) restoreAttachments(ctx context.Context, client *Client, message Message, root map[string]any) *a2aFailure {
	if len(message.Attachments) > 255 {
		return failure("FMSG_A2A_ATTACHMENT_INVALID", "An A2A message cannot have more than 255 attachments")
	}
	attachments := make(map[string]Attachment, len(message.Attachments))
	var total int64
	for _, attachment := range message.Attachments {
		key := strings.ToLower(attachment.Filename)
		if key == "" {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "An attachment has no filename")
		}
		if _, exists := attachments[key]; exists {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Attachment filenames must be unique case-insensitively")
		}
		attachments[key] = attachment
		total += attachment.Size
		if total > maxA2AAttachBytes {
			return failure("RESOURCE_EXHAUSTED", "A2A attachment data exceeds the local size limit")
		}
	}
	types := map[string]string{}
	if len(attachments) > 0 {
		var err error
		types, err = client.AttachmentTypes(ctx, message.ID)
		if err != nil {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Attachment metadata could not be verified")
		}
	}
	used := make(map[string]int, len(attachments))
	payload, _ := root["payload"].(map[string]any)
	requestMessage, _ := payload["message"].(map[string]any)
	parts, _ := requestMessage["parts"].([]any)
	for _, value := range parts {
		part, ok := value.(map[string]any)
		if !ok {
			continue
		}
		urlValue, ok := part["url"].(string)
		if !ok || !strings.HasPrefix(strings.ToLower(urlValue), "fmsg-attachment:") {
			continue
		}
		if !strings.HasPrefix(urlValue, "fmsg-attachment:") {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Attachment reference schemes must be lowercase")
		}
		filename := strings.TrimPrefix(urlValue, "fmsg-attachment:")
		if filename == "" || strings.Contains(filename, "//") || strings.ContainsAny(filename, "/?#%") {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "An attachment reference is malformed")
		}
		if _, hasRaw := part["raw"]; hasRaw {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "A mapped part cannot contain both url and raw")
		}
		key := strings.ToLower(filename)
		attachment, exists := attachments[key]
		if !exists || used[key] != 0 {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Each attachment must be referenced exactly once")
		}
		attachmentType, exists := types[key]
		if !exists {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "An attachment media type is missing")
		}
		partType, _ := part["mediaType"].(string)
		if !strings.EqualFold(attachmentType, "application/octet-stream") && !strings.EqualFold(attachmentType, partType) {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "An attachment media type does not match its part")
		}
		data, err := client.AttachmentData(ctx, message.ID, attachment.Filename)
		if err != nil || int64(len(data)) != attachment.Size {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Attachment data could not be restored")
		}
		delete(part, "url")
		part["raw"] = base64.StdEncoding.EncodeToString(data)
		used[key]++
	}
	for key := range attachments {
		if used[key] != 1 {
			return failure("FMSG_A2A_ATTACHMENT_INVALID", "Every fmsg attachment must be referenced exactly once")
		}
	}
	return nil
}

func (s *A2AServer) validateThreadParent(ctx context.Context, client *Client, self string, message Message, root map[string]any, envelope a2aRequestEnvelope) *a2aFailure {
	parent, err := client.GetMessage(ctx, *message.PID)
	if err != nil || parent.NoReply || !isA2AMediaType(parent.Type) ||
		normalizeAddr(parent.From) != normalizeAddr(self) || len(parent.To) != 1 ||
		normalizeAddr(parent.To[0]) != normalizeAddr(message.From) {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The selected fmsg parent is not a valid result from this server")
	}
	parentBody, err := client.Data(ctx, parent.ID)
	if err != nil {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The selected fmsg parent is unavailable")
	}
	parentValue, err := decodeUniqueJSON(parentBody)
	if err != nil {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The selected fmsg parent has an invalid envelope")
	}
	parentRoot, ok := parentValue.(map[string]any)
	if !ok || parentRoot["kind"] != "response" || parentRoot["operation"] != "SendMessage" {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The selected fmsg parent is not a conversational result")
	}
	if _, hasError := parentRoot["error"]; hasError {
		return failure("FMSG_A2A_CORRELATION_FAILED", "An error response cannot be a conversational parent")
	}
	currentTask, currentContext := requestScope(envelope.Operation, root["payload"])
	if currentTask == "" && currentContext == "" {
		return failure("FMSG_A2A_CORRELATION_FAILED", "An unscoped operation must start a new fmsg thread")
	}
	parentTask, parentContext := resultScope(parentRoot["payload"])
	if currentTask != "" && currentTask != parentTask {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The parent task does not match the request")
	}
	if currentContext != "" && currentContext != parentContext {
		return failure("FMSG_A2A_CORRELATION_FAILED", "The parent context does not match the request")
	}
	return nil
}

func requestScope(operation string, payload any) (string, string) {
	object, _ := payload.(map[string]any)
	switch operation {
	case "SendMessage", "SendStreamingMessage":
		message, _ := object["message"].(map[string]any)
		return stringField(message, "taskId"), stringField(message, "contextId")
	case "GetTask", "CancelTask", "SubscribeToTask":
		return stringField(object, "id"), ""
	case "ListTasks":
		return "", stringField(object, "contextId")
	case "CreateTaskPushNotificationConfig", "GetTaskPushNotificationConfig", "ListTaskPushNotificationConfigs", "DeleteTaskPushNotificationConfig":
		return stringField(object, "taskId"), ""
	default:
		return "", ""
	}
}

func resultScope(payload any) (string, string) {
	object, _ := payload.(map[string]any)
	for _, key := range []string{"message", "task", "statusUpdate", "artifactUpdate"} {
		if result, ok := object[key].(map[string]any); ok {
			taskID := stringField(result, "taskId")
			if key == "task" {
				taskID = stringField(result, "id")
			}
			return taskID, stringField(result, "contextId")
		}
	}
	return "", ""
}

func stringField(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return value
}

func failure(code, message string) *a2aFailure {
	return &a2aFailure{Code: code, Message: message}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func replayKey(server, client, id string) string {
	return normalizeAddr(server) + "\x00" + normalizeAddr(client) + "\x00" + id
}

func validCanonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

type uniqueJSONDecoder struct {
	decoder *json.Decoder
	values  int
}

func decodeUniqueJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	state := &uniqueJSONDecoder{decoder: decoder}
	value, err := state.value(0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values are not allowed")
		}
		return nil, err
	}
	return value, nil
}

func (d *uniqueJSONDecoder) value(depth int) (any, error) {
	if depth > maxA2AJSONDepth {
		return nil, errors.New("JSON nesting is too deep")
	}
	d.values++
	if d.values > maxA2AJSONValues {
		return nil, errors.New("JSON contains too many values")
	}
	token, err := d.decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		object := map[string]any{}
		for d.decoder.More() {
			keyToken, err := d.decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON property %q", key)
			}
			value, err := d.value(depth + 1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if token, err := d.decoder.Token(); err != nil || token != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		var array []any
		for d.decoder.More() {
			value, err := d.value(depth + 1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if token, err := d.decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}
