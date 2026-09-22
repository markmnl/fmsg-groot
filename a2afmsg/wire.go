package a2afmsg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/google/uuid"
)

const (
	maxJSONDepth  = 64
	maxJSONValues = 100_000
	maxSequence   = 9_007_199_254_740_991
)

type requestEnvelope struct {
	BindingVersion    string                `json:"bindingVersion"`
	A2AVersion        string                `json:"a2aVersion"`
	Kind              string                `json:"kind"`
	RequestID         string                `json:"requestId"`
	Operation         string                `json:"operation"`
	ServiceParameters map[string]string     `json:"serviceParameters"`
	Payload           any                   `json:"payload"`
	Credentials       map[string]Credential `json:"credentials,omitempty"`
}

type resultEnvelope struct {
	BindingVersion string          `json:"bindingVersion"`
	A2AVersion     string          `json:"a2aVersion"`
	Kind           string          `json:"kind"`
	RequestID      string          `json:"requestId"`
	Operation      string          `json:"operation"`
	Sequence       *uint64         `json:"sequence,omitempty"`
	Final          *bool           `json:"final,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Error          *wireError      `json:"error,omitempty"`
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details []any  `json:"details,omitempty"`
}

// BindingError is an A2A or FMSG-004 error returned by the remote agent.
type BindingError struct {
	Code    string
	Message string
	Details []any
	cause   error
}

func (e *BindingError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

func (e *BindingError) Unwrap() error { return e.cause }

// DeliveryError is an fmsg transport rejection, distinct from an A2A error.
type DeliveryError struct {
	Code      int
	Address   string
	Retryable bool
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("fmsg delivery to %s failed with response code %d", e.Address, e.Code)
}

func buildRequest(operation string, params a2aclient.ServiceParams, payload any, credentials map[string]Credential) ([]byte, []outgoingAttachment, string, error) {
	payloadValue, attachments, err := mapRequestAttachments(operation, payload)
	if err != nil {
		return nil, nil, "", err
	}
	requestID := uuid.Must(uuid.NewV7()).String()
	envelope := requestEnvelope{
		BindingVersion:    bindingVersion,
		A2AVersion:        a2aVersion,
		Kind:              "request",
		RequestID:         requestID,
		Operation:         operation,
		ServiceParameters: flattenServiceParams(params),
		Payload:           payloadValue,
		Credentials:       credentials,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, nil, "", fmt.Errorf("marshal FMSG-004 request: %w", err)
	}
	return body, attachments, requestID, nil
}

func flattenServiceParams(params a2aclient.ServiceParams) map[string]string {
	combined := make(map[string][]string)
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lower := strings.ToLower(key)
		combined[lower] = append(combined[lower], params[key]...)
	}
	result := make(map[string]string, len(combined)+1)
	for key, values := range combined {
		result[key] = strings.Join(values, ",")
	}
	result["a2a-version"] = a2aVersion
	return result
}

func mapRequestAttachments(operation string, payload any) (any, []outgoingAttachment, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal A2A request payload: %w", err)
	}
	value, err := decodeUniqueJSON(data)
	if err != nil {
		return nil, nil, err
	}
	root, ok := value.(map[string]any)
	if !ok {
		return nil, nil, errors.New("A2A request payload must be an object")
	}
	if operation != "SendMessage" && operation != "SendStreamingMessage" {
		return root, nil, nil
	}
	messageValue, _ := root["message"].(map[string]any)
	parts, _ := messageValue["parts"].([]any)
	attachments, err := mapPartRaw(parts)
	if err != nil {
		return nil, nil, err
	}
	return root, attachments, nil
}

func mapPartRaw(parts []any) ([]outgoingAttachment, error) {
	attachments := make([]outgoingAttachment, 0)
	for _, value := range parts {
		part, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if partURL, ok := part["url"].(string); ok &&
			strings.HasPrefix(strings.ToLower(partURL), "fmsg-attachment:") {
			return nil, errors.New("application-supplied Part.url uses the reserved fmsg-attachment scheme")
		}
		raw, ok := part["raw"].(string)
		if !ok || raw == "" || len(attachments) >= 255 {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("decode A2A Part.raw: %w", err)
		}
		filename := fmt.Sprintf("a2a-part-%d", len(attachments))
		mediaType, _ := part["mediaType"].(string)
		if !validAttachmentMediaType(mediaType) {
			mediaType = "application/octet-stream"
		}
		delete(part, "raw")
		part["url"] = "fmsg-attachment:" + filename
		attachments = append(attachments, outgoingAttachment{
			Filename: filename, MediaType: mediaType, Data: data,
		})
	}
	return attachments, nil
}

func validAttachmentMediaType(value string) bool {
	if value == "" || len(value) >= 256 {
		return false
	}
	for _, char := range []byte(value) {
		if char > 127 {
			return false
		}
	}
	return true
}

func parseResult(ctx context.Context, api *webAPIClient, item message, operation, requestID string, maxJSON, maxAttachments int64) (*resultEnvelope, []byte, error) {
	body, err := api.data(ctx, item.ID, maxJSON)
	if err != nil {
		return nil, nil, err
	}
	if int64(len(body)) > maxJSON {
		return nil, nil, errors.New("FMSG-004 response exceeds the JSON size limit")
	}
	if !utf8.Valid(body) || bytes.HasPrefix(body, []byte{0xef, 0xbb, 0xbf}) {
		return nil, nil, errors.New("FMSG-004 response must be BOM-free UTF-8")
	}
	value, err := decodeUniqueJSON(body)
	if err != nil {
		return nil, nil, fmt.Errorf("decode FMSG-004 response: %w", err)
	}
	root, ok := value.(map[string]any)
	if !ok {
		return nil, nil, errors.New("FMSG-004 response envelope must be an object")
	}
	if err := restoreResultAttachments(ctx, api, item, root, maxAttachments); err != nil {
		return nil, nil, err
	}
	canonical, err := json.Marshal(root)
	if err != nil {
		return nil, nil, err
	}
	var envelope resultEnvelope
	if err := json.Unmarshal(canonical, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode FMSG-004 envelope fields: %w", err)
	}
	if envelope.BindingVersion != bindingVersion || envelope.A2AVersion != a2aVersion ||
		envelope.RequestID != requestID || envelope.Operation != operation {
		return nil, nil, errors.New("FMSG-004 response correlation failed")
	}
	if envelope.Kind != "response" && envelope.Kind != "event" {
		return nil, nil, errors.New("FMSG-004 result kind must be response or event")
	}
	if (envelope.Payload == nil) == (envelope.Error == nil) {
		return nil, nil, errors.New("FMSG-004 result must contain exactly one of payload or error")
	}
	if envelope.Payload != nil {
		if _, ok := root["payload"].(map[string]any); !ok {
			return nil, nil, errors.New("FMSG-004 result payload must be an object")
		}
	}
	if envelope.Error != nil && (envelope.Error.Code == "" || envelope.Error.Message == "") {
		return nil, nil, errors.New("FMSG-004 error code and message are required")
	}
	if envelope.Kind == "event" {
		if envelope.Sequence == nil || *envelope.Sequence > maxSequence || envelope.Final == nil {
			return nil, nil, errors.New("FMSG-004 event sequence and final fields are invalid")
		}
	} else if envelope.Sequence != nil || envelope.Final != nil {
		return nil, nil, errors.New("FMSG-004 response cannot contain event fields")
	}
	return &envelope, canonical, nil
}

func restoreResultAttachments(ctx context.Context, api *webAPIClient, item message, root map[string]any, maxBytes int64) error {
	if len(item.Attachments) > 255 {
		return errors.New("FMSG-004 result has too many attachments")
	}
	byName := make(map[string]attachment, len(item.Attachments))
	var total int64
	for _, attachment := range item.Attachments {
		key := strings.ToLower(attachment.Filename)
		if key == "" || attachment.Size < 0 {
			return errors.New("FMSG-004 attachment has an invalid filename or size")
		}
		if _, exists := byName[key]; exists {
			return errors.New("FMSG-004 attachment filenames are not unique")
		}
		byName[key] = attachment
		if attachment.Size > maxBytes-total {
			return errors.New("FMSG-004 attachment data exceeds the configured limit")
		}
		total += attachment.Size
	}
	types := map[string]string{}
	if len(byName) > 0 {
		var err error
		types, err = api.attachmentTypes(ctx, item.ID)
		if err != nil {
			return fmt.Errorf("load FMSG-004 attachment types: %w", err)
		}
	}
	parts := resultParts(root)
	used := make(map[string]bool, len(byName))
	for _, part := range parts {
		urlValue, ok := part["url"].(string)
		if !ok || !strings.HasPrefix(strings.ToLower(urlValue), "fmsg-attachment:") {
			continue
		}
		if !strings.HasPrefix(urlValue, "fmsg-attachment:") {
			return errors.New("FMSG-004 attachment reference scheme must be lowercase")
		}
		filename := strings.TrimPrefix(urlValue, "fmsg-attachment:")
		if filename == "" || strings.Contains(filename, "//") || strings.ContainsAny(filename, "/?#%") {
			return errors.New("FMSG-004 attachment reference is malformed")
		}
		if _, hasRaw := part["raw"]; hasRaw {
			return errors.New("FMSG-004 part contains both url and raw")
		}
		key := strings.ToLower(filename)
		attachment, exists := byName[key]
		if !exists || used[key] {
			return errors.New("FMSG-004 attachment reference is unresolved or repeated")
		}
		partType, _ := part["mediaType"].(string)
		attachmentType, exists := types[key]
		if !exists || !strings.EqualFold(attachmentType, "application/octet-stream") &&
			!strings.EqualFold(attachmentType, partType) {
			return errors.New("FMSG-004 attachment media type does not match its part")
		}
		data, err := api.attachmentData(ctx, item.ID, attachment.Filename, attachment.Size)
		if err != nil || int64(len(data)) != attachment.Size {
			return errors.New("FMSG-004 attachment data could not be restored")
		}
		delete(part, "url")
		part["raw"] = base64.StdEncoding.EncodeToString(data)
		used[key] = true
	}
	if len(used) != len(byName) {
		return errors.New("FMSG-004 result contains an unreferenced attachment")
	}
	return nil
}

func resultParts(root map[string]any) []map[string]any {
	payload, _ := root["payload"].(map[string]any)
	operation, _ := root["operation"].(string)
	kind, _ := root["kind"].(string)
	var parts []map[string]any
	addMessage := func(value any) {
		message, _ := value.(map[string]any)
		parts = append(parts, partArray(message["parts"])...)
	}
	var addArtifact func(any)
	addArtifact = func(value any) {
		artifact, _ := value.(map[string]any)
		parts = append(parts, partArray(artifact["parts"])...)
	}
	var addTask func(any)
	addTask = func(value any) {
		task, _ := value.(map[string]any)
		status, _ := task["status"].(map[string]any)
		addMessage(status["message"])
		if history, ok := task["history"].([]any); ok {
			for _, value := range history {
				addMessage(value)
			}
		}
		if artifacts, ok := task["artifacts"].([]any); ok {
			for _, value := range artifacts {
				addArtifact(value)
			}
		}
	}
	if kind == "event" || operation == "SendMessage" {
		addMessage(payload["message"])
		addTask(payload["task"])
		if update, ok := payload["statusUpdate"].(map[string]any); ok {
			status, _ := update["status"].(map[string]any)
			addMessage(status["message"])
		}
		if update, ok := payload["artifactUpdate"].(map[string]any); ok {
			addArtifact(update["artifact"])
		}
		return parts
	}
	switch operation {
	case "GetTask", "CancelTask":
		addTask(payload)
	case "ListTasks":
		if tasks, ok := payload["tasks"].([]any); ok {
			for _, value := range tasks {
				addTask(value)
			}
		}
	}
	return parts
}

func partArray(value any) []map[string]any {
	values, _ := value.([]any)
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if part, ok := value.(map[string]any); ok {
			result = append(result, part)
		}
	}
	return result
}

func resultError(value *wireError) error {
	if value == nil {
		return nil
	}
	cause := mapErrorCode(value.Code)
	return &BindingError{Code: value.Code, Message: value.Message, Details: value.Details, cause: cause}
}

func mapErrorCode(code string) error {
	switch code {
	case "A2A_TASK_NOT_FOUND":
		return a2a.ErrTaskNotFound
	case "A2A_TASK_NOT_CANCELABLE":
		return a2a.ErrTaskNotCancelable
	case "A2A_PUSH_NOTIFICATION_NOT_SUPPORTED":
		return a2a.ErrPushNotificationNotSupported
	case "A2A_UNSUPPORTED_OPERATION":
		return a2a.ErrUnsupportedOperation
	case "A2A_CONTENT_TYPE_NOT_SUPPORTED":
		return a2a.ErrUnsupportedContentType
	case "A2A_INVALID_AGENT_RESPONSE":
		return a2a.ErrInvalidAgentResponse
	case "A2A_EXTENDED_AGENT_CARD_NOT_CONFIGURED":
		return a2a.ErrExtendedCardNotConfigured
	case "A2A_EXTENSION_SUPPORT_REQUIRED":
		return a2a.ErrExtensionSupportRequired
	case "A2A_VERSION_NOT_SUPPORTED", "FMSG_A2A_UNSUPPORTED_BINDING_VERSION":
		return a2a.ErrVersionNotSupported
	case "UNAUTHENTICATED":
		return a2a.ErrUnauthenticated
	case "PERMISSION_DENIED":
		return a2a.ErrUnauthorized
	case "INVALID_ARGUMENT", "FMSG_A2A_INVALID_ENVELOPE":
		return a2a.ErrInvalidParams
	case "FMSG_A2A_UNKNOWN_OPERATION":
		return a2a.ErrMethodNotFound
	case "FMSG_A2A_REQUEST_ID_CONFLICT":
		return a2a.ErrInvalidRequest
	case "FMSG_A2A_CORRELATION_FAILED", "FMSG_A2A_ATTACHMENT_INVALID":
		return a2a.ErrInvalidAgentResponse
	case "INTERNAL":
		return a2a.ErrInternalError
	default:
		return a2a.ErrServerError
	}
}

func isA2AMediaType(value string) bool {
	parsed, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(parsed, MediaType)
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type uniqueDecoder struct {
	decoder *json.Decoder
	values  int
}

func decodeUniqueJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	state := &uniqueDecoder{decoder: decoder}
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

func (d *uniqueDecoder) value(depth int) (any, error) {
	if depth > maxJSONDepth {
		return nil, errors.New("JSON nesting is too deep")
	}
	d.values++
	if d.values > maxJSONValues {
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
		array := []any{}
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
