package a2afmsg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// Transport implements a2aclient.Transport over FMSG-004 and fmsg-webapi.
type Transport struct {
	config Config
	api    *webAPIClient
	server string
	client string

	mu             sync.Mutex
	taskHeads      map[string]int64
	contextAnchors map[string]int64
	destroyed      bool
}

var _ a2aclient.Transport = (*Transport)(nil)

// NewTransport connects an FMSG-004 transport to one fmsg AgentInterface URL.
func NewTransport(ctx context.Context, endpoint string, config Config) (*Transport, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	server, err := ParseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	api, err := newWebAPIClient(config)
	if err != nil {
		return nil, err
	}
	client, err := api.ensureToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("authenticate fmsg transport: %w", err)
	}
	return &Transport{
		config:         config,
		api:            api,
		server:         server,
		client:         client,
		taskHeads:      make(map[string]int64),
		contextAnchors: make(map[string]int64),
	}, nil
}

type exchange struct {
	operation   string
	requestID   string
	body        []byte
	attachments []outgoingAttachment
	baseline    int64
	parent      int64
	attemptIDs  []int64
	activeID    int64
	threaded    bool
	detached    bool
}

func (t *Transport) start(ctx context.Context, operation string, params a2aclient.ServiceParams, payload any) (*exchange, error) {
	if err := t.available(); err != nil {
		return nil, err
	}
	body, attachments, requestID, err := buildRequest(operation, params, payload, t.config.Credentials)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > t.config.MaxJSONBytes {
		return nil, errors.New("FMSG-004 request exceeds the configured JSON size limit")
	}
	baseline, err := t.api.maxInboxID(ctx)
	if err != nil {
		return nil, fmt.Errorf("establish fmsg inbox cursor: %w", err)
	}
	parent := t.parentFor(operation, payload)
	exchange := &exchange{
		operation: operation, requestID: requestID, body: body, attachments: attachments,
		baseline: baseline, parent: parent, threaded: parent > 0,
	}
	id, err := t.api.createAndSend(ctx, outgoingMessage{
		To: t.server, PID: parent, Topic: "A2A " + requestID, Data: body, Attachments: attachments,
	})
	if err != nil && parent > 0 && isHTTPConflict(err) {
		_ = t.api.deleteDraft(ctx, id)
		t.forgetParent(parent)
		id, err = t.api.createAndSend(ctx, outgoingMessage{
			To: t.server, Topic: "A2A " + requestID, Data: body, Attachments: attachments,
		})
		exchange.detached = true
	}
	if err != nil {
		return nil, fmt.Errorf("send FMSG-004 %s request: %w", operation, err)
	}
	exchange.attemptIDs = append(exchange.attemptIDs, id)
	exchange.activeID = id
	return exchange, nil
}

func isHTTPConflict(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
}

func (t *Transport) available() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.destroyed {
		return errors.New("FMSG A2A transport is destroyed")
	}
	return nil
}

func (t *Transport) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, t.config.RequestTimeout)
}

func (t *Transport) roundTrip(ctx context.Context, operation string, params a2aclient.ServiceParams, payload any) (*resultEnvelope, message, error) {
	ctx, cancel := t.callContext(ctx)
	defer cancel()
	exchange, err := t.start(ctx, operation, params, payload)
	if err != nil {
		return nil, message{}, err
	}
	ticker := time.NewTicker(t.config.PollInterval)
	defer ticker.Stop()
	seen := make(map[int64]bool)
	for {
		envelope, resultMessage, found, err := t.scanResponse(ctx, exchange, seen)
		if err != nil {
			return nil, message{}, err
		}
		if found {
			if envelope.Kind != "response" {
				return nil, message{}, errors.New("FMSG-004 non-streaming request received an event")
			}
			if envelope.Error != nil {
				return nil, message{}, resultError(envelope.Error)
			}
			if operation == "SendMessage" && resultMessage.NoReply {
				return nil, message{}, errors.New("successful SendMessage response has no_reply set")
			}
			return envelope, resultMessage, nil
		}
		if err := t.checkRequestDelivery(ctx, exchange); err != nil {
			return nil, message{}, err
		}
		select {
		case <-ctx.Done():
			return nil, message{}, fmt.Errorf("wait for FMSG-004 %s response: %w", operation, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (t *Transport) scanResponse(ctx context.Context, exchange *exchange, seen map[int64]bool) (*resultEnvelope, message, bool, error) {
	messages, err := t.api.inboxAfter(ctx, exchange.baseline)
	if err != nil {
		return nil, message{}, false, err
	}
	for _, item := range messages {
		if seen[item.ID] || !t.matchesCandidate(item, exchange) {
			continue
		}
		seen[item.ID] = true
		if err := t.validateResultProfile(item, exchange); err != nil {
			return nil, message{}, false, err
		}
		envelope, _, err := parseResult(ctx, t.api, item, exchange.operation, exchange.requestID,
			t.config.MaxJSONBytes, t.config.MaxAttachmentBytes)
		if err != nil {
			return nil, message{}, false, err
		}
		return envelope, item, true, nil
	}
	return nil, message{}, false, nil
}

func (t *Transport) matchesCandidate(item message, exchange *exchange) bool {
	if !strings.EqualFold(item.From, t.server) {
		return false
	}
	if item.PID != nil {
		for _, id := range exchange.attemptIDs {
			if *item.PID == id {
				return true
			}
		}
		return false
	}
	return item.Topic == "A2A "+exchange.requestID
}

func (t *Transport) validateResultProfile(item message, exchange *exchange) error {
	if item.Version != 1 || item.HasAddTo || !isA2AMediaType(item.Type) ||
		len(item.To) != 1 || !strings.EqualFold(item.To[0], t.client) {
		return errors.New("FMSG-004 result message profile is invalid")
	}
	if item.PID != nil {
		if !item.HasPID || item.Topic != "" {
			return errors.New("FMSG-004 reply relationship is invalid")
		}
		return nil
	}
	if item.HasPID || item.Topic != "A2A "+exchange.requestID {
		return errors.New("FMSG-004 detached result relationship is invalid")
	}
	return nil
}

func (t *Transport) checkRequestDelivery(ctx context.Context, exchange *exchange) error {
	item, err := t.api.getMessage(ctx, exchange.activeID)
	if err != nil {
		return fmt.Errorf("read fmsg request delivery: %w", err)
	}
	for _, delivery := range item.ToDelivery {
		if !strings.EqualFold(delivery.Addr, t.server) || delivery.ResponseCode == nil {
			continue
		}
		code := *delivery.ResponseCode
		if code == 6 && exchange.threaded && !exchange.detached {
			t.forgetParent(exchange.parent)
			id, err := t.api.createAndSend(ctx, outgoingMessage{
				To: t.server, Topic: "A2A " + exchange.requestID, Data: exchange.body,
				Attachments: exchange.attachments,
			})
			if err != nil {
				return fmt.Errorf("resend detached FMSG-004 request: %w", err)
			}
			exchange.attemptIDs = append(exchange.attemptIDs, id)
			exchange.activeID = id
			exchange.detached = true
			return nil
		}
		return &DeliveryError{Code: code, Address: delivery.Addr, Retryable: code == 5 || code == 101}
	}
	return nil
}

func (t *Transport) forgetParent(parent int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for taskID, messageID := range t.taskHeads {
		if messageID == parent {
			delete(t.taskHeads, taskID)
		}
	}
	for contextID, messageID := range t.contextAnchors {
		if messageID == parent {
			delete(t.contextAnchors, contextID)
		}
	}
}

func (t *Transport) parentFor(operation string, payload any) int64 {
	taskID, contextID := requestScope(operation, payload)
	t.mu.Lock()
	defer t.mu.Unlock()
	if taskID != "" {
		return t.taskHeads[taskID]
	}
	if contextID != "" {
		return t.contextAnchors[contextID]
	}
	return 0
}

func requestScope(operation string, payload any) (string, string) {
	switch request := payload.(type) {
	case *a2a.SendMessageRequest:
		if request != nil && request.Message != nil {
			return string(request.Message.TaskID), request.Message.ContextID
		}
	case *a2a.GetTaskRequest:
		return string(request.ID), ""
	case *a2a.CancelTaskRequest:
		return string(request.ID), ""
	case *a2a.SubscribeToTaskRequest:
		return string(request.ID), ""
	case *a2a.ListTasksRequest:
		return "", request.ContextID
	case *a2a.PushConfig:
		return string(request.TaskID), ""
	case *a2a.GetTaskPushConfigRequest:
		return string(request.TaskID), ""
	case *a2a.ListTaskPushConfigRequest:
		return string(request.TaskID), ""
	case *a2a.DeleteTaskPushConfigRequest:
		return string(request.TaskID), ""
	}
	return "", ""
}

func (t *Transport) observe(event a2a.Event, messageID int64) {
	if event == nil {
		return
	}
	info := event.TaskInfo()
	t.mu.Lock()
	defer t.mu.Unlock()
	if info.TaskID != "" {
		t.taskHeads[string(info.TaskID)] = messageID
	}
	if info.ContextID != "" {
		if _, exists := t.contextAnchors[info.ContextID]; !exists {
			t.contextAnchors[info.ContextID] = messageID
		}
	}
}

// SendMessage implements a2aclient.Transport.
func (t *Transport) SendMessage(ctx context.Context, params a2aclient.ServiceParams, request *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	envelope, item, err := t.roundTrip(ctx, "SendMessage", params, request)
	if err != nil {
		return nil, err
	}
	var response a2a.StreamResponse
	if err := json.Unmarshal(envelope.Payload, &response); err != nil {
		return nil, fmt.Errorf("decode SendMessage response: %w", err)
	}
	switch result := response.Event.(type) {
	case *a2a.Message:
		t.observe(result, item.ID)
		return result, nil
	case *a2a.Task:
		t.observe(result, item.ID)
		return result, nil
	default:
		return nil, fmt.Errorf("SendMessage returned %T: %w", response.Event, a2a.ErrInvalidAgentResponse)
	}
}

// GetTask implements a2aclient.Transport.
func (t *Transport) GetTask(ctx context.Context, params a2aclient.ServiceParams, request *a2a.GetTaskRequest) (*a2a.Task, error) {
	envelope, _, err := t.roundTrip(ctx, "GetTask", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.Task](envelope, "GetTask")
}

// ListTasks implements a2aclient.Transport.
func (t *Transport) ListTasks(ctx context.Context, params a2aclient.ServiceParams, request *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	envelope, _, err := t.roundTrip(ctx, "ListTasks", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.ListTasksResponse](envelope, "ListTasks")
}

// CancelTask implements a2aclient.Transport.
func (t *Transport) CancelTask(ctx context.Context, params a2aclient.ServiceParams, request *a2a.CancelTaskRequest) (*a2a.Task, error) {
	envelope, _, err := t.roundTrip(ctx, "CancelTask", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.Task](envelope, "CancelTask")
}

// GetTaskPushConfig implements a2aclient.Transport.
func (t *Transport) GetTaskPushConfig(ctx context.Context, params a2aclient.ServiceParams, request *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	envelope, _, err := t.roundTrip(ctx, "GetTaskPushNotificationConfig", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.PushConfig](envelope, "GetTaskPushNotificationConfig")
}

// ListTaskPushConfigs implements a2aclient.Transport.
func (t *Transport) ListTaskPushConfigs(ctx context.Context, params a2aclient.ServiceParams, request *a2a.ListTaskPushConfigRequest) ([]*a2a.PushConfig, error) {
	envelope, _, err := t.roundTrip(ctx, "ListTaskPushNotificationConfigs", params, request)
	if err != nil {
		return nil, err
	}
	response, err := decodePayload[a2a.ListTaskPushConfigResponse](envelope, "ListTaskPushNotificationConfigs")
	if err != nil {
		return nil, err
	}
	return response.Configs, nil
}

// CreateTaskPushConfig implements a2aclient.Transport.
func (t *Transport) CreateTaskPushConfig(ctx context.Context, params a2aclient.ServiceParams, request *a2a.PushConfig) (*a2a.PushConfig, error) {
	envelope, _, err := t.roundTrip(ctx, "CreateTaskPushNotificationConfig", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.PushConfig](envelope, "CreateTaskPushNotificationConfig")
}

// DeleteTaskPushConfig implements a2aclient.Transport.
func (t *Transport) DeleteTaskPushConfig(ctx context.Context, params a2aclient.ServiceParams, request *a2a.DeleteTaskPushConfigRequest) error {
	envelope, _, err := t.roundTrip(ctx, "DeleteTaskPushNotificationConfig", params, request)
	if err != nil {
		return err
	}
	var empty map[string]any
	if err := json.Unmarshal(envelope.Payload, &empty); err != nil || len(empty) != 0 {
		return errors.New("DeleteTaskPushNotificationConfig response must be an empty object")
	}
	return nil
}

// GetExtendedAgentCard implements a2aclient.Transport.
func (t *Transport) GetExtendedAgentCard(ctx context.Context, params a2aclient.ServiceParams, request *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	envelope, _, err := t.roundTrip(ctx, "GetExtendedAgentCard", params, request)
	if err != nil {
		return nil, err
	}
	return decodePayload[a2a.AgentCard](envelope, "GetExtendedAgentCard")
}

func decodePayload[T any](envelope *resultEnvelope, operation string) (*T, error) {
	var result T
	if err := json.Unmarshal(envelope.Payload, &result); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", operation, err)
	}
	return &result, nil
}

// SendStreamingMessage implements a2aclient.Transport.
func (t *Transport) SendStreamingMessage(ctx context.Context, params a2aclient.ServiceParams, request *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return t.stream(ctx, "SendStreamingMessage", params, request, true)
}

// SubscribeToTask implements a2aclient.Transport.
func (t *Transport) SubscribeToTask(ctx context.Context, params a2aclient.ServiceParams, request *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return t.stream(ctx, "SubscribeToTask", params, request, false)
}

type bufferedEvent struct {
	envelope *resultEnvelope
	message  message
	digest   string
}

func (t *Transport) stream(ctx context.Context, operation string, params a2aclient.ServiceParams, payload any, conversational bool) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		ctx, cancel := t.callContext(ctx)
		defer cancel()
		exchange, err := t.start(ctx, operation, params, payload)
		if err != nil {
			yield(nil, err)
			return
		}
		ticker := time.NewTicker(t.config.PollInterval)
		defer ticker.Stop()
		seenMessages := make(map[int64]bool)
		emitted := make(map[uint64]string)
		buffer := make(map[uint64]bufferedEvent)
		var expected uint64
		started := false
		for {
			items, err := t.api.inboxAfter(ctx, exchange.baseline)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, item := range items {
				if seenMessages[item.ID] || !t.matchesCandidate(item, exchange) {
					continue
				}
				seenMessages[item.ID] = true
				if err := t.validateResultProfile(item, exchange); err != nil {
					yield(nil, err)
					return
				}
				envelope, canonical, err := parseResult(ctx, t.api, item, operation, exchange.requestID,
					t.config.MaxJSONBytes, t.config.MaxAttachmentBytes)
				if err != nil {
					yield(nil, err)
					return
				}
				if envelope.Kind == "response" {
					if started || envelope.Error == nil {
						yield(nil, errors.New("FMSG-004 stream received an invalid response envelope"))
					} else {
						yield(nil, resultError(envelope.Error))
					}
					return
				}
				sequence := *envelope.Sequence
				itemDigest := digest(canonical)
				if previous, exists := emitted[sequence]; exists {
					if previous != itemDigest {
						yield(nil, errors.New("FMSG-004 event sequence was reused with different content"))
						return
					}
					continue
				}
				if previous, exists := buffer[sequence]; exists {
					if previous.digest != itemDigest {
						yield(nil, errors.New("FMSG-004 event sequence was reused with different content"))
						return
					}
					continue
				}
				buffer[sequence] = bufferedEvent{envelope: envelope, message: item, digest: itemDigest}
				if len(buffer) > t.config.MaxBufferedEvents {
					yield(nil, errors.New("FMSG-004 stream exceeded the event reorder buffer"))
					return
				}
			}
			for {
				buffered, exists := buffer[expected]
				if !exists {
					break
				}
				delete(buffer, expected)
				emitted[expected] = buffered.digest
				started = true
				if buffered.envelope.Error != nil {
					if !*buffered.envelope.Final {
						yield(nil, errors.New("FMSG-004 stream error was not final"))
					} else {
						yield(nil, resultError(buffered.envelope.Error))
					}
					return
				}
				var response a2a.StreamResponse
				if err := json.Unmarshal(buffered.envelope.Payload, &response); err != nil {
					yield(nil, fmt.Errorf("decode FMSG-004 stream event: %w", err))
					return
				}
				if response.Event == nil {
					yield(nil, fmt.Errorf("FMSG-004 stream event has no payload: %w", a2a.ErrInvalidAgentResponse))
					return
				}
				if conversational && response.Event.TaskInfo().TaskID != "" && buffered.message.NoReply {
					yield(nil, errors.New("conversational task event has no_reply set"))
					return
				}
				if conversational {
					t.observe(response.Event, buffered.message.ID)
				}
				if !yield(response.Event, nil) {
					return
				}
				if *buffered.envelope.Final {
					return
				}
				expected++
			}
			if err := t.checkRequestDelivery(ctx, exchange); err != nil {
				yield(nil, err)
				return
			}
			select {
			case <-ctx.Done():
				yield(nil, fmt.Errorf("wait for FMSG-004 %s event %d: %w", operation, expected, ctx.Err()))
				return
			case <-ticker.C:
			}
		}
	}
}

// Destroy prevents new calls. In-flight calls continue until their contexts end.
func (t *Transport) Destroy() error {
	t.mu.Lock()
	t.destroyed = true
	t.mu.Unlock()
	return nil
}
