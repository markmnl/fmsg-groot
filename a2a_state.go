package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	a2aReplayRetention = 7 * 24 * time.Hour
	a2aReplayLimit     = 10_000
)

type storedA2AResponse struct {
	Digest    string          `json:"digest"`
	Body      json.RawMessage `json:"body"`
	NoReply   bool            `json:"noReply"`
	CreatedAt time.Time       `json:"createdAt"`
}

type storedA2AMessage struct {
	Digest    string          `json:"digest"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
}

type pendingA2AResult struct {
	Client    string          `json:"client"`
	RequestID string          `json:"requestId"`
	Body      json.RawMessage `json:"body"`
	NoReply   bool            `json:"noReply"`
	CreatedAt time.Time       `json:"createdAt"`
}

type persistedA2AState struct {
	Requests map[string]storedA2AResponse `json:"requests"`
	Messages map[string]storedA2AMessage  `json:"messages"`
	Pending  map[int64]pendingA2AResult   `json:"pending"`
}

// A2AState persists binding request replay, A2A message idempotency, and
// replies that still need delivery confirmation.
type A2AState struct {
	mu    sync.Mutex
	path  string
	state persistedA2AState
}

func LoadA2AState(path string) (*A2AState, error) {
	store := &A2AState{
		path: path,
		state: persistedA2AState{
			Requests: make(map[string]storedA2AResponse),
			Messages: make(map[string]storedA2AMessage),
			Pending:  make(map[int64]pendingA2AResult),
		},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, &store.state); err != nil {
		return nil, fmt.Errorf("parse A2A state file %s: %w", path, err)
	}
	store.ensureMaps()
	store.prune(time.Now())
	return store, nil
}

func (s *A2AState) LookupRequest(key, digest string) (storedA2AResponse, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.state.Requests[key]
	if !ok {
		return storedA2AResponse{}, false, false
	}
	return record, true, record.Digest != digest
}

func (s *A2AState) SaveRequest(key string, record storedA2AResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(time.Now())
	if _, exists := s.state.Requests[key]; !exists && len(s.state.Requests) >= a2aReplayLimit {
		return errors.New("A2A request replay store is full")
	}
	s.state.Requests[key] = record
	return s.saveLocked()
}

func (s *A2AState) LookupMessage(key, digest string) (storedA2AMessage, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.state.Messages[key]
	if !ok {
		return storedA2AMessage{}, false, false
	}
	return record, true, record.Digest != digest
}

func (s *A2AState) SaveMessage(key string, record storedA2AMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(time.Now())
	if _, exists := s.state.Messages[key]; !exists && len(s.state.Messages) >= a2aReplayLimit {
		return errors.New("A2A message replay store is full")
	}
	s.state.Messages[key] = record
	return s.saveLocked()
}

func (s *A2AState) AddPending(id int64, result pendingA2AResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Pending[id] = result
	return s.saveLocked()
}

func (s *A2AState) RemovePending(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.state.Pending[id]; !ok {
		return nil
	}
	delete(s.state.Pending, id)
	return s.saveLocked()
}

func (s *A2AState) Pending() map[int64]pendingA2AResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[int64]pendingA2AResult, len(s.state.Pending))
	for id, pending := range s.state.Pending {
		result[id] = pending
	}
	return result
}

func (s *A2AState) ensureMaps() {
	if s.state.Requests == nil {
		s.state.Requests = make(map[string]storedA2AResponse)
	}
	if s.state.Messages == nil {
		s.state.Messages = make(map[string]storedA2AMessage)
	}
	if s.state.Pending == nil {
		s.state.Pending = make(map[int64]pendingA2AResult)
	}
}

func (s *A2AState) prune(now time.Time) {
	cutoff := now.Add(-a2aReplayRetention)
	for key, record := range s.state.Requests {
		if record.CreatedAt.Before(cutoff) {
			delete(s.state.Requests, key)
		}
	}
	for key, record := range s.state.Messages {
		if record.CreatedAt.Before(cutoff) {
			delete(s.state.Messages, key)
		}
	}
}

func (s *A2AState) saveLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, "fmsg-groot-a2a-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
