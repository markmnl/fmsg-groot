package main

import (
	"strings"
	"unicode"
)

const (
	// GrootToo is the one-shot reply when the inbound body already mentions Groot.
	GrootToo = "I am Groot too!"
	// grootTooMarker is the lowercased substring that means "do not reply".
	grootTooMarker = "groot too!"
	// grootMarker is the lowercased substring that triggers GrootToo.
	grootMarker = "groot"
)

// ChooseReply selects the reply body for an inbound message, or skip=true.
//
// Rules (body matched after ToLower):
//  1. contains "groot too!" → skip (stops Groot-to-Groot ping-pong)
//  2. contains "groot"     → fixed "I am Groot too!"
//  3. otherwise            → random emotional variant
func ChooseReply(body string) (text string, skip bool) {
	lower := strings.ToLower(body)
	if strings.Contains(lower, grootTooMarker) {
		return "", true
	}
	if strings.Contains(lower, grootMarker) {
		return GrootToo, false
	}
	return PickLine(), false
}

// Message meta used only for building reply-all recipients.
type messageMeta struct {
	From  string
	To    []string
	AddTo []addToBatch
}

type addToBatch struct {
	AddToFrom string
	To        []string
}

// RecipientsFromMessage builds a reply-all recipient list: every participant
// on the parent message except self. Order is stable: from, then to, then
// add_to batches (add_to_from then each batch to).
func RecipientsFromMessage(msg messageMeta, self string) []string {
	selfKey := normalizeAddr(self)
	seen := map[string]struct{}{}
	var out []string
	add := func(addr string) {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return
		}
		key := normalizeAddr(addr)
		if key == "" || key == selfKey {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, addr)
	}

	add(msg.From)
	for _, a := range msg.To {
		add(a)
	}
	for _, batch := range msg.AddTo {
		add(batch.AddToFrom)
		for _, a := range batch.To {
			add(a)
		}
	}
	return out
}

// normalizeAddr lowercases and strips spaces for dedupe / self-compare.
// fmsg addresses are case-insensitive per the specification.
func normalizeAddr(addr string) string {
	var b strings.Builder
	b.Grow(len(addr))
	for _, r := range strings.TrimSpace(addr) {
		if unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
