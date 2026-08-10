package main

import (
	"strings"
	"testing"
)

func TestChooseReply(t *testing.T) {
	t.Run("groot too skips", func(t *testing.T) {
		text, skip := ChooseReply("I am Groot too!")
		if !skip || text != "" {
			t.Fatalf("got text=%q skip=%v", text, skip)
		}
		text, skip = ChooseReply("hey GROOT TOO! what")
		if !skip {
			t.Fatalf("expected skip for mixed case, got %q", text)
		}
	})

	t.Run("contains groot replies too", func(t *testing.T) {
		text, skip := ChooseReply("I am Groot.")
		if skip || text != GrootToo {
			t.Fatalf("got text=%q skip=%v want %q", text, skip, GrootToo)
		}
		text, skip = ChooseReply("who is groot anyway")
		if skip || text != GrootToo {
			t.Fatalf("got text=%q skip=%v", text, skip)
		}
	})

	t.Run("plain message random variant", func(t *testing.T) {
		text, skip := ChooseReply("Hello there")
		if skip {
			t.Fatal("unexpected skip")
		}
		if text == GrootToo {
			t.Fatal("plain message should not get GrootToo")
		}
		if !strings.Contains(strings.ToLower(text), "groot") {
			t.Fatalf("expected groot line, got %q", text)
		}
		// Must not contain the anti-loop phrase as the random table shouldn't
		if strings.Contains(strings.ToLower(text), "groot too!") {
			t.Fatalf("random line must not be groot too: %q", text)
		}
	})
}

func TestRecipientsFromMessage(t *testing.T) {
	self := "@groot@example.com"
	msg := messageMeta{
		From: "@alice@example.com",
		To:   []string{"@groot@example.com", "@bob@example.com", "@alice@example.com"},
		AddTo: []addToBatch{
			{AddToFrom: "@carol@example.com", To: []string{"@dave@example.com", "@groot@example.com"}},
		},
	}
	got := RecipientsFromMessage(msg, self)
	want := []string{
		"@alice@example.com",
		"@bob@example.com",
		"@carol@example.com",
		"@dave@example.com",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if normalizeAddr(got[i]) != normalizeAddr(want[i]) {
			t.Fatalf("index %d: got %v want %v", i, got, want)
		}
	}
}

func TestRecipientsExcludeSelfCaseInsensitive(t *testing.T) {
	got := RecipientsFromMessage(messageMeta{
		From: "@GROOT@Example.com",
		To:   []string{"@Bob@example.com"},
	}, "@groot@example.com")
	if len(got) != 1 || normalizeAddr(got[0]) != "@bob@example.com" {
		t.Fatalf("got %v", got)
	}
}

func TestRecipientsEmptyWhenOnlySelf(t *testing.T) {
	got := RecipientsFromMessage(messageMeta{
		From: "@groot@example.com",
		To:   []string{"@groot@example.com"},
	}, "@groot@example.com")
	if len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}
