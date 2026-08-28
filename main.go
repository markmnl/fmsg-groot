// Command fmsg-groot listens on an fmsg Web API inbox and replies to every
// message with a playful "I am Groot" line (Guardians of the Galaxy style).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	apiURL := flag.String("api-url", envOr("FMSG_API_URL", ""), "fmsg Web API base URL (or FMSG_API_URL)")
	apiKey := flag.String("api-key", envOr("FMSG_API_KEY", ""), "fmsg API key (or FMSG_API_KEY)")
	statePath := flag.String("state", envOr("FMSG_GROOT_STATE", "./fmsg-groot.state"), "path to last-seen message ID file")
	a2aStatePath := flag.String("a2a-state", envOr("FMSG_GROOT_A2A_STATE", ""), "path to A2A replay state (defaults to <state>.a2a)")
	flag.Parse()

	if *apiURL == "" || *apiKey == "" {
		fmt.Fprintln(os.Stderr, "FMSG_API_URL and FMSG_API_KEY are required (flags -api-url / -api-key also accepted)")
		flag.Usage()
		os.Exit(2)
	}

	client, err := NewClient(*apiURL, *apiKey)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr, err := client.EnsureToken(ctx)
	if err != nil {
		log.Fatalf("authenticate: %v", err)
	}
	log.Printf("I am Groot. Listening as %s against %s", addr, *apiURL)
	if *a2aStatePath == "" {
		*a2aStatePath = *statePath + ".a2a"
	}
	a2aState, err := LoadA2AState(*a2aStatePath)
	if err != nil {
		log.Fatalf("load A2A state: %v", err)
	}
	a2aServer := NewA2AServer(a2aState)
	go a2aServer.MonitorPending(ctx, client, addr)

	lastID, err := LoadLastID(*statePath)
	if err != nil {
		log.Fatalf("load state: %v", err)
	}
	if lastID == 0 {
		// First run: do not reply to historical inbox; start from current max.
		maxID, err := client.MaxInboxID(ctx)
		if err != nil {
			log.Fatalf("probe inbox: %v", err)
		}
		if maxID > 0 {
			lastID = maxID
			if err := SaveLastID(*statePath, lastID); err != nil {
				log.Fatalf("save state: %v", err)
			}
			log.Printf("first run: seeding cursor at message id %d (skipping history)", lastID)
		}
	} else {
		log.Printf("resuming after message id %d", lastID)
	}

	// Persist after each successful handler (Watch advances lastID only then).
	// Intentional skips also return nil so the cursor advances.
	last, err := client.Watch(ctx, lastID, func(ctx context.Context, msg Message) error {
		if err := handleMessage(ctx, client, a2aServer, addr, msg); err != nil {
			return err
		}
		if err := SaveLastID(*statePath, msg.ID); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		log.Fatalf("watch: %v", err)
	}
	if last > lastID {
		_ = SaveLastID(*statePath, last)
	}
	log.Printf("I am Groot. Shutting down (last id %d).", last)
}

func handleMessage(ctx context.Context, client *Client, a2aServer *A2AServer, self string, msg Message) error {
	if normalizeAddr(msg.From) == normalizeAddr(self) {
		log.Printf("skip id=%d from self", msg.ID)
		return nil
	}
	if isA2AMediaType(msg.Type) {
		if err := a2aServer.Handle(ctx, client, self, msg); err != nil {
			return fmt.Errorf("handle A2A request %d: %w", msg.ID, err)
		}
		log.Printf("handled A2A request id=%d from=%s", msg.ID, msg.From)
		return nil
	}
	if msg.NoReply {
		log.Printf("skip id=%d no_reply", msg.ID)
		return nil
	}

	bodyBytes, err := client.Data(ctx, msg.ID)
	if err != nil {
		return fmt.Errorf("fetch data for %d: %w", msg.ID, err)
	}
	body := string(bodyBytes)

	text, skip := ChooseReply(body)
	if skip {
		log.Printf("skip id=%d body has %q", msg.ID, grootTooMarker)
		return nil
	}

	recipients := RecipientsFromMessage(messageMeta{
		From: msg.From,
		To:   msg.To,
		AddTo: func() []addToBatch {
			out := make([]addToBatch, 0, len(msg.AddTo))
			for _, b := range msg.AddTo {
				out = append(out, addToBatch{AddToFrom: b.AddToFrom, To: b.To})
			}
			return out
		}(),
	}, self)
	if len(recipients) == 0 {
		log.Printf("skip id=%d no recipients after excluding self", msg.ID)
		return nil
	}

	sentID, err := client.CreateAndSend(ctx, self, recipients, msg.ID, text)
	if err != nil {
		return fmt.Errorf("reply to %d: %w", msg.ID, err)
	}
	log.Printf("replied to id=%d with draft id=%d to %v: %q", msg.ID, sentID, recipients, text)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
