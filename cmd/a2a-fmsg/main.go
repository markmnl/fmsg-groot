// Command a2a-fmsg sends one A2A SendMessage request through FMSG-004.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"

	"github.com/markmnl/fmsg-groot/a2afmsg"
)

func main() {
	cardURL := flag.String("card", "https://fmsg.io", "agent-card base URL or complete URL")
	apiURL := flag.String("api-url", envOr("FMSG_API_URL", ""), "caller's fmsg Web API URL")
	apiKey := flag.String("api-key", envOr("FMSG_API_KEY", ""), "caller's fmsg API key")
	timeout := flag.Duration("timeout", 3*time.Minute, "A2A response timeout")
	flag.Parse()
	if *apiURL == "" || *apiKey == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: a2a-fmsg [flags] <message>")
		flag.PrintDefaults()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	card, err := agentcard.DefaultResolver.Resolve(ctx, *cardURL)
	if err != nil {
		log.Fatalf("resolve Agent Card: %v", err)
	}
	client, err := a2aclient.NewFromCard(ctx, card,
		a2afmsg.WithTransport(a2afmsg.Config{
			WebAPIURL:      *apiURL,
			APIKey:         *apiKey,
			RequestTimeout: *timeout,
		}),
		a2aclient.WithConfig(a2aclient.Config{
			AcceptedOutputModes: []string{"text/plain"},
		}),
	)
	if err != nil {
		log.Fatalf("create A2A client: %v", err)
	}
	defer client.Destroy()

	request := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(strings.Join(flag.Args(), " "))),
	}
	result, err := client.SendMessage(ctx, request)
	if err != nil {
		log.Fatalf("SendMessage: %v", err)
	}
	data, err := json.MarshalIndent(a2a.StreamResponse{Event: result}, "", "  ")
	if err != nil {
		log.Fatalf("encode A2A result: %v", err)
	}
	fmt.Println(string(data))
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
