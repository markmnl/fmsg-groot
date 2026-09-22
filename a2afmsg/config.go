// Package a2afmsg implements the FMSG-004 v0.2.0 transport binding for the
// official A2A Go client.
package a2afmsg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

const (
	// ProtocolBinding is the provisional FMSG-004 binding identifier.
	ProtocolBinding a2a.TransportProtocol = "https://github.com/markmnl/fmsg/blob/main/standards/fmsg-004-a2a-binding.md"
	// MediaType identifies FMSG-004 envelope data.
	MediaType = "application/vnd.fmsg.a2a+json"

	bindingVersion = "0.2"
	a2aVersion     = "1.0"
)

// Credential is an optional short-lived application credential carried in an
// FMSG-004 request envelope. Long-lived secrets should not be sent because
// fmsg messages are immutable and may be retained.
type Credential struct {
	Value  string   `json:"value"`
	Scopes []string `json:"scopes,omitempty"`
}

// Config configures an FMSG A2A client transport.
type Config struct {
	WebAPIURL string
	APIKey    string

	HTTPClient         *http.Client
	PollInterval       time.Duration
	RequestTimeout     time.Duration
	MaxJSONBytes       int64
	MaxAttachmentBytes int64
	MaxBufferedEvents  int

	Credentials map[string]Credential
}

func (c Config) withDefaults() Config {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 3 * time.Minute
	}
	if c.MaxJSONBytes <= 0 {
		c.MaxJSONBytes = 4 << 20
	}
	if c.MaxAttachmentBytes <= 0 {
		c.MaxAttachmentBytes = 32 << 20
	}
	if c.MaxBufferedEvents <= 0 {
		c.MaxBufferedEvents = 256
	}
	return c
}

func (c Config) validate() error {
	if strings.TrimSpace(c.WebAPIURL) == "" {
		return errors.New("fmsg Web API URL is required")
	}
	if strings.TrimSpace(c.APIKey) == "" {
		return errors.New("fmsg API key is required")
	}
	return nil
}

// WithTransport registers FMSG-004 with the official A2A client factory.
func WithTransport(config Config) a2aclient.FactoryOption {
	return a2aclient.WithTransport(ProtocolBinding, a2aclient.TransportFactoryFn(
		func(ctx context.Context, _ *a2a.AgentCard, iface *a2a.AgentInterface) (a2aclient.Transport, error) {
			return NewTransport(ctx, iface.URL, config)
		},
	))
}

// ParseEndpoint validates an FMSG-004 interface URL and returns its address.
func ParseEndpoint(endpoint string) (string, error) {
	if separator := strings.IndexByte(endpoint, ':'); separator < 0 || endpoint[:separator] != "fmsg" {
		return "", fmt.Errorf("FMSG-004 endpoint scheme must be lowercase fmsg")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse fmsg endpoint: %w", err)
	}
	if u.Scheme != "fmsg" || u.Opaque == "" || u.Host != "" || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.HasPrefix(u.Opaque, "//") {
		return "", fmt.Errorf("invalid FMSG-004 endpoint %q", endpoint)
	}
	if err := validatePercentEncoding(u.Opaque); err != nil {
		return "", err
	}
	for _, value := range []byte(u.Opaque) {
		if value > 127 {
			return "", errors.New("non-ASCII endpoint octets must be percent-encoded")
		}
	}
	address, err := url.PathUnescape(u.Opaque)
	if err != nil {
		return "", fmt.Errorf("decode fmsg endpoint: %w", err)
	}
	if !validAddress(address) {
		return "", fmt.Errorf("invalid fmsg address in endpoint %q", endpoint)
	}
	return address, nil
}

func validatePercentEncoding(value string) error {
	for i := 0; i < len(value); i++ {
		if value[i] != '%' {
			continue
		}
		if i+2 >= len(value) || !upperHex(value[i+1]) || !upperHex(value[i+2]) {
			return errors.New("fmsg endpoint percent-encoding must use uppercase hexadecimal")
		}
		i += 2
	}
	return nil
}

func upperHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'A' && value <= 'F'
}

func validAddress(address string) bool {
	if !utf8.ValidString(address) || len(address) < 4 || len(address) >= 256 || address[0] != '@' {
		return false
	}
	separator := strings.LastIndexByte(address, '@')
	if separator <= 1 || separator == len(address)-1 {
		return false
	}
	return validUser(address[1:separator]) && validDomain(address[separator+1:])
}

func validUser(user string) bool {
	if len(user) == 0 || len(user) > 64 {
		return false
	}
	runes := []rune(user)
	special := func(value rune) bool { return value == '-' || value == '_' || value == '.' }
	if special(runes[0]) || special(runes[len(runes)-1]) {
		return false
	}
	previousSpecial := false
	for _, value := range runes {
		if unicode.IsLetter(value) || unicode.IsNumber(value) {
			previousSpecial = false
			continue
		}
		if !special(value) || previousSpecial {
			return false
		}
		previousSpecial = true
	}
	return true
}

func validDomain(domain string) bool {
	if len(domain) == 0 || len(domain) > 253 {
		return false
	}
	if domain == "localhost" {
		return true
	}
	labels := strings.Split(domain, ".")
	if len(labels) < 2 {
		return false
	}
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, value := range label {
			if !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
				value >= '0' && value <= '9' || value == '-') {
				return false
			}
		}
	}
	return true
}
