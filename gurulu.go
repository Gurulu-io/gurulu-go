// Package gurulu is the official Go SDK for the Gurulu behavioral
// analytics ingest API. It is minimal but production-grade: a single
// Client value, non-throwing Track/Identify calls, configurable transport,
// and zero third-party dependencies (stdlib only).
//
// Basic usage:
//
//	c := gurulu.New("pk_live_xxx")
//	dec, err := c.Track("button_clicked", map[string]any{"plan": "pro"})
//	if err != nil {
//	    // network/transport problem — the SDK never panics, it returns the error
//	}
//	fmt.Println(dec.Decision) // "accept" | "warn" | "quarantine" | "reject" | "duplicate"
package gurulu

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultEndpoint is the base URL used when no WithEndpoint option is given.
const DefaultEndpoint = "https://ingest.gurulu.io"

// producer is the constant producer tag the contract expects from SDK clients.
const producer = "sdk"

// EventType enumerates the three semantic event kinds in the Gurulu contract.
// Track defaults to TypeInteraction; use WithEventType to override per call.
type EventType string

const (
	TypeInteraction EventType = "interaction"
	TypeIntent      EventType = "intent"
	TypeOutcome     EventType = "outcome"
)

func (t EventType) valid() bool {
	switch t {
	case TypeInteraction, TypeIntent, TypeOutcome:
		return true
	default:
		return false
	}
}

// Decision is the server's ingest verdict for a single event.
type Decision struct {
	EventID  string   `json:"event_id"`
	Decision string   `json:"decision"` // accept | warn | quarantine | reject | duplicate
	Reasons  []string `json:"reasons,omitempty"`
}

// eventPayload is the exact wire shape POSTed to /v1/ingest/event.
type eventPayload struct {
	EventKey    string         `json:"event_key"`
	EventType   EventType      `json:"event_type"`
	OccurredAt  string         `json:"occurred_at"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	PersonID    string         `json:"person_id,omitempty"`
	Properties  map[string]any `json:"properties,omitempty"`
	Producer    string         `json:"producer"`
}

// identifyPayload is the wire shape POSTed to /v1/ingest/identify.
type identifyPayload struct {
	AnonymousID    string         `json:"anonymous_id"`
	ExternalUserID string         `json:"external_user_id,omitempty"`
	Traits         map[string]any `json:"traits,omitempty"`
}

// Client is a Gurulu ingest client. It is safe for concurrent use by
// multiple goroutines. Construct one with New.
type Client struct {
	writeKey    string
	endpoint    string
	anonymousID string
	httpClient  *http.Client
	now         func() time.Time
}

// Option configures a Client. Pass any number to New.
type Option func(*Client)

// WithEndpoint overrides the ingest base URL (trailing slash is trimmed).
func WithEndpoint(endpoint string) Option {
	return func(c *Client) {
		if endpoint != "" {
			c.endpoint = strings.TrimRight(endpoint, "/")
		}
	}
}

// WithAnonymousID sets the default anonymous_id attached to every event that
// does not specify its own. If never set and none is provided per-call, a
// random one is generated lazily on first use.
func WithAnonymousID(id string) Option {
	return func(c *Client) {
		if id != "" {
			c.anonymousID = id
		}
	}
}

// WithHTTPClient injects a custom *http.Client (timeouts, transport, proxies).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.httpClient = h
		}
	}
}

// WithTimeout sets the request timeout on the default HTTP client.
// Ignored if WithHTTPClient is also supplied.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.httpClient.Timeout = d
		}
	}
}

// New constructs a Client. writeKey should be a Gurulu publishable write key
// (pk_...). Options may override the endpoint, anonymous id, and transport.
func New(writeKey string, opts ...Option) *Client {
	c := &Client{
		writeKey:   writeKey,
		endpoint:   DefaultEndpoint,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.anonymousID == "" {
		c.anonymousID = newAnonymousID()
	}
	return c
}

// AnonymousID returns the default anonymous id this client attaches to events.
func (c *Client) AnonymousID() string { return c.anonymousID }

// TrackOption customizes a single Track call.
type TrackOption func(*eventPayload)

// WithEventType overrides the default "interaction" event_type for one call.
func WithEventType(t EventType) TrackOption {
	return func(p *eventPayload) { p.EventType = t }
}

// WithPersonID attaches a resolved person_id to a single event.
func WithPersonID(id string) TrackOption {
	return func(p *eventPayload) { p.PersonID = id }
}

// WithEventAnonymousID overrides the client default anonymous_id for one event.
func WithEventAnonymousID(id string) TrackOption {
	return func(p *eventPayload) { p.AnonymousID = id }
}

// WithOccurredAt overrides the event timestamp (defaults to time.Now()).
func WithOccurredAt(t time.Time) TrackOption {
	return func(p *eventPayload) { p.OccurredAt = t.UTC().Format(time.RFC3339Nano) }
}

// Track builds a contract-compliant event payload and POSTs it to
// {endpoint}/v1/ingest/event. It is non-throwing: it never panics and any
// transport or encoding failure is returned as the error while Decision is
// left zero. A successful HTTP round-trip returns the server Decision.
//
// eventKey must be a non-empty snake_case key (K17 convention). props may be
// nil. Use TrackOption to set event_type, person_id, etc.
func (c *Client) Track(eventKey string, props map[string]any, opts ...TrackOption) (Decision, error) {
	return c.TrackContext(context.Background(), eventKey, props, opts...)
}

// TrackContext is Track with an explicit context for cancellation/deadlines.
func (c *Client) TrackContext(ctx context.Context, eventKey string, props map[string]any, opts ...TrackOption) (Decision, error) {
	var dec Decision

	if c.writeKey == "" {
		return dec, errors.New("gurulu: write key is empty")
	}
	if eventKey == "" {
		return dec, errors.New("gurulu: event_key is empty")
	}

	p := eventPayload{
		EventKey:    eventKey,
		EventType:   TypeInteraction,
		OccurredAt:  c.now().UTC().Format(time.RFC3339Nano),
		AnonymousID: c.anonymousID,
		Properties:  props,
		Producer:    producer,
	}
	for _, opt := range opts {
		opt(&p)
	}
	if !p.EventType.valid() {
		return dec, fmt.Errorf("gurulu: invalid event_type %q", p.EventType)
	}

	return c.post(ctx, "/v1/ingest/event", p)
}

// IdentifyOption customizes a single Identify call.
type IdentifyOption func(*identifyPayload)

// WithExternalUserID sets external_user_id on the identify payload.
func WithExternalUserID(id string) IdentifyOption {
	return func(p *identifyPayload) { p.ExternalUserID = id }
}

// WithTraits sets the traits object on the identify payload.
func WithTraits(traits map[string]any) IdentifyOption {
	return func(p *identifyPayload) { p.Traits = traits }
}

// WithIdentifyAnonymousID overrides the client default anonymous_id.
func WithIdentifyAnonymousID(id string) IdentifyOption {
	return func(p *identifyPayload) { p.AnonymousID = id }
}

// Identify associates the client's anonymous id with an external user and/or
// traits via {endpoint}/v1/ingest/identify. Like Track, it is non-throwing.
func (c *Client) Identify(opts ...IdentifyOption) (Decision, error) {
	return c.IdentifyContext(context.Background(), opts...)
}

// IdentifyContext is Identify with an explicit context.
func (c *Client) IdentifyContext(ctx context.Context, opts ...IdentifyOption) (Decision, error) {
	var dec Decision

	if c.writeKey == "" {
		return dec, errors.New("gurulu: write key is empty")
	}

	p := identifyPayload{AnonymousID: c.anonymousID}
	for _, opt := range opts {
		opt(&p)
	}
	if p.AnonymousID == "" {
		return dec, errors.New("gurulu: anonymous_id is empty")
	}

	return c.post(ctx, "/v1/ingest/identify", p)
}

// post marshals body, performs the authenticated POST, and decodes the
// Decision response. All failure modes return an error rather than panicking.
func (c *Client) post(ctx context.Context, path string, body any) (Decision, error) {
	var dec Decision

	buf, err := json.Marshal(body)
	if err != nil {
		return dec, fmt.Errorf("gurulu: encode payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(buf))
	if err != nil {
		return dec, fmt.Errorf("gurulu: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.writeKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Network failure must not crash the host application.
		return dec, fmt.Errorf("gurulu: request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return dec, fmt.Errorf("gurulu: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dec, fmt.Errorf("gurulu: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		// Accepted with empty body — treat as accept, no error.
		dec.Decision = "accept"
		return dec, nil
	}
	if err := json.Unmarshal(raw, &dec); err != nil {
		return dec, fmt.Errorf("gurulu: decode response: %w", err)
	}
	return dec, nil
}

// newAnonymousID returns a random "anon_" prefixed hex id. Falls back to a
// timestamp-derived id if the crypto source is unavailable (never panics).
func newAnonymousID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("anon_%d", time.Now().UnixNano())
	}
	return "anon_" + hex.EncodeToString(b)
}
