package gurulu

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// captured holds what the mock server received so tests can assert on it.
type captured struct {
	mu     sync.Mutex
	path   string
	auth   string
	ctype  string
	method string
	body   map[string]any
}

func (c *captured) get() captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	return captured{path: c.path, auth: c.auth, ctype: c.ctype, method: c.method, body: c.body}
}

// newMock spins up an httptest.Server that records the request and replies
// with the given decision JSON. No real network is ever used.
func newMock(t *testing.T, cap *captured, status int, respJSON string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)

		cap.mu.Lock()
		cap.path = r.URL.Path
		cap.auth = r.Header.Get("Authorization")
		cap.ctype = r.Header.Get("Content-Type")
		cap.method = r.Method
		cap.body = body
		cap.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respJSON)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTrackPayloadAndHeaders(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusOK,
		`{"event_id":"evt_123","decision":"accept"}`)

	fixedNow := time.Date(2026, 6, 28, 10, 30, 0, 0, time.UTC)
	c := New("pk_test_abc",
		WithEndpoint(srv.URL),
		WithAnonymousID("anon_fixed"),
	)
	c.now = func() time.Time { return fixedNow }

	dec, err := c.Track("button_clicked", map[string]any{"plan": "pro", "count": 3})
	if err != nil {
		t.Fatalf("Track returned error: %v", err)
	}
	if dec.Decision != "accept" || dec.EventID != "evt_123" {
		t.Fatalf("unexpected decision: %+v", dec)
	}

	got := cap.get()
	if got.method != http.MethodPost {
		t.Errorf("method = %q, want POST", got.method)
	}
	if got.path != "/v1/ingest/event" {
		t.Errorf("path = %q, want /v1/ingest/event", got.path)
	}
	if got.auth != "Bearer pk_test_abc" {
		t.Errorf("auth = %q, want Bearer pk_test_abc", got.auth)
	}
	if got.ctype != "application/json" {
		t.Errorf("content-type = %q", got.ctype)
	}

	// Contract-required body fields.
	if got.body["event_key"] != "button_clicked" {
		t.Errorf("event_key = %v", got.body["event_key"])
	}
	if got.body["event_type"] != "interaction" {
		t.Errorf("event_type = %v, want interaction", got.body["event_type"])
	}
	if got.body["producer"] != "sdk" {
		t.Errorf("producer = %v, want sdk", got.body["producer"])
	}
	if got.body["anonymous_id"] != "anon_fixed" {
		t.Errorf("anonymous_id = %v", got.body["anonymous_id"])
	}
	if got.body["occurred_at"] != "2026-06-28T10:30:00Z" {
		t.Errorf("occurred_at = %v", got.body["occurred_at"])
	}
	props, ok := got.body["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties not an object: %v", got.body["properties"])
	}
	if props["plan"] != "pro" {
		t.Errorf("properties.plan = %v", props["plan"])
	}
	if props["count"] != float64(3) {
		t.Errorf("properties.count = %v", props["count"])
	}
}

func TestTrackOptionsOverride(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusOK, `{"event_id":"e","decision":"warn"}`)

	c := New("pk_x", WithEndpoint(srv.URL), WithAnonymousID("anon_default"))
	dec, err := c.Track("purchase_completed", nil,
		WithEventType(TypeOutcome),
		WithPersonID("person_42"),
		WithEventAnonymousID("anon_override"),
	)
	if err != nil {
		t.Fatalf("Track error: %v", err)
	}
	if dec.Decision != "warn" {
		t.Errorf("decision = %q", dec.Decision)
	}

	got := cap.get()
	if got.body["event_type"] != "outcome" {
		t.Errorf("event_type = %v, want outcome", got.body["event_type"])
	}
	if got.body["person_id"] != "person_42" {
		t.Errorf("person_id = %v", got.body["person_id"])
	}
	if got.body["anonymous_id"] != "anon_override" {
		t.Errorf("anonymous_id = %v, want override", got.body["anonymous_id"])
	}
	// nil props must be omitted from the wire payload.
	if _, present := got.body["properties"]; present {
		t.Errorf("properties should be omitted when nil")
	}
}

func TestIdentifyPayload(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusOK, `{"event_id":"id_1","decision":"accept"}`)

	c := New("pk_id", WithEndpoint(srv.URL), WithAnonymousID("anon_77"))
	dec, err := c.Identify(
		WithExternalUserID("user_external_9"),
		WithTraits(map[string]any{"email": "a@b.com", "vip": true}),
	)
	if err != nil {
		t.Fatalf("Identify error: %v", err)
	}
	if dec.Decision != "accept" {
		t.Errorf("decision = %q", dec.Decision)
	}

	got := cap.get()
	if got.path != "/v1/ingest/identify" {
		t.Errorf("path = %q", got.path)
	}
	if got.auth != "Bearer pk_id" {
		t.Errorf("auth = %q", got.auth)
	}
	if got.body["anonymous_id"] != "anon_77" {
		t.Errorf("anonymous_id = %v", got.body["anonymous_id"])
	}
	if got.body["external_user_id"] != "user_external_9" {
		t.Errorf("external_user_id = %v", got.body["external_user_id"])
	}
	traits, ok := got.body["traits"].(map[string]any)
	if !ok {
		t.Fatalf("traits not object: %v", got.body["traits"])
	}
	if traits["email"] != "a@b.com" || traits["vip"] != true {
		t.Errorf("traits = %v", traits)
	}
}

// TestNetworkErrorDoesNotPanic verifies a dead endpoint returns an error
// instead of crashing the host process.
func TestNetworkErrorDoesNotPanic(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusOK, `{"decision":"accept"}`)
	url := srv.URL
	srv.Close() // kill the server so the connection is refused

	c := New("pk_x", WithEndpoint(url), WithTimeout(500*time.Millisecond))
	dec, err := c.Track("x_happened", nil)
	if err == nil {
		t.Fatal("expected a network error, got nil")
	}
	if dec.Decision != "" {
		t.Errorf("decision should be zero on error, got %q", dec.Decision)
	}
}

func TestServerErrorStatus(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusUnauthorized, `{"error":"bad key"}`)

	c := New("pk_bad", WithEndpoint(srv.URL))
	_, err := c.Track("x_happened", nil)
	if err == nil {
		t.Fatal("expected error on 401 status")
	}
}

func TestValidationErrors(t *testing.T) {
	c := New("pk_x", WithEndpoint("http://127.0.0.1:0"))

	if _, err := c.Track("", nil); err == nil {
		t.Error("empty event_key should error")
	}
	if _, err := (New("")).Track("e", nil); err == nil {
		t.Error("empty write key should error")
	}
	if _, err := c.Track("e", nil, WithEventType(EventType("garbage"))); err == nil {
		t.Error("invalid event_type should error")
	}
}

func TestDefaultsAndConfig(t *testing.T) {
	c := New("pk_x")
	if c.endpoint != DefaultEndpoint {
		t.Errorf("default endpoint = %q, want %q", c.endpoint, DefaultEndpoint)
	}
	if c.AnonymousID() == "" {
		t.Error("anonymous id should auto-generate")
	}

	// Trailing slash on endpoint is trimmed.
	c2 := New("pk_x", WithEndpoint("https://example.com/"))
	if c2.endpoint != "https://example.com" {
		t.Errorf("endpoint trailing slash not trimmed: %q", c2.endpoint)
	}
}

func TestEmptyBodyTreatedAsAccept(t *testing.T) {
	cap := &captured{}
	srv := newMock(t, cap, http.StatusAccepted, ``)

	c := New("pk_x", WithEndpoint(srv.URL))
	dec, err := c.Track("e_happened", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.Decision != "accept" {
		t.Errorf("empty body decision = %q, want accept", dec.Decision)
	}
}
