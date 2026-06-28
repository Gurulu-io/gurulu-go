# @gurulu/go — Gurulu Go SDK

Minimal, dependency-free Go client for the Gurulu behavioral analytics ingest
API. Stdlib only (`net/http`). Non-throwing: network failures return an error,
they never panic your application.

## Install

```bash
go get github.com/gurulu-io/gurulu-go
```

```go
import gurulu "github.com/gurulu-io/gurulu-go"
```

## Quick start

```go
c := gurulu.New("pk_live_xxx") // your publishable write key

// Track an event. event_key is snake_case (K17 convention).
dec, err := c.Track("button_clicked", map[string]any{
    "plan":  "pro",
    "count": 3,
})
if err != nil {
    // transport/encoding problem — already non-fatal, just log it
    log.Printf("gurulu: %v", err)
}
fmt.Println(dec.Decision) // accept | warn | quarantine | reject | duplicate

// Identify the current anonymous visitor.
c.Identify(
    gurulu.WithExternalUserID("user_42"),
    gurulu.WithTraits(map[string]any{"email": "a@b.com"}),
)
```

## Configuration

`New(writeKey, ...Option)`:

| Option | Purpose | Default |
| --- | --- | --- |
| `WithEndpoint(url)` | Override ingest base URL | `https://ingest.gurulu.io` |
| `WithAnonymousID(id)` | Default `anonymous_id` for every event | random `anon_<hex>` |
| `WithHTTPClient(h)` | Inject a custom `*http.Client` | 10s timeout client |
| `WithTimeout(d)` | Set timeout on the default client | `10s` |

## Track

```go
c.Track(eventKey, props,
    gurulu.WithEventType(gurulu.TypeOutcome), // interaction (default) | intent | outcome
    gurulu.WithPersonID("person_42"),
    gurulu.WithEventAnonymousID("anon_override"),
    gurulu.WithOccurredAt(time.Now()),
)
```

Wire payload `POST {endpoint}/v1/ingest/event`:

```json
{
  "event_key": "button_clicked",
  "event_type": "interaction",
  "occurred_at": "2026-06-28T10:30:00Z",
  "anonymous_id": "anon_...",
  "properties": { "plan": "pro" },
  "producer": "sdk"
}
```

Headers: `Authorization: Bearer pk_...`, `Content-Type: application/json`.

## Identify

```go
c.Identify(
    gurulu.WithExternalUserID("user_42"),
    gurulu.WithTraits(map[string]any{"vip": true}),
    gurulu.WithIdentifyAnonymousID("anon_x"), // optional override
)
```

`POST {endpoint}/v1/ingest/identify` with body
`{ anonymous_id, external_user_id?, traits? }`.

## Response

Both methods return a `Decision`:

```go
type Decision struct {
    EventID  string   `json:"event_id"`
    Decision string   `json:"decision"`
    Reasons  []string `json:"reasons,omitempty"`
}
```

## Context

`TrackContext(ctx, ...)` and `IdentifyContext(ctx, ...)` accept a
`context.Context` for cancellation and deadlines.

## Error handling

The SDK is non-throwing. Any of: empty write key, empty `event_key`, invalid
`event_type`, JSON encode failure, network failure, or non-2xx status returns a
descriptive error with a zero-value `Decision`. It never panics, so it is safe
on a hot request path.

## Test

```bash
cd sdks/go
go test ./...
go build ./...
```

Tests use `httptest.Server` mocks and never touch the real network.
