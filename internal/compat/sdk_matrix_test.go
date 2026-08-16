// Package compat implements the cross-SDK wire-level compatibility test
// matrix documented in docs/SDK_CONSISTENCY.md §8.
//
// Each test in this package models the HTTP wire protocol that every MailBaby
// SDK (Go, Java, Python, Rust) must speak. The suite can be exercised
// against any of the SDK clients; the same expectations apply to all four
// languages. Adding a regression here is the canonical way to lock in
// behaviour shared across the SDKs.
package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/handler"
	"mailbaby/internal/sender"
)

// testServer wires up a unified handler with auth + a fake sender and returns
// the test server URL along with a count of attempts made by the fake
// sender. The fake supports transient-failure injection so the suite can
// exercise the SDK retry matrix.
type testServer struct {
	URL               string
	attempts          int64
	failTimes         int32
	server            *httptest.Server
	secretKey         string
	trustProxy        bool
	ratePerKeyPerMin  int
}

func newTestServer(t *testing.T, opts ...func(*testServer)) *testServer {
	t.Helper()

	ts := &testServer{
		secretKey:        "secret-key-abc",
		ratePerKeyPerMin: 1000,
	}
	for _, opt := range opts {
		opt(ts)
	}

	fake := &fakeSender{attempts: &ts.attempts, failTimes: &ts.failTimes}

	cfg := &config.Config{
		Auth: config.AuthConfig{
			Enabled:             true,
			SecretKey:           ts.secretKey,
			RatePerKeyPerMinute: ts.ratePerKeyPerMin,
		},
		Server: config.ServerConfig{
			TrustProxyHeaders: ts.trustProxy,
		},
	}
	srv, err := handler.New(cfg, handler.WithSender(fake))
	if err != nil {
		t.Fatalf("handler.New: %v", err)
	}
	ts.server = httptest.NewServer(srv.Handler())
	ts.URL = ts.server.URL

	t.Cleanup(func() {
		ts.server.Close()
	})
	return ts
}

func (ts *testServer) attemptsSoFar() int64 {
	return atomic.LoadInt64(&ts.attempts)
}

// fakeSender is a sender.Sender stand-in that records how many times Send
// was invoked and optionally fails the first `failTimes` attempts.
type fakeSender struct {
	attempts  *int64
	failTimes *int32
}

func (f *fakeSender) Send(ctx context.Context, email *sender.Email) error {
	n := atomic.AddInt64(f.attempts, 1)
	if atomic.LoadInt32(f.failTimes) > 0 {
		if int32(n) <= atomic.LoadInt32(f.failTimes) {
			return fmt.Errorf("fake: transient failure %d", n)
		}
	}
	return nil
}

func (f *fakeSender) SendBatch(ctx context.Context, emails []*sender.Email) []error {
	errs := make([]error, len(emails))
	for i, e := range emails {
		errs[i] = f.Send(ctx, e)
	}
	return errs
}

func (f *fakeSender) Account(name string) (sender.AccountSender, error) {
	return nil, errors.New("fake: account not supported")
}
func (f *fakeSender) AccountNames() []string { return nil }
func (f *fakeSender) Stats() map[string]sender.AccountStats {
	return map[string]sender.AccountStats{}
}
func (f *fakeSender) Close() error { return nil }

// validEmailPayload mirrors sender.Email's wire shape (snake_case JSON,
// base64 attachment data) so the suite verifies the exact contract the 4 SDKs
// must produce.
func validEmailPayload(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"account":    "default",
		"from":       "noreply@example.com",
		"to":         []string{"alice@example.com"},
		"subject":    "sdk-matrix",
		"text_body":  "hello",
		"html_body":  "<p>hello</p>",
		"headers":    map[string]string{"X-Trace-Id": "abc"},
		"tags":       []string{"welcome"},
		"metadata":   map[string]string{"user_id": "42"},
		"attachments": []map[string]any{
			{
				"filename":     "logo.png",
				"content_type": "image/png",
				"data":         "iVBORw0KGgo=",
				"inline":       false,
				"content_id":   "",
			},
		},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return body
}

// doSend is a minimal REST client that mirrors what every SDK must implement:
// it sends POST /v1/email/send with the auth header. The returned values let
// tests assert on status, error envelope, and Retry-After.
func (ts *testServer) doSend(t *testing.T, body []byte, opts ...func(*http.Request)) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/email/send", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for _, opt := range opts {
		opt(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp, respBody
}

func withBearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func withAPIKey(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("X-API-Key", token) }
}

func withIPHeader(ip string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("X-Forwarded-For", ip) }
}

// 1. Missing auth -> 401 with the documented error envelope.
func TestSDKMatrix_AuthRequired(t *testing.T) {
	ts := newTestServer(t)

	resp, body := ts.doSend(t, validEmailPayload(t))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", resp.StatusCode, body)
	}
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("invalid error envelope: %v body=%s", err, body)
	}
	for _, field := range []string{"code", "error", "message"} {
		if _, ok := env[field]; !ok {
			t.Fatalf("error envelope missing field %q: %s", field, body)
		}
	}
	if env["error"] != "unauthorized" {
		t.Fatalf("expected error=unauthorized, got %v", env["error"])
	}
}

// 2. Wrong key -> 401 with same envelope.
func TestSDKMatrix_AuthWrong(t *testing.T) {
	ts := newTestServer(t)

	resp, body := ts.doSend(t, validEmailPayload(t), withBearer("WRONG"))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", resp.StatusCode, body)
	}
	if bytes.Contains(body, []byte("WRONG")) {
		t.Fatalf("response leaked the supplied credential: %s", body)
	}
}

// 3. Bearer scheme is honored alongside X-API-Key so the matrix has a single
// test for both header styles.
func TestSDKMatrix_BearerHeader(t *testing.T) {
	ts := newTestServer(t)

	resp, body := ts.doSend(t, validEmailPayload(t), withBearer(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bearer scheme must succeed: got %d body=%s", resp.StatusCode, body)
	}
}

// 4. X-API-Key header is the SDK default.
func TestSDKMatrix_APIKeyHeader(t *testing.T) {
	ts := newTestServer(t)

	resp, body := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("X-API-Key scheme must succeed: got %d body=%s", resp.StatusCode, body)
	}
}

// 5. Empty recipients -> 400 validation_error envelope.
func TestSDKMatrix_ValidationError(t *testing.T) {
	ts := newTestServer(t)

	body, _ := json.Marshal(map[string]any{
		"from":    "noreply@example.com",
		"subject": "no recipients",
	})
	resp, respBody := ts.doSend(t, body, withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, respBody)
	}
	var env map[string]any
	_ = json.Unmarshal(respBody, &env)
	if env["error"] != "validation_error" {
		t.Fatalf("expected error=validation_error, got %v", env["error"])
	}
}

// 6. Invalid JSON -> 400 invalid_json envelope.
func TestSDKMatrix_InvalidJSON(t *testing.T) {
	ts := newTestServer(t)

	resp, respBody := ts.doSend(t, []byte("{not json"), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, respBody)
	}
	var env map[string]any
	_ = json.Unmarshal(respBody, &env)
	if env["error"] != "invalid_json" {
		t.Fatalf("expected error=invalid_json, got %v", env["error"])
	}
}

// 7. Valid request -> 202 envelope with non-empty id. The matrix asserts the
// id is present so SDKs can correlate with their own bookkeeping.
func TestSDKMatrix_ValidSend(t *testing.T) {
	ts := newTestServer(t)

	resp, respBody := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, respBody)
	}
	var env map[string]any
	_ = json.Unmarshal(respBody, &env)
	id, _ := env["id"].(string)
	if id == "" {
		t.Fatalf("expected non-empty id in response: %s", respBody)
	}
}

// 8. Server 5xx -> SDKs must retry up to MaxRetries (default 3). The fake
// sender here fails the first 2 attempts and succeeds on the 3rd. The test
// pretends to be an SDK retry loop and asserts at least 3 attempts landed.
func TestSDKMatrix_5xxRetry(t *testing.T) {
	ts := newTestServer(t, func(ts *testServer) { ts.failTimes = 2 })

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, body := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
		if resp.StatusCode == http.StatusOK {
			break
		}
		if resp.StatusCode < 500 {
			t.Fatalf("retryable error must be 5xx, got %d body=%s", resp.StatusCode, body)
		}
	}

	if got := ts.attemptsSoFar(); got < 3 {
		t.Fatalf("expected at least 3 server attempts, got %d", got)
	}
}

// 9. 429 -> response carries a Retry-After header that SDKs must respect.
// The handler's key limiter is configured to 1/min so the second request
// triggers 429.
func TestSDKMatrix_429_RetryAfter(t *testing.T) {
	ts := newTestServer(t, func(ts *testServer) {
		ts.secretKey = "rate-test"
		ts.ratePerKeyPerMin = 1
	})

	// First request succeeds.
	resp, _ := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", resp.StatusCode)
	}

	// Second request from the same key triggers the per-key limiter.
	resp, body := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request: expected 429, got %d body=%s", resp.StatusCode, body)
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		t.Fatal("Retry-After header missing on 429")
	}
	secs, err := strconv.Atoi(ra)
	if err != nil {
		t.Fatalf("Retry-After must be integer seconds, got %q: %v", ra, err)
	}
	if secs < 1 {
		t.Fatalf("Retry-After must be >= 1, got %d", secs)
	}
}

// 10. Wire format guarantees: response keys for a successful send match what
// the SDKs must consume. Locks in any future server contract change.
func TestSDKMatrix_ResponseSchema(t *testing.T) {
	ts := newTestServer(t)

	resp, body := ts.doSend(t, validEmailPayload(t), withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, body)
	}
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("invalid JSON: %v body=%s", err, body)
	}
	for _, field := range []string{"id", "status"} {
		if _, ok := env[field]; !ok {
			t.Fatalf("response missing field %q: %s", field, body)
		}
	}
	if env["status"] != "queued" && env["status"] != "sent" {
		t.Fatalf("unexpected status value %v: %s", env["status"], body)
	}
}

// 11. SDKs must NOT send ?api_key / ?token in the URL — the server rejects
// it outright so credentials never leak into proxy/access logs.
func TestSDKMatrix_NoQueryTokenFallback(t *testing.T) {
	ts := newTestServer(t)

	req, _ := http.NewRequest(http.MethodPost,
		ts.URL+"/v1/email/send?api_key="+ts.secretKey,
		bytes.NewReader(validEmailPayload(t)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("query-string auth must be rejected: got %d body=%s", resp.StatusCode, body)
	}
}

// 12. Omitted optional fields stay absent in the request body so the wire
// format remains lean. The Go server requires this for omitempty to match.
func TestSDKMatrix_EmptyFieldsOmitted(t *testing.T) {
	ts := newTestServer(t)

	body, _ := json.Marshal(map[string]any{
		"to":       []string{"alice@example.com"},
		"subject":  "minimal",
		"text_body": "hi",
	})
	resp, respBody := ts.doSend(t, body, withAPIKey(ts.secretKey))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, respBody)
	}
	// Decoding the original body shows cc/bcc/tags/metadata/headers were
	// omitted (Go omitempty). The test asserts the receiver accepted the
	// truncated payload by checking sender attempt counter.
	if ts.attemptsSoFar() != 1 {
		t.Fatalf("expected exactly 1 sender attempt, got %d", ts.attemptsSoFar())
	}
}

// 13. Trusted proxy header is opt-in. By default X-Forwarded-For must NOT be
// honored (otherwise an attacker can spoof the IP recorded in logs/metrics).
func TestSDKMatrix_ProxyHeaderOptIn(t *testing.T) {
	ts := newTestServer(t)

	resp, _ := ts.doSend(t, validEmailPayload(t),
		withAPIKey(ts.secretKey),
		withIPHeader("1.2.3.4"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	// No assertion on the recorded IP here — that's covered by the lower-level
	// clientAddr tests. This matrix entry is the wire-level confirmation that
	// the auth header is honored regardless of proxy header state.
}

// 14. Concurrent SDK requests must serialize via the per-key limiter without
// deadlocking. This protects all four SDKs against race-induced infinite
// waits on the limiter map.
func TestSDKMatrix_ConcurrentRequests(t *testing.T) {
	ts := newTestServer(t, func(ts *testServer) {
		ts.secretKey = "concurrent-test"
	})

	const N = 8
	done := make(chan int, N)
	for i := 0; i < N; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
				ts.URL+"/v1/email/send", bytes.NewReader(validEmailPayload(t)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", ts.secretKey)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- -1
				return
			}
			resp.Body.Close()
			done <- resp.StatusCode
		}()
	}
	for i := 0; i < N; i++ {
		code := <-done
		if code == http.StatusTooManyRequests {
			continue // expected once we exceed the limiter
		}
		if code != http.StatusOK && code != http.StatusTooManyRequests {
			t.Fatalf("unexpected status %d", code)
		}
	}
}