package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWebhookSender_NilSafe(t *testing.T) {
	var w *WebhookSender
	if err := w.Send(context.Background(), WebhookEvent{}, nil); err != nil {
		t.Fatalf("nil sender must be no-op, got %v", err)
	}
	if w := NewWebhookSender(WebhookConfig{Enabled: false}); w != nil {
		t.Fatalf("disabled sender must be nil")
	}
	if w := NewWebhookSender(WebhookConfig{Enabled: true}); w != nil {
		t.Fatalf("sender without URL must be nil")
	}
}

func TestWebhookSender_Signature(t *testing.T) {
	var receivedBody []byte
	var receivedSig string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		receivedSig = r.Header.Get("X-MailBaby-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := WebhookConfig{
		Enabled:    true,
		URL:        srv.URL,
		Secret:     "supersecret",
		Timeout:    time.Second,
		MaxRetries: 1,
		RetryAfter: 10 * time.Millisecond,
	}
	cfg.ApplyDefaults()
	if cfg.Timeout <= 0 || cfg.MaxRetries <= 0 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	sender := NewWebhookSender(cfg)
	if sender == nil {
		t.Fatal("expected sender")
	}

	ev := WebhookEvent{EventID: "evt-1", Type: "sent", Status: "ok"}
	payload, _ := json.Marshal(ev)

	if err := sender.Send(context.Background(), ev, payload); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if !strings.HasPrefix(receivedSig, "sha256=") {
		t.Fatalf("expected sha256 signature header, got %q", receivedSig)
	}

	mac := hmac.New(sha256.New, []byte("supersecret"))
	mac.Write(receivedBody)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if receivedSig != want {
		t.Fatalf("signature mismatch: got %q want %q", receivedSig, want)
	}
}

func TestWebhookSender_RetriesOnFailure(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewWebhookSender(WebhookConfig{
		Enabled:    true,
		URL:        srv.URL,
		MaxRetries: 5,
		RetryAfter: time.Millisecond,
		Timeout:    time.Second,
	})
	if err := sender.Send(context.Background(), WebhookEvent{}, []byte(`{}`)); err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if calls < 3 {
		t.Fatalf("expected >=3 attempts, got %d", calls)
	}
}

func TestWebhookSender_GivesUp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sender := NewWebhookSender(WebhookConfig{
		Enabled:    true,
		URL:        srv.URL,
		MaxRetries: 1,
		RetryAfter: time.Millisecond,
		Timeout:    time.Second,
	})
	if err := sender.Send(context.Background(), WebhookEvent{}, []byte(`{}`)); err == nil {
		t.Fatal("expected error after retries exhausted")
	}
}

func TestWebhookSender_ContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewWebhookSender(WebhookConfig{
		Enabled:    true,
		URL:        srv.URL,
		MaxRetries: 100,
		RetryAfter: time.Second,
		Timeout:    time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := sender.Send(ctx, WebhookEvent{}, []byte(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}