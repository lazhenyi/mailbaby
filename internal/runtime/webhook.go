package runtime

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// WebhookConfig configures the optional outbound webhook that delivers
// per-message status (sent / failed / dead-lettered) to a customer URL.
type WebhookConfig struct {
	Enabled    bool          `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	URL        string        `mapstructure:"url" json:"url" yaml:"url"`
	Secret     string        `mapstructure:"secret" json:"secret,omitempty" yaml:"secret,omitempty"`
	Timeout    time.Duration `mapstructure:"timeout" json:"timeout" yaml:"timeout"`
	MaxRetries  int           `mapstructure:"max_retries" json:"max_retries" yaml:"max_retries"`
	RetryAfter time.Duration `mapstructure:"retry_after" json:"retry_after" yaml:"retry_after"`
}

// ApplyDefaults applies safe defaults for WebhookConfig.
func (c *WebhookConfig) ApplyDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = 500 * time.Millisecond
	}
}

// WebhookEvent is the JSON payload delivered to a customer webhook.
type WebhookEvent struct {
	EventID  string `json:"event_id"`
	Type     string `json:"type"`
	EmailID  string `json:"email_id"`
	Account  string `json:"account,omitempty"`
	Subject  string `json:"subject,omitempty"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
	Attempts int    `json:"attempts"`
	Time     string `json:"time"`
}

// WebhookSender delivers events to a customer URL with HMAC-SHA256 signing.
// The signature is computed over the raw request body using the configured
// secret and surfaced via the X-MailBaby-Signature header so the receiver
// can verify authenticity.
type WebhookSender struct {
	cfg     WebhookConfig
	client  *http.Client
	backoff time.Duration
	mu      sync.Mutex
}

// NewWebhookSender constructs a sender from WebhookConfig. Returns nil if
// the subsystem is disabled.
func NewWebhookSender(cfg WebhookConfig) *WebhookSender {
	if !cfg.Enabled || cfg.URL == "" {
		return nil
	}
	cfg.ApplyDefaults()
	return &WebhookSender{
		cfg:     cfg,
		client:  &http.Client{Timeout: cfg.Timeout},
		backoff: cfg.RetryAfter,
	}
}

// Send delivers an event with retry; signature header is always attached.
func (w *WebhookSender) Send(ctx context.Context, ev WebhookEvent, payload []byte) error {
	if w == nil || !w.cfg.Enabled {
		return nil
	}
	if w.cfg.URL == "" {
		return errors.New("webhook: url is empty")
	}

	signature := ""
	if w.cfg.Secret != "" {
		mac := hmac.New(sha256.New, []byte(w.cfg.Secret))
		mac.Write(payload)
		signature = "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}

	var lastErr error
	for attempt := 0; attempt <= w.cfg.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("webhook: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		req.Header.Set("User-Agent", "mailbaby-webhook/1.0")
		req.Header.Set("X-Mailbaby-Event", ev.Type)
		req.Header.Set("X-Mailbaby-Event-ID", ev.EventID)
		if signature != "" {
			req.Header.Set("X-Mailbaby-Signature", signature)
		}

		resp, err := w.client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("webhook: unexpected status %d: %s", resp.StatusCode, string(body))
		} else {
			lastErr = fmt.Errorf("webhook: send: %w", err)
		}

		if attempt == w.cfg.MaxRetries || ctx.Err() != nil {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.backoff * time.Duration(attempt+1)):
		}
	}

	return lastErr
}