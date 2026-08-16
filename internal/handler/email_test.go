package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// mockSender implements sender.Sender for testing.
type mockSender struct {
	sendFunc      func(ctx context.Context, email *sender.Email) error
	sendBatchFunc func(ctx context.Context, emails []*sender.Email) []error
}

func (m *mockSender) Send(ctx context.Context, email *sender.Email) error {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, email)
	}
	return nil
}

func (m *mockSender) SendBatch(ctx context.Context, emails []*sender.Email) []error {
	if m.sendBatchFunc != nil {
		return m.sendBatchFunc(ctx, emails)
	}
	errs := make([]error, len(emails))
	for i, e := range emails {
		errs[i] = m.Send(ctx, e)
	}
	return errs
}

func (m *mockSender) Account(name string) (sender.AccountSender, error) {
	return nil, errors.New("not implemented")
}

func (m *mockSender) AccountNames() []string {
	return []string{"default"}
}

func (m *mockSender) Stats() map[string]sender.AccountStats {
	return nil
}

func (m *mockSender) Close() error {
	return nil
}

// mockProducer implements queue.Producer for testing.
type mockProducer struct {
	published []*queue.Message
	err       error
}

func (m *mockProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, msg)
	return nil
}

func (m *mockProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, msgs...)
	return nil
}

func (m *mockProducer) Close() error {
	return nil
}

func TestEmailHandler_SendSync(t *testing.T) {
	s := &mockSender{
		sendFunc: func(ctx context.Context, email *sender.Email) error {
			if email.Subject == "fail" {
				return errors.New("smtp connection error")
			}
			return nil
		},
	}

	h := NewEmailHandler(s, nil, "")

	t.Run("successful sync send", func(t *testing.T) {
		reqBody := SendEmailRequest{
			To:       []string{"recipient@example.com"},
			Subject:  "Test Subject",
			TextBody: "Hello world!",
		}
		data, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/email/send", bytes.NewReader(data))
		w := httptest.NewRecorder()

		h.HandleSend(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
		}

		var resp SendEmailResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.Status != "sent" {
			t.Errorf("expected status 'sent', got %q", resp.Status)
		}
		if resp.ID == "" {
			t.Errorf("expected generated ID")
		}
	})

	t.Run("delivery failure", func(t *testing.T) {
		reqBody := SendEmailRequest{
			To:      []string{"recipient@example.com"},
			Subject: "fail",
		}
		data, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/email/send", bytes.NewReader(data))
		w := httptest.NewRecorder()

		h.HandleSend(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected status 500, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "smtp connection error") {
			t.Fatalf("response body leaked the underlying SMTP error: %s", body)
		}
		var env ErrorResponse
		if err := json.Unmarshal([]byte(body), &env); err != nil {
			t.Fatalf("response body is not the public error envelope: %v body=%s", err, body)
		}
		if env.Error != "delivery_failed" {
			t.Errorf("expected public error code %q, got %q", "delivery_failed", env.Error)
		}
		if !strings.Contains(env.Details, "trace_id=") {
			t.Errorf("expected Details to carry a trace_id, got %q", env.Details)
		}
	})

	t.Run("validation failure no recipients", func(t *testing.T) {
		reqBody := SendEmailRequest{
			Subject: "No recipients",
		}
		data, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/email/send", bytes.NewReader(data))
		w := httptest.NewRecorder()

		h.HandleSend(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", w.Code)
		}
	})
}

func TestEmailHandler_SendAsync(t *testing.T) {
	prod := &mockProducer{}
	s := &mockSender{}
	h := NewEmailHandler(s, prod, "test-topic")

	reqBody := SendEmailRequest{
		To:       []string{"async@example.com"},
		Subject:  "Async Subject",
		TextBody: "Async body",
		Async:    true,
	}
	data, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/email/send", bytes.NewReader(data))
	w := httptest.NewRecorder()

	h.HandleSend(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", w.Code)
	}

	if len(prod.published) != 1 {
		t.Fatalf("expected 1 message published to queue, got %d", len(prod.published))
	}
}

func TestEmailHandler_BatchSend(t *testing.T) {
	s := &mockSender{}
	h := NewEmailHandler(s, nil, "")

	reqBody := BatchSendEmailRequest{
		Emails: []*SendEmailRequest{
			{
				To:      []string{"user1@example.com"},
				Subject: "Item 1",
			},
			{
				To:      []string{"user2@example.com"},
				Subject: "Item 2",
			},
		},
	}
	data, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/email/batch", bytes.NewReader(data))
	w := httptest.NewRecorder()

	h.HandleBatchSend(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp BatchSendEmailResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 2 || resp.Succeeded != 2 || resp.Failed != 0 {
		t.Errorf("unexpected batch result: total=%d, succ=%d, failed=%d", resp.Total, resp.Succeeded, resp.Failed)
	}
}

func TestAuthMiddleware(t *testing.T) {
	authCfg := config.AuthConfig{
		Enabled:    true,
		SecretKey:  "my_secret_token_123",
		HeaderName: "X-API-Key",
	}

	mw := AuthMiddleware(authCfg)
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authorized"))
	})
	handler := mw(dummyHandler)

	t.Run("missing token returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})

	t.Run("invalid token returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", "wrong_token")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})

	t.Run("valid X-API-Key passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", "my_secret_token_123")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("valid Authorization Bearer passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "Bearer my_secret_token_123")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("disabled auth allows all requests", func(t *testing.T) {
		disabledMW := AuthMiddleware(config.AuthConfig{Enabled: false})
		openHandler := disabledMW(dummyHandler)

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		openHandler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}
