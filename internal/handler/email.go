package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
)

// EmailHandler handles HTTP REST endpoints for email sending.
type EmailHandler struct {
	sender    sender.Sender
	producer  queue.Producer
	queueName string
}

// NewEmailHandler creates a new EmailHandler instance.
func NewEmailHandler(s sender.Sender, p queue.Producer, queueName string) *EmailHandler {
	return &EmailHandler{
		sender:    s,
		producer:  p,
		queueName: queueName,
	}
}

// SendEmailRequest represents an HTTP JSON request to deliver an email.
type SendEmailRequest struct {
	ID          string               `json:"id,omitempty"`
	Account     string               `json:"account,omitempty"`
	From        string               `json:"from,omitempty"`
	FromName    string               `json:"from_name,omitempty"`
	ReplyTo     string               `json:"reply_to,omitempty"`
	To          []string             `json:"to"`
	Cc          []string             `json:"cc,omitempty"`
	Bcc         []string             `json:"bcc,omitempty"`
	Subject     string               `json:"subject"`
	TextBody    string               `json:"text_body,omitempty"`
	HTMLBody    string               `json:"html_body,omitempty"`
	Headers     map[string]string    `json:"headers,omitempty"`
	Attachments []*AttachmentRequest `json:"attachments,omitempty"`
	Tags        []string             `json:"tags,omitempty"`
	Metadata    map[string]string    `json:"metadata,omitempty"`
	Async       bool                 `json:"async,omitempty"`
}

// AttachmentRequest represents an email attachment in the HTTP request payload.
type AttachmentRequest struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
	Inline      bool   `json:"inline"`
	ContentID   string `json:"content_id,omitempty"`
}

// SendEmailResponse represents the JSON response for an email delivery operation.
type SendEmailResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"` // "sent" or "queued"
	Message string `json:"message"`
	SentAt  int64  `json:"sent_at"`
}

// BatchSendEmailRequest represents a batch of email delivery requests.
type BatchSendEmailRequest struct {
	Emails []*SendEmailRequest `json:"emails"`
	Async  bool                `json:"async,omitempty"`
}

// BatchSendEmailResponse represents the result of a batch delivery.
type BatchSendEmailResponse struct {
	Total     int                  `json:"total"`
	Succeeded int                  `json:"succeeded"`
	Failed    int                  `json:"failed"`
	Results   []*SendEmailResponse `json:"results"`
}

// ErrorResponse represents an HTTP error response.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, statusCode int, errStr, details string) {
	writeJSON(w, statusCode, ErrorResponse{
		Code:    statusCode,
		Error:   errStr,
		Details: details,
	})
}

// HandleSend processes POST /v1/email/send.
func (h *EmailHandler) HandleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST method is supported")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 32*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "failed to read request body or body too large")
		return
	}

	var req SendEmailRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", fmt.Sprintf("failed to parse JSON payload: %v", err))
		return
	}

	email := h.requestToEmail(&req)
	if err := email.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", err.Error())
		return
	}

	isAsync := req.Async || strings.EqualFold(r.URL.Query().Get("async"), "true")
	if isAsync {
		resp, err := h.sendAsync(r.Context(), email)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "enqueue_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, resp)
		return
	}

	resp, err := h.sendSync(r.Context(), email)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delivery_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleBatchSend processes POST /v1/email/batch.
func (h *EmailHandler) HandleBatchSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST method is supported")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "failed to read request body or body too large")
		return
	}

	var req BatchSendEmailRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", fmt.Sprintf("failed to parse JSON payload: %v", err))
		return
	}

	if len(req.Emails) == 0 {
		writeError(w, http.StatusBadRequest, "empty_batch", "emails list cannot be empty")
		return
	}

	isAsync := req.Async || strings.EqualFold(r.URL.Query().Get("async"), "true")

	batchResp := &BatchSendEmailResponse{
		Total:   len(req.Emails),
		Results: make([]*SendEmailResponse, len(req.Emails)),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, item := range req.Emails {
		wg.Add(1)
		go func(idx int, emailReq *SendEmailRequest) {
			defer wg.Done()
			if emailReq == nil {
				mu.Lock()
				batchResp.Failed++
				batchResp.Results[idx] = &SendEmailResponse{
					Status:  "failed",
					Message: "nil email item in batch",
					SentAt:  time.Now().UnixNano(),
				}
				mu.Unlock()
				return
			}

			email := h.requestToEmail(emailReq)
			if err := email.Validate(); err != nil {
				mu.Lock()
				batchResp.Failed++
				batchResp.Results[idx] = &SendEmailResponse{
					ID:      email.ID,
					Status:  "failed",
					Message: fmt.Sprintf("validation failed: %v", err),
					SentAt:  time.Now().UnixNano(),
				}
				mu.Unlock()
				return
			}

			if isAsync {
				resp, err := h.sendAsync(r.Context(), email)
				mu.Lock()
				if err != nil {
					batchResp.Failed++
					batchResp.Results[idx] = &SendEmailResponse{
						ID:      email.ID,
						Status:  "failed",
						Message: fmt.Sprintf("enqueue failed: %v", err),
						SentAt:  time.Now().UnixNano(),
					}
				} else {
					batchResp.Succeeded++
					batchResp.Results[idx] = resp
				}
				mu.Unlock()
			} else {
				resp, err := h.sendSync(r.Context(), email)
				mu.Lock()
				if err != nil {
					batchResp.Failed++
					batchResp.Results[idx] = &SendEmailResponse{
						ID:      email.ID,
						Status:  "failed",
						Message: fmt.Sprintf("delivery failed: %v", err),
						SentAt:  time.Now().UnixNano(),
					}
				} else {
					batchResp.Succeeded++
					batchResp.Results[idx] = resp
				}
				mu.Unlock()
			}
		}(i, item)
	}

	wg.Wait()
	writeJSON(w, http.StatusOK, batchResp)
}

func (h *EmailHandler) sendSync(ctx context.Context, email *sender.Email) (*SendEmailResponse, error) {
	if h.sender == nil {
		return nil, errors.New("smtp sender is not initialized")
	}

	start := time.Now()
	ctx, span := tracing.StartSpan(ctx, "http.send_email_sync")
	defer span.End()

	span.SetAttribute("email.id", email.ID)
	span.SetAttribute("email.subject", email.Subject)

	err := h.sender.Send(ctx, email)
	if err != nil {
		span.RecordError(err)
		account := email.Account
		if account == "" {
			account = "default"
		}
		metrics.Get().IncEmailsSent(account, "failed")
		return nil, err
	}

	account := email.Account
	if account == "" {
		account = "default"
	}
	metrics.Get().IncEmailsSent(account, "success")
	metrics.Get().ObserveEmailDuration(account, time.Since(start))

	return &SendEmailResponse{
		ID:      email.ID,
		Status:  "sent",
		Message: "email sent successfully",
		SentAt:  time.Now().UnixNano(),
	}, nil
}

func (h *EmailHandler) sendAsync(ctx context.Context, email *sender.Email) (*SendEmailResponse, error) {
	data, err := email.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize email to JSON: %w", err)
	}

	msg := &queue.Message{
		ID:        email.ID,
		Payload:   data,
		Topic:     h.queueName,
		Timestamp: time.Now(),
		Attempts:  1,
	}

	if h.producer != nil {
		if err := h.producer.Publish(ctx, msg); err != nil {
			return nil, fmt.Errorf("failed to publish email to queue: %w", err)
		}
	} else if h.sender != nil {
		// Asynchronous background fallback if queue producer is absent
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if sendErr := h.sender.Send(bgCtx, email); sendErr != nil {
				logger.Get().WithError(sendErr).WithField("email_id", email.ID).Error("async background fallback delivery failed")
			}
		}()
	} else {
		return nil, errors.New("neither queue producer nor sender is available")
	}

	return &SendEmailResponse{
		ID:      email.ID,
		Status:  "queued",
		Message: "email enqueued successfully",
		SentAt:  time.Now().UnixNano(),
	}, nil
}

func (h *EmailHandler) requestToEmail(req *SendEmailRequest) *sender.Email {
	id := req.ID
	if strings.TrimSpace(id) == "" {
		id = generateID()
	}

	attachments := make([]*sender.Attachment, 0, len(req.Attachments))
	for _, att := range req.Attachments {
		if att != nil {
			attachments = append(attachments, &sender.Attachment{
				Filename:    att.Filename,
				ContentType: att.ContentType,
				Data:        att.Data,
				Inline:      att.Inline,
				ContentID:   att.ContentID,
			})
		}
	}

	return &sender.Email{
		ID:          id,
		Account:     req.Account,
		From:        req.From,
		FromName:    req.FromName,
		ReplyTo:     req.ReplyTo,
		To:          req.To,
		Cc:          req.Cc,
		Bcc:         req.Bcc,
		Subject:     req.Subject,
		TextBody:    req.TextBody,
		HTMLBody:    req.HTMLBody,
		Headers:     req.Headers,
		Attachments: attachments,
		Tags:        req.Tags,
		Metadata:    req.Metadata,
	}
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
