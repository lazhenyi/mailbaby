package core

import (
	"context"
	"time"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
)

type SendService struct {
	Sender    sender.Sender
	Producer  queue.Producer
	QueueName string
}

func (s *SendService) SendSync(ctx context.Context, email *sender.Email) (*SendOutcome, error) {
	if s.Sender == nil {
		return nil, ErrSenderUnavailable
	}
	start := time.Now()
	ctx, span := tracing.StartSpan(ctx, "core.send_sync")
	defer span.End()
	span.SetAttribute("email.id", email.ID)

	err := s.Sender.Send(ctx, email)
	account := email.Account
	if account == "" {
		account = "default"
	}
	if err != nil {
		span.RecordError(err)
		metrics.Get().IncEmailsSent(account, "failed")
		return nil, err
	}
	metrics.Get().IncEmailsSent(account, "success")
	metrics.Get().ObserveEmailDuration(account, time.Since(start))
	return &SendOutcome{ID: email.ID, Status: "sent", Message: "email sent successfully", SentAt: time.Now().UnixNano()}, nil
}

func (s *SendService) SendAsync(ctx context.Context, email *sender.Email) (*SendOutcome, error) {
	data, err := email.ToJSON()
	if err != nil {
		return nil, err
	}
	msg := &queue.Message{ID: email.ID, Payload: data, Topic: s.QueueName, Timestamp: time.Now(), Attempts: 1}
	if s.Producer != nil {
		if err := s.Producer.Publish(ctx, msg); err != nil {
			return nil, err
		}
		return &SendOutcome{ID: email.ID, Status: "queued", Message: "email enqueued successfully", SentAt: time.Now().UnixNano()}, nil
	}
	if s.Sender != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Get().WithField("panic", panicString(r)).Error("async background send panic recovered")
				}
			}()
			bgCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if sendErr := s.Sender.Send(bgCtx, email); sendErr != nil {
				logger.Get().WithError(sendErr).WithField("email_id", email.ID).Error("async background fallback delivery failed")
			}
		}()
		return &SendOutcome{ID: email.ID, Status: "queued", Message: "email enqueued successfully", SentAt: time.Now().UnixNano()}, nil
	}
	return nil, ErrNoTransport
}

type SendOutcome struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
	SentAt  int64  `json:"sent_at"`
}

var (
	ErrSenderUnavailable = sentry("core: smtp sender is not initialized")
	ErrNoTransport       = sentry("core: neither queue producer nor sender is available")
)

func sentry(s string) error { return stringError(s) }

type stringError string

func (e stringError) Error() string { return string(e) }

func panicString(r any) string {
	if r == nil {
		return ""
	}
	if s, ok := r.(string); ok {
		return s
	}
	if e, ok := r.(error); ok {
		return e.Error()
	}
	return "non-string panic"
}