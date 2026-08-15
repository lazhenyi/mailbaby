package runtime

import (
	"context"
	"fmt"
	"log"
	"time"

	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// RecoveryMiddleware catches any panics during message processing and returns a formatted error.
func RecoveryMiddleware() Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("runtime: panic recovered in message handler: %v", r)
					log.Printf("[ERROR] %v", err)
				}
			}()
			return next(ctx, msg, email)
		}
	}
}

// MetricsMiddleware records telemetry data to Prometheus, StatsD, and internal metrics.
func MetricsMiddleware(driver, topic string) Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			m := metrics.Get()
			m.IncQueueReceived(driver, topic)

			start := time.Now()
			err := next(ctx, msg, email)
			duration := time.Since(start)

			status := "success"
			if err != nil {
				status = "failed"
			}

			m.IncQueueProcessed(driver, topic, status)
			m.ObserveQueueProcessDuration(driver, topic, duration)

			if email != nil {
				account := email.Account
				if account == "" {
					account = "default"
				}
				m.IncEmailsSent(account, status)
				m.ObserveEmailDuration(account, duration)
				m.AddEmailBytes(account, int64(len(msg.Payload)))
				m.AddEmailRecipients(account, "consolidated", len(email.AllRecipients()))
			}

			return err
		}
	}
}

// LoggingMiddleware logs structured execution traces for each email delivery attempt.
func LoggingMiddleware() Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			start := time.Now()
			err := next(ctx, msg, email)
			duration := time.Since(start)

			subject := ""
			account := "default"
			recipientCount := 0
			if email != nil {
				subject = email.Subject
				if email.Account != "" {
					account = email.Account
				}
				recipientCount = len(email.AllRecipients())
			}

			if err != nil {
				log.Printf("[ERROR] runtime: msg_id=%s account=%s rcpt_count=%d subject=%q duration=%v error=%v",
					msg.ID, account, recipientCount, subject, duration, err)
			} else {
				log.Printf("[INFO] runtime: msg_id=%s account=%s rcpt_count=%d subject=%q duration=%v delivered=true",
					msg.ID, account, recipientCount, subject, duration)
			}

			return err
		}
	}
}

// buildChain composes an array of Middlewares into a single ProcessFunc wrapper.
func buildChain(middlewares []Middleware, final ProcessFunc) ProcessFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		final = middlewares[i](final)
	}
	return final
}
