package runtime

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
)

// RecoveryMiddleware captures panics occurring inside any stage of the message handler pipeline.
func RecoveryMiddleware() Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := string(debug.Stack())
					msgID := "unknown"
					if msg != nil {
						msgID = msg.ID
					}
					logger.Get().WithContext(ctx).WithFields(logger.Fields{
						"msg_id": msgID,
						"panic":  fmt.Sprint(r),
						"stack":  stack,
					}).Error("panic recovered in runtime processing pipeline")

					err = fmt.Errorf("runtime panic: %v", r)
				}
			}()
			return next(ctx, msg, email)
		}
	}
}

// TracingMiddleware begins a root Span for each message processing transaction and records tags and errors.
func TracingMiddleware(driver, topic string) Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			msgID := "unknown"
			if msg != nil {
				msgID = msg.ID
			}

			spanCtx, span := tracing.StartSpan(ctx, "runtime.process_message")
			defer span.End()

			span.SetAttribute("messaging.system", driver)
			span.SetAttribute("messaging.destination", topic)
			span.SetAttribute("messaging.message_id", msgID)

			if email != nil {
				span.SetAttribute("email.account", email.Account)
				span.SetAttribute("email.subject", email.Subject)
				span.SetAttribute("email.recipients_count", len(email.AllRecipients()))
			}

			err := next(spanCtx, msg, email)
			if err != nil {
				span.RecordError(err)
			}
			return err
		}
	}
}

// MetricsMiddleware records Prometheus metrics and latency histograms for each processed message.
func MetricsMiddleware(driver, topic string) Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			start := time.Now()
			metrics.Get().IncQueueReceived(driver, topic)

			account := "default"
			if email != nil && email.Account != "" {
				account = email.Account
			}

			err := next(ctx, msg, email)
			duration := time.Since(start)

			metrics.Get().ObserveQueueProcessDuration(driver, topic, duration)

			if err != nil {
				metrics.Get().IncQueueProcessed(driver, topic, "failed")
				metrics.Get().IncEmailsSent(account, "failed")
			} else {
				metrics.Get().IncQueueProcessed(driver, topic, "success")
				metrics.Get().IncEmailsSent(account, "success")
				metrics.Get().ObserveEmailDuration(account, duration)

				if email != nil {
					metrics.Get().AddEmailRecipients(account, "to", len(email.To))
					metrics.Get().AddEmailRecipients(account, "cc", len(email.Cc))
					metrics.Get().AddEmailRecipients(account, "bcc", len(email.Bcc))
				}
			}

			return err
		}
	}
}

// LoggingMiddleware logs detailed structured delivery results using contextual tracing logger.
func LoggingMiddleware() Middleware {
	return func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			start := time.Now()
			msgID := "unknown"
			if msg != nil {
				msgID = msg.ID
			}

			account := "default"
			rcptCount := 0
			subject := ""
			if email != nil {
				if email.Account != "" {
					account = email.Account
				}
				rcptCount = len(email.AllRecipients())
				subject = email.Subject
			}

			err := next(ctx, msg, email)
			duration := time.Since(start)

			logEntry := logger.Get().WithContext(ctx).WithFields(logger.Fields{
				"msg_id":     msgID,
				"account":    account,
				"rcpt_count": rcptCount,
				"subject":    subject,
				"duration":   duration.String(),
			})

			if err != nil {
				logEntry.WithError(err).Error("email delivery failed in runtime pipeline")
			} else {
				logEntry.WithField("delivered", true).Info("email delivered successfully")
			}

			return err
		}
	}
}

// buildChain composes multiple middlewares into a single ProcessFunc.
func buildChain(middlewares []Middleware, final ProcessFunc) ProcessFunc {
	if len(middlewares) == 0 {
		return final
	}
	chain := final
	for i := len(middlewares) - 1; i >= 0; i-- {
		chain = middlewares[i](chain)
	}
	return chain
}
