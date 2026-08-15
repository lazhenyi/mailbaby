package queue

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"mailbaby/internal/logger"
)

// Middleware is an interceptor around a message Handler.
type Middleware func(Handler) Handler

// Chain combines multiple Middlewares into a single Middleware applied in order (first-to-last).
func Chain(middlewares ...Middleware) Middleware {
	return func(final Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			if middlewares[i] != nil {
				final = middlewares[i](final)
			}
		}
		return final
	}
}

// RecoveryMiddleware captures panics occurring during message handling and converts them to errors.
func RecoveryMiddleware(onPanic ...func(p any, msg *Message)) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					err = fmt.Errorf("queue: panic recovered while handling message %q: %v\nstack:\n%s", msg.ID, r, stack)
					for _, hook := range onPanic {
						if hook != nil {
							hook(r, msg)
						}
					}
				}
			}()
			return next(ctx, msg)
		}
	}
}

// RetryMiddleware retries failed message handling up to maxRetries times with the given backoff interval.
func RetryMiddleware(maxRetries int, backoff time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			var err error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if attempt > 0 {
					msg.Attempts++
					if backoff > 0 {
						select {
						case <-time.After(backoff):
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}

				err = next(ctx, msg)
				if err == nil {
					return nil
				}
			}
			return fmt.Errorf("queue: max retries (%d) reached: %w", maxRetries, err)
		}
	}
}

// LoggingMiddleware logs the duration and status of message handling using the structured logger.
func LoggingMiddleware(loggerFn ...func(level, msg string, fields logger.Fields)) Middleware {
	logFunc := func(msg string, fields logger.Fields) {
		logger.Get().WithFields(fields).Info(msg)
	}
	if len(loggerFn) > 0 && loggerFn[0] != nil {
		logFunc = func(msg string, fields logger.Fields) {
			loggerFn[0]("info", msg, fields)
		}
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			start := time.Now()
			err := next(ctx, msg)
			duration := time.Since(start)

			fields := logger.Fields{
				"msg_id":   msg.ID,
				"topic":    msg.Topic,
				"duration": duration.String(),
			}
			if err != nil {
				fields["status"] = "FAILED"
				fields["error"] = err.Error()
				logFunc("queue message handling failed", fields)
			} else {
				fields["status"] = "OK"
				logFunc("queue message handled", fields)
			}
			return err
		}
	}
}
