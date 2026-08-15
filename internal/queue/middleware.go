package queue

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"
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

// LoggingMiddleware logs the duration and status of message handling using standard logger.
func LoggingMiddleware(logger ...func(format string, args ...any)) Middleware {
	logFunc := func(format string, args ...any) {
		log.Printf(format, args...)
	}
	if len(logger) > 0 && logger[0] != nil {
		logFunc = logger[0]
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			start := time.Now()
			err := next(ctx, msg)
			duration := time.Since(start)

			if err != nil {
				logFunc("[WARN] Queue handle msg_id=%s topic=%s duration=%v status=FAILED error=%v",
					msg.ID, msg.Topic, duration, err)
			} else {
				logFunc("[DEBUG] Queue handle msg_id=%s topic=%s duration=%v status=OK",
					msg.ID, msg.Topic, duration)
			}
			return err
		}
	}
}
