package runtime

import (
	"time"

	"mailbaby/internal/queue"
)

// Option configures an Engine instance.
type Option func(*Engine)

// WithConcurrency sets the number of concurrent worker goroutines.
func WithConcurrency(workers int) Option {
	return func(e *Engine) {
		if workers > 0 {
			e.concurrency = workers
		}
	}
}

// WithMiddlewares appends custom middleware interceptors to the processing pipeline.
func WithMiddlewares(mws ...Middleware) Option {
	return func(e *Engine) {
		e.middlewares = append(e.middlewares, mws...)
	}
}

// WithErrorHandler sets a custom callback for unrecoverable errors and DLQ events.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(e *Engine) {
		e.errorHandler = handler
	}
}

// WithDLQ sets the Dead Letter Queue producer and topic for exhausted messages.
func WithDLQ(dlqProducer queue.Producer, dlqTopic string) Option {
	return func(e *Engine) {
		e.dlqProducer = dlqProducer
		e.dlqTopic = dlqTopic
	}
}

// WithShutdownTimeout sets the timeout duration for draining in-flight messages during shutdown.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(e *Engine) {
		if timeout > 0 {
			e.shutdownTimeout = timeout
		}
	}
}

// WithMaxRetries sets the fallback maximum retries for messages that do not specify MaxRetries.
func WithMaxRetries(retries int) Option {
	return func(e *Engine) {
		if retries >= 0 {
			e.maxRetries = retries
		}
	}
}

// WithRetryInterval sets the backoff interval between send attempts.
func WithRetryInterval(interval time.Duration) Option {
	return func(e *Engine) {
		if interval > 0 {
			e.retryInterval = interval
		}
	}
}
