package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// defaultRetryInterval is used when no retry interval is configured.
const defaultRetryInterval = 5 * time.Second

// processMessage handles the end-to-end execution pipeline for an individual queue message.
func (e *Engine) processMessage(ctx context.Context, msg *queue.Message) error {
	atomic.AddInt64(&e.inFlight, 1)
	defer atomic.AddInt64(&e.inFlight, -1)

	atomic.AddInt64(&e.totalReceived, 1)

	if msg == nil || len(msg.Payload) == 0 {
		atomic.AddInt64(&e.totalFailed, 1)
		if msg != nil {
			_ = msg.Ack(ctx) // acknowledge corrupted empty message
		}
		return ErrInvalidPayload
	}

	// 1. Deserialize email payload
	var email sender.Email
	if err := email.FromJSON(msg.Payload); err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		parseErr := fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		e.routeToDLQ(ctx, msg, nil, parseErr)
		return parseErr
	}

	// 2. Validate email structure
	if err := email.Validate(); err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		validateErr := fmt.Errorf("runtime: email validation failed: %w", err)
		e.routeToDLQ(ctx, msg, &email, validateErr)
		return validateErr
	}

	// 3. Execute middleware processing chain leading to sender.Send().
	//    Bounded retries with backoff happen inside the final stage
	//    (see sendWithRetry), so brokers only ever sees one final Ack per message.
	pipeline := buildChain(e.middlewares, func(chainCtx context.Context, m *queue.Message, mailItem *sender.Email) error {
		return e.sendWithRetry(chainCtx, m, mailItem)
	})

	err := pipeline(ctx, msg, &email)
	if err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		e.routeToDLQ(ctx, msg, &email, err)
		return err
	}

	atomic.AddInt64(&e.totalSuccess, 1)
	return nil
}

// sendWithRetry attempts sender.Send up to maxRetries times with backoff.
// The number of attempts is derived from the message delivery count so that
// already-redelivered broker messages are not retried forever.
func (e *Engine) sendWithRetry(ctx context.Context, msg *queue.Message, email *sender.Email) error {
	maxAttempts := e.maxRetries
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	startAttempt := 1
	if msg != nil && msg.Attempts > 1 {
		startAttempt = msg.Attempts
	}
	if startAttempt > maxAttempts {
		startAttempt = maxAttempts
	}

	interval := e.retryInterval
	if interval <= 0 {
		interval = defaultRetryInterval
	}

	var lastErr error
	for attempt := startAttempt; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Backoff between attempts
		if attempt > startAttempt {
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		lastErr = e.sender.Send(ctx, email)
		if lastErr == nil {
			return nil
		}

		atomic.AddInt64(&e.totalRetried, 1)
		metrics.Get().IncQueueRetried(string(e.queue.Driver()), e.queue.Name())
		logger.Get().WithContext(ctx).WithFields(logger.Fields{
			"msg_id":   msgIDOf(msg),
			"attempt":  attempt,
			"max":      maxAttempts,
			"interval": interval.String(),
			"error":    lastErr.Error(),
		}).Warn("email delivery failed, scheduling retry")
	}

	return lastErr
}

// routeToDLQ publishes the message to the configured DLQ (if any), invokes the
// error handler and then acknowledges the message so the broker stops redelivering it.
func (e *Engine) routeToDLQ(ctx context.Context, msg *queue.Message, email *sender.Email, err error) {
	driverStr := string(e.queue.Driver())
	topicStr := e.queue.Name()

	atomic.AddInt64(&e.totalDeadLetter, 1)
	metrics.Get().IncQueueDeadLetter(driverStr, topicStr)

	logger.Get().WithContext(ctx).WithFields(logger.Fields{
		"msg_id":   msgIDOf(msg),
		"attempts": msgAttemptsOf(msg),
		"error":    err.Error(),
	}).Warn("message exceeded max retries, routing to DLQ")

	// Route to DLQ if configured
	if e.dlqProducer != nil {
		dlqMsg := &queue.Message{
			ID:        msg.ID,
			Payload:   msg.Payload,
			Topic:     e.dlqTopic,
			Headers:   make(map[string]string),
			Timestamp: msg.Timestamp,
			Attempts:  msg.Attempts,
		}
		for k, v := range msg.Headers {
			dlqMsg.Headers[k] = v
		}
		dlqMsg.Headers["X-DLQ-Error"] = err.Error()

		if dlqErr := e.dlqProducer.Publish(ctx, dlqMsg); dlqErr != nil {
			logger.Get().WithContext(ctx).WithFields(logger.Fields{
				"msg_id":    msg.ID,
				"dlq_topic": e.dlqTopic,
				"error":     dlqErr.Error(),
			}).Error("failed to publish message to DLQ")
		}
	}

	if e.errorHandler != nil {
		e.errorHandler(ctx, msg, email, err)
	}

	// Acknowledge the message so the broker stops requeueing the exhausted message.
	_ = msg.Ack(ctx)
}

func msgIDOf(msg *queue.Message) string {
	if msg == nil {
		return "unknown"
	}
	return msg.ID
}

func msgAttemptsOf(msg *queue.Message) int {
	if msg == nil {
		return 0
	}
	return msg.Attempts
}
