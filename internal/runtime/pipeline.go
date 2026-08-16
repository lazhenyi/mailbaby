package runtime

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// defaultRetryInterval is used when no retry interval is configured.
const defaultRetryInterval = 5 * time.Second

// maxBackoff caps the exponential backoff so retries never sleep for hours.
const maxBackoff = 5 * time.Minute

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
		// If DLQ routing fails, still ack the corrupted message (it will never be valid).
		// This is the only case where ack-on-failure is acceptable.
		if dlqErr := e.routeToDLQ(ctx, msg, nil, parseErr); dlqErr != nil {
			logger.Get().WithContext(ctx).WithError(dlqErr).Warn("DLQ routing failed for invalid payload; acking to avoid stuck queue")
			_ = msg.Ack(ctx)
		}
		return parseErr
	}

	// 2. Validate email structure
	if err := email.Validate(); err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		validateErr := fmt.Errorf("runtime: email validation failed: %w", err)
		if dlqErr := e.routeToDLQ(ctx, msg, &email, validateErr); dlqErr != nil {
			logger.Get().WithContext(ctx).WithError(dlqErr).Warn("DLQ routing failed for invalid email; acking to avoid stuck queue")
			_ = msg.Ack(ctx)
		}
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
		// Critical: do NOT ack if DLQ publish failed. The broker will redeliver
		// and we'll retry from the start. This prevents silent message loss.
		if dlqErr := e.routeToDLQ(ctx, msg, &email, err); dlqErr != nil {
			return dlqErr
		}
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
		// Exponential backoff with jitter to avoid retry storms.
		// base * 2^(attempt-1) capped at maxBackoff, then +/- 25% jitter.
		if attempt > startAttempt {
			backoff := interval << (attempt - startAttempt - 1)
			if backoff <= 0 || backoff > maxBackoff {
				backoff = maxBackoff
			}
			// Apply +/- 25% jitter to break synchronized retry waves.
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2)) // [0, backoff/2)
			sleep := backoff - backoff/4 + jitter                    // [backoff*3/4, backoff*5/4)
			select {
			case <-time.After(sleep):
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
			"msg_id":  msgIDOf(msg),
			"attempt": attempt,
			"max":     maxAttempts,
			"backoff": interval.String(),
			"error":   lastErr.Error(),
		}).Warn("email delivery failed, scheduling retry")
	}

	return lastErr
}

// routeToDLQ publishes the message to the configured DLQ (if any), invokes the
// error handler and then acknowledges the message so the broker stops redelivering it.
//
// IMPORTANT: the message is only acknowledged AFTER the DLQ publish succeeds.
// Previously the message was acked unconditionally, which meant a DLQ publish
// failure caused silent message loss — the message disappeared from the source
// queue with no DLQ record. We now treat a DLQ publish failure as fatal and
// return an error so the caller does NOT ack.
func (e *Engine) routeToDLQ(ctx context.Context, msg *queue.Message, email *sender.Email, err error) error {
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
			// Refuse to ack: surface the error so the broker redelivers the message.
			logger.Get().WithContext(ctx).WithFields(logger.Fields{
				"msg_id":    msg.ID,
				"dlq_topic": e.dlqTopic,
				"error":     dlqErr.Error(),
			}).Error("failed to publish message to DLQ; will NOT ack to allow redelivery")
			return fmt.Errorf("runtime: DLQ publish failed: %w", dlqErr)
		}
	}

	if e.errorHandler != nil {
		e.errorHandler(ctx, msg, email, err)
	}

	// Acknowledge the message so the broker stops requeueing the exhausted message.
	if ackErr := msg.Ack(ctx); ackErr != nil {
		logger.Get().WithContext(ctx).WithError(ackErr).WithField("msg_id", msgIDOf(msg)).Warn("failed to ack message after DLQ routing")
	}
	return nil
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
