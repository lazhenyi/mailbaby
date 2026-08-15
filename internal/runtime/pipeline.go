package runtime

import (
	"context"
	"fmt"
	"sync/atomic"

	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

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
		e.handleFailure(ctx, msg, nil, parseErr)
		return parseErr
	}

	// 2. Validate email structure
	if err := email.Validate(); err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		validateErr := fmt.Errorf("runtime: email validation failed: %w", err)
		e.handleFailure(ctx, msg, &email, validateErr)
		return validateErr
	}

	// 3. Execute middleware processing chain leading to sender.Send()
	pipeline := buildChain(e.middlewares, func(chainCtx context.Context, m *queue.Message, mailItem *sender.Email) error {
		return e.sender.Send(chainCtx, mailItem)
	})

	err := pipeline(ctx, msg, &email)
	if err != nil {
		atomic.AddInt64(&e.totalFailed, 1)
		e.handleFailure(ctx, msg, &email, err)
		return err
	}

	atomic.AddInt64(&e.totalSuccess, 1)
	return nil
}

// handleFailure decides whether to retry or route to Dead Letter Queue (DLQ).
func (e *Engine) handleFailure(ctx context.Context, msg *queue.Message, email *sender.Email, err error) {
	driverStr := string(e.queue.Driver())
	topicStr := e.queue.Name()

	maxRetries := e.maxRetries

	// Check if retries are exhausted
	if msg.Attempts >= maxRetries {
		atomic.AddInt64(&e.totalDeadLetter, 1)
		metrics.Get().IncQueueDeadLetter(driverStr, topicStr)

		logger.Get().WithContext(ctx).WithFields(logger.Fields{
			"msg_id":      msg.ID,
			"attempts":    msg.Attempts,
			"max_retries": maxRetries,
			"error":       err.Error(),
		}).Warn("message exceeded max_retries, routing to DLQ")

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

		// Acknowledge the message so broker stops requeueing exhausted message
		_ = msg.Ack(ctx)
		return
	}

	// Message can be retried
	atomic.AddInt64(&e.totalRetried, 1)
	metrics.Get().IncQueueRetried(driverStr, topicStr)

	if e.errorHandler != nil {
		e.errorHandler(ctx, msg, email, err)
	}
}
