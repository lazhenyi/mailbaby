package queue

import "errors"

var (
	// ErrDriverNotFound is returned when attempting to initialize an unregistered queue driver.
	ErrDriverNotFound = errors.New("queue: driver not found or not registered")

	// ErrQueueClosed is returned when an operation is performed on a closed queue, producer or consumer.
	ErrQueueClosed = errors.New("queue: queue connection is closed")

	// ErrInvalidMessage is returned when a message is nil or lacks essential content.
	ErrInvalidMessage = errors.New("queue: invalid or empty message")

	// ErrPublishFailed is returned when a message fails to be published.
	ErrPublishFailed = errors.New("queue: failed to publish message")

	// ErrConsumeFailed is returned when consumer encounters an unrecoverable failure.
	ErrConsumeFailed = errors.New("queue: consumer encountered an error")

	// ErrTimeout is returned when an MQ operation times out.
	ErrTimeout = errors.New("queue: operation timed out")

	// ErrAckNotSupported is returned when Ack is called on a message without an ack callback.
	ErrAckNotSupported = errors.New("queue: ack function is not configured on message")

	// ErrNackNotSupported is returned when Nack is called on a message without a nack callback.
	ErrNackNotSupported = errors.New("queue: nack function is not configured on message")

	// ErrNilHandler is returned when a nil handler is passed to Consume.
	ErrNilHandler = errors.New("queue: message handler cannot be nil")
)
