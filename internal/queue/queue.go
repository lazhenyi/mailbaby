package queue

import (
	"context"
	"mailbaby/internal/config"
)

// Handler is a function type that processes a consumed message.
// Returning nil indicates successful processing.
// Returning a non-nil error will trigger the negative acknowledgment (Nack) or retry policy.
type Handler func(ctx context.Context, msg *Message) error

// MessageHandler is an interface for processing incoming messages.
type MessageHandler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc is an adapter allowing the use of ordinary functions as MessageHandlers.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f(ctx, msg).
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}

// Producer defines the contract for publishing messages to a message queue.
type Producer interface {
	// Publish sends a single message to the queue/topic.
	Publish(ctx context.Context, msg *Message, opts ...PublishOption) error

	// PublishBatch sends multiple messages in a single batch operation.
	PublishBatch(ctx context.Context, msgs []*Message, opts ...PublishOption) error

	// Close closes the producer and flushes any buffered messages.
	Close() error
}

// Consumer defines the contract for receiving and processing messages from a queue.
type Consumer interface {
	// Consume starts a continuous subscription loop, dispatching received messages to the Handler.
	// This call blocks until ctx is canceled or an unrecoverable error occurs.
	Consume(ctx context.Context, handler Handler, opts ...ConsumeOption) error

	// Receive pulls a single message from the queue in a pull-based fashion.
	// Returns ErrTimeout if no message arrives within the specified timeout.
	Receive(ctx context.Context, opts ...ReceiveOption) (*Message, error)

	// Close shuts down the consumer, waiting for in-flight messages to complete.
	Close() error
}

// Queue is the unified interface representing a message broker instance.
// It manages client connections and acts as a factory for Producer and Consumer instances.
type Queue interface {
	// Driver returns the identifier of this queue driver (e.g. rabbitmq, kafka, redis, memory).
	Driver() config.QueueDriver

	// Name returns the primary topic or queue name associated with this queue instance.
	Name() string

	// Producer returns a Producer client for this queue.
	Producer() (Producer, error)

	// Consumer returns a Consumer client for this queue.
	Consumer() (Consumer, error)

	// Ping verifies network connectivity to the underlying message broker.
	Ping(ctx context.Context) error

	// Stats retrieves runtime queue metrics and depth.
	Stats(ctx context.Context) (Stats, error)

	// Close closes the queue and releases underlying connections and resources.
	Close() error
}
