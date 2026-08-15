package queue

import "time"

// PublishOptions holds configuration for publishing a message.
type PublishOptions struct {
	// Topic overrides the default topic/queue for this publish operation.
	Topic string

	// Key is the routing/partition key.
	Key string

	// Delay specifies delivery delay.
	Delay time.Duration

	// Headers adds extra headers for the message during publish.
	Headers map[string]string

	// Timeout specifies timeout for the publish call.
	Timeout time.Duration
}

// PublishOption is a functional option for publishing.
type PublishOption func(*PublishOptions)

// WithTopic overrides the default destination topic or queue name.
func WithTopic(topic string) PublishOption {
	return func(o *PublishOptions) {
		o.Topic = topic
	}
}

// WithKey sets the partition/routing key.
func WithKey(key string) PublishOption {
	return func(o *PublishOptions) {
		o.Key = key
	}
}

// WithDelay specifies a delivery delay.
func WithDelay(delay time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.Delay = delay
	}
}

// WithHeader adds a single header key-value pair.
func WithHeader(key, value string) PublishOption {
	return func(o *PublishOptions) {
		if o.Headers == nil {
			o.Headers = make(map[string]string)
		}
		o.Headers[key] = value
	}
}

// WithHeaders sets or merges multiple headers.
func WithHeaders(headers map[string]string) PublishOption {
	return func(o *PublishOptions) {
		if o.Headers == nil {
			o.Headers = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			o.Headers[k] = v
		}
	}
}

// WithPublishTimeout sets the maximum duration to wait for the publish operation.
func WithPublishTimeout(timeout time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.Timeout = timeout
	}
}

// ConsumeOptions holds configuration for consuming messages.
type ConsumeOptions struct {
	// Topic overrides the default topic/queue to consume from.
	Topic string

	// Concurrency is the number of concurrent worker goroutines.
	Concurrency int

	// AutoAck specifies whether messages are automatically acknowledged upon receipt.
	AutoAck bool

	// MaxRetries is the maximum retry count on failure.
	MaxRetries int

	// RetryInterval is the delay before retrying a failed message.
	RetryInterval time.Duration

	// PrefetchCount is the number of messages to prefetch from the broker.
	PrefetchCount int

	// BatchSize is the batch size for batch processing.
	BatchSize int

	// Middlewares are applied in order around the message Handler.
	Middlewares []Middleware
}

// ConsumeOption is a functional option for configuring a consumer.
type ConsumeOption func(*ConsumeOptions)

// WithConsumeTopic specifies the topic/queue to consume.
func WithConsumeTopic(topic string) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.Topic = topic
	}
}

// WithConcurrency sets the number of concurrent worker goroutines.
func WithConcurrency(concurrency int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.Concurrency = concurrency
	}
}

// WithAutoAck sets whether messages are automatically acknowledged.
func WithAutoAck(autoAck bool) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.AutoAck = autoAck
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(retries int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.MaxRetries = retries
	}
}

// WithRetryInterval sets the duration to wait before retrying.
func WithRetryInterval(interval time.Duration) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.RetryInterval = interval
	}
}

// WithPrefetchCount sets the prefetch buffer size.
func WithPrefetchCount(count int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.PrefetchCount = count
	}
}

// WithBatchSize sets the batch size for batch processing.
func WithBatchSize(batchSize int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.BatchSize = batchSize
	}
}

// WithMiddlewares attaches middleware interceptors to the consumer.
func WithMiddlewares(middlewares ...Middleware) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.Middlewares = append(o.Middlewares, middlewares...)
	}
}

// ReceiveOptions holds configuration for pull-based message receiving.
type ReceiveOptions struct {
	// Timeout is the maximum duration to wait for a message.
	Timeout time.Duration
}

// ReceiveOption is a functional option for pull-based receiving.
type ReceiveOption func(*ReceiveOptions)

// WithReceiveTimeout sets the timeout for pulling a single message.
func WithReceiveTimeout(timeout time.Duration) ReceiveOption {
	return func(o *ReceiveOptions) {
		o.Timeout = timeout
	}
}
