package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/metrics"
	"mailbaby/internal/tracing"
)

func init() {
	Register(config.DriverMemory, newMemoryQueue)
}

// sendMessage delivers msg to the channel, blocking until the queue accepts it.
// It returns false (instead of panicking) if the channel was closed concurrently.
func sendMessage(ch chan *Message, msg *Message) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	ch <- msg
	return true
}

func newMemoryQueue(cfg *config.Config) (Queue, error) {
	bufSize := 1024
	if cfg != nil && cfg.Queue.Memory.BufferSize > 0 {
		bufSize = cfg.Queue.Memory.BufferSize
	}
	return NewMemoryQueue("memory_default", bufSize, cfg), nil
}

// MemoryQueue is an in-memory Queue implementation backed by Go channels.
type MemoryQueue struct {
	name      string
	ch        chan *Message
	cfg       *config.Config
	closed    bool
	mu        sync.RWMutex
	closeOnce sync.Once

	consumersTotal int64
	inFlight       int64
	totalPublished int64
}

// NewMemoryQueue constructs an in-memory queue instance.
func NewMemoryQueue(name string, bufferSize int, cfg *config.Config) *MemoryQueue {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	if name == "" {
		name = "memory_default"
	}
	return &MemoryQueue{
		name: name,
		ch:   make(chan *Message, bufferSize),
		cfg:  cfg,
	}
}

// Driver returns DriverMemory.
func (q *MemoryQueue) Driver() config.QueueDriver {
	return config.DriverMemory
}

// Name returns the queue name.
func (q *MemoryQueue) Name() string {
	return q.name
}

// Producer returns a Producer client for this MemoryQueue.
func (q *MemoryQueue) Producer() (Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	return &memoryProducer{q: q}, nil
}

// Consumer returns a Consumer client for this MemoryQueue.
func (q *MemoryQueue) Consumer() (Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	return &memoryConsumer{q: q}, nil
}

// Ping checks if the queue is active.
func (q *MemoryQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrQueueClosed
	}
	return nil
}

// Stats returns the current runtime metrics for this memory queue.
func (q *MemoryQueue) Stats(ctx context.Context) (Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ready := int64(len(q.ch))
	return Stats{
		Driver:    config.DriverMemory,
		Name:      q.name,
		Ready:     ready,
		InFlight:  atomic.LoadInt64(&q.inFlight),
		Total:     atomic.LoadInt64(&q.totalPublished),
		Consumers: int(atomic.LoadInt64(&q.consumersTotal)),
		Extra: map[string]any{
			"buffer_capacity": cap(q.ch),
			"closed":          q.closed,
		},
	}, nil
}

// Close closes the MemoryQueue.
func (q *MemoryQueue) Close() error {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.ch)
		q.mu.Unlock()
	})
	return nil
}

type memoryProducer struct {
	q *MemoryQueue
}

func (p *memoryProducer) Publish(ctx context.Context, msg *Message, opts ...PublishOption) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	p.q.mu.RLock()
	if p.q.closed {
		p.q.mu.RUnlock()
		return ErrQueueClosed
	}
	p.q.mu.RUnlock()

	var po PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	if po.Topic != "" {
		msg.Topic = po.Topic
	} else if msg.Topic == "" {
		msg.Topic = p.q.name
	}
	if po.Key != "" {
		msg.Key = po.Key
	}
	if po.Delay > 0 {
		msg.Delay = po.Delay
	}
	if len(po.Headers) > 0 {
		for k, v := range po.Headers {
			msg.SetHeader(k, v)
		}
	}

	if po.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, po.Timeout)
		defer cancel()
	}

	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	tracing.InjectHeaders(ctx, msg.Headers)

	publishStart := time.Now()
	publishItem := func(m *Message) error {
		if err := ctx.Err(); err != nil {
			metrics.Get().ObserveQueuePublish(string(config.DriverMemory), m.Topic, "failed", time.Since(publishStart))
			return err
		}
		if !sendMessage(p.q.ch, m) {
			metrics.Get().ObserveQueuePublish(string(config.DriverMemory), m.Topic, "failed", time.Since(publishStart))
			return ErrQueueClosed
		}
		atomic.AddInt64(&p.q.totalPublished, 1)
		metrics.Get().ObserveQueuePublish(string(config.DriverMemory), m.Topic, "success", time.Since(publishStart))
		return nil
	}

	if msg.Delay > 0 {
		go func(m *Message, delay time.Duration) {
			select {
			case <-time.After(delay):
				_ = publishItem(m)
			case <-ctx.Done():
			}
		}(msg.Clone(), msg.Delay)
		return nil
	}

	return publishItem(msg.Clone())
}

func (p *memoryProducer) PublishBatch(ctx context.Context, msgs []*Message, opts ...PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return fmt.Errorf("%w: %v", ErrPublishFailed, err)
		}
	}
	return nil
}

func (p *memoryProducer) Close() error {
	return nil
}

type memoryConsumer struct {
	q *MemoryQueue
}

func (c *memoryConsumer) Consume(ctx context.Context, handler Handler, opts ...ConsumeOption) error {
	if handler == nil {
		return ErrNilHandler
	}

	var co ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = Chain(co.Middlewares...)(handler)
	}

	atomic.AddInt64(&c.q.consumersTotal, int64(concurrency))
	defer atomic.AddInt64(&c.q.consumersTotal, -int64(concurrency))

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-c.q.ch:
					if !ok {
						return
					}
					c.processMessage(ctx, msg, finalHandler, co)
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *memoryConsumer) processMessage(ctx context.Context, msg *Message, handler Handler, co ConsumeOptions) {
	atomic.AddInt64(&c.q.inFlight, 1)
	defer atomic.AddInt64(&c.q.inFlight, -1)

	// Extract W3C TraceContext if available
	msgCtx := tracing.ExtractHeaders(ctx, msg.Headers)

	// Record Queue Lag metric
	if !msg.Timestamp.IsZero() {
		metrics.Get().ObserveQueueLag(string(config.DriverMemory), msg.Topic, time.Since(msg.Timestamp))
	}

	msg.SetAckFunc(func(ctx context.Context) error {
		return nil
	})
	msg.SetNackFunc(func(ctx context.Context, requeue bool) error {
		if requeue {
			requeued := msg.Clone()
			requeued.Attempts++
			// Best-effort requeue; never panic if the queue is closed concurrently.
			if !sendMessage(c.q.ch, requeued) {
				return ErrQueueClosed
			}
		}
		return nil
	})

	if co.AutoAck {
		_ = msg.Ack(ctx)
	}

	err := handler(msgCtx, msg)
	if err != nil && !msg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; the broker only needs
		// to drop the message once processing exhausts its retry budget.
		_ = msg.Ack(ctx)
	} else if err == nil && !msg.IsAcknowledged() {
		_ = msg.Ack(ctx)
	}
}

func (c *memoryConsumer) Receive(ctx context.Context, opts ...ReceiveOption) (*Message, error) {
	var ro ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	timeout := 5 * time.Second
	if ro.Timeout > 0 {
		timeout = ro.Timeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	case msg, ok := <-c.q.ch:
		if !ok {
			return nil, ErrQueueClosed
		}
		msg.SetAckFunc(func(ctx context.Context) error { return nil })
		msg.SetNackFunc(func(ctx context.Context, requeue bool) error {
			if requeue && !sendMessage(c.q.ch, msg.Clone()) {
				return ErrQueueClosed
			}
			return nil
		})
		return msg, nil
	}
}

func (c *memoryConsumer) Close() error {
	return nil
}
