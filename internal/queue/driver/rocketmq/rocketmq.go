package rocketmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func init() {
	queue.Register(config.DriverRocketMQ, New)
}

// RocketMQQueue implements queue.Queue for Apache RocketMQ.
type RocketMQQueue struct {
	cfg        *config.Config
	rCfg       config.RocketMQConfig
	producer   rocketmq.Producer
	closed     bool
	mu         sync.RWMutex
	inFlight   int64
	totalSent  int64
	activeCons int64
}

// New creates and initializes a new RocketMQ Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidMessage)
	}

	rCfg := cfg.Queue.RocketMQ
	if err := rCfg.Validate(); err != nil {
		return nil, fmt.Errorf("rocketmq: config validation failed: %w", err)
	}

	opts := []producer.Option{
		producer.WithNameServer(rCfg.NameServers),
		producer.WithGroupName(rCfg.Group),
		producer.WithRetry(2),
	}

	if rCfg.AccessKey != "" && rCfg.SecretKey != "" {
		opts = append(opts, producer.WithCredentials(primitive.Credentials{
			AccessKey:     rCfg.AccessKey,
			SecretKey:     rCfg.SecretKey,
			SecurityToken: rCfg.SecurityToken,
		}))
	}

	if rCfg.Namespace != "" {
		opts = append(opts, producer.WithNamespace(rCfg.Namespace))
	}

	prod, err := rocketmq.NewProducer(opts...)
	if err != nil {
		return nil, fmt.Errorf("rocketmq: create producer failed: %w", err)
	}

	if err := prod.Start(); err != nil {
		return nil, fmt.Errorf("rocketmq: start producer failed: %w", err)
	}

	return &RocketMQQueue{
		cfg:      cfg,
		rCfg:     rCfg,
		producer: prod,
	}, nil
}

// Driver returns DriverRocketMQ.
func (q *RocketMQQueue) Driver() config.QueueDriver {
	return config.DriverRocketMQ
}

// Name returns the configured RocketMQ topic.
func (q *RocketMQQueue) Name() string {
	return q.rCfg.Topic
}

// Producer returns a Producer client.
func (q *RocketMQQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &rmqProducer{q: q}, nil
}

// Consumer returns a Consumer client.
func (q *RocketMQQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &rmqConsumer{q: q}, nil
}

// Ping checks if RocketMQ is available.
func (q *RocketMQQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed || q.producer == nil {
		return queue.ErrQueueClosed
	}
	return nil
}

// Stats returns runtime RocketMQ metrics.
func (q *RocketMQQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	return queue.Stats{
		Driver:    config.DriverRocketMQ,
		Name:      q.rCfg.Topic,
		InFlight:  atomic.LoadInt64(&q.inFlight),
		Total:     atomic.LoadInt64(&q.totalSent),
		Consumers: int(atomic.LoadInt64(&q.activeCons)),
		Extra: map[string]any{
			"group":        q.rCfg.Group,
			"name_servers": q.rCfg.NameServers,
		},
	}, nil
}

// Close gracefully closes the RocketMQ producer.
func (q *RocketMQQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if q.producer != nil {
		_ = q.producer.Shutdown()
	}
	return nil
}

type rmqProducer struct {
	q *RocketMQQueue
}

func (p *rmqProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	topic := p.q.rCfg.Topic
	if po.Topic != "" {
		topic = po.Topic
	} else if msg.Topic != "" {
		topic = msg.Topic
	}

	rMsg := primitive.NewMessage(topic, msg.Payload)
	for k, v := range msg.Headers {
		rMsg.WithProperty(k, v)
	}
	for k, v := range po.Headers {
		rMsg.WithProperty(k, v)
	}
	if po.Key != "" {
		rMsg.WithKeys([]string{po.Key})
	} else if msg.Key != "" {
		rMsg.WithKeys([]string{msg.Key})
	}

	_, err := p.q.producer.SendSync(ctx, rMsg)
	if err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	atomic.AddInt64(&p.q.totalSent, 1)
	return nil
}

func (p *rmqProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *rmqProducer) Close() error {
	return nil
}

type rmqConsumer struct {
	q        *RocketMQQueue
	consumer rocketmq.PushConsumer
	mu       sync.Mutex
}

func (c *rmqConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
	if handler == nil {
		return queue.ErrNilHandler
	}

	var co queue.ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	topic := c.q.rCfg.Topic
	if co.Topic != "" {
		topic = co.Topic
	}

	consumerOpts := []consumer.Option{
		consumer.WithNameServer(c.q.rCfg.NameServers),
		consumer.WithGroupName(c.q.rCfg.Group),
	}

	if c.q.rCfg.AccessKey != "" && c.q.rCfg.SecretKey != "" {
		consumerOpts = append(consumerOpts, consumer.WithCredentials(primitive.Credentials{
			AccessKey:     c.q.rCfg.AccessKey,
			SecretKey:     c.q.rCfg.SecretKey,
			SecurityToken: c.q.rCfg.SecurityToken,
		}))
	}
	if c.q.rCfg.Namespace != "" {
		consumerOpts = append(consumerOpts, consumer.WithNamespace(c.q.rCfg.Namespace))
	}

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}
	consumerOpts = append(consumerOpts, consumer.WithConsumeGoroutineNums(concurrency))

	pushCons, err := rocketmq.NewPushConsumer(consumerOpts...)
	if err != nil {
		return fmt.Errorf("rocketmq: create push consumer failed: %w", err)
	}

	c.mu.Lock()
	c.consumer = pushCons
	c.mu.Unlock()

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	atomic.AddInt64(&c.q.activeCons, 1)
	defer atomic.AddInt64(&c.q.activeCons, -1)

	err = pushCons.Subscribe(topic, consumer.MessageSelector{}, func(cctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, ext := range msgs {
			atomic.AddInt64(&c.q.inFlight, 1)
			defer atomic.AddInt64(&c.q.inFlight, -1)

			qMsg := &queue.Message{
				ID:        ext.MsgId,
				Topic:     ext.Topic,
				Payload:   ext.Body,
				Headers:   ext.GetProperties(),
				Attempts:  int(ext.ReconsumeTimes) + 1,
				Timestamp: time.Unix(0, ext.BornTimestamp*int64(time.Millisecond)),
				Raw:       ext,
			}

			qMsg.SetAckFunc(func(context.Context) error { return nil })
			qMsg.SetNackFunc(func(context.Context, bool) error { return nil })

			if err := finalHandler(cctx, qMsg); err != nil {
				// Retries are handled inside the runtime engine; acknowledge so
				// RocketMQ does not re-deliver (and re-trigger DLQ publishing) again.
				return consumer.ConsumeSuccess, nil
			}
		}
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		return fmt.Errorf("rocketmq: subscribe failed: %w", err)
	}

	if err := pushCons.Start(); err != nil {
		return fmt.Errorf("rocketmq: start consumer failed: %w", err)
	}

	<-ctx.Done()
	_ = pushCons.Shutdown()
	return nil
}

func (c *rmqConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	return nil, fmt.Errorf("rocketmq: pull-based Receive is not supported in PushConsumer model")
}

func (c *rmqConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumer != nil {
		return c.consumer.Shutdown()
	}
	return nil
}
