package pulsar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/queue/driver/common"

	"github.com/apache/pulsar-client-go/pulsar"
)

func init() {
	queue.Register(config.DriverPulsar, New)
}

// PulsarQueue implements queue.Queue for Apache Pulsar.
type PulsarQueue struct {
	cfg    *config.Config
	pCfg   config.PulsarConfig
	client pulsar.Client
	closed bool
	mu     sync.RWMutex
	common.BaseStats
}

// New creates and initializes a new Apache Pulsar Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidConfig)
	}

	pCfg := cfg.Queue.Pulsar
	if err := pCfg.Validate(); err != nil {
		return nil, fmt.Errorf("pulsar: config validation failed: %w", err)
	}

	clientOpts := pulsar.ClientOptions{
		URL:               pCfg.URL,
		ConnectionTimeout: 10 * time.Second,
		OperationTimeout:  10 * time.Second,
	}

	if pCfg.AuthToken != "" {
		clientOpts.Authentication = pulsar.NewAuthenticationToken(pCfg.AuthToken)
	}

	if pCfg.TLS.Enable {
		clientOpts.TLSTrustCertsFilePath = pCfg.TLS.CAFile
		clientOpts.TLSAllowInsecureConnection = pCfg.TLS.InsecureSkipVerify
	}

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("pulsar: create client failed: %w", err)
	}

	return &PulsarQueue{
		cfg:    cfg,
		pCfg:   pCfg,
		client: client,
	}, nil
}

// Driver returns DriverPulsar.
func (q *PulsarQueue) Driver() config.QueueDriver {
	return config.DriverPulsar
}

// Name returns the configured Pulsar topic.
func (q *PulsarQueue) Name() string {
	return q.pCfg.Topic
}

// Producer creates a new Producer client.
func (q *PulsarQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}

	prod, err := q.client.CreateProducer(pulsar.ProducerOptions{
		Topic: q.pCfg.Topic,
	})
	if err != nil {
		return nil, fmt.Errorf("pulsar: create producer failed: %w", err)
	}

	return &pulsarProducer{
		q:    q,
		prod: prod,
	}, nil
}

// Consumer creates a new Consumer client.
func (q *PulsarQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}

	var subType pulsar.SubscriptionType
	switch strings.ToLower(q.pCfg.SubscriptionType) {
	case "exclusive":
		subType = pulsar.Exclusive
	case "failover":
		subType = pulsar.Failover
	case "key_shared":
		subType = pulsar.KeyShared
	default:
		subType = pulsar.Shared
	}

	cons, err := q.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            q.pCfg.Topic,
		SubscriptionName: q.pCfg.SubscriptionName,
		Type:             subType,
	})
	if err != nil {
		return nil, fmt.Errorf("pulsar: create consumer failed: %w", err)
	}

	return &pulsarConsumer{
		q:    q,
		cons: cons,
	}, nil
}

// Ping checks if Pulsar is active.
func (q *PulsarQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed || q.client == nil {
		return queue.ErrQueueClosed
	}
	return nil
}

// Stats returns Pulsar queue statistics.
func (q *PulsarQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	_, totalSent, consumers := q.Snapshot()
	return queue.Stats{
		Driver:    config.DriverPulsar,
		Name:      q.pCfg.Topic,
		InFlight:  q.InFlight,
		Total:     totalSent,
		Consumers: consumers,
		Extra: map[string]any{
			"subscription":      q.pCfg.SubscriptionName,
			"subscription_type": q.pCfg.SubscriptionType,
		},
	}, nil
}

// Close closes the Pulsar client.
func (q *PulsarQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if q.client != nil {
		q.client.Close()
	}
	return nil
}

type pulsarProducer struct {
	q    *PulsarQueue
	prod pulsar.Producer
	mu   sync.Mutex
}

func (p *pulsarProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	props := make(map[string]string)
	for k, v := range msg.Headers {
		props[k] = v
	}
	for k, v := range po.Headers {
		props[k] = v
	}

	key := msg.Key
	if po.Key != "" {
		key = po.Key
	}

	pMsg := &pulsar.ProducerMessage{
		Payload:    msg.Payload,
		Key:        key,
		Properties: props,
		EventTime:  msg.Timestamp,
	}
	if msg.ID != "" {
		pMsg.Properties["X-Message-ID"] = msg.ID
	}
	if po.Delay > 0 {
		pMsg.DeliverAfter = po.Delay
	} else if msg.Delay > 0 {
		pMsg.DeliverAfter = msg.Delay
	}

	p.mu.Lock()
	_, err := p.prod.Send(ctx, pMsg)
	p.mu.Unlock()

	if err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	p.q.IncTotalSent(1)
	return nil
}

func (p *pulsarProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *pulsarProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prod != nil {
		p.prod.Close()
	}
	return nil
}

type pulsarConsumer struct {
	q    *PulsarQueue
	cons pulsar.Consumer
	mu   sync.Mutex
}

func (c *pulsarConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
	if handler == nil {
		return queue.ErrNilHandler
	}

	var co queue.ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}

	c.q.IncActiveCons(concurrency)
	defer c.q.DecActiveCons(concurrency)

	backoff := common.NewBackoff(100*time.Millisecond, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				pMsg, err := c.cons.Receive(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if !backoff.Wait(ctx) {
						return
					}
					continue
				}

				c.processMessage(ctx, pMsg, finalHandler, co)
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *pulsarConsumer) processMessage(ctx context.Context, pMsg pulsar.Message, handler queue.Handler, co queue.ConsumeOptions) {
	c.q.IncInFlight()
	defer c.q.DecInFlight()

	props := pMsg.Properties()
	msgID := ""
	if props != nil {
		msgID = props["X-Message-ID"]
	}
	if msgID == "" {
		msgID = fmt.Sprintf("%v", pMsg.ID())
	}

	qMsg := &queue.Message{
		ID:        msgID,
		Topic:     pMsg.Topic(),
		Payload:   pMsg.Payload(),
		Headers:   props,
		Key:       pMsg.Key(),
		Timestamp: pMsg.PublishTime(),
		Attempts:  int(pMsg.RedeliveryCount()) + 1,
		Raw:       pMsg,
	}

	qMsg.SetAckFunc(func(context.Context) error {
		return c.cons.Ack(pMsg)
	})
	qMsg.SetNackFunc(func(context.Context, bool) error {
		c.cons.Nack(pMsg)
		return nil
	})

	if co.AutoAck {
		_ = qMsg.Ack(ctx)
	}

	err := handler(ctx, qMsg)
	if err == nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Ack(ctx)
	} else if err != nil && !qMsg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; ack so the message
		// is not redelivered indefinitely.
		_ = qMsg.Ack(ctx)
	}
}

func (c *pulsarConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	var ro queue.ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	timeout := 5 * time.Second
	if ro.Timeout > 0 {
		timeout = ro.Timeout
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pMsg, err := c.cons.Receive(ctxTimeout)
	if err != nil {
		if ctxTimeout.Err() != nil {
			return nil, queue.ErrTimeout
		}
		return nil, err
	}

	qMsg := &queue.Message{
		ID:        fmt.Sprintf("%v", pMsg.ID()),
		Topic:     pMsg.Topic(),
		Payload:   pMsg.Payload(),
		Headers:   pMsg.Properties(),
		Key:       pMsg.Key(),
		Timestamp: pMsg.PublishTime(),
		Attempts:  1,
		Raw:       pMsg,
	}
	qMsg.SetAckFunc(func(context.Context) error { return c.cons.Ack(pMsg) })
	qMsg.SetNackFunc(func(context.Context, bool) error { c.cons.Nack(pMsg); return nil })
	return qMsg, nil
}

func (c *pulsarConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cons != nil {
		c.cons.Close()
	}
	return nil
}
