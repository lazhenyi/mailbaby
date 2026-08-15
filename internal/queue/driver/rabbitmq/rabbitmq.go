package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"

	amqp "github.com/rabbitmq/amqp091-go"
)

func init() {
	queue.Register(config.DriverRabbitMQ, New)
}

// RabbitMQQueue implements queue.Queue for RabbitMQ (AMQP 0-9-1).
type RabbitMQQueue struct {
	cfg        *config.Config
	rCfg       config.RabbitMQConfig
	conn       *amqp.Connection
	ch         *amqp.Channel
	url        string
	mu         sync.RWMutex
	closed     bool
	inFlight   int64
	totalSent  int64
	activeCons int64
}

// New creates and initializes a new RabbitMQ Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidMessage)
	}

	rCfg := cfg.Queue.RabbitMQ
	if err := rCfg.Validate(); err != nil {
		return nil, fmt.Errorf("rabbitmq: config validation failed: %w", err)
	}

	amqpURL := rCfg.URL
	if amqpURL == "" {
		scheme := "amqp"
		if rCfg.TLS.Enable {
			scheme = "amqps"
		}
		auth := ""
		if rCfg.Username != "" {
			auth = rCfg.Username
			if rCfg.Password != "" {
				auth += ":" + rCfg.Password
			}
			auth += "@"
		}
		vhost := rCfg.VHost
		if !strings.HasPrefix(vhost, "/") {
			vhost = "/" + vhost
		}
		amqpURL = fmt.Sprintf("%s://%s%s:%d%s", scheme, auth, rCfg.Host, rCfg.Port, vhost)
	}

	var amqpConfig amqp.Config
	if rCfg.TLS.Enable {
		tlsConf, err := buildTLSConfig(rCfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq: build tls config failed: %w", err)
		}
		amqpConfig.TLSClientConfig = tlsConf
	}

	conn, err := amqp.DialConfig(amqpURL, amqpConfig)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq: open channel failed: %w", err)
	}

	rq := &RabbitMQQueue{
		cfg:   cfg,
		rCfg:  rCfg,
		conn:  conn,
		ch:    ch,
		url:   amqpURL,
	}

	if err := rq.setupTopology(); err != nil {
		_ = rq.Close()
		return nil, fmt.Errorf("rabbitmq: setup topology failed: %w", err)
	}

	return rq, nil
}

func (q *RabbitMQQueue) setupTopology() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var args amqp.Table
	if len(q.rCfg.Args) > 0 {
		args = amqp.Table(q.rCfg.Args)
	}

	if q.rCfg.Exchange != "" {
		if err := q.ch.ExchangeDeclare(
			q.rCfg.Exchange,
			"topic",
			q.rCfg.Durable,
			q.rCfg.AutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("exchange declare failed: %w", err)
		}
	}

	if q.rCfg.Queue != "" {
		_, err := q.ch.QueueDeclare(
			q.rCfg.Queue,
			q.rCfg.Durable,
			q.rCfg.AutoDelete,
			q.rCfg.Exclusive,
			q.rCfg.NoWait,
			args,
		)
		if err != nil {
			return fmt.Errorf("queue declare failed: %w", err)
		}

		if q.rCfg.Exchange != "" {
			routingKey := q.rCfg.RoutingKey
			if routingKey == "" {
				routingKey = "#"
			}
			if err := q.ch.QueueBind(
				q.rCfg.Queue,
				routingKey,
				q.rCfg.Exchange,
				false,
				nil,
			); err != nil {
				return fmt.Errorf("queue bind failed: %w", err)
			}
		}
	}

	return nil
}

// Driver returns DriverRabbitMQ.
func (q *RabbitMQQueue) Driver() config.QueueDriver {
	return config.DriverRabbitMQ
}

// Name returns the configured queue name.
func (q *RabbitMQQueue) Name() string {
	return q.rCfg.Queue
}

// Producer creates a new Producer client.
func (q *RabbitMQQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}

	ch, err := q.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: create producer channel failed: %w", err)
	}

	return &rabbitProducer{
		q:  q,
		ch: ch,
	}, nil
}

// Consumer creates a new Consumer client.
func (q *RabbitMQQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}

	ch, err := q.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: create consumer channel failed: %w", err)
	}

	return &rabbitConsumer{
		q:  q,
		ch: ch,
	}, nil
}

// Ping checks if the RabbitMQ connection is active.
func (q *RabbitMQQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed || q.conn == nil || q.conn.IsClosed() {
		return queue.ErrQueueClosed
	}
	return nil
}

// Stats returns runtime queue depth and consumer count.
func (q *RabbitMQQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed || q.ch == nil {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	info, err := q.ch.QueueInspect(q.rCfg.Queue)
	if err != nil {
		return queue.Stats{
			Driver:    config.DriverRabbitMQ,
			Name:      q.rCfg.Queue,
			Consumers: int(atomic.LoadInt64(&q.activeCons)),
			InFlight:  atomic.LoadInt64(&q.inFlight),
			Total:     atomic.LoadInt64(&q.totalSent),
		}, nil
	}

	return queue.Stats{
		Driver:    config.DriverRabbitMQ,
		Name:      q.rCfg.Queue,
		Ready:     int64(info.Messages),
		Consumers: info.Consumers,
		InFlight:  atomic.LoadInt64(&q.inFlight),
		Total:     atomic.LoadInt64(&q.totalSent),
		Extra: map[string]any{
			"exchange": q.rCfg.Exchange,
			"vhost":    q.rCfg.VHost,
		},
	}, nil
}

// Close gracefully closes the RabbitMQ channel and connection.
func (q *RabbitMQQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true

	var errs []string
	if q.ch != nil {
		if err := q.ch.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if q.conn != nil {
		if err := q.conn.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("rabbitmq close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

type rabbitProducer struct {
	q  *RabbitMQQueue
	ch *amqp.Channel
	mu sync.Mutex
}

func (p *rabbitProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	exchange := p.q.rCfg.Exchange
	routingKey := p.q.rCfg.RoutingKey
	if po.Topic != "" {
		routingKey = po.Topic
	} else if msg.Topic != "" {
		routingKey = msg.Topic
	}
	if po.Key != "" {
		routingKey = po.Key
	}

	table := make(amqp.Table)
	for k, v := range msg.Headers {
		table[k] = v
	}
	for k, v := range po.Headers {
		table[k] = v
	}

	publishing := amqp.Publishing{
		Headers:         table,
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
		MessageId:       msg.ID,
		Timestamp:       msg.Timestamp,
		Body:            msg.Payload,
	}

	if ct, ok := table["Content-Type"].(string); ok && ct != "" {
		publishing.ContentType = ct
	}

	p.mu.Lock()
	err := p.ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		publishing,
	)
	p.mu.Unlock()

	if err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	atomic.AddInt64(&p.q.totalSent, 1)
	return nil
}

func (p *rabbitProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *rabbitProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		return p.ch.Close()
	}
	return nil
}

type rabbitConsumer struct {
	q  *RabbitMQQueue
	ch *amqp.Channel
	mu sync.Mutex
}

func (c *rabbitConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
	if handler == nil {
		return queue.ErrNilHandler
	}

	var co queue.ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}

	prefetch := 10
	if co.PrefetchCount > 0 {
		prefetch = co.PrefetchCount
	} else if c.q.rCfg.PrefetchCount > 0 {
		prefetch = c.q.rCfg.PrefetchCount
	}

	c.mu.Lock()
	if err := c.ch.Qos(prefetch, 0, false); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("rabbitmq: set qos failed: %w", err)
	}

	queueName := c.q.rCfg.Queue
	if co.Topic != "" {
		queueName = co.Topic
	}

	deliveries, err := c.ch.ConsumeWithContext(
		ctx,
		queueName,
		"",
		c.q.rCfg.AutoAck || co.AutoAck,
		c.q.rCfg.Exclusive,
		false,
		c.q.rCfg.NoWait,
		nil,
	)
	c.mu.Unlock()

	if err != nil {
		return fmt.Errorf("rabbitmq: consume failed: %w", err)
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	atomic.AddInt64(&c.q.activeCons, int64(concurrency))
	defer atomic.AddInt64(&c.q.activeCons, -int64(concurrency))

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					c.processDelivery(ctx, d, finalHandler, co)
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *rabbitConsumer) processDelivery(ctx context.Context, d amqp.Delivery, handler queue.Handler, co queue.ConsumeOptions) {
	atomic.AddInt64(&c.q.inFlight, 1)
	defer atomic.AddInt64(&c.q.inFlight, -1)

	headers := make(map[string]string, len(d.Headers))
	for k, v := range d.Headers {
		headers[k] = fmt.Sprintf("%v", v)
	}

	msg := &queue.Message{
		ID:        d.MessageId,
		Topic:     d.RoutingKey,
		Payload:   d.Body,
		Headers:   headers,
		Timestamp: d.Timestamp,
		Attempts:  1,
		Raw:       d,
	}
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("amqp-%d", d.DeliveryTag)
	}

	msg.SetAckFunc(func(ctx context.Context) error {
		return d.Ack(false)
	})
	msg.SetNackFunc(func(ctx context.Context, requeue bool) error {
		return d.Nack(false, requeue)
	})

	if co.AutoAck || c.q.rCfg.AutoAck {
		_ = msg.Ack(ctx)
	}

	err := handler(ctx, msg)
	if err != nil && !msg.IsAcknowledged() {
		_ = msg.Nack(ctx, true)
	} else if err == nil && !msg.IsAcknowledged() {
		_ = msg.Ack(ctx)
	}
}

func (c *rabbitConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	var ro queue.ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	msg, ok, err := c.ch.Get(c.q.rCfg.Queue, c.q.rCfg.AutoAck)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, queue.ErrTimeout
	}

	headers := make(map[string]string, len(msg.Headers))
	for k, v := range msg.Headers {
		headers[k] = fmt.Sprintf("%v", v)
	}

	qMsg := &queue.Message{
		ID:        msg.MessageId,
		Topic:     msg.RoutingKey,
		Payload:   msg.Body,
		Headers:   headers,
		Timestamp: msg.Timestamp,
		Attempts:  1,
		Raw:       msg,
	}
	qMsg.SetAckFunc(func(ctx context.Context) error { return msg.Ack(false) })
	qMsg.SetNackFunc(func(ctx context.Context, requeue bool) error { return msg.Nack(false, requeue) })

	return qMsg, nil
}

func (c *rabbitConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ch != nil {
		return c.ch.Close()
	}
	return nil
}

func buildTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		ServerName:         cfg.ServerName,
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file failed: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load cert/key failed: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
