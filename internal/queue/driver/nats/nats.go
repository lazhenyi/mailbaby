package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"

	nats "github.com/nats-io/nats.go"
)

func init() {
	queue.Register(config.DriverNATS, New)
}

// NATSQueue implements queue.Queue for NATS & NATS JetStream.
type NATSQueue struct {
	cfg        *config.Config
	nCfg       config.NATSConfig
	nc         *nats.Conn
	js         nats.JetStreamContext
	closed     bool
	mu         sync.RWMutex
	inFlight   int64
	totalSent  int64
	activeCons int64
}

// New creates and initializes a new NATS Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidMessage)
	}

	nCfg := cfg.Queue.NATS
	if err := nCfg.Validate(); err != nil {
		return nil, fmt.Errorf("nats: config validation failed: %w", err)
	}

	opts := []nats.Option{
		nats.Name("mailbaby_worker"),
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
	}

	if nCfg.Token != "" {
		opts = append(opts, nats.Token(nCfg.Token))
	} else if nCfg.Username != "" {
		opts = append(opts, nats.UserInfo(nCfg.Username, nCfg.Password))
	} else if nCfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(nCfg.CredsFile))
	}

	if nCfg.TLS.Enable {
		tlsConfig, err := buildTLSConfig(nCfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("nats: build tls config failed: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	servers := strings.Join(nCfg.Servers, ",")
	nc, err := nats.Connect(servers, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect failed: %w", err)
	}

	nq := &NATSQueue{
		cfg:  cfg,
		nCfg: nCfg,
		nc:   nc,
	}

	if nCfg.JetStream {
		js, err := nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("nats: init jetstream failed: %w", err)
		}
		nq.js = js
	}

	return nq, nil
}

// Driver returns DriverNATS.
func (q *NATSQueue) Driver() config.QueueDriver {
	return config.DriverNATS
}

// Name returns the configured NATS subject.
func (q *NATSQueue) Name() string {
	return q.nCfg.Subject
}

// Producer creates a new Producer client.
func (q *NATSQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &natsProducer{q: q}, nil
}

// Consumer creates a new Consumer client.
func (q *NATSQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &natsConsumer{q: q}, nil
}

// Ping checks if the NATS connection is active.
func (q *NATSQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed || q.nc == nil || !q.nc.IsConnected() {
		return queue.ErrQueueClosed
	}
	return nil
}

// Stats returns NATS connection statistics.
func (q *NATSQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed || q.nc == nil {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	nStats := q.nc.Stats()
	return queue.Stats{
		Driver:    config.DriverNATS,
		Name:      q.nCfg.Subject,
		InFlight:  atomic.LoadInt64(&q.inFlight),
		Total:     atomic.LoadInt64(&q.totalSent),
		Consumers: int(atomic.LoadInt64(&q.activeCons)),
		Extra: map[string]any{
			"jetstream":   q.nCfg.JetStream,
			"in_msgs":     nStats.InMsgs,
			"out_msgs":    nStats.OutMsgs,
			"reconnects":  nStats.Reconnects,
		},
	}, nil
}

// Close closes the NATS connection.
func (q *NATSQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if q.nc != nil {
		q.nc.Close()
	}
	return nil
}

type natsProducer struct {
	q *NATSQueue
}

func (p *natsProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	subject := p.q.nCfg.Subject
	if po.Topic != "" {
		subject = po.Topic
	} else if msg.Topic != "" {
		subject = msg.Topic
	}

	nMsg := &nats.Msg{
		Subject: subject,
		Data:    msg.Payload,
		Header:  make(nats.Header),
	}

	for k, v := range msg.Headers {
		nMsg.Header.Set(k, v)
	}
	for k, v := range po.Headers {
		nMsg.Header.Set(k, v)
	}
	if msg.ID != "" {
		nMsg.Header.Set("Nats-Msg-Id", msg.ID)
	}

	var err error
	if p.q.nCfg.JetStream && p.q.js != nil {
		_, err = p.q.js.PublishMsg(nMsg, nats.Context(ctx))
	} else {
		err = p.q.nc.PublishMsg(nMsg)
	}

	if err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	atomic.AddInt64(&p.q.totalSent, 1)
	return nil
}

func (p *natsProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *natsProducer) Close() error {
	return nil
}

type natsConsumer struct {
	q *NATSQueue
}

func (c *natsConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
	if handler == nil {
		return queue.ErrNilHandler
	}

	var co queue.ConsumeOptions
	for _, opt := range opts {
		opt(&co)
	}

	subject := c.q.nCfg.Subject
	if co.Topic != "" {
		subject = co.Topic
	}

	queueGroup := c.q.nCfg.QueueGroup
	if queueGroup == "" {
		queueGroup = "mailbaby_workers"
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	atomic.AddInt64(&c.q.activeCons, 1)
	defer atomic.AddInt64(&c.q.activeCons, -1)

	msgChan := make(chan *nats.Msg, 1024)

	var sub *nats.Subscription
	var err error

	if c.q.nCfg.JetStream && c.q.js != nil {
		subOpts := []nats.SubOpt{
			nats.ManualAck(),
		}
		if c.q.nCfg.Durable != "" {
			subOpts = append(subOpts, nats.Durable(c.q.nCfg.Durable))
		}
		sub, err = c.q.js.QueueSubscribeSync(subject, queueGroup, subOpts...)
	} else {
		sub, err = c.q.nc.QueueSubscribeSync(subject, queueGroup)
	}

	if err != nil {
		return fmt.Errorf("nats: subscribe failed: %w", err)
	}
	defer sub.Unsubscribe()

	concurrency := 1
	if co.Concurrency > 0 {
		concurrency = co.Concurrency
	} else if c.q.cfg != nil && c.q.cfg.Queue.Concurrency > 0 {
		concurrency = c.q.cfg.Queue.Concurrency
	}

	var wg sync.WaitGroup
	// Worker pool
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case nMsg, ok := <-msgChan:
					if !ok {
						return
					}
					c.processMsg(ctx, nMsg, finalHandler, co)
				}
			}
		}()
	}

	// Fetcher loop
	go func() {
		defer close(msgChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			nMsg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(50 * time.Millisecond)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case msgChan <- nMsg:
			}
		}
	}()

	wg.Wait()
	return nil
}

func (c *natsConsumer) processMsg(ctx context.Context, nMsg *nats.Msg, handler queue.Handler, co queue.ConsumeOptions) {
	atomic.AddInt64(&c.q.inFlight, 1)
	defer atomic.AddInt64(&c.q.inFlight, -1)

	headers := make(map[string]string)
	var msgID string
	for k, vals := range nMsg.Header {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
		if k == "Nats-Msg-Id" && len(vals) > 0 {
			msgID = vals[0]
		}
	}
	if msgID == "" {
		msgID = fmt.Sprintf("nats-%d", time.Now().UnixNano())
	}

	qMsg := &queue.Message{
		ID:        msgID,
		Topic:     nMsg.Subject,
		Payload:   nMsg.Data,
		Headers:   headers,
		Timestamp: time.Now(),
		Attempts:  1,
		Raw:       nMsg,
	}

	qMsg.SetAckFunc(func(cctx context.Context) error {
		return nMsg.Ack()
	})
	qMsg.SetNackFunc(func(cctx context.Context, requeue bool) error {
		if c.q.nCfg.JetStream {
			return nMsg.Nak()
		}
		return nil
	})

	if co.AutoAck {
		_ = qMsg.Ack(ctx)
	}

	err := handler(ctx, qMsg)
	if err == nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Ack(ctx)
	} else if err != nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Nack(ctx, true)
	}
}

func (c *natsConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	var ro queue.ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	timeout := 5 * time.Second
	if ro.Timeout > 0 {
		timeout = ro.Timeout
	}

	sub, err := c.q.nc.SubscribeSync(c.q.nCfg.Subject)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	nMsg, err := sub.NextMsg(timeout)
	if err != nil {
		if err == nats.ErrTimeout {
			return nil, queue.ErrTimeout
		}
		return nil, err
	}

	qMsg := &queue.Message{
		ID:        nMsg.Header.Get("Nats-Msg-Id"),
		Topic:     nMsg.Subject,
		Payload:   nMsg.Data,
		Timestamp: time.Now(),
		Attempts:  1,
		Raw:       nMsg,
	}
	qMsg.SetAckFunc(func(ctx context.Context) error { return nMsg.Ack() })
	qMsg.SetNackFunc(func(ctx context.Context, requeue bool) error { return nil })
	return qMsg, nil
}

func (c *natsConsumer) Close() error {
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
