package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/queue/driver/common"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func init() {
	queue.Register(config.DriverKafka, New)
}

// KafkaQueue implements queue.Queue for Apache Kafka.
type KafkaQueue struct {
	cfg        *config.Config
	kCfg       config.KafkaConfig
	writer     *kafka.Writer
	dialer     *kafka.Dialer
	transport  *kafka.Transport
	closed     bool
	mu         sync.RWMutex
	common.BaseStats
}

// New creates and initializes a Kafka Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidConfig)
	}

	kCfg := cfg.Queue.Kafka
	if err := kCfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: config validation failed: %w", err)
	}

	dialer, transport, err := setupKafkaTransport(kCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: setup transport failed: %w", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kCfg.Brokers...),
		Topic:        kCfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		Transport:    transport,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaQueue{
		cfg:       cfg,
		kCfg:      kCfg,
		writer:    writer,
		dialer:    dialer,
		transport: transport,
	}, nil
}

func setupKafkaTransport(kCfg config.KafkaConfig) (*kafka.Dialer, *kafka.Transport, error) {
	var tlsConfig *tls.Config
	if kCfg.TLS.Enable {
		var err error
		tlsConfig, err = buildTLSConfig(kCfg.TLS)
		if err != nil {
			return nil, nil, err
		}
	}

	var mechanism sasl.Mechanism
	if kCfg.SASL.Enable {
		mech := strings.ToUpper(strings.TrimSpace(kCfg.SASL.Mechanism))
		switch mech {
		case "PLAIN", "":
			mechanism = plain.Mechanism{
				Username: kCfg.SASL.User,
				Password: kCfg.SASL.Password,
			}
		case "SCRAM-SHA-256":
			var err error
			mechanism, err = scram.Mechanism(scram.SHA256, kCfg.SASL.User, kCfg.SASL.Password)
			if err != nil {
				return nil, nil, fmt.Errorf("kafka: scram-sha-256 error: %w", err)
			}
		case "SCRAM-SHA-512":
			var err error
			mechanism, err = scram.Mechanism(scram.SHA512, kCfg.SASL.User, kCfg.SASL.Password)
			if err != nil {
				return nil, nil, fmt.Errorf("kafka: scram-sha-512 error: %w", err)
			}
		default:
			return nil, nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", kCfg.SASL.Mechanism)
		}
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
		ClientID:      kCfg.ClientID,
	}

	transport := &kafka.Transport{
		TLS:      tlsConfig,
		SASL:     mechanism,
		ClientID: kCfg.ClientID,
		Dial:     dialer.DialFunc,
	}

	return dialer, transport, nil
}

// Driver returns DriverKafka.
func (q *KafkaQueue) Driver() config.QueueDriver {
	return config.DriverKafka
}

// Name returns the configured Kafka topic.
func (q *KafkaQueue) Name() string {
	return q.kCfg.Topic
}

// Producer creates a new Producer client.
func (q *KafkaQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &kafkaProducer{q: q}, nil
}

// Consumer creates a new Consumer client.
func (q *KafkaQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &kafkaConsumer{q: q}, nil
}

// Ping checks if brokers are reachable.
func (q *KafkaQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.ErrQueueClosed
	}
	if len(q.kCfg.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured")
	}
	conn, err := q.dialer.DialContext(ctx, "tcp", q.kCfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("kafka: ping broker failed: %w", err)
	}
	_ = conn.Close()
	return nil
}

// Stats retrieves Kafka statistics.
func (q *KafkaQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	wStats := q.writer.Stats()
	inFlight, totalSent, consumers := q.Snapshot()
	return queue.Stats{
		Driver:    config.DriverKafka,
		Name:      q.kCfg.Topic,
		Ready:     0,
		InFlight:  inFlight,
		Total:     totalSent,
		Consumers: consumers,
		Extra: map[string]any{
			"group_id":        q.kCfg.GroupID,
			"writer_writes":   wStats.Writes,
			"writer_messages": wStats.Messages,
			"writer_errors":   wStats.Errors,
		},
	}, nil
}

// Close closes the Kafka writer.
func (q *KafkaQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if q.writer != nil {
		return q.writer.Close()
	}
	return nil
}

type kafkaProducer struct {
	q *KafkaQueue
}

func (p *kafkaProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	topic := p.q.kCfg.Topic
	if po.Topic != "" {
		topic = po.Topic
	} else if msg.Topic != "" {
		topic = msg.Topic
	}

	key := []byte(msg.Key)
	if po.Key != "" {
		key = []byte(po.Key)
	}

	var headers []kafka.Header
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	for k, v := range po.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	if msg.ID != "" {
		headers = append(headers, kafka.Header{Key: "X-Message-ID", Value: []byte(msg.ID)})
	}

	kMsg := kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   msg.Payload,
		Headers: headers,
		Time:    msg.Timestamp,
	}

	if err := p.q.writer.WriteMessages(ctx, kMsg); err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	p.q.IncTotalSent(1)
	return nil
}

func (p *kafkaProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	kMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		var po queue.PublishOptions
		for _, opt := range opts {
			opt(&po)
		}

		topic := p.q.kCfg.Topic
		if po.Topic != "" {
			topic = po.Topic
		} else if msg.Topic != "" {
			topic = msg.Topic
		}

		var headers []kafka.Header
		for k, v := range msg.Headers {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}
		for k, v := range po.Headers {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}

		kMsgs = append(kMsgs, kafka.Message{
			Topic:   topic,
			Key:     []byte(msg.Key),
			Value:   msg.Payload,
			Headers: headers,
			Time:    msg.Timestamp,
		})
	}

	if err := p.q.writer.WriteMessages(ctx, kMsgs...); err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	p.q.IncTotalSent(int64(len(kMsgs)))
	return nil
}

func (p *kafkaProducer) Close() error {
	return nil
}

type kafkaConsumer struct {
	q       *KafkaQueue
	readers []*kafka.Reader
	mu      sync.Mutex
}

func (c *kafkaConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
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

	topic := c.q.kCfg.Topic
	if co.Topic != "" {
		topic = co.Topic
	}

	startOffset := kafka.LastOffset
	if strings.ToLower(c.q.kCfg.InitialOffset) == "oldest" {
		startOffset = kafka.FirstOffset
	}

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	c.q.IncActiveCons(concurrency)
	defer c.q.DecActiveCons(concurrency)

	backoff := common.NewBackoff(100*time.Millisecond, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        c.q.kCfg.Brokers,
			GroupID:        c.q.kCfg.GroupID,
			Topic:          topic,
			Dialer:         c.q.dialer,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			StartOffset:    startOffset,
			CommitInterval: 1 * time.Second,
		})

		c.mu.Lock()
		c.readers = append(c.readers, reader)
		c.mu.Unlock()

		wg.Add(1)
		go func(r *kafka.Reader) {
			defer wg.Done()
			defer func() { _ = r.Close() }()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				kMsg, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if !backoff.Wait(ctx) {
						return
					}
					continue
				}

				c.processMessage(ctx, r, kMsg, finalHandler, co)
			}
		}(reader)
	}

	wg.Wait()
	return nil
}

func (c *kafkaConsumer) processMessage(ctx context.Context, r *kafka.Reader, kMsg kafka.Message, handler queue.Handler, co queue.ConsumeOptions) {
	c.q.IncInFlight()
	defer c.q.DecInFlight()

	headers := make(map[string]string, len(kMsg.Headers))
	var msgID string
	for _, h := range kMsg.Headers {
		headers[h.Key] = string(h.Value)
		if h.Key == "X-Message-ID" {
			msgID = string(h.Value)
		}
	}
	if msgID == "" {
		msgID = fmt.Sprintf("k-%s-%d-%d", kMsg.Topic, kMsg.Partition, kMsg.Offset)
	}

	msg := &queue.Message{
		ID:        msgID,
		Topic:     kMsg.Topic,
		Payload:   kMsg.Value,
		Headers:   headers,
		Key:       string(kMsg.Key),
		Timestamp: kMsg.Time,
		Attempts:  1,
		Raw:       kMsg,
	}

	msg.SetAckFunc(func(cctx context.Context) error {
		return r.CommitMessages(cctx, kMsg)
	})
	msg.SetNackFunc(func(cctx context.Context, requeue bool) error {
		return nil
	})

	if co.AutoAck || c.q.kCfg.AutoCommit {
		_ = msg.Ack(ctx)
	}

	err := handler(ctx, msg)
	if err == nil && !msg.IsAcknowledged() {
		_ = msg.Ack(ctx)
	} else if err != nil && !msg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; commit the offset to
		// avoid redelivering the exhausted message.
		_ = msg.Ack(ctx)
	}
}

func (c *kafkaConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.q.kCfg.Brokers,
		GroupID:     c.q.kCfg.GroupID,
		Topic:       c.q.kCfg.Topic,
		Dialer:      c.q.dialer,
		StartOffset: kafka.LastOffset,
	})
	defer func() { _ = reader.Close() }()

	kMsg, err := reader.FetchMessage(ctxTimeout)
	if err != nil {
		if errorsIsContext(err) {
			return nil, queue.ErrTimeout
		}
		return nil, err
	}

	msg := &queue.Message{
		ID:        fmt.Sprintf("k-%d-%d", kMsg.Partition, kMsg.Offset),
		Topic:     kMsg.Topic,
		Payload:   kMsg.Value,
		Timestamp: kMsg.Time,
		Attempts:  1,
		Raw:       kMsg,
	}
	msg.SetAckFunc(func(cctx context.Context) error { return reader.CommitMessages(cctx, kMsg) })
	msg.SetNackFunc(func(cctx context.Context, requeue bool) error { return nil })
	return msg, nil
}

func (c *kafkaConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.readers {
		_ = r.Close()
	}
	c.readers = nil
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

func errorsIsContext(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
