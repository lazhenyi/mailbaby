package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"

	redis "github.com/redis/go-redis/v9"
)

func init() {
	queue.Register(config.DriverRedis, New)
}

// RedisQueue implements queue.Queue for Redis (Stream, List, PubSub).
type RedisQueue struct {
	cfg        *config.Config
	rCfg       config.RedisConfig
	client     redis.UniversalClient
	mode       string
	closed     bool
	mu         sync.RWMutex
	inFlight   int64
	totalSent  int64
	activeCons int64
}

// New creates and initializes a new Redis Queue instance.
func New(cfg *config.Config) (queue.Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", queue.ErrInvalidMessage)
	}

	rCfg := cfg.Queue.Redis
	if err := rCfg.Validate(); err != nil {
		return nil, fmt.Errorf("redis: config validation failed: %w", err)
	}

	addrs := rCfg.Addrs
	if len(addrs) == 0 && rCfg.Host != "" {
		port := rCfg.Port
		if port == 0 {
			port = 6379
		}
		addrs = []string{fmt.Sprintf("%s:%d", rCfg.Host, port)}
	}

	var tlsConfig *tls.Config
	if rCfg.TLS.Enable {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: rCfg.TLS.InsecureSkipVerify,
			ServerName:         rCfg.TLS.ServerName,
		}
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        addrs,
		Username:     rCfg.Username,
		Password:     rCfg.Password,
		DB:           rCfg.DB,
		MasterName:   rCfg.MasterName,
		TLSConfig:    tlsConfig,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	mode := strings.ToLower(strings.TrimSpace(rCfg.Mode))
	if mode == "" {
		mode = "stream"
	}

	rq := &RedisQueue{
		cfg:    cfg,
		rCfg:   rCfg,
		client: client,
		mode:   mode,
	}

	if mode == "stream" && rCfg.Group != "" {
		_ = client.XGroupCreateMkStream(context.Background(), rCfg.Key, rCfg.Group, "$").Err()
	}

	return rq, nil
}

// Driver returns DriverRedis.
func (q *RedisQueue) Driver() config.QueueDriver {
	return config.DriverRedis
}

// Name returns the configured Redis key/stream.
func (q *RedisQueue) Name() string {
	return q.rCfg.Key
}

// Producer creates a new Producer client.
func (q *RedisQueue) Producer() (queue.Producer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &redisProducer{q: q}, nil
}

// Consumer creates a new Consumer client.
func (q *RedisQueue) Consumer() (queue.Consumer, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return nil, queue.ErrQueueClosed
	}
	return &redisConsumer{q: q}, nil
}

// Ping checks if Redis server is reachable.
func (q *RedisQueue) Ping(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.ErrQueueClosed
	}
	return q.client.Ping(ctx).Err()
}

// Stats retrieves queue depth and metrics.
func (q *RedisQueue) Stats(ctx context.Context) (queue.Stats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return queue.Stats{}, queue.ErrQueueClosed
	}

	var ready int64
	var inFlight = atomic.LoadInt64(&q.inFlight)

	switch q.mode {
	case "stream":
		xlen, _ := q.client.XLen(ctx, q.rCfg.Key).Result()
		ready = xlen
		if q.rCfg.Group != "" {
			pend, err := q.client.XPending(ctx, q.rCfg.Key, q.rCfg.Group).Result()
			if err == nil {
				inFlight = pend.Count
			}
		}
	case "list":
		llen, _ := q.client.LLen(ctx, q.rCfg.Key).Result()
		ready = llen
	case "pubsub":
		res, _ := q.client.PubSubNumSub(ctx, q.rCfg.Key).Result()
		ready = 0
		if num, ok := res[q.rCfg.Key]; ok {
			inFlight = num
		}
	}

	return queue.Stats{
		Driver:    config.DriverRedis,
		Name:      q.rCfg.Key,
		Ready:     ready,
		InFlight:  inFlight,
		Total:     atomic.LoadInt64(&q.totalSent),
		Consumers: int(atomic.LoadInt64(&q.activeCons)),
		Extra: map[string]any{
			"mode":  q.mode,
			"group": q.rCfg.Group,
		},
	}, nil
}

// Close closes the Redis connection.
func (q *RedisQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.client.Close()
}

type redisMessageEnvelope struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Attempts  int               `json:"attempts"`
}

type redisProducer struct {
	q *RedisQueue
}

func (p *redisProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	if msg == nil {
		return queue.ErrInvalidMessage
	}

	var po queue.PublishOptions
	for _, opt := range opts {
		opt(&po)
	}

	key := p.q.rCfg.Key
	if po.Topic != "" {
		key = po.Topic
	} else if msg.Topic != "" {
		key = msg.Topic
	}

	headers := make(map[string]string)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	for k, v := range po.Headers {
		headers[k] = v
	}

	env := redisMessageEnvelope{
		ID:        msg.ID,
		Topic:     key,
		Payload:   msg.Payload,
		Headers:   headers,
		Timestamp: msg.Timestamp,
		Attempts:  msg.Attempts,
	}

	rawJSON, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("redis: failed to marshal envelope: %w", err)
	}

	publishAction := func() error {
		switch p.q.mode {
		case "stream":
			args := &redis.XAddArgs{
				Stream: key,
				Values: map[string]any{
					"id":      env.ID,
					"payload": env.Payload,
					"data":    rawJSON,
				},
			}
			if p.q.rCfg.MaxLen > 0 {
				args.MaxLen = p.q.rCfg.MaxLen
				args.Approx = true
			}
			return p.q.client.XAdd(ctx, args).Err()
		case "list":
			return p.q.client.RPush(ctx, key, rawJSON).Err()
		case "pubsub":
			return p.q.client.Publish(ctx, key, rawJSON).Err()
		default:
			return fmt.Errorf("redis: unsupported mode %q", p.q.mode)
		}
	}

	if po.Delay > 0 || msg.Delay > 0 {
		delay := po.Delay
		if delay == 0 {
			delay = msg.Delay
		}
		go func(d time.Duration) {
			select {
			case <-time.After(d):
				_ = publishAction()
			case <-ctx.Done():
			}
		}(delay)
		return nil
	}

	if err := publishAction(); err != nil {
		return fmt.Errorf("%w: %v", queue.ErrPublishFailed, err)
	}

	atomic.AddInt64(&p.q.totalSent, 1)
	return nil
}

func (p *redisProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (p *redisProducer) Close() error {
	return nil
}

type redisConsumer struct {
	q *RedisQueue
}

func (c *redisConsumer) Consume(ctx context.Context, handler queue.Handler, opts ...queue.ConsumeOption) error {
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

	finalHandler := handler
	if len(co.Middlewares) > 0 {
		finalHandler = queue.Chain(co.Middlewares...)(handler)
	}

	atomic.AddInt64(&c.q.activeCons, int64(concurrency))
	defer atomic.AddInt64(&c.q.activeCons, -int64(concurrency))

	switch c.q.mode {
	case "stream":
		return c.consumeStream(ctx, finalHandler, co, concurrency)
	case "list":
		return c.consumeList(ctx, finalHandler, co, concurrency)
	case "pubsub":
		return c.consumePubSub(ctx, finalHandler, co)
	default:
		return fmt.Errorf("redis: unsupported mode %q", c.q.mode)
	}
}

func (c *redisConsumer) consumeStream(ctx context.Context, handler queue.Handler, co queue.ConsumeOptions, concurrency int) error {
	key := c.q.rCfg.Key
	if co.Topic != "" {
		key = co.Topic
	}
	group := c.q.rCfg.Group
	if group == "" {
		group = "mailbaby_consumer_group"
	}

	blockTime := c.q.rCfg.BlockTime
	if blockTime <= 0 {
		blockTime = 2 * time.Second
	}

	prefetch := int64(10)
	if co.PrefetchCount > 0 {
		prefetch = int64(co.PrefetchCount)
	}
	if co.BatchSize > 0 {
		prefetch = int64(co.BatchSize)
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerID := fmt.Sprintf("%s-%d", c.q.rCfg.Consumer, i)
		if c.q.rCfg.Consumer == "" {
			workerID = fmt.Sprintf("worker-%d", i)
		}
		wg.Add(1)
		go func(consumerName string) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				entries, err := c.q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumerName,
					Streams:  []string{key, ">"},
					Count:    prefetch,
					Block:    blockTime,
				}).Result()

				if err != nil {
					if errorsIsRedisNil(err) || ctx.Err() != nil {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for _, stream := range entries {
					for _, xmsg := range stream.Messages {
						c.processStreamMessage(ctx, stream.Stream, group, xmsg, handler, co)
					}
				}
			}
		}(workerID)
	}

	wg.Wait()
	return nil
}

func (c *redisConsumer) processStreamMessage(ctx context.Context, stream, group string, xmsg redis.XMessage, handler queue.Handler, co queue.ConsumeOptions) {
	atomic.AddInt64(&c.q.inFlight, 1)
	defer atomic.AddInt64(&c.q.inFlight, -1)

	var qMsg *queue.Message
	if raw, ok := xmsg.Values["data"].(string); ok && raw != "" {
		var env redisMessageEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err == nil {
			qMsg = &queue.Message{
				ID:        env.ID,
				Topic:     stream,
				Payload:   env.Payload,
				Headers:   env.Headers,
				Timestamp: env.Timestamp,
				Attempts:  env.Attempts,
				Raw:       xmsg,
			}
		}
	}

	if qMsg == nil {
		var payload []byte
		if p, ok := xmsg.Values["payload"].(string); ok {
			payload = []byte(p)
		}
		qMsg = &queue.Message{
			ID:        xmsg.ID,
			Topic:     stream,
			Payload:   payload,
			Timestamp: time.Now(),
			Attempts:  1,
			Raw:       xmsg,
		}
	}

	qMsg.SetAckFunc(func(cctx context.Context) error {
		return c.q.client.XAck(cctx, stream, group, xmsg.ID).Err()
	})
	qMsg.SetNackFunc(func(cctx context.Context, requeue bool) error {
		return nil
	})

	if co.AutoAck {
		_ = qMsg.Ack(ctx)
	}

	err := handler(ctx, qMsg)
	if err == nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Ack(ctx)
	} else if err != nil && !qMsg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; acknowledge the stream
		// message so it does not stay pending in the PEL forever.
		_ = qMsg.Ack(ctx)
	}
}

func (c *redisConsumer) consumeList(ctx context.Context, handler queue.Handler, co queue.ConsumeOptions, concurrency int) error {
	key := c.q.rCfg.Key
	if co.Topic != "" {
		key = co.Topic
	}

	blockTime := c.q.rCfg.BlockTime
	if blockTime <= 0 {
		blockTime = 2 * time.Second
	}

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

				res, err := c.q.client.BLPop(ctx, blockTime, key).Result()
				if err != nil {
					if errorsIsRedisNil(err) || ctx.Err() != nil {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if len(res) < 2 {
					continue
				}

				rawData := res[1]
				c.processListMessage(ctx, key, rawData, handler, co)
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *redisConsumer) processListMessage(ctx context.Context, key, rawData string, handler queue.Handler, co queue.ConsumeOptions) {
	atomic.AddInt64(&c.q.inFlight, 1)
	defer atomic.AddInt64(&c.q.inFlight, -1)

	var env redisMessageEnvelope
	var qMsg *queue.Message
	if err := json.Unmarshal([]byte(rawData), &env); err == nil {
		qMsg = &queue.Message{
			ID:        env.ID,
			Topic:     key,
			Payload:   env.Payload,
			Headers:   env.Headers,
			Timestamp: env.Timestamp,
			Attempts:  env.Attempts,
			Raw:       rawData,
		}
	} else {
		qMsg = &queue.Message{
			ID:        "",
			Topic:     key,
			Payload:   []byte(rawData),
			Timestamp: time.Now(),
			Attempts:  1,
			Raw:       rawData,
		}
	}

	qMsg.SetAckFunc(func(ctx context.Context) error { return nil })
	qMsg.SetNackFunc(func(cctx context.Context, requeue bool) error {
		if requeue {
			return c.q.client.LPush(cctx, key, rawData).Err()
		}
		return nil
	})

	err := handler(ctx, qMsg)
	if err != nil && !qMsg.IsAcknowledged() {
		// Retries are handled inside the runtime engine; drop instead of
		// requeueing to avoid tight retry loops on poison messages.
		_ = qMsg.Ack(ctx)
	} else if err == nil && !qMsg.IsAcknowledged() {
		_ = qMsg.Ack(ctx)
	}
}

func (c *redisConsumer) consumePubSub(ctx context.Context, handler queue.Handler, co queue.ConsumeOptions) error {
	key := c.q.rCfg.Key
	if co.Topic != "" {
		key = co.Topic
	}

	pubsub := c.q.client.Subscribe(ctx, key)
	defer func() { _ = pubsub.Close() }()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case rMsg, ok := <-ch:
			if !ok {
				return nil
			}

			qMsg := &queue.Message{
				Topic:     key,
				Payload:   []byte(rMsg.Payload),
				Timestamp: time.Now(),
				Attempts:  1,
				Raw:       rMsg,
			}
			qMsg.SetAckFunc(func(ctx context.Context) error { return nil })
			qMsg.SetNackFunc(func(ctx context.Context, requeue bool) error { return nil })

			_ = handler(ctx, qMsg)
		}
	}
}

func (c *redisConsumer) Receive(ctx context.Context, opts ...queue.ReceiveOption) (*queue.Message, error) {
	var ro queue.ReceiveOptions
	for _, opt := range opts {
		opt(&ro)
	}

	timeout := 2 * time.Second
	if ro.Timeout > 0 {
		timeout = ro.Timeout
	}

	key := c.q.rCfg.Key
	switch c.q.mode {
	case "list":
		res, err := c.q.client.BLPop(ctx, timeout, key).Result()
		if err != nil {
			if errorsIsRedisNil(err) {
				return nil, queue.ErrTimeout
			}
			return nil, err
		}
		if len(res) < 2 {
			return nil, queue.ErrTimeout
		}
		return queue.NewMessage([]byte(res[1])), nil
	default:
		return nil, fmt.Errorf("redis: Receive only directly supported for list mode")
	}
}

func (c *redisConsumer) Close() error {
	return nil
}

func errorsIsRedisNil(err error) bool {
	return err == redis.Nil || strings.Contains(err.Error(), "redis: nil")
}
