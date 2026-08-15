package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
)

type samplePayload struct {
	Subject string `json:"subject"`
	To      string `json:"to"`
	Body    string `json:"body"`
}

func TestMessage_Basic(t *testing.T) {
	payload := []byte("hello world")
	msg := NewMessage(payload)

	if msg.ID == "" {
		t.Fatal("expected non-empty message ID")
	}
	if string(msg.Payload) != "hello world" {
		t.Fatalf("unexpected payload: %s", string(msg.Payload))
	}
	if msg.Attempts != 1 {
		t.Fatalf("expected attempts 1, got %d", msg.Attempts)
	}

	msg.SetHeader("X-Trace-ID", "trace-123")
	if val := msg.GetHeader("X-Trace-ID"); val != "trace-123" {
		t.Fatalf("expected header 'trace-123', got %q", val)
	}
	if val := msg.GetHeader("Non-Existent"); val != "" {
		t.Fatalf("expected empty header, got %q", val)
	}

	clone := msg.Clone()
	if clone.ID != msg.ID || string(clone.Payload) != string(msg.Payload) {
		t.Fatal("clone mismatch")
	}
	clone.SetHeader("X-Trace-ID", "modified")
	if msg.GetHeader("X-Trace-ID") != "trace-123" {
		t.Fatal("clone header modification leaked to original")
	}

	str := msg.String()
	if str == "" {
		t.Fatal("expected non-empty string representation")
	}
}

func TestMessage_JSON(t *testing.T) {
	data := samplePayload{
		Subject: "Test Email",
		To:      "user@example.com",
		Body:    "This is a test body",
	}

	msg, err := NewJSONMessage(data)
	if err != nil {
		t.Fatalf("NewJSONMessage failed: %v", err)
	}
	if ct := msg.GetHeader("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	var parsed samplePayload
	if err := msg.BindJSON(&parsed); err != nil {
		t.Fatalf("BindJSON failed: %v", err)
	}
	if parsed != data {
		t.Fatalf("parsed %+v != original %+v", parsed, data)
	}

	var emptyMsg *Message
	if err := emptyMsg.BindJSON(&parsed); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("expected ErrInvalidMessage, got %v", err)
	}
}

func TestMessage_AckNack(t *testing.T) {
	ctx := context.Background()

	// 1. Without ack callback
	msg := NewMessage([]byte("test"))
	if err := msg.Ack(ctx); !errors.Is(err, ErrAckNotSupported) {
		t.Fatalf("expected ErrAckNotSupported, got %v", err)
	}
	if err := msg.Nack(ctx, true); !errors.Is(err, ErrNackNotSupported) {
		t.Fatalf("expected ErrNackNotSupported, got %v", err)
	}

	// 2. With ack callback
	var ackCalled, nackCalled bool
	msg.SetAckFunc(func(ctx context.Context) error {
		ackCalled = true
		return nil
	})
	msg.SetNackFunc(func(ctx context.Context, requeue bool) error {
		nackCalled = true
		return nil
	})

	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	if !ackCalled || !msg.IsAcknowledged() {
		t.Fatal("expected message to be marked acknowledged")
	}

	// Idempotent Ack
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("idempotent Ack failed: %v", err)
	}

	// Nack after Ack should be no-op
	if err := msg.Nack(ctx, true); err != nil {
		t.Fatalf("Nack after Ack failed: %v", err)
	}
	if nackCalled {
		t.Fatal("nack should not be called after already acknowledged")
	}
}

func TestRegistry(t *testing.T) {
	drivers := GetRegisteredDrivers()
	if len(drivers) == 0 {
		t.Fatal("expected at least one registered driver (memory)")
	}

	if !IsDriverRegistered(config.DriverMemory) {
		t.Fatal("expected memory driver to be registered")
	}

	// Test New with default memory driver
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 5,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if q.Driver() != config.DriverMemory {
		t.Fatalf("expected DriverMemory, got %s", q.Driver())
	}
	defer q.Close()

	// Test New with unregistered driver
	cfgUnregistered := &config.Config{
		Queue: config.QueueConfig{
			Driver: "unregistered_foo",
		},
	}
	if _, err := New(cfgUnregistered); !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("expected ErrDriverNotFound, got %v", err)
	}

	// Test custom register and unregister
	customDriver := config.QueueDriver("mock_driver")
	Register(customDriver, func(cfg *config.Config) (Queue, error) {
		return NewMemoryQueue("mock", 10, cfg), nil
	})
	if !IsDriverRegistered(customDriver) {
		t.Fatal("expected mock_driver to be registered")
	}
	Unregister(customDriver)
	if IsDriverRegistered(customDriver) {
		t.Fatal("expected mock_driver to be unregistered")
	}
}

func TestMemoryQueue_PublishConsume(t *testing.T) {
	q := NewMemoryQueue("test_mail_queue", 100, nil)
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := q.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	producer, err := q.Producer()
	if err != nil {
		t.Fatalf("Producer failed: %v", err)
	}

	consumer, err := q.Consumer()
	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	const messageCount = 20
	var receivedCount int64
	var receivedMu sync.Mutex
	receivedIDs := make(map[string]bool)

	// Publish messages in batch
	messages := make([]*Message, messageCount)
	for i := range messageCount {
		msg := NewMessageWithID(fmt.Sprintf("msg-%d", i), []byte(fmt.Sprintf("email-task-%d", i)))
		msg.SetHeader("Priority", "high")
		messages[i] = msg
	}

	if err := producer.PublishBatch(ctx, messages, WithTopic("test_mail_queue")); err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Consume messages concurrently
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	handler := func(c context.Context, msg *Message) error {
		receivedMu.Lock()
		receivedIDs[msg.ID] = true
		receivedMu.Unlock()
		current := atomic.AddInt64(&receivedCount, 1)
		if current == messageCount {
			consumeCancel()
		}
		return nil
	}

	err = consumer.Consume(consumeCtx, handler, WithConcurrency(4))
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if atomic.LoadInt64(&receivedCount) != messageCount {
		t.Fatalf("expected %d received messages, got %d", messageCount, receivedCount)
	}

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.Total != messageCount {
		t.Fatalf("expected stats.Total %d, got %d", messageCount, stats.Total)
	}
}

func TestMemoryQueue_Receive(t *testing.T) {
	q := NewMemoryQueue("test_receive_queue", 10, nil)
	defer q.Close()

	ctx := context.Background()
	producer, _ := q.Producer()
	consumer, _ := q.Consumer()

	// 1. Receive with timeout when empty
	_, err := consumer.Receive(ctx, WithReceiveTimeout(50*time.Millisecond))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	// 2. Publish and pull
	msg := NewMessage([]byte("pull message"))
	if err := producer.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	pulled, err := consumer.Receive(ctx, WithReceiveTimeout(1*time.Second))
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if string(pulled.Payload) != "pull message" {
		t.Fatalf("unexpected pulled payload: %s", string(pulled.Payload))
	}
}

func TestMiddlewares(t *testing.T) {
	ctx := context.Background()

	// 1. Recovery Middleware
	var panicHookCalled bool
	recovery := RecoveryMiddleware(func(p any, msg *Message) {
		panicHookCalled = true
	})

	panickingHandler := func(c context.Context, msg *Message) error {
		panic("database connection blew up")
	}

	wrapped := recovery(panickingHandler)
	err := wrapped(ctx, NewMessage([]byte("panic-test")))
	if err == nil {
		t.Fatal("expected error from recovered panic, got nil")
	}
	if !panicHookCalled {
		t.Fatal("expected panic hook to be triggered")
	}

	// 2. Retry Middleware
	var attempts int
	retryHandler := func(c context.Context, msg *Message) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	retryWrapped := RetryMiddleware(3, 5*time.Millisecond)(retryHandler)
	err = retryWrapped(ctx, NewMessage([]byte("retry-test")))
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	// 3. Chain & Logging
	var logs []string
	logMiddleware := LoggingMiddleware(func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})

	chain := Chain(recovery, logMiddleware)
	successHandler := func(c context.Context, msg *Message) error {
		return nil
	}
	finalHandler := chain(successHandler)
	if err := finalHandler(ctx, NewMessage([]byte("ok"))); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(logs) == 0 {
		t.Fatal("expected logs from logging middleware")
	}
}

func TestOptions(t *testing.T) {
	var po PublishOptions
	WithTopic("orders")(&po)
	WithKey("key-1")(&po)
	WithDelay(10 * time.Second)(&po)
	WithHeader("k1", "v1")(&po)
	WithHeaders(map[string]string{"k2": "v2"})(&po)
	WithPublishTimeout(3 * time.Second)(&po)

	if po.Topic != "orders" || po.Key != "key-1" || po.Delay != 10*time.Second || po.Timeout != 3*time.Second {
		t.Fatalf("unexpected publish options: %+v", po)
	}
	if po.Headers["k1"] != "v1" || po.Headers["k2"] != "v2" {
		t.Fatalf("unexpected headers: %+v", po.Headers)
	}

	var co ConsumeOptions
	WithConsumeTopic("tasks")(&co)
	WithConcurrency(8)(&co)
	WithAutoAck(true)(&co)
	WithMaxRetries(5)(&co)
	WithRetryInterval(2 * time.Second)(&co)
	WithPrefetchCount(50)(&co)
	WithBatchSize(10)(&co)

	if co.Topic != "tasks" || co.Concurrency != 8 || !co.AutoAck || co.MaxRetries != 5 ||
		co.RetryInterval != 2*time.Second || co.PrefetchCount != 50 || co.BatchSize != 10 {
		t.Fatalf("unexpected consume options: %+v", co)
	}
}
