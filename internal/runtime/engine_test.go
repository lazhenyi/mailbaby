package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	_ "mailbaby/internal/queue/driver/all"
	"mailbaby/internal/sender"
)

// mockSender implements sender.Sender for testing purposes.
type mockSender struct {
	mu           sync.Mutex
	sentEmails   []*sender.Email
	failNext     bool
	alwaysFail   bool
	failErr      error
	delaySending time.Duration
}

func newMockSender() *mockSender {
	return &mockSender{
		sentEmails: make([]*sender.Email, 0),
		failErr:    errors.New("mock smtp delivery failure"),
	}
}

func (m *mockSender) Send(ctx context.Context, email *sender.Email) error {
	if m.delaySending > 0 {
		time.Sleep(m.delaySending)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.alwaysFail || m.failNext {
		m.failNext = false
		return m.failErr
	}

	m.sentEmails = append(m.sentEmails, email)
	return nil
}

func (m *mockSender) SendBatch(ctx context.Context, emails []*sender.Email) []error {
	errs := make([]error, len(emails))
	for i, e := range emails {
		errs[i] = m.Send(ctx, e)
	}
	return errs
}

func (m *mockSender) Account(name string) (sender.AccountSender, error) {
	return nil, nil
}

func (m *mockSender) AccountNames() []string {
	return []string{"default"}
}

func (m *mockSender) Stats() map[string]sender.AccountStats {
	return map[string]sender.AccountStats{
		"default": {TotalSent: int64(len(m.sentEmails))},
	}
}

func (m *mockSender) Close() error {
	return nil
}

func (m *mockSender) getSentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sentEmails)
}

// mockProducer implements queue.Producer for dead letter queue verification.
type mockProducer struct {
	mu            sync.Mutex
	publishedMsgs []*queue.Message
}

func (p *mockProducer) Publish(ctx context.Context, msg *queue.Message, opts ...queue.PublishOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishedMsgs = append(p.publishedMsgs, msg)
	return nil
}

func (p *mockProducer) PublishBatch(ctx context.Context, msgs []*queue.Message, opts ...queue.PublishOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishedMsgs = append(p.publishedMsgs, msgs...)
	return nil
}

func (p *mockProducer) Close() error {
	return nil
}

func TestEngineSuccessFlow(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 2,
			MaxRetries:  3,
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("failed to create memory queue: %v", err)
	}
	defer q.Close()

	mockSend := newMockSender()

	engine, err := New(q, mockSend, cfg, WithConcurrency(2))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// 1. Check health before starting
	if err := engine.CheckHealth(context.Background()); !errors.Is(err, ErrEngineNotRunning) {
		t.Errorf("expected ErrEngineNotRunning before start, got: %v", err)
	}

	// 2. Publish email message into queue
	email := sender.NewEmail().
		SetFrom("service@example.com").
		AddTo("user@example.com").
		SetSubject("Welcome").
		SetTextBody("Hello World")

	payload, err := email.ToJSON()
	if err != nil {
		t.Fatalf("failed to serialize email: %v", err)
	}

	producer, err := q.Producer()
	if err != nil {
		t.Fatalf("failed to get producer: %v", err)
	}

	msg := &queue.Message{
		ID:        "msg-001",
		Payload:   payload,
		Topic:     "test_email_queue",
		Timestamp: time.Now(),
	}

	if err := producer.Publish(context.Background(), msg); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// 3. Start Engine
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}

	// Check health when running
	if err := engine.CheckHealth(ctx); err != nil {
		t.Errorf("expected healthy engine, got: %v", err)
	}

	// Wait for message consumption
	for i := 0; i < 20; i++ {
		if mockSend.getSentCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if mockSend.getSentCount() != 1 {
		t.Fatalf("expected 1 sent email, got %d", mockSend.getSentCount())
	}

	mockSend.mu.Lock()
	receivedMail := mockSend.sentEmails[0]
	mockSend.mu.Unlock()

	if receivedMail.Subject != "Welcome" {
		t.Errorf("expected subject 'Welcome', got %q", receivedMail.Subject)
	}

	stats := engine.Stats()
	if stats.TotalSuccess != 1 {
		t.Errorf("expected TotalSuccess=1, got %d", stats.TotalSuccess)
	}

	// 4. Stop Engine
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop engine: %v", err)
	}

	if engine.IsRunning() {
		t.Error("expected engine not running after stop")
	}
}

func TestEngineInvalidPayloadAndValidation(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 2,
			MaxRetries:  0,
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("failed to create memory queue: %v", err)
	}
	defer q.Close()

	mockSend := newMockSender()
	engine, err := New(q, mockSend, cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	producer, _ := q.Producer()

	// 1. Corrupted JSON payload
	badJsonMsg := &queue.Message{
		ID:        "msg-corrupted",
		Payload:   []byte("NOT_A_VALID_JSON"),
		Topic:     "test_invalid_queue",
		Timestamp: time.Now(),
	}
	_ = producer.Publish(context.Background(), badJsonMsg)

	// 2. Missing recipients (Validation failure)
	noRcptMail := sender.NewEmail().SetFrom("test@example.com").SetSubject("No recipients")
	noRcptPayload, _ := noRcptMail.ToJSON()
	badRcptMsg := &queue.Message{
		ID:        "msg-no-rcpt",
		Payload:   noRcptPayload,
		Topic:     "test_invalid_queue",
		Timestamp: time.Now(),
	}
	_ = producer.Publish(context.Background(), badRcptMsg)

	_ = engine.Start(context.Background())
	defer engine.Stop(context.Background())

	// Wait for failures to be recorded
	for i := 0; i < 20; i++ {
		if engine.Stats().TotalFailed >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := engine.Stats()
	if stats.TotalFailed < 2 {
		t.Errorf("expected at least 2 failed messages, got %d", stats.TotalFailed)
	}
	if mockSend.getSentCount() != 0 {
		t.Errorf("expected 0 sent emails, got %d", mockSend.getSentCount())
	}
}

func TestEngineRetryAndDLQ(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 1,
			MaxRetries:  2,
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("failed to create memory queue: %v", err)
	}
	defer q.Close()

	mockSend := newMockSender()
	mockSend.alwaysFail = true

	dlqProd := &mockProducer{}
	var errorHandlerCalled int32

	engine, err := New(q, mockSend, cfg,
		WithDLQ(dlqProd, "test_dlq_topic"),
		WithErrorHandler(func(ctx context.Context, msg *queue.Message, email *sender.Email, err error) {
			atomic.AddInt32(&errorHandlerCalled, 1)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	email := sender.NewEmail().
		SetFrom("test@example.com").
		AddTo("target@example.com").
		SetSubject("Will Fail")
	payload, _ := email.ToJSON()

	producer, _ := q.Producer()

	// Publish message that has already reached MaxRetries (Attempts=2, MaxRetries=2)
	exhaustedMsg := &queue.Message{
		ID:        "msg-exhausted",
		Payload:   payload,
		Topic:     "test_dlq_queue",
		Attempts:  2,
		Timestamp: time.Now(),
	}
	_ = producer.Publish(context.Background(), exhaustedMsg)

	_ = engine.Start(context.Background())
	defer engine.Stop(context.Background())

	for i := 0; i < 20; i++ {
		if engine.Stats().TotalDeadLetter >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := engine.Stats()
	if stats.TotalDeadLetter != 1 {
		t.Errorf("expected TotalDeadLetter=1, got %d", stats.TotalDeadLetter)
	}

	dlqProd.mu.Lock()
	dlqCount := len(dlqProd.publishedMsgs)
	dlqProd.mu.Unlock()

	if dlqCount != 1 {
		t.Errorf("expected 1 DLQ published message, got %d", dlqCount)
	}
	if atomic.LoadInt32(&errorHandlerCalled) == 0 {
		t.Error("expected ErrorHandler callback to be triggered")
	}
}

func TestEngineCustomMiddleware(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 1,
			MaxRetries:  1,
		},
	}

	q, _ := queue.New(cfg)
	defer q.Close()
	mockSend := newMockSender()

	var order []string
	var mu sync.Mutex

	customMw := func(next ProcessFunc) ProcessFunc {
		return func(ctx context.Context, msg *queue.Message, email *sender.Email) error {
			mu.Lock()
			order = append(order, "before")
			mu.Unlock()

			err := next(ctx, msg, email)

			mu.Lock()
			order = append(order, "after")
			mu.Unlock()
			return err
		}
	}

	engine, _ := New(q, mockSend, cfg, WithMiddlewares(customMw))

	email := sender.NewEmail().AddTo("mw@example.com").SetSubject("MW Test")
	payload, _ := email.ToJSON()
	producer, _ := q.Producer()
	_ = producer.Publish(context.Background(), &queue.Message{ID: "mw-1", Payload: payload, Topic: "test_middleware_queue"})

	_ = engine.Start(context.Background())
	defer engine.Stop(context.Background())

	for i := 0; i < 20; i++ {
		mu.Lock()
		count := len(order)
		mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "before" || order[1] != "after" {
		t.Errorf("unexpected middleware execution order: %v", order)
	}
}

func TestEngineConcurrencyAndShutdown(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 5,
			MaxRetries:  1,
		},
	}

	q, _ := queue.New(cfg)
	defer q.Close()

	mockSend := newMockSender()
	mockSend.delaySending = 5 * time.Millisecond

	engine, _ := New(q, mockSend, cfg, WithConcurrency(5))

	producer, _ := q.Producer()
	totalMessages := 10

	for i := 0; i < totalMessages; i++ {
		email := sender.NewEmail().AddTo("user@example.com").SetSubject("Concurrent Test")
		payload, _ := email.ToJSON()
		_ = producer.Publish(context.Background(), &queue.Message{
			ID:        "msg-batch",
			Payload:   payload,
			Topic:     "test_concurrent_queue",
			Timestamp: time.Now(),
		})
	}

	_ = engine.Start(context.Background())

	// Wait for all messages to be processed
	for i := 0; i < 50; i++ {
		if mockSend.getSentCount() >= totalMessages {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stop engine with timeout
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := engine.Stop(stopCtx)
	if err != nil {
		t.Fatalf("engine.Stop failed: %v", err)
	}

	if mockSend.getSentCount() != totalMessages {
		t.Errorf("expected %d messages sent, got %d", totalMessages, mockSend.getSentCount())
	}
}

func TestEngineTransientFailureRetriesThenSucceeds(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:        config.DriverMemory,
			Concurrency:   1,
			MaxRetries:    3,
			RetryInterval: 10 * time.Millisecond,
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("failed to create memory queue: %v", err)
	}
	defer q.Close()

	mockSend := newMockSender()
	mockSend.failNext = true // first attempt fails, retry should succeed

	engine, err := New(q, mockSend, cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	payload, _ := sender.NewEmail().
		SetFrom("test@example.com").
		AddTo("t@example.com").
		SetSubject("Transient").
		ToJSON()

	producer, _ := q.Producer()
	_ = producer.Publish(context.Background(), &queue.Message{
		ID:        "retry-transient",
		Payload:   payload,
		Topic:     "test_retry_queue",
		Timestamp: time.Now(),
	})

	_ = engine.Start(context.Background())
	defer engine.Stop(context.Background())

	for i := 0; i < 50; i++ {
		if engine.Stats().TotalSuccess >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := engine.Stats()
	if stats.TotalSuccess != 1 {
		t.Fatalf("expected TotalSuccess=1 after retry, got %d", stats.TotalSuccess)
	}
	// mockSender only records successful sends; a fail-then-succeed flow must
	// have exactly one recorded delivery but one counted retry.
	if mockSend.getSentCount() != 1 {
		t.Errorf("expected 1 successful send, got %d", mockSend.getSentCount())
	}
	if stats.TotalRetried != 1 {
		t.Errorf("expected TotalRetried=1, got %d", stats.TotalRetried)
	}
	if stats.TotalDeadLetter != 0 {
		t.Errorf("expected no DLQ for transient failure, got %d", stats.TotalDeadLetter)
	}
}

func TestEngineRetryExhaustedPublishesDLQOnceAndAcks(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:        config.DriverMemory,
			Concurrency:   1,
			MaxRetries:    2,
			RetryInterval: 5 * time.Millisecond,
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("failed to create memory queue: %v", err)
	}
	defer q.Close()

	mockSend := newMockSender()
	mockSend.alwaysFail = true

	dlqProd := &mockProducer{}
	var errorHandlerCount int32

	engine, err := New(q, mockSend, cfg,
		WithDLQ(dlqProd, "test_dlq_queue"),
		WithErrorHandler(func(ctx context.Context, msg *queue.Message, email *sender.Email, err error) {
			atomic.AddInt32(&errorHandlerCount, 1)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	payload, _ := sender.NewEmail().
		SetFrom("test@example.com").
		AddTo("t@example.com").
		SetSubject("Exhausted").
		ToJSON()

	producer, _ := q.Producer()
	_ = producer.Publish(context.Background(), &queue.Message{
		ID:        "retry-exhausted",
		Payload:   payload,
		Topic:     "test_retry_queue",
		Timestamp: time.Now(),
	})

	_ = engine.Start(context.Background())
	defer engine.Stop(context.Background())

	for i := 0; i < 50; i++ {
		if engine.Stats().TotalDeadLetter >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := engine.Stats()
	if stats.TotalDeadLetter != 1 {
		t.Fatalf("expected TotalDeadLetter=1, got %d", stats.TotalDeadLetter)
	}
	if stats.TotalFailed != 1 {
		t.Errorf("expected TotalFailed=1, got %d", stats.TotalFailed)
	}

	dlqProd.mu.Lock()
	dlqCount := len(dlqProd.publishedMsgs)
	dlqProd.mu.Unlock()
	if dlqCount != 1 {
		t.Errorf("expected exactly 1 DLQ message, got %d", dlqCount)
	}
	if atomic.LoadInt32(&errorHandlerCount) != 1 {
		t.Errorf("expected errorHandler invoked exactly once, got %d", atomic.LoadInt32(&errorHandlerCount))
	}
	if mockSend.getSentCount() != 0 {
		t.Errorf("expected 0 successful sends, got %d", mockSend.getSentCount())
	}
}
