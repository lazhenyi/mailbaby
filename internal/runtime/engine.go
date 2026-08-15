package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// Engine is the central execution runtime connecting the Message Queue to the Email Sender.
type Engine struct {
	queue           queue.Queue
	sender          sender.Sender
	cfg             *config.Config
	consumer        queue.Consumer
	dlqProducer     queue.Producer
	dlqTopic        string
	concurrency     int
	maxRetries      int
	retryInterval   time.Duration
	shutdownTimeout time.Duration
	middlewares     []Middleware
	errorHandler    ErrorHandler

	state           int32 // atomic EngineState
	inFlight        int64
	totalReceived   int64
	totalSuccess    int64
	totalFailed     int64
	totalRetried    int64
	totalDeadLetter int64
	startTime       time.Time

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// New creates and initializes a new Runtime Engine.
func New(q queue.Queue, s sender.Sender, cfg *config.Config, opts ...Option) (*Engine, error) {
	if q == nil {
		return nil, ErrNilQueue
	}
	if s == nil {
		return nil, ErrNilSender
	}

	concurrency := 10
	maxRetries := 3
	retryInterval := defaultRetryInterval
	shutdownTimeout := 10 * time.Second

	if cfg != nil {
		if cfg.Queue.Concurrency > 0 {
			concurrency = cfg.Queue.Concurrency
		}
		if cfg.Queue.MaxRetries >= 0 {
			maxRetries = cfg.Queue.MaxRetries
		}
		if cfg.Queue.RetryInterval > 0 {
			retryInterval = cfg.Queue.RetryInterval
		}
		if cfg.App.ShutdownTimeout > 0 {
			shutdownTimeout = cfg.App.ShutdownTimeout
		}
	}

	engine := &Engine{
		queue:           q,
		sender:          s,
		cfg:             cfg,
		concurrency:     concurrency,
		maxRetries:      maxRetries,
		retryInterval:   retryInterval,
		shutdownTimeout: shutdownTimeout,
		state:           int32(StateStopped),
	}

	// Apply default built-in middlewares
	driverStr := string(q.Driver())
	topicStr := q.Name()
	engine.middlewares = []Middleware{
		RecoveryMiddleware(),
		TracingMiddleware(driverStr, topicStr),
		MetricsMiddleware(driverStr, topicStr),
		LoggingMiddleware(),
	}

	// Apply custom options
	for _, opt := range opts {
		opt(engine)
	}

	return engine, nil
}

// Start launches the message queue subscription and worker pool.
func (e *Engine) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&e.state, int32(StateStopped), int32(StateStarting)) {
		return ErrEngineAlreadyRunning
	}

	consumer, err := e.queue.Consumer()
	if err != nil {
		atomic.StoreInt32(&e.state, int32(StateStopped))
		return fmt.Errorf("runtime: failed to obtain consumer from queue: %w", err)
	}
	e.consumer = consumer

	runCtx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	e.startTime = time.Now()

	atomic.StoreInt32(&e.state, int32(StateRunning))
	logger.Get().WithFields(logger.Fields{
		"driver":      string(e.queue.Driver()),
		"topic":       e.queue.Name(),
		"concurrency": e.concurrency,
		"retries":     e.maxRetries,
	}).Info("runtime execution engine started")

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			atomic.StoreInt32(&e.state, int32(StateStopped))
		}()

		consumeOpts := []queue.ConsumeOption{queue.WithConcurrency(e.concurrency)}
		if e.cfg != nil && e.cfg.Queue.BatchSize > 0 {
			consumeOpts = append(consumeOpts, queue.WithBatchSize(e.cfg.Queue.BatchSize))
		}

		consumeErr := e.consumer.Consume(runCtx, e.processMessage, consumeOpts...)
		if consumeErr != nil && consumeErr != context.Canceled {
			logger.Get().WithFields(logger.Fields{
				"driver": string(e.queue.Driver()),
				"topic":  e.queue.Name(),
				"error":  consumeErr.Error(),
			}).Error("consumer subscription exited")
		}
	}()

	return nil
}

// Stop gracefully shuts down the engine, waiting for in-flight messages to complete within timeout.
func (e *Engine) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&e.state, int32(StateRunning), int32(StateStopping)) {
		// If already stopped or stopping, return without error
		return nil
	}

	logger.Get().Info("stopping execution engine, waiting for in-flight tasks to finish")

	// 1. Cancel consumer context to stop accepting new messages
	if e.cancelFunc != nil {
		e.cancelFunc()
	}

	// 2. Close consumer
	if e.consumer != nil {
		_ = e.consumer.Close()
	}

	// 3. Wait for background consumer loop and in-flight tasks to drain
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Get().Info("runtime: all in-flight tasks drained successfully. Engine stopped.")
	case <-ctx.Done():
		logger.Get().WithError(ctx.Err()).Warn("graceful shutdown timed out")
		return ctx.Err()
	}

	atomic.StoreInt32(&e.state, int32(StateStopped))
	return nil
}

// IsRunning returns true if the engine is currently active.
func (e *Engine) IsRunning() bool {
	return EngineState(atomic.LoadInt32(&e.state)) == StateRunning
}

// Stats returns a snapshot of runtime metrics.
func (e *Engine) Stats() RuntimeStats {
	uptime := time.Duration(0)
	if !e.startTime.IsZero() && e.IsRunning() {
		uptime = time.Since(e.startTime)
	}

	return RuntimeStats{
		State:           EngineState(atomic.LoadInt32(&e.state)).String(),
		TotalReceived:   atomic.LoadInt64(&e.totalReceived),
		TotalSuccess:    atomic.LoadInt64(&e.totalSuccess),
		TotalFailed:     atomic.LoadInt64(&e.totalFailed),
		TotalRetried:    atomic.LoadInt64(&e.totalRetried),
		TotalDeadLetter: atomic.LoadInt64(&e.totalDeadLetter),
		InFlight:        atomic.LoadInt64(&e.inFlight),
		Uptime:          uptime,
		StartTime:       e.startTime,
	}
}

// CheckHealth implements handler.Checker for /readyz Kubernetes probe.
func (e *Engine) CheckHealth(ctx context.Context) error {
	if !e.IsRunning() {
		return ErrEngineNotRunning
	}
	if e.queue != nil {
		if err := e.queue.Ping(ctx); err != nil {
			return fmt.Errorf("queue broker unreachable: %w", err)
		}
	}
	return nil
}
