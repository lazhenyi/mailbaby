package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/handler"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/runtime"
	"mailbaby/internal/sender"
)

// App is the top-level application container coordinating the lifecycle of all subsystems.
type App struct {
	cfg     *config.Config
	queue   queue.Queue
	sender  sender.Sender
	metrics *metrics.Metrics
	engine  *runtime.Engine
	server  *handler.Server
	mu      sync.Mutex
	running bool
}

// NewApp initializes and wires all application dependencies based on the provided configuration.
func NewApp(cfg *config.Config) (*App, error) {
	if cfg == nil {
		return nil, errors.New("cmd: config cannot be nil")
	}

	// 1. Initialize Queue driver
	q, err := queue.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("cmd: failed to initialize queue driver %q: %w", cfg.Queue.Driver, err)
	}

	// 2. Initialize SMTP Sender subsystem
	mailSender, err := sender.NewFromConfig(cfg)
	if err != nil {
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize mail sender: %w", err)
	}

	// 3. Initialize Metrics subsystem
	m, err := metrics.Init(cfg.Metrics)
	if err != nil {
		_ = mailSender.Close()
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize metrics: %w", err)
	}

	// 4. Initialize Execution Engine (Queue -> Sender Core)
	engine, err := runtime.New(q, mailSender, cfg)
	if err != nil {
		_ = metrics.Close()
		_ = mailSender.Close()
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize execution engine: %w", err)
	}

	// 5. Initialize Unified HTTP Server (Metrics, Health, Pprof)
	httpServer := handler.New(cfg)

	// Register readiness health checkers
	httpServer.RegisterChecker("queue", func(ctx context.Context) error {
		if q == nil {
			return errors.New("queue driver is nil")
		}
		return q.Ping(ctx)
	})
	httpServer.RegisterChecker("smtp", func(ctx context.Context) error {
		if mailSender == nil {
			return errors.New("smtp sender is nil")
		}
		return nil
	})
	httpServer.RegisterChecker("runtime", engine.CheckHealth)

	return &App{
		cfg:     cfg,
		queue:   q,
		sender:  mailSender,
		metrics: m,
		engine:  engine,
		server:  httpServer,
	}, nil
}

// Start boots all asynchronous workers and servers in the application.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return errors.New("cmd: application is already running")
	}
	a.running = true
	a.mu.Unlock()

	// 1. Start execution engine
	if err := a.engine.Start(ctx); err != nil {
		return fmt.Errorf("cmd: failed to start engine: %w", err)
	}
	log.Printf("[INFO] cmd: runtime engine started (driver=%s, topic=%s)", a.queue.Driver(), a.queue.Name())

	// 2. Start HTTP server
	if err := a.server.Start(ctx); err != nil {
		_ = a.engine.Stop(ctx)
		return fmt.Errorf("cmd: failed to start http server: %w", err)
	}
	log.Printf("[INFO] cmd: unified HTTP server listening on http://%s", a.cfg.Server.Address())

	return nil
}

// Shutdown stops all running services and releases all allocated connections in reverse order.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}
	a.running = false

	log.Println("[INFO] cmd: initiating graceful shutdown...")

	var firstErr error

	// 1. Stop HTTP server (stops incoming traffic and health probes)
	if a.server != nil {
		if err := a.server.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to stop http server: %w", err)
		}
	}

	// 2. Stop runtime engine and drain in-flight jobs
	if a.engine != nil {
		if err := a.engine.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to stop engine: %w", err)
		}
	}

	// 3. Close SMTP connection pools
	if a.sender != nil {
		if err := a.sender.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to close sender pools: %w", err)
		}
	}

	// 4. Close Queue connections
	if a.queue != nil {
		if err := a.queue.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to close queue: %w", err)
		}
	}

	// 5. Close metrics clients
	_ = metrics.Close()

	log.Println("[INFO] cmd: graceful shutdown completed cleanly.")
	return firstErr
}

// Run starts the application and blocks until an OS termination signal is received.
func (a *App) Run(ctx context.Context) error {
	if err := a.Start(ctx); err != nil {
		return err
	}

	// Setup OS signal trapping
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigChan:
		log.Printf("[INFO] cmd: received OS signal: %v", sig)
	case <-ctx.Done():
		log.Println("[INFO] cmd: context canceled")
	}

	// Graceful shutdown with timeout
	shutdownTimeout := a.cfg.App.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 10 * time.Second
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	return a.Shutdown(shutdownCtx)
}

// Engine returns the underlying runtime engine.
func (a *App) Engine() *runtime.Engine {
	return a.engine
}

// Queue returns the underlying queue instance.
func (a *App) Queue() queue.Queue {
	return a.queue
}

// Sender returns the underlying sender instance.
func (a *App) Sender() sender.Sender {
	return a.sender
}

// Server returns the underlying HTTP server.
func (a *App) Server() *handler.Server {
	return a.server
}
