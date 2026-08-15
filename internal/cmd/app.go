package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/handler"
	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/rpc"
	"mailbaby/internal/runtime"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
)

// App is the top-level application container coordinating the lifecycle of all subsystems.
type App struct {
	cfg       *config.Config
	queue     queue.Queue
	sender    sender.Sender
	metrics   *metrics.Metrics
	tracer    *tracing.TracerProvider
	engine    *runtime.Engine
	server    *handler.Server
	rpcServer *rpc.Server
	statsStop chan struct{}
	statsWG   sync.WaitGroup
	mu        sync.Mutex
	running   bool
}

// NewApp initializes and wires all application dependencies based on the provided configuration.
func NewApp(cfg *config.Config) (*App, error) {
	if cfg == nil {
		return nil, errors.New("cmd: config cannot be nil")
	}

	// 1. Initialize Logger
	if err := logger.Init(cfg.Log); err != nil {
		return nil, fmt.Errorf("cmd: failed to initialize logger: %w", err)
	}

	// 2. Initialize Distributed Tracing
	tracer, err := tracing.Init(cfg.Observability.Tracing)
	if err != nil {
		return nil, fmt.Errorf("cmd: failed to initialize tracing: %w", err)
	}

	// 3. Initialize Queue driver
	q, err := queue.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("cmd: failed to initialize queue driver %q: %w", cfg.Queue.Driver, err)
	}

	// 4. Initialize SMTP Sender subsystem
	mailSender, err := sender.NewFromConfig(cfg)
	if err != nil {
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize mail sender: %w", err)
	}

	// 5. Initialize Metrics subsystem
	m, err := metrics.Init(cfg.Metrics)
	if err != nil {
		_ = mailSender.Close()
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize metrics: %w", err)
	}

	// 6. Initialize Execution Engine (Queue -> Sender Core)
	engine, err := runtime.New(q, mailSender, cfg)
	if err != nil {
		_ = metrics.Close()
		_ = mailSender.Close()
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize execution engine: %w", err)
	}

	// 7. Obtain Queue Producer for HTTP/RPC async sending
	producer, _ := q.Producer()

	// 8. Initialize Unified HTTP Server (Metrics, Health, Pprof, Email REST API)
	httpServer, err := handler.New(cfg,
		handler.WithSender(mailSender),
		handler.WithProducer(producer, cfg.Queue.TopicName()),
	)
	if err != nil {
		_ = metrics.Close()
		_ = mailSender.Close()
		_ = q.Close()
		return nil, fmt.Errorf("cmd: failed to initialize http server: %w", err)
	}

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

	// 9. Initialize gRPC RPC Server if enabled
	var rpcSrv *rpc.Server
	if cfg.GRPC.Enabled {
		rpcSrv, err = rpc.New(cfg, mailSender, producer)
		if err != nil {
			_ = metrics.Close()
			_ = mailSender.Close()
			_ = q.Close()
			return nil, fmt.Errorf("cmd: failed to initialize rpc server: %w", err)
		}
	}

	return &App{
		cfg:       cfg,
		queue:     q,
		sender:    mailSender,
		metrics:   m,
		tracer:    tracer,
		engine:    engine,
		server:    httpServer,
		rpcServer: rpcSrv,
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
	logger.Get().WithFields(logger.Fields{
		"driver": string(a.queue.Driver()),
		"topic":  a.queue.Name(),
	}).Info("runtime engine started")

	// 2. Start HTTP server
	if err := a.server.Start(ctx); err != nil {
		_ = a.engine.Stop(ctx)
		return fmt.Errorf("cmd: failed to start http server: %w", err)
	}
	logger.Get().WithField("addr", a.cfg.Server.Address()).Info("unified HTTP server listening")

	// 3. Start gRPC server if configured
	if a.rpcServer != nil {
		if err := a.rpcServer.Start(ctx); err != nil {
			_ = a.server.Stop(ctx)
			_ = a.engine.Stop(ctx)
			return fmt.Errorf("cmd: failed to start gRPC server: %w", err)
		}
		logger.Get().WithField("addr", a.cfg.GRPC.Address()).Info("gRPC email server listening")
	}

	// 4. Start background metrics collector (queue depth / SMTP pool stats / uptime)
	if a.cfg.Metrics.Enabled {
		metrics.Get().SetAppInfo(a.cfg.App.Name, a.cfg.App.Env, Version)
		a.startMetricsCollector()
	}

	return nil
}

// startMetricsCollector periodically refreshes queue depth, SMTP pool stats and
// app uptime gauges, gated by the collect_queue_stats / collect_smtp_stats flags.
func (a *App) startMetricsCollector() {
	if a.statsStop != nil {
		return
	}
	a.statsStop = make(chan struct{})
	a.statsWG.Add(1)

	go func() {
		defer a.statsWG.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				if a.cfg.Metrics.CollectQueueStats && a.queue != nil {
					if st, err := a.queue.Stats(ctx); err == nil {
						metrics.Get().SetQueueDepth(string(st.Driver), st.Name, float64(st.Ready))
						metrics.Get().SetQueueInFlight(string(st.Driver), st.Name, float64(st.InFlight))
					}
				}

				if a.cfg.Metrics.CollectSmtpStats && a.sender != nil {
					for account, st := range a.sender.Stats() {
						metrics.Get().SetSmtpPoolStats(account, st.Pool.ActiveConns, st.Pool.IdleConns)
					}
				}

				metrics.Get().UpdateAppUptime()
				cancel()

			case <-a.statsStop:
				return
			}
		}
	}()
}

func (a *App) stopMetricsCollector() {
	if a.statsStop == nil {
		return
	}
	close(a.statsStop)
	a.statsWG.Wait()
	a.statsStop = nil
}

// Shutdown stops all running services and releases all allocated connections in reverse order.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}
	a.running = false

	logger.Get().Info("initiating graceful shutdown...")

	var firstErr error

	// 0. Stop metrics collector before tearing down dependencies
	a.stopMetricsCollector()

	// 1. Stop gRPC server
	if a.rpcServer != nil {
		if err := a.rpcServer.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to stop gRPC server: %w", err)
		}
	}

	// 2. Stop HTTP server (stops incoming traffic and health probes)
	if a.server != nil {
		if err := a.server.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cmd: failed to stop http server: %w", err)
		}
	}

	// 3. Stop runtime engine and drain in-flight jobs
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

	// 6. Close tracing exporter
	if a.tracer != nil {
		_ = a.tracer.Close(ctx)
	}

	// 7. Flush logs
	_ = logger.Sync()

	logger.Get().Info("graceful shutdown completed cleanly")
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
		logger.Get().WithField("signal", sig.String()).Info("received OS signal, shutting down")
	case <-ctx.Done():
		logger.Get().Info("context canceled, shutting down")
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

// RPCServer returns the underlying gRPC server (nil if not enabled).
func (a *App) RPCServer() *rpc.Server {
	return a.rpcServer
}
