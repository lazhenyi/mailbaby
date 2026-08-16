package handler

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
	"mailbaby/internal/tracing"
)

// Option configures Server options.
type Option func(*Server)

// WithSender configures the SMTP sender for direct HTTP email delivery.
func WithSender(s sender.Sender) Option {
	return func(srv *Server) {
		srv.sender = s
	}
}

// WithProducer configures the queue producer for asynchronous HTTP email delivery.
func WithProducer(p queue.Producer, queueName string) Option {
	return func(srv *Server) {
		srv.producer = p
		srv.queueName = queueName
	}
}

// Server coordinates HTTP endpoints for health probes, Prometheus metrics, and pprof profiling.
type Server struct {
	cfg        *config.Config
	mux        *http.ServeMux
	httpServer *http.Server
	health     *HealthManager
	sender     sender.Sender
	producer   queue.Producer
	queueName  string
	mu         sync.Mutex
	running    bool
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// New creates and configures the unified HTTP Server based on configuration.
func New(cfg *config.Config, opts ...Option) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("handler: config cannot be nil")
	}

	mux := http.NewServeMux()
	healthMgr := NewHealthManager(cfg.Observability.Health)

	// 1. Mount Health Probes if enabled
	if cfg.Observability.Health.Enabled {
		healthMgr.Mount(mux)
	}

	// 2. Mount Metrics if enabled
	if cfg.Metrics.Enabled {
		m := metrics.Get()
		if m != nil && m.Registry() != nil {
			metricsPath := cfg.Metrics.Path
			if metricsPath == "" {
				metricsPath = "/metrics"
			}
			mux.Handle(metricsPath, promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{
				EnableOpenMetrics: true,
			}))
		}

		if strings.EqualFold(string(cfg.Metrics.Provider), string(config.MetricsProviderExpVar)) {
			mux.Handle("/debug/vars", expvar.Handler())
		}
	}

	// 3. Mount Pprof Profiler if enabled
	if cfg.Observability.Pprof.Enabled {
		MountPprof(mux, cfg.Observability.Pprof)
	}

	srv := &Server{
		cfg:       cfg,
		mux:       mux,
		health:    healthMgr,
	}

	for _, opt := range opts {
		opt(srv)
	}

	// 4. Mount Email Sending Routes if Sender or Producer configured
	if srv.sender != nil || srv.producer != nil {
		srv.MountEmailRoutes()
	}

	readTimeout := cfg.Server.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 10 * time.Second
	}
	writeTimeout := cfg.Server.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	idleTimeout := cfg.Server.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}

	// HTTP Logging & Metrics Interceptor
	corsOrigin := cfg.Server.CORSAllowedOrigin
	trustProxy := cfg.Server.TrustProxyHeaders
	interceptor := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqCtx, span := tracing.StartSpan(r.Context(), "http.server_request")
		defer span.End()

		clientIP := clientAddr(r, trustProxy)
		span.SetAttribute("http.method", r.Method)
		span.SetAttribute("http.url", r.URL.Path)
		span.SetAttribute("http.remote_addr", clientIP)

		if corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", corsOrigin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Request-ID")
			w.Header().Set("Access-Control-Max-Age", "600")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}

		srw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		defer func() {
			if rec := recover(); rec != nil {
				logger.Get().WithContext(reqCtx).WithFields(logger.Fields{
					"panic":      fmt.Sprintf("%v", rec),
					"http.path":  r.URL.Path,
					"http.method": r.Method,
				}).Error("recovered panic in HTTP handler")
				if srw.statusCode == http.StatusOK {
					srw.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"code":500,"error":"internal_error"}`))
				}
			}
		}()
		mux.ServeHTTP(srw, r.WithContext(reqCtx))

		duration := time.Since(start)
		span.SetAttribute("http.status_code", srw.statusCode)

		// Use the registered route pattern (Go 1.22+ ServeMux) as the metric
		// label to avoid unbounded label cardinality from arbitrary paths.
		handlerLabel := r.Pattern
		if handlerLabel == "" {
			handlerLabel = "unmatched"
		}

		// Record HTTP metrics
		metrics.Get().ObserveHTTPRequest(handlerLabel, r.Method, srw.statusCode, duration)

		// Structured request logging
		logger.Get().WithContext(reqCtx).WithFields(logger.Fields{
			"method":   r.Method,
			"path":     r.URL.Path,
			"handler":  handlerLabel,
			"status":   srw.statusCode,
			"duration": duration.String(),
			"client":   clientIP,
		}).Debug("served HTTP request")
	})

	httpServer := &http.Server{
		Addr:         cfg.Server.Address(),
		Handler:      interceptor,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	return &Server{
		cfg:        cfg,
		mux:        mux,
		httpServer: httpServer,
		health:     healthMgr,
	}, nil
}

// RegisterChecker registers a component health checker for /readyz probe.
// Checkers are guarded by an internal mutex and may be registered while running.
func (s *Server) RegisterChecker(name string, checker Checker) {
	if s.health != nil {
		s.health.RegisterChecker(name, checker)
	}
}

// RegisterRoute registers a custom HTTP route handler on the server mux.
// Routing must be configured before Start; the Go ServeMux panics if modified after serving begins.
func (s *Server) RegisterRoute(pattern string, h http.Handler) {
	s.assertNotRunning()
	s.mux.Handle(pattern, h)
}

// RegisterHandleFunc registers a custom HTTP handler function on the server mux.
func (s *Server) RegisterHandleFunc(pattern string, h http.HandlerFunc) {
	s.assertNotRunning()
	s.mux.HandleFunc(pattern, h)
}

func (s *Server) assertNotRunning() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		panic("handler: cannot register HTTP routes after the server has started")
	}
}

// Handler returns the underlying http.Handler (useful for testing).
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// Address returns the listening host:port address.
func (s *Server) Address() string {
	return s.httpServer.Addr
}

// Start starts the unified HTTP server in a background goroutine.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.mu.Unlock()

	errChan := make(chan error, 1)
	go func() {
		var serveErr error
		if s.cfg.Server.TLSEnabled && s.cfg.Server.TLSCertPath != "" && s.cfg.Server.TLSKeyPath != "" {
			serveErr = s.httpServer.ListenAndServeTLS(s.cfg.Server.TLSCertPath, s.cfg.Server.TLSKeyPath)
		} else {
			serveErr = s.httpServer.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errChan <- serveErr
		}
		close(errChan)
	}()

	// Brief wait to detect immediate bind failures
	select {
	case err := <-errChan:
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		return fmt.Errorf("http: server start error on %s: %w", s.httpServer.Addr, err)
	case <-time.After(50 * time.Millisecond):
		logger.Get().WithFields(logger.Fields{
			"addr":    s.httpServer.Addr,
			"tls":     s.cfg.Server.TLSEnabled,
			"metrics": s.cfg.Metrics.Enabled,
			"health":  s.cfg.Observability.Health.Enabled,
			"pprof":   s.cfg.Observability.Pprof.Enabled,
		}).Info("unified HTTP server started")
		return nil
	}
}

// Stop gracefully shuts down the HTTP server. If ctx expires before all
// in-flight requests complete, the server's underlying net.Listener is
// closed forcefully so the caller is not blocked indefinitely.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	logger.Get().Info("stopping unified HTTP server...")
	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		logger.Get().WithError(err).Warn("HTTP graceful shutdown timed out; forcing close")
		_ = s.httpServer.Close()
	}
	return err
}

// HealthManager returns the internal health manager.
func (s *Server) HealthManager() *HealthManager {
	return s.health
}

// MountEmailRoutes mounts email delivery HTTP endpoints with authentication middleware.
// Routes follow the /v1/email/* canonical path. The legacy /api/v1/* aliases are
// deprecated and only mounted when cfg.Server.LegacyAPIV1 is true.
func (s *Server) MountEmailRoutes() {
	s.assertNotRunning()
	emailH := NewEmailHandler(s.sender, s.producer, s.queueName)
	authMW := AuthMiddleware(s.cfg.Auth)

	s.mux.Handle("POST /v1/email/send", authMW(http.HandlerFunc(emailH.HandleSend)))
	s.mux.Handle("POST /v1/email/batch", authMW(http.HandlerFunc(emailH.HandleBatchSend)))

	// Optional legacy aliases (kept behind a flag for backward compatibility).
	if s.cfg.Server.LegacyAPIV1 {
		s.mux.Handle("POST /api/v1/send", authMW(http.HandlerFunc(emailH.HandleSend)))
		s.mux.Handle("POST /api/v1/batch", authMW(http.HandlerFunc(emailH.HandleBatchSend)))
	}
}
