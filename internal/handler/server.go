package handler

import (
	"context"
	"expvar"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mailbaby/internal/config"
	"mailbaby/internal/metrics"
)

// Server represents the unified HTTP server hosting metrics, health checks, and pprof profiling.
type Server struct {
	cfg        *config.Config
	mux        *http.ServeMux
	httpServer *http.Server
	health     *HealthManager
	mu         sync.Mutex
	running    bool
}

// New creates and configures a new unified HTTP Server.
func New(cfg *config.Config) *Server {
	if cfg == nil {
		cfg = &config.Config{}
	}
	cfg.Server.ApplyDefaults()

	mux := http.NewServeMux()
	healthMgr := NewHealthManager(cfg.Observability.Health)

	// 1. Mount Health Probes if enabled
	if cfg.Observability.Health.Enabled {
		livePath := cfg.Observability.Health.LivePath
		if livePath == "" {
			livePath = "/livez"
		}
		readyPath := cfg.Observability.Health.ReadyPath
		if readyPath == "" {
			readyPath = "/readyz"
		}

		mux.HandleFunc(livePath, healthMgr.LivenessHandler())
		mux.HandleFunc(readyPath, healthMgr.ReadinessHandler())
		mux.HandleFunc("/healthz", healthMgr.HealthzHandler())
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

	httpServer := &http.Server{
		Addr:         cfg.Server.Address(),
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	return &Server{
		cfg:        cfg,
		mux:        mux,
		httpServer: httpServer,
		health:     healthMgr,
	}
}

// RegisterChecker registers a component health checker for /readyz probe.
func (s *Server) RegisterChecker(name string, checker Checker) {
	if s.health != nil {
		s.health.RegisterChecker(name, checker)
	}
}

// RegisterRoute registers a custom HTTP route handler on the server mux.
func (s *Server) RegisterRoute(pattern string, h http.Handler) {
	s.mux.Handle(pattern, h)
}

// RegisterHandleFunc registers a custom HTTP handler function on the server mux.
func (s *Server) RegisterHandleFunc(pattern string, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, h)
}

// Handler returns the underlying http.Handler (useful for testing).
func (s *Server) Handler() http.Handler {
	return s.mux
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

	go func() {
		log.Printf("[INFO] http: unified server listening on %s (metrics=%v, health=%v, pprof=%v)",
			s.httpServer.Addr, s.cfg.Metrics.Enabled, s.cfg.Observability.Health.Enabled, s.cfg.Observability.Pprof.Enabled)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[ERROR] http: unified server exited with error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}
	s.running = false

	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
