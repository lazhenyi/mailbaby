package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"mailbaby/internal/config"
)

// Checker is a function that tests the readiness of a subsystem (e.g. DB, MQ, SMTP).
type Checker func(ctx context.Context) error

// HealthManager manages liveness and readiness probe handlers.
type HealthManager struct {
	cfg      config.HealthConfig
	checkers map[string]Checker
	mu       sync.RWMutex
}

// NewHealthManager creates a new HealthManager.
func NewHealthManager(cfg config.HealthConfig) *HealthManager {
	cfg.ApplyDefaults()
	return &HealthManager{
		cfg:      cfg,
		checkers: make(map[string]Checker),
	}
}

// RegisterChecker registers a component health check callback.
func (h *HealthManager) RegisterChecker(name string, checker Checker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers[name] = checker
}

// LivenessHandler handles Kubernetes /livez probes.
func (h *HealthManager) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "UP",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// ReadinessHandler handles Kubernetes /readyz probes by checking all registered components.
func (h *HealthManager) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timeout := h.cfg.CheckTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		h.mu.RLock()
		checkers := make(map[string]Checker, len(h.checkers))
		for k, v := range h.checkers {
			checkers[k] = v
		}
		h.mu.RUnlock()

		components := make(map[string]string, len(checkers))
		isHealthy := true

		var wg sync.WaitGroup
		var mu sync.Mutex

		for name, checker := range checkers {
			wg.Add(1)
			go func(n string, chk Checker) {
				defer wg.Done()
				if err := chk(ctx); err != nil {
					mu.Lock()
					isHealthy = false
					components[n] = "DOWN: " + err.Error()
					mu.Unlock()
				} else {
					mu.Lock()
					components[n] = "UP"
					mu.Unlock()
				}
			}(name, checker)
		}
		wg.Wait()

		w.Header().Set("Content-Type", "application/json")
		statusStr := "UP"
		statusCode := http.StatusOK

		if !isHealthy {
			statusStr = "DOWN"
			statusCode = http.StatusServiceUnavailable
		}

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     statusStr,
			"components": components,
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// HealthzHandler provides a simple general health probe.
func (h *HealthManager) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	}
}

// Mount attaches health check endpoints to the provided HTTP ServeMux.
func (h *HealthManager) Mount(mux *http.ServeMux) {
	livePath := h.cfg.LivePath
	if livePath == "" {
		livePath = "/livez"
	}
	readyPath := h.cfg.ReadyPath
	if readyPath == "" {
		readyPath = "/readyz"
	}
	mux.HandleFunc(livePath, h.LivenessHandler())
	mux.HandleFunc(readyPath, h.ReadinessHandler())
	mux.HandleFunc("/healthz", h.HealthzHandler())
}
