package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/metrics"
)

func TestUnifiedHTTPServer(t *testing.T) {
	// Find available free port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	// Initialize metrics subsystem
	metricsCfg := config.MetricsConfig{
		Enabled:        true,
		Provider:       config.MetricsProviderPrometheus,
		Path:           "/metrics",
		CollectRuntime: false,
	}
	_, err = metrics.Init(metricsCfg)
	if err != nil {
		t.Fatalf("failed to init metrics: %v", err)
	}
	defer func() { _ = metrics.Close() }()

	metrics.Get().IncEmailsSent("default", "success")

	// Setup Config
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "127.0.0.1",
			Port: port,
		},
		Metrics: metricsCfg,
		Observability: config.ObservabilityConfig{
			Health: config.HealthConfig{
				Enabled:   true,
				LivePath:  "/livez",
				ReadyPath: "/readyz",
			},
			Pprof: config.PprofConfig{
				Enabled: true,
				Path:    "/debug/pprof",
			},
		},
	}

	server, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Register readiness checkers
	server.RegisterChecker("queue", func(ctx context.Context) error {
		return nil // healthy
	})
	server.RegisterChecker("smtp", func(ctx context.Context) error {
		return nil // healthy
	})

	// Register custom route
	server.RegisterHandleFunc("/api/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() { _ = server.Stop(ctx) }()

	// Wait briefly for server startup
	time.Sleep(60 * time.Millisecond)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// 1. Test /metrics
	t.Run("GET /metrics", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/metrics")
		if err != nil {
			t.Fatalf("failed to get /metrics: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on /metrics, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "mailbaby_email_sent_total") {
			t.Errorf("expected /metrics to contain mailbaby_email_sent_total, got:\n%s", string(body))
		}
	})

	// 2. Test /livez
	t.Run("GET /livez", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/livez")
		if err != nil {
			t.Fatalf("failed to get /livez: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on /livez, got %d", resp.StatusCode)
		}

		var payload map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode JSON from /livez: %v", err)
		}
		if payload["status"] != "UP" {
			t.Errorf("expected status 'UP', got %v", payload["status"])
		}
	})

	// 3. Test /readyz (All Healthy)
	t.Run("GET /readyz (Healthy)", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/readyz")
		if err != nil {
			t.Fatalf("failed to get /readyz: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on /readyz, got %d", resp.StatusCode)
		}

		var payload struct {
			Status     string            `json:"status"`
			Components map[string]string `json:"components"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode JSON from /readyz: %v", err)
		}
		if payload.Status != "UP" {
			t.Errorf("expected status 'UP', got %q", payload.Status)
		}
		if payload.Components["queue"] != "UP" || payload.Components["smtp"] != "UP" {
			t.Errorf("unexpected components status: %v", payload.Components)
		}
	})

	// 4. Test /readyz (Unhealthy Component)
	t.Run("GET /readyz (Unhealthy)", func(t *testing.T) {
		server.RegisterChecker("failing_service", func(ctx context.Context) error {
			return errors.New("connection refused")
		})

		resp, err := http.Get(baseURL + "/readyz")
		if err != nil {
			t.Fatalf("failed to get /readyz: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 on /readyz when component fails, got %d", resp.StatusCode)
		}

		var payload struct {
			Status     string            `json:"status"`
			Components map[string]string `json:"components"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		if payload.Status != "DOWN" {
			t.Errorf("expected status 'DOWN', got %q", payload.Status)
		}
		if !strings.Contains(payload.Components["failing_service"], "DOWN") {
			t.Errorf("expected failing_service to be DOWN, got %v", payload.Components)
		}
	})

	// 5. Test /healthz
	t.Run("GET /healthz", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			t.Fatalf("failed to get /healthz: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 on /healthz, got %d", resp.StatusCode)
		}
	})

	// 6. Test /debug/pprof/
	t.Run("GET /debug/pprof/", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/debug/pprof/")
		if err != nil {
			t.Fatalf("failed to get /debug/pprof/: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on /debug/pprof/, got %d", resp.StatusCode)
		}
	})

	// 7. Test /debug/pprof/cmdline
	t.Run("GET /debug/pprof/cmdline", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/debug/pprof/cmdline")
		if err != nil {
			t.Fatalf("failed to get /debug/pprof/cmdline: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 on /debug/pprof/cmdline, got %d", resp.StatusCode)
		}
	})

	// 8. Test Custom Route
	t.Run("GET /api/ping", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/api/ping")
		if err != nil {
			t.Fatalf("failed to get /api/ping: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "pong" {
			t.Errorf("expected 'pong', got %q", string(body))
		}
	})

	// 9. Metric label cardinality must not grow with arbitrary paths
	t.Run("GET unknown path is bucketed as unmatched", func(t *testing.T) {
		// Hit a path that has no registered route.
		resp, err := http.Get(baseURL + "/some/random/path/12345")
		if err != nil {
			t.Fatalf("failed to GET random path: %v", err)
		}
		_ = resp.Body.Close()

		time.Sleep(10 * time.Millisecond)

		metricsResp, err := http.Get(baseURL + "/metrics")
		if err != nil {
			t.Fatalf("failed to get /metrics: %v", err)
		}
		defer func() { _ = metricsResp.Body.Close() }()

		body, _ := io.ReadAll(metricsResp.Body)
		metricsBody := string(body)
		if !strings.Contains(metricsBody, `handler="unmatched"`) {
			t.Errorf("expected requests_total to use handler=\"unmatched\", got:\n%s", metricsBody)
		}
		if strings.Contains(metricsBody, `handler="/some/random/path/12345"`) {
			t.Errorf("metric label must not expose raw request paths (cardinality DoS):\n%s", metricsBody)
		}
	})
}
