package metrics

import (
	"context"
	"sync"

	"mailbaby/internal/config"
)

var (
	globalMetrics *Metrics
	globalMu      sync.RWMutex
	noopMetrics   = &Metrics{}
)

// Init initializes the global Metrics instance and starts background workers (e.g. PushGateway).
func Init(cfg config.MetricsConfig) (*Metrics, error) {
	globalMu.Lock()
	defer globalMu.Unlock()

	if !cfg.Enabled {
		globalMetrics = noopMetrics
		return noopMetrics, nil
	}

	m, err := NewMetrics(cfg)
	if err != nil {
		return nil, err
	}

	// Start PushGateway if enabled
	if cfg.PushGateway.Enabled && m.Registry() != nil {
		pusher := NewPushGatewayPusher(cfg.PushGateway, m.Registry())
		pusher.Start(context.Background())
	}

	globalMetrics = m
	return m, nil
}

// Get returns the global Metrics collector instance.
// If metrics are not initialized or disabled, a safe no-op instance is returned.
func Get() *Metrics {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if globalMetrics == nil {
		return noopMetrics
	}
	return globalMetrics
}

// Close closes all metric network clients.
func Close() error {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalMetrics != nil && globalMetrics != noopMetrics {
		err := globalMetrics.Close()
		globalMetrics = nil
		return err
	}
	return nil
}
