package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
)

// PushGatewayPusher periodically pushes metrics from a prometheus.Gatherer to Prometheus PushGateway.
type PushGatewayPusher struct {
	cfg      config.PushGatewayConfig
	pusher   *push.Pusher
	stopChan chan struct{}
}

// NewPushGatewayPusher creates a new PushGatewayPusher.
func NewPushGatewayPusher(cfg config.PushGatewayConfig, gatherer prometheus.Gatherer) *PushGatewayPusher {
	pusher := push.New(cfg.URL, cfg.Job).Gatherer(gatherer)
	if cfg.BasicAuth.Username != "" || cfg.BasicAuth.Password != "" {
		pusher.BasicAuth(cfg.BasicAuth.Username, cfg.BasicAuth.Password)
	}

	return &PushGatewayPusher{
		cfg:      cfg,
		pusher:   pusher,
		stopChan: make(chan struct{}),
	}
}

// Start launches the background periodic pushing loop.
func (p *PushGatewayPusher) Start(ctx context.Context) {
	interval := p.cfg.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := p.pusher.Push(); err != nil {
					logger.Get().WithFields(logger.Fields{
						"url":   logger.RedactURL(p.cfg.URL),
						"error": err.Error(),
					}).Warn("failed to push metrics to PushGateway")
				}
			case <-p.stopChan:
				_ = p.pusher.Push() // final push
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Push performs an immediate manual push to PushGateway.
func (p *PushGatewayPusher) Push() error {
	return p.pusher.Push()
}

// Stop stops the background pusher loop.
func (p *PushGatewayPusher) Stop() {
	select {
	case <-p.stopChan:
	default:
		close(p.stopChan)
	}
}
