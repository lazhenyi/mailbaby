package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
)

// PushGatewayPusher periodically pushes metrics from a prometheus.Gatherer to Prometheus PushGateway.
type PushGatewayPusher struct {
	cfg       config.PushGatewayConfig
	pusher    *push.Pusher
	stopChan  chan struct{}
	doneChan  chan struct{}
	mu        sync.Mutex
	closeOnce sync.Once
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
		doneChan: make(chan struct{}),
	}
}

// Start launches the background periodic pushing loop.
func (p *PushGatewayPusher) Start(ctx context.Context) {
	interval := p.cfg.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	go func() {
		defer close(p.doneChan)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Backoff state for failed pushes. Bounded to 5 minutes so transient
		// outages don't permanently disable pushing.
		backoff := time.Duration(0)
		const maxBackoff = 5 * time.Minute
		const warnEveryFailures = 5
		consecutiveFailures := 0

		pushOnce := func() bool {
			if err := p.pusher.Push(); err != nil {
				consecutiveFailures++
				if consecutiveFailures == 1 || consecutiveFailures%warnEveryFailures == 0 {
					logger.Get().WithFields(logger.Fields{
						"url":               logger.RedactURL(p.cfg.URL),
						"error":             err.Error(),
						"consecutive_failures": consecutiveFailures,
						"backoff_seconds":   backoff.Seconds(),
					}).Warn("failed to push metrics to PushGateway")
				}
				if backoff == 0 {
					backoff = interval
				} else {
					backoff *= 2
				}
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				return false
			}
			consecutiveFailures = 0
			backoff = 0
			return true
		}

		for {
			select {
			case <-ticker.C:
				pushOnce()
			case <-p.stopChan:
				_ = p.pusher.Push() // final push, ignore error
				return
			case <-ctx.Done():
				return
			}

			if backoff > 0 {
				t := time.NewTimer(backoff)
				select {
				case <-t.C:
				case <-p.stopChan:
					_ = t.Stop()
					_ = p.pusher.Push()
					return
				case <-ctx.Done():
					_ = t.Stop()
					return
				}
			}
		}
	}()
}

// Push performs an immediate manual push to PushGateway.
func (p *PushGatewayPusher) Push() error {
	return p.pusher.Push()
}

// Stop stops the background pusher loop. Safe to call multiple times.
func (p *PushGatewayPusher) Stop() {
	p.closeOnce.Do(func() {
		close(p.stopChan)
	})
	<-p.doneChan
}
