package sender

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/metrics"
)

// PoolStats provides runtime metrics for the SMTP connection pool.
type PoolStats struct {
	MaxOpenConns int   `json:"max_open_conns"`
	MaxIdleConns int   `json:"max_idle_conns"`
	ActiveConns  int64 `json:"active_conns"`
	IdleConns    int   `json:"idle_conns"`
}

// SmtpConnPool manages a thread-safe connection pool for a specific SMTP account.
type SmtpConnPool struct {
	cfg         config.SmtpAccountConfig
	idleConns   chan *SmtpClient
	activeConns int64
	maxOpen     int
	maxIdle     int
	idleTimeout time.Duration

	mu     sync.RWMutex
	closed bool
	sem    chan struct{} // limits max concurrent open connections
}

// NewSmtpConnPool creates a new SMTP connection pool for the given account configuration.
func NewSmtpConnPool(cfg config.SmtpAccountConfig) *SmtpConnPool {
	maxIdle := cfg.Pool.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 5
	}
	maxOpen := cfg.Pool.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 20
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}

	idleTimeout := cfg.Pool.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 60 * time.Second
	}

	return &SmtpConnPool{
		cfg:         cfg,
		idleConns:   make(chan *SmtpClient, maxIdle),
		maxOpen:     maxOpen,
		maxIdle:     maxIdle,
		idleTimeout: idleTimeout,
		sem:         make(chan struct{}, maxOpen),
	}
}

// Acquire retrieves an idle connection or establishes a new connection within context deadline.
func (p *SmtpConnPool) Acquire(ctx context.Context) (*SmtpClient, error) {
	waitStart := time.Now()
	defer func() {
		metrics.Get().ObserveSmtpPoolWait(p.cfg.From, time.Since(waitStart))
		metrics.Get().SetSmtpPoolStats(p.cfg.From, atomic.LoadInt64(&p.activeConns), len(p.idleConns))
	}()

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	// 1. Try to get an existing idle connection first
	for {
		select {
		case client := <-p.idleConns:
			// A nil value is received when the idle channel has been closed
			// concurrently by Close().
			if client == nil {
				return nil, ErrPoolClosed
			}
			// Check if idle connection has timed out
			if time.Since(client.LastUsed()) > p.idleTimeout {
				_ = client.Close()
				p.decrementActive()
				continue
			}

			// Validate connection liveness using RSET
			if err := client.Reset(); err != nil {
				_ = client.Close()
				p.decrementActive()
				continue
			}

			return client, nil
		default:
			// No idle connection readily available in channel
			goto createNew
		}
	}

createNew:
	// Check if semaphore is already full
	if len(p.sem) >= p.maxOpen {
		metrics.Get().IncSmtpPoolExhausted(p.cfg.From)
	}

	// 2. Acquire a semaphore token to create a new connection
	select {
	case p.sem <- struct{}{}:
		// Semaphore acquired, increment active count and dial
		atomic.AddInt64(&p.activeConns, 1)

		client, err := Dial(ctx, p.cfg)
		if err != nil {
			p.decrementActive()
			return nil, fmt.Errorf("sender pool: failed to dial SMTP: %w", err)
		}
		return client, nil

	case client := <-p.idleConns:
		// Another goroutine released a connection while we waited
		if client == nil {
			return nil, ErrPoolClosed
		}
		if time.Since(client.LastUsed()) > p.idleTimeout || client.Reset() != nil {
			_ = client.Close()
			p.decrementActive()
			return p.Acquire(ctx)
		}
		return client, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Release returns a connection to the pool or closes it if broken / pool closed.
func (p *SmtpConnPool) Release(client *SmtpClient, err error) {
	if client == nil {
		return
	}

	defer func() {
		metrics.Get().SetSmtpPoolStats(p.cfg.From, atomic.LoadInt64(&p.activeConns), len(p.idleConns))
	}()

	p.mu.RLock()
	isClosed := p.closed
	p.mu.RUnlock()

	// If sending failed or pool closed, destroy connection
	if err != nil || isClosed {
		_ = client.Close()
		p.decrementActive()
		return
	}

	// Try to return to idle channel
	select {
	case p.idleConns <- client:
		// Returned successfully
	default:
		// Idle queue is full, close excess connection
		_ = client.Close()
		p.decrementActive()
	}
}

func (p *SmtpConnPool) decrementActive() {
	atomic.AddInt64(&p.activeConns, -1)
	select {
	case <-p.sem:
	default:
	}
}

// Close closes all idle connections and terminates the pool.
func (p *SmtpConnPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	close(p.idleConns)
	for client := range p.idleConns {
		_ = client.Close()
		p.decrementActive()
	}

	metrics.Get().SetSmtpPoolStats(p.cfg.From, 0, 0)
	return nil
}

// Stats returns current statistics of this connection pool.
func (p *SmtpConnPool) Stats() PoolStats {
	return PoolStats{
		MaxOpenConns: p.maxOpen,
		MaxIdleConns: p.maxIdle,
		ActiveConns:  atomic.LoadInt64(&p.activeConns),
		IdleConns:    len(p.idleConns),
	}
}
