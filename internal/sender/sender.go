package sender

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/tracing"
)

// AccountStats reports runtime performance numbers for a single SMTP account.
type AccountStats struct {
	Name        string    `json:"name"`
	Host        string    `json:"host"`
	Port        int       `json:"port"`
	TotalSent   int64     `json:"total_sent"`
	TotalFailed int64     `json:"total_failed"`
	Pool        PoolStats `json:"pool"`
}

// AccountSender represents the sending operations for a specific SMTP account.
type AccountSender interface {
	Name() string
	Config() config.SmtpAccountConfig
	Send(ctx context.Context, email *Email) error
	Stats() AccountStats
	Close() error
}

// Sender defines the top-level mail sending interface.
type Sender interface {
	// Send delivers a single email message through the designated or default SMTP account.
	Send(ctx context.Context, email *Email) error

	// SendBatch delivers multiple email messages concurrently or sequentially.
	SendBatch(ctx context.Context, emails []*Email) []error

	// Account returns the AccountSender for a specific named SMTP account.
	Account(name string) (AccountSender, error)

	// AccountNames returns all configured account names.
	AccountNames() []string

	// Stats returns aggregated sending statistics across all accounts.
	Stats() map[string]AccountStats

	// Close closes all connection pools across all accounts.
	Close() error
}

type accountSender struct {
	name        string
	cfg         config.SmtpAccountConfig
	pool        *SmtpConnPool
	rateLimiter chan struct{} // rate limiting tokens
	stopRate    chan struct{}
	totalSent   int64
	totalFailed int64
}

func newAccountSender(name string, cfg config.SmtpAccountConfig) *accountSender {
	cfg.ApplyDefaults()
	as := &accountSender{
		name: name,
		cfg:  cfg,
		pool: NewSmtpConnPool(cfg),
	}

	// Initialize rate limiter if configured (> 0 emails per second).
	// The token capacity is capped to bound memory usage; refill continues at
	// the configured rate afterwards.
	if cfg.RateLimit.EmailsPerSecond > 0 {
		rate := cfg.RateLimit.EmailsPerSecond
		// Cap the bucket at the configured rate to avoid unbounded memory.
		capacity := rate
		if capacity > 1000 {
			capacity = 1000
		}
		as.rateLimiter = make(chan struct{}, capacity)
		as.stopRate = make(chan struct{})

		// Prefill bucket
		for i := 0; i < capacity; i++ {
			as.rateLimiter <- struct{}{}
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Get().WithField("panic", fmt.Sprintf("%v", r)).Error("rate limiter goroutine panic recovered")
				}
			}()
			interval := time.Second / time.Duration(rate)
			if interval < time.Millisecond {
				interval = time.Millisecond
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					select {
					case as.rateLimiter <- struct{}{}:
					default:
					}
				case <-as.stopRate:
					return
				}
			}
		}()
	}

	return as
}

func (a *accountSender) Name() string {
	return a.name
}

func (a *accountSender) Config() config.SmtpAccountConfig {
	return a.cfg
}

func (a *accountSender) Send(ctx context.Context, email *Email) error {
	ctx, span := tracing.StartSpan(ctx, "sender.send_email")
	defer span.End()

	if email == nil {
		span.RecordError(ErrNilEmail)
		return ErrNilEmail
	}

	span.SetAttribute("email.account", a.name)

	if err := email.Validate(); err != nil {
		atomic.AddInt64(&a.totalFailed, 1)
		span.RecordError(err)
		return err
	}

	recipients := email.AllRecipients()
	if len(recipients) == 0 {
		atomic.AddInt64(&a.totalFailed, 1)
		span.RecordError(ErrNoRecipients)
		return ErrNoRecipients
	}

	span.SetAttribute("email.recipients_count", len(recipients))

	// Verify maximum recipients limit
	maxRcpt := a.cfg.RateLimit.MaxRecipientsPerEmail
	if maxRcpt > 0 && len(recipients) > maxRcpt {
		atomic.AddInt64(&a.totalFailed, 1)
		err := fmt.Errorf("%w: got %d recipients, limit is %d", ErrMaxRecipientsExceeded, len(recipients), maxRcpt)
		span.RecordError(err)
		return err
	}

	// Enforce maximum email size limit before building the MIME payload.
	if limit := a.cfg.RateLimit.EmailSizeLimit; limit > 0 {
		if data, err := email.ToJSON(); err == nil && int64(len(data)) > limit {
			atomic.AddInt64(&a.totalFailed, 1)
			sizeErr := fmt.Errorf("%w: payload %d bytes exceeds limit %d", ErrEmailTooLarge, len(data), limit)
			span.RecordError(sizeErr)
			return sizeErr
		}
	}

	// Wait for rate limiter token if configured
	if a.rateLimiter != nil {
		select {
		case <-a.rateLimiter:
		case <-ctx.Done():
			atomic.AddInt64(&a.totalFailed, 1)
			span.RecordError(ctx.Err())
			return ctx.Err()
		}
	}

	// Build MIME payload
	msgBytes, err := BuildMIME(email, a.cfg.From, a.cfg.FromName)
	if err != nil {
		atomic.AddInt64(&a.totalFailed, 1)
		span.RecordError(err)
		return fmt.Errorf("sender: failed to build MIME message: %w", err)
	}

	span.SetAttribute("email.bytes", len(msgBytes))

	// Determine envelope FROM
	envelopeFrom := email.From
	if envelopeFrom == "" {
		envelopeFrom = a.cfg.From
	}

	// Acquire connection from pool
	client, err := a.pool.Acquire(ctx)
	if err != nil {
		atomic.AddInt64(&a.totalFailed, 1)
		span.RecordError(err)
		return err
	}

	// Send message
	sendErr := client.Send(ctx, envelopeFrom, recipients, msgBytes)
	a.pool.Release(client, sendErr)

	if sendErr != nil {
		atomic.AddInt64(&a.totalFailed, 1)
		span.RecordError(sendErr)
		logger.Get().WithContext(ctx).WithFields(logger.Fields{
			"account": a.name,
			"rcpt":    len(recipients),
			"error":   sendErr.Error(),
		}).Error("failed to deliver email via SMTP")
		return sendErr
	}

	atomic.AddInt64(&a.totalSent, 1)
	logger.Get().WithContext(ctx).WithFields(logger.Fields{
		"account": a.name,
		"rcpt":    len(recipients),
		"bytes":   len(msgBytes),
	}).Debug("email delivered successfully via SMTP")

	return nil
}

func (a *accountSender) Stats() AccountStats {
	return AccountStats{
		Name:        a.name,
		Host:        a.cfg.Host,
		Port:        a.cfg.Port,
		TotalSent:   atomic.LoadInt64(&a.totalSent),
		TotalFailed: atomic.LoadInt64(&a.totalFailed),
		Pool:        a.pool.Stats(),
	}
}

func (a *accountSender) Close() error {
	if a.stopRate != nil {
		close(a.stopRate)
	}
	return a.pool.Close()
}

// maxBatchConcurrencyUpper is a hard cap for concurrent sends in SendBatch
// to avoid unbounded goroutine/connection explosion on very large batches.
const maxBatchConcurrencyUpper = 512
const minBatchConcurrency = 16

// MultiAccountSender manages sending operations across multiple SMTP accounts.
type MultiAccountSender struct {
	accounts map[string]*accountSender
	mu       sync.RWMutex
	batchSem chan struct{}
}

// New creates a new Sender instance using the provided SmtpConfig.
func New(cfg config.SmtpConfig) (Sender, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("sender: invalid smtp config: %w", err)
	}

	m := &MultiAccountSender{
		accounts: make(map[string]*accountSender, len(cfg)),
	}

	// Bound SendBatch parallelism by the total number of SMTP connections the
	// pools can hold (each account defaults to max_open_conns=20).
	batchLimit := 0
	for _, accCfg := range cfg {
		pool := accCfg.Pool.MaxOpenConns
		if pool <= 0 {
			pool = 20
		}
		batchLimit += pool
	}
	if batchLimit < minBatchConcurrency {
		batchLimit = minBatchConcurrency
	}
	if batchLimit > maxBatchConcurrencyUpper {
		batchLimit = maxBatchConcurrencyUpper
	}
	m.batchSem = make(chan struct{}, batchLimit)

	for name, accCfg := range cfg {
		m.accounts[strings.ToLower(name)] = newAccountSender(name, accCfg)
	}

	return m, nil
}

// NewFromConfig creates a new Sender instance from the global Config object.
func NewFromConfig(cfg *config.Config) (Sender, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sender: config is nil")
	}
	return New(cfg.SMTP)
}

// Send sends an email using the account specified in email.Account, or the default account.
func (m *MultiAccountSender) Send(ctx context.Context, email *Email) error {
	if email == nil {
		return ErrNilEmail
	}

	target := strings.ToLower(strings.TrimSpace(email.Account))
	if target == "" {
		target = config.DefaultAccountName
	}

	m.mu.RLock()
	acc, ok := m.accounts[target]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %q", ErrAccountNotFound, target)
	}

	return acc.Send(ctx, email)
}

// SendBatch delivers multiple email messages concurrently using a bounded worker pool.
func (m *MultiAccountSender) SendBatch(ctx context.Context, emails []*Email) []error {
	errorsList := make([]error, len(emails))
	var wg sync.WaitGroup

	for i, mailItem := range emails {
		wg.Add(1)
		go func(idx int, item *Email) {
			defer wg.Done()
			select {
			case m.batchSem <- struct{}{}:
				defer func() { <-m.batchSem }()
			case <-ctx.Done():
				errorsList[idx] = ctx.Err()
				return
			}
			errorsList[idx] = m.Send(ctx, item)
		}(i, mailItem)
	}

	wg.Wait()
	return errorsList
}

// Account returns the AccountSender for a specific named account.
func (m *MultiAccountSender) Account(name string) (AccountSender, error) {
	target := strings.ToLower(strings.TrimSpace(name))
	if target == "" {
		target = config.DefaultAccountName
	}

	m.mu.RLock()
	acc, ok := m.accounts[target]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrAccountNotFound, target)
	}
	return acc, nil
}

// AccountNames returns all configured account names.
func (m *MultiAccountSender) AccountNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.accounts))
	for _, acc := range m.accounts {
		names = append(names, acc.name)
	}
	return names
}

// Stats returns stats for all configured accounts.
func (m *MultiAccountSender) Stats() map[string]AccountStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make(map[string]AccountStats, len(m.accounts))
	for k, acc := range m.accounts {
		res[k] = acc.Stats()
	}
	return res
}

// Close closes all underlying account connection pools.
func (m *MultiAccountSender) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, acc := range m.accounts {
		if err := acc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
