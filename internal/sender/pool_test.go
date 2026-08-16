package sender

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
)

func TestPool_AcquireReleaseSerial(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com",
		Host: "127.0.0.1",
		Port: 1, // unused; we never dial
		Pool: config.SmtpPoolConfig{MaxOpenConns: 2, MaxIdleConns: 1, IdleTimeout: time.Second},
	}
	p := NewSmtpConnPool(cfg)
	defer func() { _ = p.Close() }()

	stats := p.Stats()
	if stats.MaxOpenConns != 2 {
		t.Fatalf("MaxOpenConns: expected 2, got %d", stats.MaxOpenConns)
	}
	if stats.MaxIdleConns != 1 {
		t.Fatalf("MaxIdleConns: expected 1, got %d", stats.MaxIdleConns)
	}
}

func TestPool_IdleDisable(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 2, MaxIdleConns: -1},
	}
	p := NewSmtpConnPool(cfg)
	defer func() { _ = p.Close() }()
	if p.maxIdle != 0 {
		t.Fatalf("expected maxIdle=0 when MaxIdleConns=-1, got %d", p.maxIdle)
	}
}

func TestPool_DefaultIdleIsFive(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 20, MaxIdleConns: 0},
	}
	p := NewSmtpConnPool(cfg)
	defer func() { _ = p.Close() }()
	if p.maxIdle != 5 {
		t.Fatalf("expected default maxIdle=5, got %d", p.maxIdle)
	}
}

func TestPool_ReleaseNilNoPanic(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 2},
	}
	p := NewSmtpConnPool(cfg)
	defer func() { _ = p.Close() }()
	p.Release(nil, nil)
	p.Release(nil, nil)
}

func TestPool_AcquireAfterClose(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 2},
	}
	p := NewSmtpConnPool(cfg)
	_ = p.Close()
	_, err := p.Acquire(nil)
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestPool_CloseIdempotent(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 2},
	}
	p := NewSmtpConnPool(cfg)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second close returned %v, want nil", err)
	}
}

func TestPool_ConcurrentReleaseNilSafety(t *testing.T) {
	cfg := config.SmtpAccountConfig{
		From: "test@example.com", Host: "127.0.0.1", Port: 1,
		Pool: config.SmtpPoolConfig{MaxOpenConns: 4, MaxIdleConns: 0},
	}
	p := NewSmtpConnPool(cfg)
	defer func() { _ = p.Close() }()

	var wg sync.WaitGroup
	var released int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Release(nil, nil)
			atomic.AddInt64(&released, 1)
		}()
	}
	wg.Wait()
	if atomic.LoadInt64(&released) != 32 {
		t.Fatalf("expected 32 releases, got %d", released)
	}
}