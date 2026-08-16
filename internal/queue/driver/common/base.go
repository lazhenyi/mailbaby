package common

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"
)

// BaseStats aggregates counters that every queue driver maintains. Drivers
// embed *BaseStats (or BaseStats by value) and call the helper methods to
// mutate and read the counters in a uniform way.
type BaseStats struct {
	InFlight   int64
	TotalSent  int64
	ActiveCons int64
}

func (b *BaseStats) IncTotalSent(n int64) {
	atomic.AddInt64(&b.TotalSent, n)
}

func (b *BaseStats) IncInFlight() {
	atomic.AddInt64(&b.InFlight, 1)
}

func (b *BaseStats) DecInFlight() {
	atomic.AddInt64(&b.InFlight, -1)
}

func (b *BaseStats) IncActiveCons(n int) {
	atomic.AddInt64(&b.ActiveCons, int64(n))
}

func (b *BaseStats) DecActiveCons(n int) {
	atomic.AddInt64(&b.ActiveCons, -int64(n))
}

func (b *BaseStats) Snapshot() (inFlight, totalSent int64, consumers int) {
	return atomic.LoadInt64(&b.InFlight),
		atomic.LoadInt64(&b.TotalSent),
		int(atomic.LoadInt64(&b.ActiveCons))
}

// Backoff produces exponentially-growing sleep durations with jitter for
// driver receive loops when the broker is unavailable or returns transient
// errors. Without this, drivers busy-loop with a fixed sleep and pin a CPU.
// A driver calls Wait(ctx, attempt) inside its receive loop.
type Backoff struct {
	Base     time.Duration
	Max      time.Duration
	attempt  uint32
	jittered bool
}

func NewBackoff(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	if max <= 0 || max < base {
		max = 5 * time.Second
	}
	return &Backoff{Base: base, Max: max}
}

func (b *Backoff) Wait(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	n := atomic.AddUint32(&b.attempt, 1)
	d := b.Base << min(n, 10)
	if d <= 0 || d > b.Max {
		d = b.Max
	}
	if !b.jittered {
		// ±20% jitter to avoid thundering-herd reconnects across replicas.
		window := int64(d) / 5
		if window > 0 {
			d += time.Duration(rand.Int63n(window*2) - window)
		}
		b.jittered = true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}