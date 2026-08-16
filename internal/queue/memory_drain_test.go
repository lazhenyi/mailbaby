package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailbaby/internal/config"
)

func TestMemoryQueue_DrainWaitsForInFlight(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:        config.DriverMemory,
			Concurrency:   1,
			DrainTimeout:  5 * time.Second,
		},
	}
	q := NewMemoryQueue("drain_test", 16, cfg)

	var processed int32
	var releaseHandler sync.WaitGroup
	releaseHandler.Add(1)

	prod, err := q.Producer()
	if err != nil {
		t.Fatal(err)
	}
	if err := prod.Publish(context.Background(), NewMessage([]byte("slow"))); err != nil {
		t.Fatal(err)
	}

	cons, err := q.Consumer()
	if err != nil {
		t.Fatal(err)
	}

	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- cons.Consume(context.Background(), func(ctx context.Context, msg *Message) error {
			atomic.AddInt32(&processed, 1)
			releaseHandler.Wait()
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("handler not yet entered: processed=%d", processed)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- q.Close()
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned before handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	releaseHandler.Done()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after handler released")
	}

	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}

	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Consume did not return")
	}
}

func TestMemoryQueue_DrainTimeout(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:        config.DriverMemory,
			Concurrency:   1,
			DrainTimeout:  150 * time.Millisecond,
		},
	}
	q := NewMemoryQueue("drain_timeout_test", 16, cfg)

	prod, err := q.Producer()
	if err != nil {
		t.Fatal(err)
	}
	if err := prod.Publish(context.Background(), NewMessage([]byte("stuck"))); err != nil {
		t.Fatal(err)
	}

	cons, err := q.Consumer()
	if err != nil {
		t.Fatal(err)
	}

	released := make(chan struct{})
	go func() {
		_ = cons.Consume(context.Background(), func(ctx context.Context, msg *Message) error {
			<-released
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := q.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Fatalf("Close blocked too long: %s", elapsed)
	}
	close(released)
}

// TestMemoryQueue_DelayedPublishWaitsForClose asserts that Close() awaits
// outstanding delayed-publish goroutines via the delayedWg WaitGroup. Without
// that tracking, a delayed goroutine would outlive the queue and either write
// to a closed channel (panic) or leak a pending timer.
//
// The test publishes a message with a short delay (50ms), then calls Close
// while that goroutine is still parked in time.NewTimer. Close must wait for
// the delayed goroutine to finish (or hit the drain timeout) — it should NOT
// return before the delay fires when the bound is generous.
func TestMemoryQueue_DelayedPublishWaitsForClose(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:       config.DriverMemory,
			Concurrency:  1,
			DrainTimeout: 2 * time.Second,
		},
	}
	q := NewMemoryQueue("delayed_drain_test", 16, cfg)

	prod, err := q.Producer()
	if err != nil {
		t.Fatal(err)
	}

	delayed := NewMessage([]byte("delayed-payload"))
	delayed.Delay = 50 * time.Millisecond
	if err := prod.Publish(context.Background(), delayed); err != nil {
		t.Fatalf("publish delayed: %v", err)
	}

	closeStart := time.Now()
	if err := q.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	closeElapsed := time.Since(closeStart)

	// Close must wait for the delayed publish to fire (≥50ms) but must NOT
	// hang past the drain timeout (2s). Allow generous upper bound.
	if closeElapsed < 40*time.Millisecond {
		t.Fatalf("Close returned before delayed publish fired: %s (expected >= 50ms)", closeElapsed)
	}
	if closeElapsed > 1500*time.Millisecond {
		t.Fatalf("Close blocked far longer than drain timeout: %s", closeElapsed)
	}
}

// TestMemoryQueue_DelayedPublishCloseTimeoutBound asserts that if a delayed
// publish outlives the drain timeout, Close() returns within the timeout
// rather than blocking forever. The delayed goroutine then exits via its own
// recover() / log path.
func TestMemoryQueue_DelayedPublishCloseTimeoutBound(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:       config.DriverMemory,
			Concurrency:  1,
			DrainTimeout: 100 * time.Millisecond,
		},
	}
	q := NewMemoryQueue("delayed_timeout_test", 16, cfg)

	prod, err := q.Producer()
	if err != nil {
		t.Fatal(err)
	}

	delayed := NewMessage([]byte("long-delayed"))
	delayed.Delay = 5 * time.Second
	if err := prod.Publish(context.Background(), delayed); err != nil {
		t.Fatalf("publish delayed: %v", err)
	}

	closeStart := time.Now()
	if err := q.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	closeElapsed := time.Since(closeStart)

	if closeElapsed > 500*time.Millisecond {
		t.Fatalf("Close blocked far longer than drain timeout: %s", closeElapsed)
	}
}