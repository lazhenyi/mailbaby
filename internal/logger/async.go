package logger

import (
	"bytes"
	"io"
	"sync"
	"time"
)

// AsyncWriter provides a non-blocking, buffered writer for high-throughput logging.
type AsyncWriter struct {
	writer   io.Writer
	ch       chan []byte
	stopChan chan struct{}
	doneChan chan struct{}

	mu       sync.Mutex // guards closed flag
	writerMu sync.Mutex // serializes direct writes with worker flushes

	closed  bool
	bufPool sync.Pool
}

// NewAsyncWriter creates a new AsyncWriter wrapping the target destination.
func NewAsyncWriter(w io.Writer, bufferSize int) *AsyncWriter {
	if bufferSize <= 0 {
		bufferSize = 4096
	}

	aw := &AsyncWriter{
		writer:   w,
		ch:       make(chan []byte, bufferSize),
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}

	go aw.worker()
	return aw
}

func (aw *AsyncWriter) worker() {
	defer close(aw.doneChan)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var batch bytes.Buffer

	flush := func() {
		if batch.Len() > 0 && aw.writer != nil {
			aw.writerMu.Lock()
			_, _ = aw.writer.Write(batch.Bytes())
			aw.writerMu.Unlock()
			batch.Reset()
		}
	}

	write := func(data []byte) {
		batch.Write(data)
		if batch.Len() > 16384 {
			flush()
		}
	}

	for {
		select {
		case data, ok := <-aw.ch:
			if !ok {
				flush()
				return
			}
			write(data)
		case <-ticker.C:
			flush()
		case <-aw.stopChan:
			// Drain remaining messages from channel
			for {
				select {
				case data, ok := <-aw.ch:
					if ok {
						write(data)
					} else {
						flush()
						return
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// Write queues the log data non-blockingly.
func (aw *AsyncWriter) Write(p []byte) (n int, err error) {
	aw.mu.Lock()
	if aw.closed {
		aw.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	aw.mu.Unlock()

	data := make([]byte, len(p))
	copy(data, p)

	select {
	case aw.ch <- data:
		return len(p), nil
	default:
		// Channel buffer full, write synchronously as fallback.
		aw.writerMu.Lock()
		defer aw.writerMu.Unlock()
		if aw.writer != nil {
			return aw.writer.Write(p)
		}
		return len(p), nil
	}
}

// Sync drains and flushes all queued log entries.
// The underlying channel is never closed, so concurrent Write calls can
// never panic with a "send on closed channel".
func (aw *AsyncWriter) Sync() error {
	aw.mu.Lock()
	if aw.closed {
		aw.mu.Unlock()
		return nil
	}
	aw.closed = true
	aw.mu.Unlock()

	close(aw.stopChan)
	<-aw.doneChan

	if syncer, ok := aw.writer.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}
