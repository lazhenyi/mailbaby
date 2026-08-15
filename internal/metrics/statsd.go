package metrics

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"
)

// StatsDClient is a lightweight, buffered UDP client for the StatsD metric format.
type StatsDClient struct {
	conn     net.Conn
	prefix   string
	buf      bytes.Buffer
	mu       sync.Mutex
	flushDur time.Duration
	stopChan chan struct{}
	closed   bool
}

// NewStatsDClient creates a new StatsDClient connected to the specified UDP address.
func NewStatsDClient(addr, prefix string, flushInterval time.Duration) (*StatsDClient, error) {
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}

	conn, err := net.DialTimeout("udp", addr, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("statsd: failed to dial udp %s: %w", addr, err)
	}

	c := &StatsDClient{
		conn:     conn,
		prefix:   prefix,
		flushDur: flushInterval,
		stopChan: make(chan struct{}),
	}

	go c.flushLoop()

	return c, nil
}

func (c *StatsDClient) flushLoop() {
	ticker := time.NewTicker(c.flushDur)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.Flush()
		case <-c.stopChan:
			c.Flush()
			return
		}
	}
}

// Count increments a counter metric in StatsD.
func (c *StatsDClient) Count(stat string, count int64) {
	c.send(fmt.Sprintf("%s%s:%d|c\n", c.prefix, stat, count))
}

// Gauge sets a gauge metric in StatsD.
func (c *StatsDClient) Gauge(stat string, value float64) {
	c.send(fmt.Sprintf("%s%s:%f|g\n", c.prefix, stat, value))
}

// Timing records a timing metric in milliseconds in StatsD.
func (c *StatsDClient) Timing(stat string, d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	c.send(fmt.Sprintf("%s%s:%.2f|ms\n", c.prefix, stat, ms))
}

func (c *StatsDClient) send(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	c.buf.WriteString(line)
	// If buffer exceeds MTU size (approx 1400 bytes), flush immediately
	if c.buf.Len() > 1400 {
		c.flushLocked()
	}
}

// Flush writes any buffered metric lines to the UDP socket.
func (c *StatsDClient) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
}

func (c *StatsDClient) flushLocked() {
	if c.buf.Len() == 0 || c.conn == nil {
		return
	}
	_, _ = c.conn.Write(c.buf.Bytes())
	c.buf.Reset()
}

// Close flushes buffered metrics and closes the UDP connection.
func (c *StatsDClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.stopChan)
	return c.conn.Close()
}
