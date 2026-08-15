package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"mailbaby/internal/logger"
)

// SpanEvent represents a timestamped event inside a span.
type SpanEvent struct {
	Name      string         `json:"name"`
	Timestamp time.Time      `json:"timestamp"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Span represents a single unit of work in distributed tracing.
type Span struct {
	traceID      string
	spanID       string
	parentSpanID string
	name         string
	startTime    time.Time
	endTime      time.Time
	attributes   map[string]any
	events       []SpanEvent
	status       string
	err          error
	provider     *TracerProvider
	mu           sync.RWMutex
	ended        bool
}

// TraceID returns the 32-character hex trace ID.
func (s *Span) TraceID() string {
	if s == nil {
		return ""
	}
	return s.traceID
}

// SpanID returns the 16-character hex span ID.
func (s *Span) SpanID() string {
	if s == nil {
		return ""
	}
	return s.spanID
}

// SetAttribute sets a key-value attribute on the span.
func (s *Span) SetAttribute(key string, val any) *Span {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]any)
	}
	s.attributes[key] = val
	return s
}

// SetAttributes sets multiple attributes on the span.
func (s *Span) SetAttributes(attrs map[string]any) *Span {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]any)
	}
	for k, v := range attrs {
		s.attributes[k] = v
	}
	return s
}

// RecordError records an error event and marks the span status as ERROR.
func (s *Span) RecordError(err error) *Span {
	if s == nil || err == nil {
		return s
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	s.status = "ERROR"
	s.events = append(s.events, SpanEvent{
		Name:      "exception",
		Timestamp: time.Now(),
		Fields: map[string]any{
			"exception.message": err.Error(),
		},
	})
	return s
}

// AddEvent adds a custom timestamped annotation event to the span.
func (s *Span) AddEvent(name string, fields map[string]any) *Span {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, SpanEvent{
		Name:      name,
		Timestamp: time.Now(),
		Fields:    fields,
	})
	return s
}

// End finishes the span and records its end time.
func (s *Span) End() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.endTime = time.Now()
	s.mu.Unlock()

	if s.provider != nil && s.provider.exporter != nil {
		s.provider.exporter.ExportSpan(s)
	}
}

// Duration returns the span's execution duration.
func (s *Span) Duration() time.Duration {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ended {
		return s.endTime.Sub(s.startTime)
	}
	return time.Since(s.startTime)
}

type spanKeyType struct{}

var spanContextKey = spanKeyType{}

// ContextWithSpan attaches the span to the context.
func ContextWithSpan(ctx context.Context, s *Span) context.Context {
	if s == nil {
		return ctx
	}
	// Also populate logger context keys for log correlation
	ctx = context.WithValue(ctx, logger.TraceIDKey, s.TraceID())
	ctx = context.WithValue(ctx, logger.SpanIDKey, s.SpanID())
	return context.WithValue(ctx, spanContextKey, s)
}

// SpanFromContext extracts the current active span from context.
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	if s, ok := ctx.Value(spanContextKey).(*Span); ok {
		return s
	}
	return nil
}

func generateTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func generateSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
