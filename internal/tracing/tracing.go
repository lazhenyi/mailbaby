package tracing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
)

// W3C TraceContext header constants.
const (
	HeaderTraceParent = "traceparent"
	HeaderTraceState  = "tracestate"
)

// SpanExporter interface for exporting finished spans.
type SpanExporter interface {
	ExportSpan(s *Span)
	Close() error
}

// StdoutSpanExporter logs span completions.
type StdoutSpanExporter struct{}

func (e *StdoutSpanExporter) ExportSpan(s *Span) {
	logger.Get().WithFields(logger.Fields{
		"trace_id": s.TraceID(),
		"span_id":  s.SpanID(),
		"name":     s.name,
		"duration": s.Duration().String(),
		"status":   s.status,
		"attrs":    s.attributes,
	}).Debug("trace span finished")
}

func (e *StdoutSpanExporter) Close() error {
	return nil
}

// TracerProvider manages span creation and lifecycle.
type TracerProvider struct {
	cfg      config.TracingConfig
	exporter SpanExporter
	mu       sync.RWMutex
}

var (
	defaultTracer = &TracerProvider{}
	globalTracer  = defaultTracer
	globalMu      sync.RWMutex
)

// Init initializes the global TracerProvider based on TracingConfig.
// Note: the current implementation exports finished spans to stdout only.
// The tracing.provider / tracing.endpoint configuration keys are reserved for
// future OTLP/gRPC exporters and are not yet consumed.
func Init(cfg config.TracingConfig) (*TracerProvider, error) {
	cfg.ApplyDefaults()

	var exporter SpanExporter
	if cfg.Enabled {
		exporter = &StdoutSpanExporter{}
	}

	tp := &TracerProvider{
		cfg:      cfg,
		exporter: exporter,
	}

	globalMu.Lock()
	globalTracer = tp
	globalMu.Unlock()

	return tp, nil
}

// Get returns the global TracerProvider.
func Get() *TracerProvider {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if globalTracer == nil {
		return defaultTracer
	}
	return globalTracer
}

// StartSpan creates and begins a new Span as a child of the span in ctx if present.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	tp := Get()

	var traceID string
	var parentSpanID string

	parent := SpanFromContext(ctx)
	if parent != nil {
		traceID = parent.TraceID()
		parentSpanID = parent.SpanID()
	} else {
		traceID = generateTraceID()
	}

	spanID := generateSpanID()

	s := &Span{
		traceID:      traceID,
		spanID:       spanID,
		parentSpanID: parentSpanID,
		name:         name,
		startTime:    time.Now(),
		status:       "OK",
		provider:     tp,
		attributes:   make(map[string]any),
	}

	childCtx := ContextWithSpan(ctx, s)
	return childCtx, s
}

// InjectHeaders serializes the active span context into map headers using W3C TraceContext.
func InjectHeaders(ctx context.Context, headers map[string]string) {
	if headers == nil {
		return
	}
	span := SpanFromContext(ctx)
	if span == nil {
		return
	}
	// W3C traceparent format: 00-{trace_id}-{span_id}-01
	headers[HeaderTraceParent] = fmt.Sprintf("00-%s-%s-01", span.TraceID(), span.SpanID())
}

// ExtractHeaders parses W3C TraceContext headers and returns a context populated with a remote parent span.
func ExtractHeaders(ctx context.Context, headers map[string]string) context.Context {
	if headers == nil {
		return ctx
	}

	tpHeader, ok := headers[HeaderTraceParent]
	if !ok || tpHeader == "" {
		// Case-insensitive check
		for k, v := range headers {
			if strings.EqualFold(k, HeaderTraceParent) {
				tpHeader = v
				break
			}
		}
	}

	if tpHeader == "" {
		return ctx
	}

	// Parse 00-{trace_id}-{span_id}-{flags}
	parts := strings.Split(tpHeader, "-")
	if len(parts) >= 3 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		remoteSpan := &Span{
			traceID: parts[1],
			spanID:  parts[2],
			name:    "remote_parent",
			status:  "OK",
		}
		return ContextWithSpan(ctx, remoteSpan)
	}

	return ctx
}

// Close flushes and shuts down the tracer exporter.
func (tp *TracerProvider) Close(ctx context.Context) error {
	if tp == nil || tp.exporter == nil {
		return nil
	}
	return tp.exporter.Close()
}
