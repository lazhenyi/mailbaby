package tracing

import (
	"context"
	"testing"
	"time"

	"mailbaby/internal/config"
)

func TestTracingSpanAndCascade(t *testing.T) {
	_, err := Init(config.TracingConfig{
		Enabled:  true,
		Provider: config.TracingProviderStdout,
	})
	if err != nil {
		t.Fatalf("Init tracing failed: %v", err)
	}

	// 1. Root span
	ctx, rootSpan := StartSpan(context.Background(), "test.root")
	rootSpan.SetAttribute("env", "test")

	if rootSpan.TraceID() == "" || rootSpan.SpanID() == "" {
		t.Fatal("expected non-empty traceID and spanID")
	}

	time.Sleep(10 * time.Millisecond)

	// 2. Child span
	childCtx, childSpan := StartSpan(ctx, "test.child")
	childSpan.SetAttribute("task", "sub-work")

	if childSpan.TraceID() != rootSpan.TraceID() {
		t.Errorf("expected child to inherit traceID %s, got %s", rootSpan.TraceID(), childSpan.TraceID())
	}
	if childSpan.parentSpanID != rootSpan.SpanID() {
		t.Errorf("expected child parentSpanID to be %s, got %s", rootSpan.SpanID(), childSpan.parentSpanID)
	}

	childSpan.End()
	rootSpan.End()

	_ = childCtx
}

func TestW3CTraceContextPropagation(t *testing.T) {
	ctx, span := StartSpan(context.Background(), "test.producer")
	defer span.End()

	headers := make(map[string]string)
	InjectHeaders(ctx, headers)

	if headers[HeaderTraceParent] == "" {
		t.Fatal("expected traceparent header to be injected")
	}

	// Consumer extracts headers
	extractedCtx := ExtractHeaders(context.Background(), headers)
	consumerCtx, consumerSpan := StartSpan(extractedCtx, "test.consumer")
	defer consumerSpan.End()

	if consumerSpan.TraceID() != span.TraceID() {
		t.Errorf("expected consumer span to have traceID %s, got %s", span.TraceID(), consumerSpan.TraceID())
	}
	if consumerSpan.parentSpanID != span.SpanID() {
		t.Errorf("expected consumer parentSpanID to be %s, got %s", span.SpanID(), consumerSpan.parentSpanID)
	}

	_ = consumerCtx
}
