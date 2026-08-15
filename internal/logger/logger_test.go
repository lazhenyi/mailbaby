package logger

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"mailbaby/internal/config"
)

func TestLoggerFormatsAndLevels(t *testing.T) {
	// 1. JSON format test
	var jsonBuf bytes.Buffer
	jsonLogger := &Logger{
		cfg: config.LogConfig{
			Level:  "debug",
			Format: "json",
		},
		level: DebugLevel,
		out:   &jsonBuf,
	}

	jsonLogger.WithField("queue", "kafka").Infof("processing message %d", 100)
	outStr := jsonBuf.String()
	if !strings.Contains(outStr, `"level":"INFO"`) || !strings.Contains(outStr, `"queue":"kafka"`) {
		t.Errorf("unexpected JSON log output:\n%s", outStr)
	}

	// 2. Text format with Trace context test
	var textBuf bytes.Buffer
	textLogger := &Logger{
		cfg: config.LogConfig{
			Level:  "info",
			Format: "text",
		},
		level: InfoLevel,
		out:   &textBuf,
	}

	ctx := context.WithValue(context.Background(), TraceIDKey, "trace-abc-123")
	textLogger.WithContext(ctx).WithError(errors.New("connection failed")).Warn("retry attempted")

	textOut := textBuf.String()
	if !strings.Contains(textOut, "[trace_id=trace-abc-123]") || !strings.Contains(textOut, "error=connection failed") {
		t.Errorf("unexpected text log output:\n%s", textOut)
	}
}

func TestAsyncWriter(t *testing.T) {
	var buf bytes.Buffer
	aw := NewAsyncWriter(&buf, 100)

	_, err := aw.Write([]byte("async log line\n"))
	if err != nil {
		t.Fatalf("async write failed: %v", err)
	}

	_ = aw.Sync()

	if !strings.Contains(buf.String(), "async log line") {
		t.Errorf("expected buffered output after sync, got:\n%s", buf.String())
	}
}
