package logger

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestAsyncWriterConcurrentSync(t *testing.T) {
	var buf bytes.Buffer
	aw := NewAsyncWriter(&buf, 32)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, _ = aw.Write([]byte("concurrent log line\n"))
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		_ = aw.Sync()
	}()

	wg.Wait()
	// The test passes if no "send on closed channel" panic occurred.

	if _, err := aw.Write([]byte("after sync\n")); err != io.ErrClosedPipe {
		t.Errorf("expected ErrClosedPipe after Sync, got %v", err)
	}
}

func TestLogTextEscaping(t *testing.T) {
	var buf bytes.Buffer
	textLogger := &Logger{
		cfg: config.LogConfig{
			Level:  "info",
			Format: "text",
		},
		level: InfoLevel,
		out:   &buf,
	}

	textLogger.WithField("subject", "hello\r\nBcc: victim@example.com").Info("line1\nline2")

	out := buf.String()
	if strings.Contains(out, "line1\nline2") {
		t.Errorf("raw newline in log message must be escaped:\n%q", out)
	}
	if strings.Contains(out, "hello\nBcc") {
		t.Errorf("CRLF in field value must be escaped, otherwise log lines can be forged:\n%q", out)
	}
	if !strings.Contains(out, "\\n") {
		t.Errorf("expected escaped newline marker in text output:\n%q", out)
	}
}
