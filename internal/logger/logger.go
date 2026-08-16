package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"mailbaby/internal/config"
)

// Level defines the logging verbosity level.
type Level int

const (
	TraceLevel Level = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
	PanicLevel
)

func ParseLevel(lvl string) Level {
	switch strings.ToLower(strings.TrimSpace(lvl)) {
	case "trace":
		return TraceLevel
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn", "warning":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "fatal":
		return FatalLevel
	case "panic":
		return PanicLevel
	default:
		return InfoLevel
	}
}

func (l Level) String() string {
	switch l {
	case TraceLevel:
		return "TRACE"
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	case PanicLevel:
		return "PANIC"
	default:
		return "INFO"
	}
}

// Fields represents key-value structured logging fields.
type Fields map[string]any

// ContextKey is a typed string for context extraction.
type ContextKey string

const (
	TraceIDKey ContextKey = "trace_id"
	SpanIDKey  ContextKey = "span_id"
)

// Logger is a high-performance structured logger.
type Logger struct {
	cfg         config.LogConfig
	level       Level
	out         io.Writer
	asyncWriter *AsyncWriter
	mu          sync.RWMutex
}

var (
	globalLogger *Logger
	globalMu     sync.RWMutex
)

func init() {
	// Initialize default stdout logger
	defaultCfg := config.LogConfig{
		Level:  "info",
		Format: "text",
		Output: "stdout",
	}
	_ = Init(defaultCfg)
}

// Init initializes the global Logger instance based on LogConfig.
func Init(cfg config.LogConfig) error {
	cfg.ApplyDefaults()

	var writers []io.Writer

	switch strings.ToLower(string(cfg.Output)) {
	case string(config.LogOutputStdout):
		writers = append(writers, os.Stdout)
	case string(config.LogOutputStderr):
		writers = append(writers, os.Stderr)
	case string(config.LogOutputFile):
		if cfg.FilePath != "" {
			_ = os.MkdirAll(filepath.Dir(cfg.FilePath), 0755)
			writers = append(writers, &lumberjack.Logger{
				Filename:   cfg.FilePath,
				MaxSize:    cfg.MaxSize,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,
				Compress:   cfg.Compress,
			})
		} else {
			writers = append(writers, os.Stdout)
		}
	case string(config.LogOutputBoth):
		writers = append(writers, os.Stdout)
		if cfg.FilePath != "" {
			_ = os.MkdirAll(filepath.Dir(cfg.FilePath), 0755)
			writers = append(writers, &lumberjack.Logger{
				Filename:   cfg.FilePath,
				MaxSize:    cfg.MaxSize,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,
				Compress:   cfg.Compress,
			})
		}
	default:
		writers = append(writers, os.Stdout)
	}

	multi := io.MultiWriter(writers...)
	finalOut := io.Writer(multi)
	var asyncW *AsyncWriter

	if cfg.Async {
		asyncW = NewAsyncWriter(multi, cfg.BufferSize)
		finalOut = asyncW
	}

	l := &Logger{
		cfg:         cfg,
		level:       ParseLevel(string(cfg.Level)),
		out:         finalOut,
		asyncWriter: asyncW,
	}

	globalMu.Lock()
	if globalLogger != nil && globalLogger.asyncWriter != nil {
		_ = globalLogger.asyncWriter.Sync()
	}
	globalLogger = l
	globalMu.Unlock()

	return nil
}

// Get returns the global Logger instance.
func Get() *Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalLogger
}

// Set swaps the global Logger. Intended for tests; production code should
// call Init instead. Passing nil restores a default no-op logger so callers
// never observe a nil dereference.
func Set(l *Logger) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if l == nil {
		globalLogger = newNoopLogger()
		return
	}
	globalLogger = l
}

// newNoopLogger returns a Logger that swallows every entry. It is used as a
// safe default when Set(nil) is invoked.
func newNoopLogger() *Logger {
	return &Logger{}
}

// Sync flushes all pending buffered logs.
func Sync() error {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if globalLogger != nil && globalLogger.asyncWriter != nil {
		return globalLogger.asyncWriter.Sync()
	}
	return nil
}

// Entry is a contextual logging record.
type Entry struct {
	logger *Logger
	ctx    context.Context
	fields Fields
}

func (l *Logger) newEntry() *Entry {
	return &Entry{
		logger: l,
		fields: make(Fields),
	}
}

func (l *Logger) Trace(args ...any)                 { l.newEntry().Trace(args...) }
func (l *Logger) Tracef(format string, args ...any) { l.newEntry().Tracef(format, args...) }
func (l *Logger) Debug(args ...any)                 { l.newEntry().Debug(args...) }
func (l *Logger) Debugf(format string, args ...any) { l.newEntry().Debugf(format, args...) }
func (l *Logger) Info(args ...any)                  { l.newEntry().Info(args...) }
func (l *Logger) Infof(format string, args ...any)  { l.newEntry().Infof(format, args...) }
func (l *Logger) Warn(args ...any)                  { l.newEntry().Warn(args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.newEntry().Warnf(format, args...) }
func (l *Logger) Error(args ...any)                 { l.newEntry().Error(args...) }
func (l *Logger) Errorf(format string, args ...any) { l.newEntry().Errorf(format, args...) }
func (l *Logger) Fatal(args ...any)                 { l.newEntry().Fatal(args...) }
func (l *Logger) Fatalf(format string, args ...any) { l.newEntry().Fatalf(format, args...) }

// WithField adds a single key-value field to the log entry.
func (l *Logger) WithField(key string, val any) *Entry {
	e := l.newEntry()
	e.fields[key] = val
	return e
}

// WithFields adds multiple key-value fields to the log entry.
func (l *Logger) WithFields(fields Fields) *Entry {
	e := l.newEntry()
	for k, v := range fields {
		e.fields[k] = v
	}
	return e
}

// WithError adds an error field to the log entry.
func (l *Logger) WithError(err error) *Entry {
	e := l.newEntry()
	if err != nil {
		e.fields["error"] = err.Error()
	}
	return e
}

// WithContext adds a context, automatically extracting trace_id and span_id if present.
func (l *Logger) WithContext(ctx context.Context) *Entry {
	e := l.newEntry()
	e.ctx = ctx
	if ctx != nil {
		if tid, ok := ctx.Value(TraceIDKey).(string); ok && tid != "" {
			e.fields["trace_id"] = tid
		}
		if sid, ok := ctx.Value(SpanIDKey).(string); ok && sid != "" {
			e.fields["span_id"] = sid
		}
	}
	return e
}

func (e *Entry) WithField(key string, val any) *Entry {
	e.fields[key] = val
	return e
}

func (e *Entry) WithFields(fields Fields) *Entry {
	for k, v := range fields {
		e.fields[k] = v
	}
	return e
}

func (e *Entry) WithError(err error) *Entry {
	if err != nil {
		e.fields["error"] = err.Error()
	}
	return e
}

func (e *Entry) WithContext(ctx context.Context) *Entry {
	e.ctx = ctx
	if ctx != nil {
		if tid, ok := ctx.Value(TraceIDKey).(string); ok && tid != "" {
			e.fields["trace_id"] = tid
		}
		if sid, ok := ctx.Value(SpanIDKey).(string); ok && sid != "" {
			e.fields["span_id"] = sid
		}
	}
	return e
}

func (e *Entry) log(lvl Level, msg string) {
	if e.logger == nil || lvl < e.logger.level {
		return
	}

	now := time.Now()
	timeStr := now.Format("2006-01-02 15:04:05.000")
	if e.logger.cfg.TimeFormat != "" {
		timeStr = now.Format(e.logger.cfg.TimeFormat)
	}

	var callerStr string
	if e.logger.cfg.ShowCaller {
		if _, file, line, ok := runtime.Caller(3); ok {
			callerStr = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		}
	}

	var buf bytes.Buffer

	switch strings.ToLower(string(e.logger.cfg.Format)) {
	case string(config.LogFormatJSON):
		payload := map[string]any{
			"timestamp": timeStr,
			"level":     lvl.String(),
			"message":   msg,
		}
		if callerStr != "" {
			payload["caller"] = callerStr
		}
		for k, v := range e.fields {
			switch val := v.(type) {
			case string:
				payload[k] = RedactSecrets(val)
			case error:
				payload[k] = RedactSecrets(val.Error())
			default:
				payload[k] = v
			}
		}
		data, _ := json.Marshal(payload)
		buf.Write(data)
		buf.WriteByte('\n')

	case string(config.LogFormatLogfmt):
		fmt.Fprintf(&buf, "time=%q level=%s msg=%q", timeStr, lvl.String(), escapeLogText(RedactSecrets(msg)))
		if callerStr != "" {
			fmt.Fprintf(&buf, " caller=%q", callerStr)
		}
		for k, v := range e.fields {
			fmt.Fprintf(&buf, " %s=%q", k, escapeLogText(RedactSecrets(fmt.Sprint(v))))
		}
		buf.WriteByte('\n')

	default: // Text / Console
		fmt.Fprintf(&buf, "[%s] [%s]", timeStr, lvl.String())
		if callerStr != "" {
			fmt.Fprintf(&buf, " [%s]", callerStr)
		}
		if tid, ok := e.fields["trace_id"]; ok {
			fmt.Fprintf(&buf, " [trace_id=%v]", escapeLogText(fmt.Sprint(tid)))
		}
		fmt.Fprintf(&buf, " %s", escapeLogText(RedactSecrets(msg)))

		// Append extra fields
		var extra []string
		for k, v := range e.fields {
			if k != "trace_id" && k != "span_id" {
				extra = append(extra, fmt.Sprintf("%s=%v", k, escapeLogText(RedactSecrets(fmt.Sprint(v)))))
			}
		}
		if len(extra) > 0 {
			buf.WriteString(" | " + strings.Join(extra, " "))
		}
		buf.WriteByte('\n')
	}

	e.logger.mu.Lock()
	if e.logger.out != nil {
		_, _ = e.logger.out.Write(buf.Bytes())
	}
	e.logger.mu.Unlock()

	if lvl == FatalLevel {
		os.Exit(1)
	}
	if lvl == PanicLevel {
		panic(msg)
	}
}

// escapeLogText escapes CR/LF in log messages and field values so untrusted
// content cannot forge additional log lines in text/logfmt output.
func escapeLogText(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	replacer := strings.NewReplacer("\r", "\\r", "\n", "\\n")
	return replacer.Replace(s)
}

func (e *Entry) Trace(args ...any) { e.log(TraceLevel, fmt.Sprint(args...)) }
func (e *Entry) Tracef(format string, args ...any) {
	e.log(TraceLevel, fmt.Sprintf(format, args...))
}
func (e *Entry) Debug(args ...any) { e.log(DebugLevel, fmt.Sprint(args...)) }
func (e *Entry) Debugf(format string, args ...any) {
	e.log(DebugLevel, fmt.Sprintf(format, args...))
}
func (e *Entry) Info(args ...any) { e.log(InfoLevel, fmt.Sprint(args...)) }
func (e *Entry) Infof(format string, args ...any) {
	e.log(InfoLevel, fmt.Sprintf(format, args...))
}
func (e *Entry) Warn(args ...any) { e.log(WarnLevel, fmt.Sprint(args...)) }
func (e *Entry) Warnf(format string, args ...any) {
	e.log(WarnLevel, fmt.Sprintf(format, args...))
}
func (e *Entry) Error(args ...any) { e.log(ErrorLevel, fmt.Sprint(args...)) }
func (e *Entry) Errorf(format string, args ...any) {
	e.log(ErrorLevel, fmt.Sprintf(format, args...))
}
func (e *Entry) Fatal(args ...any) { e.log(FatalLevel, fmt.Sprint(args...)) }
func (e *Entry) Fatalf(format string, args ...any) {
	e.log(FatalLevel, fmt.Sprintf(format, args...))
}

// Global package-level logging helper delegates
func Trace(args ...any)                 { Get().newEntry().Trace(args...) }
func Tracef(format string, args ...any) { Get().newEntry().Tracef(format, args...) }
func Debug(args ...any)                 { Get().newEntry().Debug(args...) }
func Debugf(format string, args ...any) { Get().newEntry().Debugf(format, args...) }
func Info(args ...any)                  { Get().newEntry().Info(args...) }
func Infof(format string, args ...any)  { Get().newEntry().Infof(format, args...) }
func Warn(args ...any)                  { Get().newEntry().Warn(args...) }
func Warnf(format string, args ...any)  { Get().newEntry().Warnf(format, args...) }
func Error(args ...any)                 { Get().newEntry().Error(args...) }
func Errorf(format string, args ...any) { Get().newEntry().Errorf(format, args...) }
func Fatal(args ...any)                 { Get().newEntry().Fatal(args...) }
func Fatalf(format string, args ...any) { Get().newEntry().Fatalf(format, args...) }

func WithField(key string, val any) *Entry   { return Get().WithField(key, val) }
func WithFields(fields Fields) *Entry        { return Get().WithFields(fields) }
func WithError(err error) *Entry             { return Get().WithError(err) }
func WithContext(ctx context.Context) *Entry { return Get().WithContext(ctx) }
