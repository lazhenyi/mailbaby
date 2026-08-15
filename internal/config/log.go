package config

import (
	"errors"
	"fmt"
	"strings"
)

type LogLevel string

const (
	LogLevelTrace LogLevel = "trace"
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelFatal LogLevel = "fatal"
	LogLevelPanic LogLevel = "panic"
)

type LogFormat string

const (
	LogFormatJSON    LogFormat = "json"
	LogFormatText    LogFormat = "text"
	LogFormatConsole LogFormat = "console"
	LogFormatLogfmt  LogFormat = "logfmt"
)

type LogOutput string

const (
	LogOutputStdout LogOutput = "stdout"
	LogOutputStderr LogOutput = "stderr"
	LogOutputFile   LogOutput = "file"
	LogOutputBoth   LogOutput = "both"
)

// LogConfig defines logging parameters, formats, outputs, and rotation rules.
type LogConfig struct {
	Level          LogLevel  `mapstructure:"level" json:"level" yaml:"level"`
	Format         LogFormat `mapstructure:"format" json:"format" yaml:"format"`
	Output         LogOutput `mapstructure:"output" json:"output" yaml:"output"`
	FilePath       string    `mapstructure:"file_path" json:"file_path" yaml:"file_path"`
	MaxSize        int       `mapstructure:"max_size" json:"max_size" yaml:"max_size"`                      // in megabytes
	MaxBackups     int       `mapstructure:"max_backups" json:"max_backups" yaml:"max_backups"`             // max count of old files
	MaxAge         int       `mapstructure:"max_age" json:"max_age" yaml:"max_age"`                         // in days
	Compress       bool      `mapstructure:"compress" json:"compress" yaml:"compress"`                      // whether to compress old logs
	ShowCaller     bool      `mapstructure:"show_caller" json:"show_caller" yaml:"show_caller"`             // attach caller file and line
	ShowStacktrace string    `mapstructure:"show_stacktrace" json:"show_stacktrace" yaml:"show_stacktrace"` // stacktrace level: none, warn, error, panic
	TimeFormat     string    `mapstructure:"time_format" json:"time_format" yaml:"time_format"`             // custom time format e.g. RFC3339
	Async          bool      `mapstructure:"async" json:"async" yaml:"async"`                               // asynchronous logging
	BufferSize     int       `mapstructure:"buffer_size" json:"buffer_size" yaml:"buffer_size"`             // buffer size for async mode
}

// ApplyDefaults applies default settings for LogConfig.
func (c *LogConfig) ApplyDefaults() {
	if c.Level == "" {
		c.Level = LogLevelInfo
	}
	if c.Format == "" {
		c.Format = LogFormatText
	}
	if c.Output == "" {
		c.Output = LogOutputStdout
	}
	if c.MaxSize <= 0 {
		c.MaxSize = 100
	}
	if c.MaxBackups <= 0 {
		c.MaxBackups = 3
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 7
	}
	if c.ShowStacktrace == "" {
		c.ShowStacktrace = "error"
	}
	if c.Async && c.BufferSize <= 0 {
		c.BufferSize = 4096
	}
}

// Validate validates the LogConfig parameters.
func (c *LogConfig) Validate() error {
	level := strings.ToLower(strings.TrimSpace(string(c.Level)))
	switch level {
	case "trace", "debug", "info", "warn", "error", "fatal", "panic", "":
	default:
		return fmt.Errorf("log: invalid level %q (supported: trace, debug, info, warn, error, fatal, panic)", c.Level)
	}

	format := strings.ToLower(strings.TrimSpace(string(c.Format)))
	switch format {
	case "json", "text", "console", "logfmt", "":
	default:
		return fmt.Errorf("log: invalid format %q (supported: json, text, console, logfmt)", c.Format)
	}

	output := strings.ToLower(strings.TrimSpace(string(c.Output)))
	switch output {
	case "stdout", "stderr", "file", "both", "":
	default:
		return fmt.Errorf("log: invalid output %q (supported: stdout, stderr, file, both)", c.Output)
	}

	if (output == "file" || output == "both") && strings.TrimSpace(c.FilePath) == "" {
		return errors.New("log: file_path is required when output is 'file' or 'both'")
	}

	if c.MaxSize < 0 {
		return errors.New("log: max_size cannot be negative")
	}
	if c.MaxBackups < 0 {
		return errors.New("log: max_backups cannot be negative")
	}
	if c.MaxAge < 0 {
		return errors.New("log: max_age cannot be negative")
	}

	return nil
}

// IsDebug returns true if logging level is debug or trace.
func (c *LogConfig) IsDebug() bool {
	l := strings.ToLower(string(c.Level))
	return l == string(LogLevelDebug) || l == string(LogLevelTrace)
}

// IsJSON returns true if logging format is JSON.
func (c *LogConfig) IsJSON() bool {
	return strings.ToLower(string(c.Format)) == string(LogFormatJSON)
}
