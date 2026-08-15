package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type TracingProvider string

const (
	TracingProviderOTel   TracingProvider = "otlp"
	TracingProviderJaeger TracingProvider = "jaeger"
	TracingProviderZipkin TracingProvider = "zipkin"
	TracingProviderStdout TracingProvider = "stdout"
	TracingProviderNone   TracingProvider = "none"
)

// TracingConfig defines distributed tracing settings.
type TracingConfig struct {
	Enabled       bool            `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Provider      TracingProvider `mapstructure:"provider" json:"provider" yaml:"provider"` // otlp, jaeger, zipkin, stdout
	Endpoint      string          `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"` // e.g. "localhost:4317" or "http://localhost:4318/v1/traces"
	Insecure      bool            `mapstructure:"insecure" json:"insecure" yaml:"insecure"` // skip TLS for OTLP gRPC/HTTP
	SampleRate    float64         `mapstructure:"sample_rate" json:"sample_rate" yaml:"sample_rate"` // 0.0 to 1.0 (1.0 = 100%)
	ServiceName   string          `mapstructure:"service_name" json:"service_name" yaml:"service_name"`
	BatchTimeout  time.Duration   `mapstructure:"batch_timeout" json:"batch_timeout" yaml:"batch_timeout"`
	MaxQueueSize  int             `mapstructure:"max_queue_size" json:"max_queue_size" yaml:"max_queue_size"`
	ExportTimeout time.Duration   `mapstructure:"export_timeout" json:"export_timeout" yaml:"export_timeout"`
}

// ApplyDefaults applies default settings for TracingConfig.
func (c *TracingConfig) ApplyDefaults() {
	if c.Provider == "" {
		c.Provider = TracingProviderOTel
	}
	if c.SampleRate <= 0 && c.Enabled {
		c.SampleRate = 1.0
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 5 * time.Second
	}
	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = 2048
	}
	if c.ExportTimeout <= 0 {
		c.ExportTimeout = 30 * time.Second
	}
}

// Validate validates TracingConfig.
func (c *TracingConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	provider := strings.ToLower(strings.TrimSpace(string(c.Provider)))
	switch provider {
	case "otlp", "jaeger", "zipkin", "stdout", "none", "":
	default:
		return fmt.Errorf("tracing: unsupported provider %q (supported: otlp, jaeger, zipkin, stdout, none)", c.Provider)
	}

	if provider != string(TracingProviderStdout) && provider != string(TracingProviderNone) && strings.TrimSpace(c.Endpoint) == "" {
		return fmt.Errorf("tracing: endpoint is required when provider is %q", c.Provider)
	}

	if c.SampleRate < 0.0 || c.SampleRate > 1.0 {
		return fmt.Errorf("tracing: sample_rate must be between 0.0 and 1.0 (got %f)", c.SampleRate)
	}

	if c.MaxQueueSize < 0 {
		return errors.New("tracing: max_queue_size cannot be negative")
	}

	return nil
}

// HealthConfig defines HTTP health check probes (/livez, /readyz).
type HealthConfig struct {
	Enabled      bool          `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Host         string        `mapstructure:"host" json:"host" yaml:"host"`
	Port         int           `mapstructure:"port" json:"port" yaml:"port"`
	LivePath     string        `mapstructure:"live_path" json:"live_path" yaml:"live_path"`       // liveness probe path, e.g. "/livez"
	ReadyPath    string        `mapstructure:"ready_path" json:"ready_path" yaml:"ready_path"`    // readiness probe path, e.g. "/readyz"
	CheckTimeout time.Duration `mapstructure:"check_timeout" json:"check_timeout" yaml:"check_timeout"`
}

// ApplyDefaults applies default settings for HealthConfig.
func (c *HealthConfig) ApplyDefaults() {
	if c.Host == "" {
		c.Host = "0.0.0.0"
	}
	if c.Port <= 0 {
		c.Port = 8080
	}
	if c.LivePath == "" {
		c.LivePath = "/livez"
	}
	if c.ReadyPath == "" {
		c.ReadyPath = "/readyz"
	}
	if c.CheckTimeout <= 0 {
		c.CheckTimeout = 5 * time.Second
	}
}

// Validate validates HealthConfig.
func (c *HealthConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("health: invalid port %d (must be between 1 and 65535)", c.Port)
	}

	if !strings.HasPrefix(c.LivePath, "/") {
		return fmt.Errorf("health: live_path must start with '/' (got %q)", c.LivePath)
	}
	if !strings.HasPrefix(c.ReadyPath, "/") {
		return fmt.Errorf("health: ready_path must start with '/' (got %q)", c.ReadyPath)
	}

	return nil
}

// PprofConfig defines runtime performance profiling (pprof) settings.
type PprofConfig struct {
	Enabled      bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Host         string `mapstructure:"host" json:"host" yaml:"host"`
	Port         int    `mapstructure:"port" json:"port" yaml:"port"`
	Path         string `mapstructure:"path" json:"path" yaml:"path"`
	ProfileMutex bool   `mapstructure:"profile_mutex" json:"profile_mutex" yaml:"profile_mutex"`
	ProfileBlock bool   `mapstructure:"profile_block" json:"profile_block" yaml:"profile_block"`
	BlockRate    int    `mapstructure:"block_rate" json:"block_rate" yaml:"block_rate"`
	MutexRate    int    `mapstructure:"mutex_rate" json:"mutex_rate" yaml:"mutex_rate"`
}

// ApplyDefaults applies default settings for PprofConfig.
func (c *PprofConfig) ApplyDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port <= 0 {
		c.Port = 6060
	}
	if c.Path == "" {
		c.Path = "/debug/pprof"
	}
}

// Validate validates PprofConfig.
func (c *PprofConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("pprof: invalid port %d (must be between 1 and 65535)", c.Port)
	}

	if !strings.HasPrefix(c.Path, "/") {
		return fmt.Errorf("pprof: path must start with '/' (got %q)", c.Path)
	}

	return nil
}

// ObservabilityConfig integrates tracing, health checks, and profiling subsystems.
type ObservabilityConfig struct {
	Tracing TracingConfig `mapstructure:"tracing" json:"tracing" yaml:"tracing"`
	Health  HealthConfig  `mapstructure:"health" json:"health" yaml:"health"`
	Pprof   PprofConfig   `mapstructure:"pprof" json:"pprof" yaml:"pprof"`
}

// ApplyDefaults applies default settings for all observability subsystems.
func (c *ObservabilityConfig) ApplyDefaults() {
	c.Tracing.ApplyDefaults()
	c.Health.ApplyDefaults()
	c.Pprof.ApplyDefaults()
}

// Validate validates all observability subsystems.
func (c *ObservabilityConfig) Validate() error {
	c.ApplyDefaults()

	if err := c.Tracing.Validate(); err != nil {
		return err
	}
	if err := c.Health.Validate(); err != nil {
		return err
	}
	if err := c.Pprof.Validate(); err != nil {
		return err
	}

	return nil
}
