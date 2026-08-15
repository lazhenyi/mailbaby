package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type MetricsProvider string

const (
	MetricsProviderPrometheus MetricsProvider = "prometheus"
	MetricsProviderStatsD     MetricsProvider = "statsd"
	MetricsProviderOTel       MetricsProvider = "otlp"
	MetricsProviderExpVar     MetricsProvider = "expvar"
	MetricsProviderNone       MetricsProvider = "none"
)

// StatsDConfig defines StatsD exporter settings.
type StatsDConfig struct {
	Address       string        `mapstructure:"address" json:"address" yaml:"address"`             // e.g. "127.0.0.1:8125"
	Prefix        string        `mapstructure:"prefix" json:"prefix" yaml:"prefix"`                // metric prefix, e.g. "mailbaby."
	FlushInterval time.Duration `mapstructure:"flush_interval" json:"flush_interval" yaml:"flush_interval"` // e.g. 100ms
}

// BasicAuthConfig defines HTTP Basic Authentication credentials.
type BasicAuthConfig struct {
	Username string `mapstructure:"username" json:"username" yaml:"username"`
	Password string `mapstructure:"password" json:"password" yaml:"password"`
}

// PushGatewayConfig defines Prometheus PushGateway client settings.
type PushGatewayConfig struct {
	Enabled   bool            `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	URL       string          `mapstructure:"url" json:"url" yaml:"url"`                   // e.g. "http://127.0.0.1:9091"
	Job       string          `mapstructure:"job" json:"job" yaml:"job"`                   // job name e.g. "mailbaby_worker"
	Interval  time.Duration   `mapstructure:"interval" json:"interval" yaml:"interval"`    // push interval e.g. 15s
	BasicAuth BasicAuthConfig `mapstructure:"basic_auth" json:"basic_auth" yaml:"basic_auth"`
}

// MetricsConfig defines configuration for metrics collection, monitoring exporters, and telemetry stats.
type MetricsConfig struct {
	Enabled           bool              `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Provider          MetricsProvider   `mapstructure:"provider" json:"provider" yaml:"provider"`
	Host              string            `mapstructure:"host" json:"host" yaml:"host"`
	Port              int               `mapstructure:"port" json:"port" yaml:"port"`
	Path              string            `mapstructure:"path" json:"path" yaml:"path"`
	CollectRuntime    bool              `mapstructure:"collect_runtime" json:"collect_runtime" yaml:"collect_runtime"` // Go runtime / GC / goroutines
	RuntimeInterval   time.Duration     `mapstructure:"runtime_interval" json:"runtime_interval" yaml:"runtime_interval"`
	CollectQueueStats bool              `mapstructure:"collect_queue_stats" json:"collect_queue_stats" yaml:"collect_queue_stats"` // Queue depth, throughput, lag
	CollectSmtpStats  bool              `mapstructure:"collect_smtp_stats" json:"collect_smtp_stats" yaml:"collect_smtp_stats"`    // SMTP pool, delivery latency, error rates
	StatsD            StatsDConfig      `mapstructure:"statsd" json:"statsd" yaml:"statsd"`
	PushGateway       PushGatewayConfig `mapstructure:"pushgateway" json:"pushgateway" yaml:"pushgateway"`
	CustomLabels      map[string]string `mapstructure:"custom_labels" json:"custom_labels" yaml:"custom_labels"`
	HistogramBuckets  []float64         `mapstructure:"histogram_buckets" json:"histogram_buckets" yaml:"histogram_buckets"`
}

// ApplyDefaults applies default settings for MetricsConfig.
func (c *MetricsConfig) ApplyDefaults() {
	if c.Provider == "" {
		c.Provider = MetricsProviderPrometheus
	}
	if c.Host == "" {
		c.Host = "0.0.0.0"
	}
	if c.Port <= 0 {
		c.Port = 9090
	}
	if c.Path == "" {
		c.Path = "/metrics"
	}
	if c.RuntimeInterval <= 0 {
		c.RuntimeInterval = 10 * time.Second
	}
	if c.StatsD.FlushInterval <= 0 {
		c.StatsD.FlushInterval = 100 * time.Millisecond
	}
	if c.PushGateway.Interval <= 0 {
		c.PushGateway.Interval = 15 * time.Second
	}
	if c.PushGateway.Job == "" {
		c.PushGateway.Job = "mailbaby"
	}
	if len(c.HistogramBuckets) == 0 {
		c.HistogramBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
}

// Validate validates the MetricsConfig.
func (c *MetricsConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	provider := strings.ToLower(strings.TrimSpace(string(c.Provider)))
	switch provider {
	case "prometheus", "statsd", "otlp", "expvar", "none", "":
	default:
		return fmt.Errorf("metrics: unsupported provider %q (supported: prometheus, statsd, otlp, expvar, none)", c.Provider)
	}

	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("metrics: invalid port %d (must be between 1 and 65535)", c.Port)
	}

	if !strings.HasPrefix(c.Path, "/") {
		return fmt.Errorf("metrics: path must start with '/' (got %q)", c.Path)
	}

	if provider == string(MetricsProviderStatsD) && strings.TrimSpace(c.StatsD.Address) == "" {
		return errors.New("metrics: statsd address is required when provider is 'statsd'")
	}

	if c.PushGateway.Enabled {
		if strings.TrimSpace(c.PushGateway.URL) == "" {
			return errors.New("metrics: pushgateway url is required when pushgateway is enabled")
		}
		if _, err := url.ParseRequestURI(c.PushGateway.URL); err != nil {
			return fmt.Errorf("metrics: invalid pushgateway url %q: %w", c.PushGateway.URL, err)
		}
		if strings.TrimSpace(c.PushGateway.Job) == "" {
			return errors.New("metrics: pushgateway job name is required")
		}
	}

	return nil
}

// Address returns the combined host:port string for the metrics HTTP server.
func (c *MetricsConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
