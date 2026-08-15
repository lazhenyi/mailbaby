package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

var (
	globalConfig *Config
	globalMu     sync.RWMutex
)

type AppConfig struct {
	Name            string        `mapstructure:"name" json:"name" yaml:"name"`
	Env             string        `mapstructure:"env" json:"env" yaml:"env"`
	Debug           bool          `mapstructure:"debug" json:"debug" yaml:"debug"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout" json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

func (c *AppConfig) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("app: name cannot be empty")
	}
	return nil
}

type Config struct {
	App           AppConfig           `mapstructure:"app" json:"app" yaml:"app"`
	Server        ServerConfig        `mapstructure:"server" json:"server" yaml:"server"`
	Queue         QueueConfig         `mapstructure:"queue" json:"queue" yaml:"queue"`
	SMTP          SmtpConfig          `mapstructure:"smtp" json:"smtp" yaml:"smtp"`
	Log           LogConfig           `mapstructure:"log" json:"log" yaml:"log"`
	Metrics       MetricsConfig       `mapstructure:"metrics" json:"metrics" yaml:"metrics"`
	Observability ObservabilityConfig `mapstructure:"observability" json:"observability" yaml:"observability"`
}

func (c *Config) Validate() error {
	if err := c.App.Validate(); err != nil {
		return err
	}
	c.Server.ApplyDefaults()
	if err := c.Server.Validate(); err != nil {
		return err
	}
	c.Log.ApplyDefaults()
	if err := c.Log.Validate(); err != nil {
		return err
	}
	c.Metrics.ApplyDefaults()
	if err := c.Metrics.Validate(); err != nil {
		return err
	}
	c.Observability.ApplyDefaults()
	if err := c.Observability.Validate(); err != nil {
		return err
	}
	if err := c.SMTP.Validate(); err != nil {
		return err
	}
	if err := c.Queue.Validate(); err != nil {
		return err
	}
	return nil
}

func Get() *Config {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalConfig
}

func Set(cfg *Config) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalConfig = cfg
}

func Load(configPath ...string) (*Config, error) {
	v := viper.New()

	setDefaults(v)

	v.SetEnvPrefix("MAILBABY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	var targetFile string
	if len(configPath) > 0 && strings.TrimSpace(configPath[0]) != "" {
		targetFile = configPath[0]
	} else if envPath := os.Getenv("MAILBABY_CONFIG"); envPath != "" {
		targetFile = envPath
	}

	if targetFile != "" {
		v.SetConfigFile(targetFile)
		if strings.HasSuffix(targetFile, ".yaml.example") || strings.HasSuffix(targetFile, ".yml.example") || strings.HasSuffix(targetFile, ".example") {
			v.SetConfigType("yaml")
		}
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file %q: %w", targetFile, err)
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./configs")
		v.AddConfigPath("../configs")
		v.AddConfigPath("/etc/mailbaby")

		if err := v.ReadInConfig(); err != nil {
			var configFileNotFound viper.ConfigFileNotFoundError
			if !errors.As(err, &configFileNotFound) && !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to load config file: %w", err)
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	Set(&cfg)

	return &cfg, nil
}

func LoadFromBytes(data []byte, configType string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	v.SetEnvPrefix("MAILBABY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if configType == "" {
		configType = "yaml"
	}
	v.SetConfigType(configType)

	if err := v.ReadConfig(strings.NewReader(string(data))); err != nil {
		return nil, fmt.Errorf("failed to parse config content: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "mailbaby")
	v.SetDefault("app.env", "development")
	v.SetDefault("app.debug", false)
	v.SetDefault("app.shutdown_timeout", 10*time.Second)

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 10*time.Second)
	v.SetDefault("server.write_timeout", 10*time.Second)
	v.SetDefault("server.idle_timeout", 30*time.Second)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
	v.SetDefault("log.output", "stdout")
	v.SetDefault("log.file_path", "")
	v.SetDefault("log.max_size", 100)
	v.SetDefault("log.max_backups", 3)
	v.SetDefault("log.max_age", 7)
	v.SetDefault("log.compress", true)
	v.SetDefault("log.show_caller", false)
	v.SetDefault("log.show_stacktrace", "error")
	v.SetDefault("log.async", false)
	v.SetDefault("log.buffer_size", 4096)

	v.SetDefault("metrics.enabled", false)
	v.SetDefault("metrics.provider", string(MetricsProviderPrometheus))
	v.SetDefault("metrics.host", "0.0.0.0")
	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("metrics.collect_runtime", true)
	v.SetDefault("metrics.runtime_interval", 10*time.Second)
	v.SetDefault("metrics.collect_queue_stats", true)
	v.SetDefault("metrics.collect_smtp_stats", true)
	v.SetDefault("metrics.statsd.address", "")
	v.SetDefault("metrics.statsd.prefix", "")
	v.SetDefault("metrics.statsd.flush_interval", 100*time.Millisecond)
	v.SetDefault("metrics.pushgateway.enabled", false)
	v.SetDefault("metrics.pushgateway.url", "")
	v.SetDefault("metrics.pushgateway.job", "mailbaby")
	v.SetDefault("metrics.pushgateway.interval", 15*time.Second)
	v.SetDefault("metrics.pushgateway.basic_auth.username", "")
	v.SetDefault("metrics.pushgateway.basic_auth.password", "")

	v.SetDefault("observability.tracing.enabled", false)
	v.SetDefault("observability.tracing.provider", string(TracingProviderOTel))
	v.SetDefault("observability.tracing.endpoint", "")
	v.SetDefault("observability.tracing.insecure", false)
	v.SetDefault("observability.tracing.sample_rate", 1.0)
	v.SetDefault("observability.tracing.service_name", "")
	v.SetDefault("observability.tracing.batch_timeout", 5*time.Second)
	v.SetDefault("observability.tracing.max_queue_size", 2048)
	v.SetDefault("observability.tracing.export_timeout", 30*time.Second)

	v.SetDefault("observability.health.enabled", false)
	v.SetDefault("observability.health.host", "0.0.0.0")
	v.SetDefault("observability.health.port", 8080)
	v.SetDefault("observability.health.live_path", "/livez")
	v.SetDefault("observability.health.ready_path", "/readyz")
	v.SetDefault("observability.health.check_timeout", 5*time.Second)

	v.SetDefault("observability.pprof.enabled", false)
	v.SetDefault("observability.pprof.host", "127.0.0.1")
	v.SetDefault("observability.pprof.port", 6060)
	v.SetDefault("observability.pprof.path", "/debug/pprof")
	v.SetDefault("observability.pprof.profile_mutex", false)
	v.SetDefault("observability.pprof.profile_block", false)
	v.SetDefault("observability.pprof.block_rate", 0)
	v.SetDefault("observability.pprof.mutex_rate", 0)

	v.SetDefault("queue.driver", string(DriverMemory))
	v.SetDefault("queue.concurrency", 10)
	v.SetDefault("queue.max_retries", 3)
	v.SetDefault("queue.retry_interval", 5*time.Second)
	v.SetDefault("queue.prefetch_count", 10)
	v.SetDefault("queue.batch_size", 1)

	v.SetDefault("queue.rabbitmq.url", "")
	v.SetDefault("queue.rabbitmq.host", "127.0.0.1")
	v.SetDefault("queue.rabbitmq.port", 5672)
	v.SetDefault("queue.rabbitmq.username", "")
	v.SetDefault("queue.rabbitmq.password", "")
	v.SetDefault("queue.rabbitmq.vhost", "/")
	v.SetDefault("queue.rabbitmq.queue", "")
	v.SetDefault("queue.rabbitmq.exchange", "")
	v.SetDefault("queue.rabbitmq.routing_key", "")
	v.SetDefault("queue.rabbitmq.durable", true)
	v.SetDefault("queue.rabbitmq.auto_delete", false)
	v.SetDefault("queue.rabbitmq.exclusive", false)
	v.SetDefault("queue.rabbitmq.auto_ack", false)
	v.SetDefault("queue.rabbitmq.prefetch_count", 10)

	v.SetDefault("queue.kafka.brokers", []string{})
	v.SetDefault("queue.kafka.topic", "")
	v.SetDefault("queue.kafka.group_id", "")
	v.SetDefault("queue.kafka.client_id", "mailbaby-consumer")
	v.SetDefault("queue.kafka.version", "")
	v.SetDefault("queue.kafka.initial_offset", "newest")
	v.SetDefault("queue.kafka.auto_commit", true)

	v.SetDefault("queue.redis.addrs", []string{})
	v.SetDefault("queue.redis.host", "127.0.0.1")
	v.SetDefault("queue.redis.port", 6379)
	v.SetDefault("queue.redis.username", "")
	v.SetDefault("queue.redis.password", "")
	v.SetDefault("queue.redis.db", 0)
	v.SetDefault("queue.redis.master_name", "")
	v.SetDefault("queue.redis.mode", "stream")
	v.SetDefault("queue.redis.key", "")
	v.SetDefault("queue.redis.group", "mailbaby-workers")
	v.SetDefault("queue.redis.consumer", "worker-1")
	v.SetDefault("queue.redis.block_time", 2*time.Second)
	v.SetDefault("queue.redis.max_len", 0)

	v.SetDefault("queue.rocketmq.name_servers", []string{})
	v.SetDefault("queue.rocketmq.topic", "")
	v.SetDefault("queue.rocketmq.group", "")
	v.SetDefault("queue.rocketmq.access_key", "")
	v.SetDefault("queue.rocketmq.secret_key", "")
	v.SetDefault("queue.rocketmq.security_token", "")
	v.SetDefault("queue.rocketmq.namespace", "")
	v.SetDefault("queue.rocketmq.consume_orderly", false)

	v.SetDefault("queue.nats.servers", []string{})
	v.SetDefault("queue.nats.subject", "")
	v.SetDefault("queue.nats.queue_group", "mailbaby-group")
	v.SetDefault("queue.nats.jetstream", false)
	v.SetDefault("queue.nats.stream", "")
	v.SetDefault("queue.nats.durable", "")

	v.SetDefault("queue.pulsar.url", "")
	v.SetDefault("queue.pulsar.topic", "")
	v.SetDefault("queue.pulsar.subscription_name", "")
	v.SetDefault("queue.pulsar.subscription_type", "Shared")
	v.SetDefault("queue.pulsar.auth_token", "")

	v.SetDefault("queue.sqs.region", "")
	v.SetDefault("queue.sqs.queue_url", "")
	v.SetDefault("queue.sqs.access_key_id", "")
	v.SetDefault("queue.sqs.secret_access_key", "")
	v.SetDefault("queue.sqs.session_token", "")
	v.SetDefault("queue.sqs.endpoint", "")
	v.SetDefault("queue.sqs.wait_time_seconds", 20)
	v.SetDefault("queue.sqs.visibility_timeout", 30)
	v.SetDefault("queue.sqs.max_number_of_messages", 10)

	v.SetDefault("queue.memory.buffer_size", 1024)

	v.SetDefault("smtp.default.host", "")
	v.SetDefault("smtp.default.port", 587)
	v.SetDefault("smtp.default.username", "")
	v.SetDefault("smtp.default.password", "")
	v.SetDefault("smtp.default.from", "")
	v.SetDefault("smtp.default.from_name", "")
	v.SetDefault("smtp.default.reply_to", "")
	v.SetDefault("smtp.default.encryption", string(SmtpEncryptionAuto))
	v.SetDefault("smtp.default.insecure_skip_verify", false)
	v.SetDefault("smtp.default.helo_hostname", "")
	v.SetDefault("smtp.default.auth_type", string(SmtpAuthTypeAuto))
	v.SetDefault("smtp.default.connect_timeout", 10*time.Second)
	v.SetDefault("smtp.default.send_timeout", 30*time.Second)
	v.SetDefault("smtp.default.keep_alive", 30*time.Second)
	v.SetDefault("smtp.default.pool.max_idle_conns", 5)
	v.SetDefault("smtp.default.pool.max_open_conns", 20)
	v.SetDefault("smtp.default.pool.idle_timeout", 60*time.Second)
	v.SetDefault("smtp.default.rate_limit.emails_per_second", 0)
	v.SetDefault("smtp.default.rate_limit.max_recipients_per_email", 50)
	v.SetDefault("smtp.default.rate_limit.email_size_limit", 15*1024*1024)
}
