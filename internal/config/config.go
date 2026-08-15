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

type LogConfig struct {
	Level      string `mapstructure:"level" json:"level" yaml:"level"`
	Format     string `mapstructure:"format" json:"format" yaml:"format"`
	Output     string `mapstructure:"output" json:"output" yaml:"output"`
	FilePath   string `mapstructure:"file_path" json:"file_path" yaml:"file_path"`
	MaxSize    int    `mapstructure:"max_size" json:"max_size" yaml:"max_size"`
	MaxBackups int    `mapstructure:"max_backups" json:"max_backups" yaml:"max_backups"`
	MaxAge     int    `mapstructure:"max_age" json:"max_age" yaml:"max_age"`
	Compress   bool   `mapstructure:"compress" json:"compress" yaml:"compress"`
}

func (c *LogConfig) Validate() error {
	level := strings.ToLower(strings.TrimSpace(c.Level))
	switch level {
	case "debug", "info", "warn", "error":
	case "":
	default:
		return fmt.Errorf("log: invalid level %q (must be debug, info, warn, or error)", c.Level)
	}

	format := strings.ToLower(strings.TrimSpace(c.Format))
	switch format {
	case "json", "text":
	case "":
	default:
		return fmt.Errorf("log: invalid format %q (must be json or text)", c.Format)
	}

	output := strings.ToLower(strings.TrimSpace(c.Output))
	if output == "file" && strings.TrimSpace(c.FilePath) == "" {
		return errors.New("log: file_path is required when output is 'file'")
	}
	return nil
}

type Config struct {
	App   AppConfig   `mapstructure:"app" json:"app" yaml:"app"`
	Queue QueueConfig `mapstructure:"queue" json:"queue" yaml:"queue"`
	SMTP  SmtpConfig  `mapstructure:"smtp" json:"smtp" yaml:"smtp"`
	Log   LogConfig   `mapstructure:"log" json:"log" yaml:"log"`
}

func (c *Config) Validate() error {
	if err := c.App.Validate(); err != nil {
		return err
	}
	if err := c.Log.Validate(); err != nil {
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

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
	v.SetDefault("log.output", "stdout")
	v.SetDefault("log.file_path", "")
	v.SetDefault("log.max_size", 100)
	v.SetDefault("log.max_backups", 3)
	v.SetDefault("log.max_age", 7)
	v.SetDefault("log.compress", true)

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

	v.SetDefault("smtp.host", "")
	v.SetDefault("smtp.port", 587)
	v.SetDefault("smtp.username", "")
	v.SetDefault("smtp.password", "")
	v.SetDefault("smtp.from", "")
	v.SetDefault("smtp.from_name", "")
	v.SetDefault("smtp.reply_to", "")
	v.SetDefault("smtp.encryption", string(SmtpEncryptionAuto))
	v.SetDefault("smtp.insecure_skip_verify", false)
	v.SetDefault("smtp.helo_hostname", "")
	v.SetDefault("smtp.auth_type", string(SmtpAuthTypeAuto))
	v.SetDefault("smtp.connect_timeout", 10*time.Second)
	v.SetDefault("smtp.send_timeout", 30*time.Second)
	v.SetDefault("smtp.keep_alive", 30*time.Second)
	v.SetDefault("smtp.pool.max_idle_conns", 5)
	v.SetDefault("smtp.pool.max_open_conns", 20)
	v.SetDefault("smtp.pool.idle_timeout", 60*time.Second)
	v.SetDefault("smtp.rate_limit.emails_per_second", 0)
	v.SetDefault("smtp.rate_limit.max_recipients_per_email", 50)
}
