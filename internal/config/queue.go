package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type QueueDriver string

const (
	DriverRabbitMQ QueueDriver = "rabbitmq"
	DriverKafka    QueueDriver = "kafka"
	DriverRedis    QueueDriver = "redis"
	DriverRocketMQ QueueDriver = "rocketmq"
	DriverNATS     QueueDriver = "nats"
	DriverPulsar   QueueDriver = "pulsar"
	DriverSQS      QueueDriver = "sqs"
	DriverMemory   QueueDriver = "memory"
)

type TLSConfig struct {
	Enable             bool   `mapstructure:"enable" json:"enable" yaml:"enable"`
	CAFile             string `mapstructure:"ca_file" json:"ca_file" yaml:"ca_file"`
	CertFile           string `mapstructure:"cert_file" json:"cert_file" yaml:"cert_file"`
	KeyFile            string `mapstructure:"key_file" json:"key_file" yaml:"key_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify" json:"insecure_skip_verify" yaml:"insecure_skip_verify"`
	ServerName         string `mapstructure:"server_name" json:"server_name" yaml:"server_name"`
}

type RabbitMQConfig struct {
	URL           string         `mapstructure:"url" json:"url" yaml:"url"`
	Host          string         `mapstructure:"host" json:"host" yaml:"host"`
	Port          int            `mapstructure:"port" json:"port" yaml:"port"`
	Username      string         `mapstructure:"username" json:"username" yaml:"username"`
	Password      string         `mapstructure:"password" json:"password" yaml:"password"`
	VHost         string         `mapstructure:"vhost" json:"vhost" yaml:"vhost"`
	Queue         string         `mapstructure:"queue" json:"queue" yaml:"queue"`
	Exchange      string         `mapstructure:"exchange" json:"exchange" yaml:"exchange"`
	RoutingKey    string         `mapstructure:"routing_key" json:"routing_key" yaml:"routing_key"`
	Durable       bool           `mapstructure:"durable" json:"durable" yaml:"durable"`
	AutoDelete    bool           `mapstructure:"auto_delete" json:"auto_delete" yaml:"auto_delete"`
	Exclusive     bool           `mapstructure:"exclusive" json:"exclusive" yaml:"exclusive"`
	NoWait        bool           `mapstructure:"no_wait" json:"no_wait" yaml:"no_wait"`
	AutoAck       bool           `mapstructure:"auto_ack" json:"auto_ack" yaml:"auto_ack"`
	PrefetchCount int            `mapstructure:"prefetch_count" json:"prefetch_count" yaml:"prefetch_count"`
	Args          map[string]any `mapstructure:"args" json:"args" yaml:"args"`
	TLS           TLSConfig      `mapstructure:"tls" json:"tls" yaml:"tls"`
}

func (c *RabbitMQConfig) Validate() error {
	if c.URL == "" && c.Host == "" {
		return errors.New("queue.rabbitmq: either url or host must be specified")
	}
	if c.URL == "" && (c.Port <= 0 || c.Port > 65535) {
		return fmt.Errorf("queue.rabbitmq: invalid port %d", c.Port)
	}
	if strings.TrimSpace(c.Queue) == "" {
		return errors.New("queue.rabbitmq: queue name is required")
	}
	return nil
}

type KafkaSASLConfig struct {
	Enable    bool   `mapstructure:"enable" json:"enable" yaml:"enable"`
	Mechanism string `mapstructure:"mechanism" json:"mechanism" yaml:"mechanism"`
	User      string `mapstructure:"user" json:"user" yaml:"user"`
	Password  string `mapstructure:"password" json:"password" yaml:"password"`
}

type KafkaConfig struct {
	Brokers       []string        `mapstructure:"brokers" json:"brokers" yaml:"brokers"`
	Topic         string          `mapstructure:"topic" json:"topic" yaml:"topic"`
	GroupID       string          `mapstructure:"group_id" json:"group_id" yaml:"group_id"`
	ClientID      string          `mapstructure:"client_id" json:"client_id" yaml:"client_id"`
	Version       string          `mapstructure:"version" json:"version" yaml:"version"`
	InitialOffset string          `mapstructure:"initial_offset" json:"initial_offset" yaml:"initial_offset"`
	AutoCommit    bool            `mapstructure:"auto_commit" json:"auto_commit" yaml:"auto_commit"`
	SASL          KafkaSASLConfig `mapstructure:"sasl" json:"sasl" yaml:"sasl"`
	TLS           TLSConfig       `mapstructure:"tls" json:"tls" yaml:"tls"`
}

func (c *KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("queue.kafka: brokers list cannot be empty")
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("queue.kafka: topic is required")
	}
	if strings.TrimSpace(c.GroupID) == "" {
		return errors.New("queue.kafka: group_id is required")
	}
	return nil
}

type RedisConfig struct {
	Addrs      []string      `mapstructure:"addrs" json:"addrs" yaml:"addrs"`
	Host       string        `mapstructure:"host" json:"host" yaml:"host"`
	Port       int           `mapstructure:"port" json:"port" yaml:"port"`
	Username   string        `mapstructure:"username" json:"username" yaml:"username"`
	Password   string        `mapstructure:"password" json:"password" yaml:"password"`
	DB         int           `mapstructure:"db" json:"db" yaml:"db"`
	MasterName string        `mapstructure:"master_name" json:"master_name" yaml:"master_name"`
	Mode       string        `mapstructure:"mode" json:"mode" yaml:"mode"`
	Key        string        `mapstructure:"key" json:"key" yaml:"key"`
	Group      string        `mapstructure:"group" json:"group" yaml:"group"`
	Consumer   string        `mapstructure:"consumer" json:"consumer" yaml:"consumer"`
	BlockTime  time.Duration `mapstructure:"block_time" json:"block_time" yaml:"block_time"`
	MaxLen     int64         `mapstructure:"max_len" json:"max_len" yaml:"max_len"`
	TLS        TLSConfig     `mapstructure:"tls" json:"tls" yaml:"tls"`
}

func (c *RedisConfig) Validate() error {
	if len(c.Addrs) == 0 && c.Host == "" {
		return errors.New("queue.redis: addrs or host must be specified")
	}
	if strings.TrimSpace(c.Key) == "" {
		return errors.New("queue.redis: key/stream name is required")
	}
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		mode = "stream"
	}
	switch mode {
	case "stream":
		if strings.TrimSpace(c.Group) == "" {
			return errors.New("queue.redis: group is required when mode is 'stream'")
		}
	case "list", "pubsub":
	default:
		return fmt.Errorf("queue.redis: unsupported mode %q (must be 'stream', 'list', or 'pubsub')", c.Mode)
	}
	return nil
}

type RocketMQConfig struct {
	NameServers    []string `mapstructure:"name_servers" json:"name_servers" yaml:"name_servers"`
	Topic          string   `mapstructure:"topic" json:"topic" yaml:"topic"`
	Group          string   `mapstructure:"group" json:"group" yaml:"group"`
	AccessKey      string   `mapstructure:"access_key" json:"access_key" yaml:"access_key"`
	SecretKey      string   `mapstructure:"secret_key" json:"secret_key" yaml:"secret_key"`
	SecurityToken  string   `mapstructure:"security_token" json:"security_token" yaml:"security_token"`
	Namespace      string   `mapstructure:"namespace" json:"namespace" yaml:"namespace"`
	ConsumeOrderly bool     `mapstructure:"consume_orderly" json:"consume_orderly" yaml:"consume_orderly"`
}

func (c *RocketMQConfig) Validate() error {
	if len(c.NameServers) == 0 {
		return errors.New("queue.rocketmq: name_servers list cannot be empty")
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("queue.rocketmq: topic is required")
	}
	if strings.TrimSpace(c.Group) == "" {
		return errors.New("queue.rocketmq: group is required")
	}
	return nil
}

type NATSConfig struct {
	Servers    []string  `mapstructure:"servers" json:"servers" yaml:"servers"`
	Subject    string    `mapstructure:"subject" json:"subject" yaml:"subject"`
	QueueGroup string    `mapstructure:"queue_group" json:"queue_group" yaml:"queue_group"`
	JetStream  bool      `mapstructure:"jetstream" json:"jetstream" yaml:"jetstream"`
	Stream     string    `mapstructure:"stream" json:"stream" yaml:"stream"`
	Durable    string    `mapstructure:"durable" json:"durable" yaml:"durable"`
	Token      string    `mapstructure:"token" json:"token" yaml:"token"`
	Username   string    `mapstructure:"username" json:"username" yaml:"username"`
	Password   string    `mapstructure:"password" json:"password" yaml:"password"`
	CredsFile  string    `mapstructure:"creds_file" json:"creds_file" yaml:"creds_file"`
	TLS        TLSConfig `mapstructure:"tls" json:"tls" yaml:"tls"`
}

func (c *NATSConfig) Validate() error {
	if len(c.Servers) == 0 {
		return errors.New("queue.nats: servers list cannot be empty")
	}
	if strings.TrimSpace(c.Subject) == "" {
		return errors.New("queue.nats: subject is required")
	}
	return nil
}

type PulsarConfig struct {
	URL              string    `mapstructure:"url" json:"url" yaml:"url"`
	Topic            string    `mapstructure:"topic" json:"topic" yaml:"topic"`
	SubscriptionName string    `mapstructure:"subscription_name" json:"subscription_name" yaml:"subscription_name"`
	SubscriptionType string    `mapstructure:"subscription_type" json:"subscription_type" yaml:"subscription_type"`
	AuthToken        string    `mapstructure:"auth_token" json:"auth_token" yaml:"auth_token"`
	TLS              TLSConfig `mapstructure:"tls" json:"tls" yaml:"tls"`
}

func (c *PulsarConfig) Validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("queue.pulsar: url is required")
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("queue.pulsar: topic is required")
	}
	if strings.TrimSpace(c.SubscriptionName) == "" {
		return errors.New("queue.pulsar: subscription_name is required")
	}
	return nil
}

type SQSConfig struct {
	Region              string `mapstructure:"region" json:"region" yaml:"region"`
	QueueURL            string `mapstructure:"queue_url" json:"queue_url" yaml:"queue_url"`
	AccessKeyID         string `mapstructure:"access_key_id" json:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey     string `mapstructure:"secret_access_key" json:"secret_access_key" yaml:"secret_access_key"`
	SessionToken        string `mapstructure:"session_token" json:"session_token" yaml:"session_token"`
	Endpoint            string `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"`
	WaitTimeSeconds     int32  `mapstructure:"wait_time_seconds" json:"wait_time_seconds" yaml:"wait_time_seconds"`
	VisibilityTimeout   int32  `mapstructure:"visibility_timeout" json:"visibility_timeout" yaml:"visibility_timeout"`
	MaxNumberOfMessages int32  `mapstructure:"max_number_of_messages" json:"max_number_of_messages" yaml:"max_number_of_messages"`
}

func (c *SQSConfig) Validate() error {
	if strings.TrimSpace(c.QueueURL) == "" {
		return errors.New("queue.sqs: queue_url is required")
	}
	if strings.TrimSpace(c.Region) == "" && c.Endpoint == "" {
		return errors.New("queue.sqs: region is required when custom endpoint is not set")
	}
	return nil
}

type MemoryConfig struct {
	BufferSize int `mapstructure:"buffer_size" json:"buffer_size" yaml:"buffer_size"`
}

func (c *MemoryConfig) Validate() error {
	if c.BufferSize < 0 {
		return errors.New("queue.memory: buffer_size cannot be negative")
	}
	return nil
}

type QueueConfig struct {
	Driver QueueDriver `mapstructure:"driver" json:"driver" yaml:"driver"`

	Concurrency   int           `mapstructure:"concurrency" json:"concurrency" yaml:"concurrency"`
	MaxRetries    int           `mapstructure:"max_retries" json:"max_retries" yaml:"max_retries"`
	RetryInterval time.Duration `mapstructure:"retry_interval" json:"retry_interval" yaml:"retry_interval"`
	PrefetchCount int           `mapstructure:"prefetch_count" json:"prefetch_count" yaml:"prefetch_count"`
	BatchSize     int           `mapstructure:"batch_size" json:"batch_size" yaml:"batch_size"`

	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq" json:"rabbitmq" yaml:"rabbitmq"`
	Kafka    KafkaConfig    `mapstructure:"kafka" json:"kafka" yaml:"kafka"`
	Redis    RedisConfig    `mapstructure:"redis" json:"redis" yaml:"redis"`
	RocketMQ RocketMQConfig `mapstructure:"rocketmq" json:"rocketmq" yaml:"rocketmq"`
	NATS     NATSConfig     `mapstructure:"nats" json:"nats" yaml:"nats"`
	Pulsar   PulsarConfig   `mapstructure:"pulsar" json:"pulsar" yaml:"pulsar"`
	SQS      SQSConfig      `mapstructure:"sqs" json:"sqs" yaml:"sqs"`
	Memory   MemoryConfig   `mapstructure:"memory" json:"memory" yaml:"memory"`
}

func (c *QueueConfig) Validate() error {
	driver := QueueDriver(strings.ToLower(strings.TrimSpace(string(c.Driver))))
	if driver == "" {
		return errors.New("queue: driver is required (e.g. rabbitmq, kafka, redis, rocketmq, nats, pulsar, sqs, memory)")
	}

	if c.Concurrency <= 0 {
		return fmt.Errorf("queue: invalid concurrency %d (must be > 0)", c.Concurrency)
	}

	switch driver {
	case DriverRabbitMQ:
		return c.RabbitMQ.Validate()
	case DriverKafka:
		return c.Kafka.Validate()
	case DriverRedis:
		return c.Redis.Validate()
	case DriverRocketMQ:
		return c.RocketMQ.Validate()
	case DriverNATS:
		return c.NATS.Validate()
	case DriverPulsar:
		return c.Pulsar.Validate()
	case DriverSQS:
		return c.SQS.Validate()
	case DriverMemory:
		return c.Memory.Validate()
	default:
		return fmt.Errorf("queue: unsupported queue driver %q", c.Driver)
	}
}
