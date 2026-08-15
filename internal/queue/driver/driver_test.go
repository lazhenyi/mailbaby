package driver_test

import (
	"context"
	"testing"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	_ "mailbaby/internal/queue/driver/all"
)

func TestAllDriversRegistered(t *testing.T) {
	expectedDrivers := []config.QueueDriver{
		config.DriverMemory,
		config.DriverRabbitMQ,
		config.DriverKafka,
		config.DriverRedis,
		config.DriverRocketMQ,
		config.DriverNATS,
		config.DriverPulsar,
		config.DriverSQS,
	}

	registered := queue.GetRegisteredDrivers()
	if len(registered) < len(expectedDrivers) {
		t.Fatalf("expected at least %d registered drivers, got %d: %v",
			len(expectedDrivers), len(registered), registered)
	}

	for _, d := range expectedDrivers {
		if !queue.IsDriverRegistered(d) {
			t.Errorf("driver %q is not registered", d)
		}
	}
}

func TestDriverValidationOnNew(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *config.Config
		errMsg string
	}{
		{
			name: "RabbitMQ Invalid (empty queue/url/host)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver:   config.DriverRabbitMQ,
					RabbitMQ: config.RabbitMQConfig{},
				},
			},
			errMsg: "rabbitmq",
		},
		{
			name: "Kafka Invalid (empty brokers)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver: config.DriverKafka,
					Kafka:  config.KafkaConfig{},
				},
			},
			errMsg: "kafka",
		},
		{
			name: "Redis Invalid (empty key)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver: config.DriverRedis,
					Redis: config.RedisConfig{
						Host: "127.0.0.1",
						Port: 6379,
						Key:  "",
					},
				},
			},
			errMsg: "redis",
		},
		{
			name: "RocketMQ Invalid (empty name_servers)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver:   config.DriverRocketMQ,
					RocketMQ: config.RocketMQConfig{},
				},
			},
			errMsg: "rocketmq",
		},
		{
			name: "NATS Invalid (empty servers)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver: config.DriverNATS,
					NATS:   config.NATSConfig{},
				},
			},
			errMsg: "nats",
		},
		{
			name: "Pulsar Invalid (empty url)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver: config.DriverPulsar,
					Pulsar: config.PulsarConfig{},
				},
			},
			errMsg: "pulsar",
		},
		{
			name: "SQS Invalid (empty queue_url)",
			cfg: &config.Config{
				Queue: config.QueueConfig{
					Driver: config.DriverSQS,
					SQS:    config.SQSConfig{},
				},
			},
			errMsg: "sqs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := queue.New(tc.cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errMsg)
			}
		})
	}
}

func TestMemoryQueueViaRegistry(t *testing.T) {
	cfg := &config.Config{
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 2,
			Memory: config.MemoryConfig{
				BufferSize: 100,
			},
		},
	}

	q, err := queue.New(cfg)
	if err != nil {
		t.Fatalf("queue.New with memory driver failed: %v", err)
	}
	defer func() { _ = q.Close() }()

	if q.Driver() != config.DriverMemory {
		t.Fatalf("expected DriverMemory, got %s", q.Driver())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	prod, err := q.Producer()
	if err != nil {
		t.Fatalf("Producer failed: %v", err)
	}

	cons, err := q.Consumer()
	if err != nil {
		t.Fatalf("Consumer failed: %v", err)
	}

	msg := queue.NewMessage([]byte("payload-1"))
	if err := prod.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	pulled, err := cons.Receive(ctx, queue.WithReceiveTimeout(1*time.Second))
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if string(pulled.Payload) != "payload-1" {
		t.Fatalf("expected payload-1, got %s", string(pulled.Payload))
	}
}
