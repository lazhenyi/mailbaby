package config

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestLoadDefaultsAndMemory(t *testing.T) {
	yamlContent := `
smtp:
  default:
    host: "smtp.example.com"
    port: 587
    from: "sender@example.com"
`
	cfg, err := LoadFromBytes([]byte(yamlContent), "yaml")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.App.Name != "mailbaby" {
		t.Errorf("expected App.Name 'mailbaby', got %q", cfg.App.Name)
	}
	if cfg.App.ShutdownTimeout != 10*time.Second {
		t.Errorf("expected ShutdownTimeout 10s, got %v", cfg.App.ShutdownTimeout)
	}

	if cfg.Log.Level != "info" {
		t.Errorf("expected Log.Level 'info', got %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("expected Log.Format 'text', got %q", cfg.Log.Format)
	}

	if cfg.Queue.Driver != DriverMemory {
		t.Errorf("expected Driver 'memory', got %q", cfg.Queue.Driver)
	}
	if cfg.Queue.Concurrency != 10 {
		t.Errorf("expected Concurrency 10, got %d", cfg.Queue.Concurrency)
	}
	if cfg.Queue.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", cfg.Queue.MaxRetries)
	}
	if cfg.Queue.Memory.BufferSize != 1024 {
		t.Errorf("expected BufferSize 1024, got %d", cfg.Queue.Memory.BufferSize)
	}

	defaultAcc, err := cfg.SMTP.Default()
	if err != nil {
		t.Fatalf("expected default account, got error: %v", err)
	}
	if defaultAcc.Encryption != SmtpEncryptionAuto {
		t.Errorf("expected Encryption 'Auto', got %q", defaultAcc.Encryption)
	}
	if defaultAcc.Pool.MaxIdleConns != 5 {
		t.Errorf("expected MaxIdleConns 5, got %d", defaultAcc.Pool.MaxIdleConns)
	}
	if defaultAcc.Pool.MaxOpenConns != 20 {
		t.Errorf("expected MaxOpenConns 20, got %d", defaultAcc.Pool.MaxOpenConns)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("MAILBABY_APP_NAME", "custom-mailbaby")
	t.Setenv("MAILBABY_QUEUE_DRIVER", "memory")
	t.Setenv("MAILBABY_QUEUE_CONCURRENCY", "42")
	t.Setenv("MAILBABY_SMTP_DEFAULT_HOST", "smtp.custom.org")
	t.Setenv("MAILBABY_SMTP_DEFAULT_PORT", "465")
	t.Setenv("MAILBABY_SMTP_DEFAULT_FROM", "test@custom.org")

	cfg, err := LoadFromBytes([]byte(""), "yaml")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.App.Name != "custom-mailbaby" {
		t.Errorf("expected App.Name 'custom-mailbaby', got %q", cfg.App.Name)
	}
	if cfg.Queue.Concurrency != 42 {
		t.Errorf("expected Concurrency 42, got %d", cfg.Queue.Concurrency)
	}

	defaultAcc, err := cfg.SMTP.Default()
	if err != nil {
		t.Fatalf("expected default account, got error: %v", err)
	}
	if defaultAcc.Host != "smtp.custom.org" {
		t.Errorf("expected Host 'smtp.custom.org', got %q", defaultAcc.Host)
	}
	if defaultAcc.Port != 465 {
		t.Errorf("expected Port 465, got %d", defaultAcc.Port)
	}
}

func TestSmtpAccountValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  SmtpAccountConfig
		wantErr bool
	}{
		{
			name: "valid full smtp config",
			config: SmtpAccountConfig{
				Host:       "smtp.mailgun.org",
				Port:       587,
				Username:   "postmaster@mailgun.org",
				Password:   "secret",
				From:       "test@example.com",
				FromName:   "Mailer",
				ReplyTo:    "support@example.com",
				Encryption: SmtpEncryptionSTARTTLS,
				AuthType:   SmtpAuthTypePlain,
			},
			wantErr: false,
		},
		{
			name: "missing host",
			config: SmtpAccountConfig{
				Host: "",
				Port: 587,
				From: "test@example.com",
			},
			wantErr: true,
		},
		{
			name: "invalid port 0",
			config: SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 0,
				From: "test@example.com",
			},
			wantErr: true,
		},
		{
			name: "invalid port 70000",
			config: SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 70000,
				From: "test@example.com",
			},
			wantErr: true,
		},
		{
			name: "missing from",
			config: SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 587,
				From: "",
			},
			wantErr: true,
		},
		{
			name: "invalid from address",
			config: SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 587,
				From: "not-an-email",
			},
			wantErr: true,
		},
		{
			name: "invalid reply_to address",
			config: SmtpAccountConfig{
				Host:    "smtp.example.com",
				Port:    587,
				From:    "valid@example.com",
				ReplyTo: "invalid-reply",
			},
			wantErr: true,
		},
		{
			name: "invalid encryption type",
			config: SmtpAccountConfig{
				Host:       "smtp.example.com",
				Port:       587,
				From:       "valid@example.com",
				Encryption: "INVALID_CRYPTO",
			},
			wantErr: true,
		},
		{
			name: "invalid auth type",
			config: SmtpAccountConfig{
				Host:     "smtp.example.com",
				Port:     587,
				From:     "valid@example.com",
				AuthType: "INVALID_AUTH",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate("test")
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSmtpMultiAccountValidation(t *testing.T) {
	t.Run("missing default account", func(t *testing.T) {
		cfg := SmtpConfig{
			"marketing": SmtpAccountConfig{
				Host: "smtp.marketing.com",
				Port: 587,
				From: "marketing@example.com",
			},
		}
		err := cfg.Validate()
		if !errors.Is(err, ErrDefaultAccountRequired) {
			t.Errorf("expected ErrDefaultAccountRequired, got %v", err)
		}
	})

	t.Run("empty config", func(t *testing.T) {
		var cfg SmtpConfig
		err := cfg.Validate()
		if !errors.Is(err, ErrDefaultAccountRequired) {
			t.Errorf("expected ErrDefaultAccountRequired, got %v", err)
		}
	})

	t.Run("valid multi-accounts", func(t *testing.T) {
		cfg := SmtpConfig{
			"default": SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 587,
				From: "default@example.com",
			},
			"marketing": SmtpAccountConfig{
				Host: "smtp.mailgun.org",
				Port: 587,
				From: "marketing@example.com",
			},
			"alert": SmtpAccountConfig{
				Host: "smtp.sendgrid.net",
				Port: 465,
				From: "alert@example.com",
			},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid config, got error: %v", err)
		}

		if len(cfg.AccountNames()) != 3 {
			t.Errorf("expected 3 accounts, got %d", len(cfg.AccountNames()))
		}
	})

	t.Run("invalid secondary account", func(t *testing.T) {
		cfg := SmtpConfig{
			"default": SmtpAccountConfig{
				Host: "smtp.example.com",
				Port: 587,
				From: "default@example.com",
			},
			"invalid_acc": SmtpAccountConfig{
				Host: "", // missing host
				Port: 587,
				From: "invalid@example.com",
			},
		}
		err := cfg.Validate()
		if err == nil {
			t.Error("expected error for invalid secondary account, got nil")
		}
	})
}

func TestSmtpGetAccount(t *testing.T) {
	cfg := SmtpConfig{
		"default": SmtpAccountConfig{
			Host: "smtp.default.com",
			Port: 587,
			From: "default@example.com",
		},
		"marketing": SmtpAccountConfig{
			Host: "smtp.marketing.com",
			Port: 465,
			From: "marketing@example.com",
		},
	}

	// 1. Get default account explicitly
	acc, err := cfg.GetAccount("default")
	if err != nil || acc.Host != "smtp.default.com" {
		t.Fatalf("failed to get default account: %v", err)
	}

	// 2. Get default account with empty string
	accEmpty, err := cfg.GetAccount("")
	if err != nil || accEmpty.Host != "smtp.default.com" {
		t.Fatalf("failed to get default account with empty name: %v", err)
	}

	// 3. Get secondary account
	accMkt, err := cfg.GetAccount("marketing")
	if err != nil || accMkt.Host != "smtp.marketing.com" {
		t.Fatalf("failed to get marketing account: %v", err)
	}

	// 4. Case-insensitive lookup
	accCase, err := cfg.GetAccount("MARKETING")
	if err != nil || accCase.Host != "smtp.marketing.com" {
		t.Fatalf("failed case-insensitive lookup: %v", err)
	}

	// 5. Non-existent account
	_, err = cfg.GetAccount("nonexistent")
	if !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("expected ErrAccountNotFound, got %v", err)
	}

	// 6. HasAccount
	if !cfg.HasAccount("default") || !cfg.HasAccount("marketing") {
		t.Error("expected HasAccount to return true for existing accounts")
	}
	if cfg.HasAccount("unknown") {
		t.Error("expected HasAccount to return false for unknown account")
	}

	// 7. MustGetAccount
	mustAcc := cfg.MustGetAccount("marketing")
	if mustAcc.Host != "smtp.marketing.com" {
		t.Errorf("expected host 'smtp.marketing.com', got %q", mustAcc.Host)
	}
}

func TestRabbitMQValidation(t *testing.T) {
	valid := RabbitMQConfig{
		Host:  "127.0.0.1",
		Port:  5672,
		Queue: "mail_queue",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	urlValid := RabbitMQConfig{
		URL:   "amqp://guest:guest@127.0.0.1:5672/",
		Queue: "mail_queue",
	}
	if err := urlValid.Validate(); err != nil {
		t.Errorf("expected valid with URL, got %v", err)
	}

	missingHostAndURL := RabbitMQConfig{
		Queue: "mail_queue",
	}
	if err := missingHostAndURL.Validate(); err == nil {
		t.Error("expected error for missing host and url, got nil")
	}

	missingQueue := RabbitMQConfig{
		Host: "127.0.0.1",
		Port: 5672,
	}
	if err := missingQueue.Validate(); err == nil {
		t.Error("expected error for missing queue, got nil")
	}
}

func TestKafkaValidation(t *testing.T) {
	valid := KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "mail_topic",
		GroupID: "mail_group",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	emptyBrokers := KafkaConfig{
		Brokers: []string{},
		Topic:   "mail_topic",
		GroupID: "mail_group",
	}
	if err := emptyBrokers.Validate(); err == nil {
		t.Error("expected error for empty brokers, got nil")
	}

	missingTopic := KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "mail_group",
	}
	if err := missingTopic.Validate(); err == nil {
		t.Error("expected error for missing topic, got nil")
	}

	missingGroup := KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "mail_topic",
	}
	if err := missingGroup.Validate(); err == nil {
		t.Error("expected error for missing group, got nil")
	}
}

func TestRedisValidation(t *testing.T) {
	validStream := RedisConfig{
		Addrs: []string{"127.0.0.1:6379"},
		Key:   "mail_stream",
		Mode:  "stream",
		Group: "mail_group",
	}
	if err := validStream.Validate(); err != nil {
		t.Errorf("expected valid stream, got %v", err)
	}

	validList := RedisConfig{
		Host: "127.0.0.1",
		Port: 6379,
		Key:  "mail_list",
		Mode: "list",
	}
	if err := validList.Validate(); err != nil {
		t.Errorf("expected valid list, got %v", err)
	}

	missingGroupInStream := RedisConfig{
		Addrs: []string{"127.0.0.1:6379"},
		Key:   "mail_stream",
		Mode:  "stream",
	}
	if err := missingGroupInStream.Validate(); err == nil {
		t.Error("expected error for missing group in stream mode, got nil")
	}

	invalidMode := RedisConfig{
		Addrs: []string{"127.0.0.1:6379"},
		Key:   "mail_stream",
		Mode:  "unsupported_mode",
	}
	if err := invalidMode.Validate(); err == nil {
		t.Error("expected error for invalid mode, got nil")
	}
}

func TestRocketMQValidation(t *testing.T) {
	valid := RocketMQConfig{
		NameServers: []string{"127.0.0.1:9876"},
		Topic:       "mail_topic",
		Group:       "mail_group",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	emptyNameServers := RocketMQConfig{
		Topic: "mail_topic",
		Group: "mail_group",
	}
	if err := emptyNameServers.Validate(); err == nil {
		t.Error("expected error for empty name_servers, got nil")
	}
}

func TestNATSValidation(t *testing.T) {
	valid := NATSConfig{
		Servers: []string{"nats://127.0.0.1:4222"},
		Subject: "mail.send",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("exptected valid, got %v", err)
	}

	emptyServers := NATSConfig{
		Subject: "mail.send",
	}
	if err := emptyServers.Validate(); err == nil {
		t.Error("expected error for empty servers, got nil")
	}
}

func TestPulsarValidation(t *testing.T) {
	valid := PulsarConfig{
		URL:              "pulsar://127.0.0.1:6650",
		Topic:            "persistent://public/default/mail",
		SubscriptionName: "mail_sub",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	missingTopic := PulsarConfig{
		URL:              "pulsar://127.0.0.1:6650",
		SubscriptionName: "mail_sub",
	}
	if err := missingTopic.Validate(); err == nil {
		t.Error("expected error for missing topic, got nil")
	}
}

func TestSQSValidation(t *testing.T) {
	valid := SQSConfig{
		Region:   "us-east-1",
		QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/mail_queue",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	missingURL := SQSConfig{
		Region: "us-east-1",
	}
	if err := missingURL.Validate(); err == nil {
		t.Error("expected error for missing queue_url, got nil")
	}
}

func TestMemoryValidation(t *testing.T) {
	valid := MemoryConfig{BufferSize: 512}
	if err := valid.Validate(); err != nil {
		t.Errorf("expected valid, got %v", err)
	}

	invalid := MemoryConfig{BufferSize: -1}
	if err := invalid.Validate(); err == nil {
		t.Error("expected error for negative buffer size, got nil")
	}
}

func TestQueueConfigValidation(t *testing.T) {
	q := QueueConfig{
		Driver:      QueueDriver("invalid_driver"),
		Concurrency: 10,
	}
	if err := q.Validate(); err == nil {
		t.Error("expected error for invalid driver, got nil")
	}

	qInvalidConcurrency := QueueConfig{
		Driver:      DriverMemory,
		Concurrency: 0,
	}
	if err := qInvalidConcurrency.Validate(); err == nil {
		t.Error("expected error for concurrency <= 0, got nil")
	}
}

func TestAppAndLogValidation(t *testing.T) {
	app := AppConfig{Name: ""}
	if err := app.Validate(); err == nil {
		t.Error("expected error for empty app name, got nil")
	}

	logInvalidLevel := LogConfig{Level: "super_verbose"}
	if err := logInvalidLevel.Validate(); err == nil {
		t.Error("expected error for invalid log level, got nil")
	}

	logFileMissingPath := LogConfig{Output: "file", FilePath: ""}
	if err := logFileMissingPath.Validate(); err == nil {
		t.Error("expected error for file output with missing path, got nil")
	}
}

func TestLoadFromFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "mailbaby-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp config: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := `
app:
  name: "mailbaby-test-file"
  env: "production"
  debug: false
queue:
  driver: "memory"
  concurrency: 8
smtp:
  default:
    host: "smtp.example.org"
    port: 465
    from: "admin@example.org"
    encryption: "SSL"
  marketing:
    host: "smtp.mailgun.org"
    port: 587
    from: "marketing@example.org"
log:
  level: "warn"
`
	if _, err := tempFile.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tempFile.Close()

	cfg, err := Load(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to load from file: %v", err)
	}

	if cfg.App.Name != "mailbaby-test-file" {
		t.Errorf("expected app.name 'mailbaby-test-file', got %q", cfg.App.Name)
	}
	if cfg.App.Env != "production" {
		t.Errorf("expected app.env 'production', got %q", cfg.App.Env)
	}
	if cfg.Queue.Concurrency != 8 {
		t.Errorf("expected queue.concurrency 8, got %d", cfg.Queue.Concurrency)
	}

	defaultAcc, err := cfg.SMTP.Default()
	if err != nil {
		t.Fatalf("expected default account, got %v", err)
	}
	if defaultAcc.Encryption != SmtpEncryptionSSL {
		t.Errorf("expected smtp.default.encryption 'SSL', got %q", defaultAcc.Encryption)
	}

	mktAcc, err := cfg.SMTP.GetAccount("marketing")
	if err != nil {
		t.Fatalf("expected marketing account, got %v", err)
	}
	if mktAcc.Host != "smtp.mailgun.org" {
		t.Errorf("expected smtp.marketing.host 'smtp.mailgun.org', got %q", mktAcc.Host)
	}

	if cfg.Log.Level != "warn" {
		t.Errorf("expected log.level 'warn', got %q", cfg.Log.Level)
	}

	global := Get()
	if global == nil || global.App.Name != "mailbaby-test-file" {
		t.Errorf("expected global config to be set correctly")
	}
}
