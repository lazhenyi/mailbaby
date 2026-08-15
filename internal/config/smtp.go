package config

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
)

type SmtpEncryption string

const (
	SmtpEncryptionAuto     SmtpEncryption = "Auto"
	SmtpEncryptionSSL      SmtpEncryption = "SSL"
	SmtpEncryptionTLS      SmtpEncryption = "TLS"
	SmtpEncryptionSTARTTLS SmtpEncryption = "STARTTLS"
	SmtpEncryptionNone     SmtpEncryption = "None"
)

type SmtpAuthType string

const (
	SmtpAuthTypeAuto    SmtpAuthType = "Auto"
	SmtpAuthTypePlain   SmtpAuthType = "PLAIN"
	SmtpAuthTypeLogin   SmtpAuthType = "LOGIN"
	SmtpAuthTypeCramMD5 SmtpAuthType = "CRAM-MD5"
	SmtpAuthTypeNone    SmtpAuthType = "None"
)

type SmtpPoolConfig struct {
	MaxIdleConns int           `mapstructure:"max_idle_conns" json:"max_idle_conns" yaml:"max_idle_conns"`
	MaxOpenConns int           `mapstructure:"max_open_conns" json:"max_open_conns" yaml:"max_open_conns"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout" json:"idle_timeout" yaml:"idle_timeout"`
}

type SmtpRateLimitConfig struct {
	EmailsPerSecond       int `mapstructure:"emails_per_second" json:"emails_per_second" yaml:"emails_per_second"`
	MaxRecipientsPerEmail int `mapstructure:"max_recipients_per_email" json:"max_recipients_per_email" yaml:"max_recipients_per_email"`
}

type SmtpConfig struct {
	Host     string `mapstructure:"host" json:"host" yaml:"host"`
	Port     int    `mapstructure:"port" json:"port" yaml:"port"`
	Username string `mapstructure:"username" json:"username" yaml:"username"`
	Password string `mapstructure:"password" json:"password" yaml:"password"`

	From     string `mapstructure:"from" json:"from" yaml:"from"`
	FromName string `mapstructure:"from_name" json:"from_name" yaml:"from_name"`
	ReplyTo  string `mapstructure:"reply_to" json:"reply_to" yaml:"reply_to"`

	Encryption         SmtpEncryption `mapstructure:"encryption" json:"encryption" yaml:"encryption"`
	InsecureSkipVerify bool           `mapstructure:"insecure_skip_verify" json:"insecure_skip_verify" yaml:"insecure_skip_verify"`
	HeloHostname       string         `mapstructure:"helo_hostname" json:"helo_hostname" yaml:"helo_hostname"`

	AuthType SmtpAuthType `mapstructure:"auth_type" json:"auth_type" yaml:"auth_type"`

	ConnectTimeout time.Duration `mapstructure:"connect_timeout" json:"connect_timeout" yaml:"connect_timeout"`
	SendTimeout    time.Duration `mapstructure:"send_timeout" json:"send_timeout" yaml:"send_timeout"`
	KeepAlive      time.Duration `mapstructure:"keep_alive" json:"keep_alive" yaml:"keep_alive"`

	Pool SmtpPoolConfig `mapstructure:"pool" json:"pool" yaml:"pool"`

	RateLimit SmtpRateLimitConfig `mapstructure:"rate_limit" json:"rate_limit" yaml:"rate_limit"`
}

func (c *SmtpConfig) Validate() error {
	if strings.TrimSpace(c.Host) == "" {
		return errors.New("smtp: host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("smtp: invalid port %d (must be between 1 and 65535)", c.Port)
	}
	if strings.TrimSpace(c.From) == "" {
		return errors.New("smtp: from address is required")
	}
	if _, err := mail.ParseAddress(c.From); err != nil {
		return fmt.Errorf("smtp: invalid from email address %q: %w", c.From, err)
	}
	if c.ReplyTo != "" {
		if _, err := mail.ParseAddress(c.ReplyTo); err != nil {
			return fmt.Errorf("smtp: invalid reply_to email address %q: %w", c.ReplyTo, err)
		}
	}

	switch strings.ToUpper(string(c.Encryption)) {
	case "", "AUTO", "SSL", "TLS", "STARTTLS", "NONE":
	default:
		return fmt.Errorf("smtp: unsupported encryption type %q", c.Encryption)
	}

	switch strings.ToUpper(string(c.AuthType)) {
	case "", "AUTO", "PLAIN", "LOGIN", "CRAM-MD5", "NONE":
	default:
		return fmt.Errorf("smtp: unsupported auth_type %q", c.AuthType)
	}

	return nil
}
