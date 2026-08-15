package config

import (
	"errors"
	"fmt"
	"net/mail"
	"sort"
	"strings"
	"time"
)

const (
	// DefaultAccountName is the required default SMTP account identifier.
	DefaultAccountName = "default"
)

var (
	// ErrDefaultAccountRequired indicates the required 'default' SMTP account is missing.
	ErrDefaultAccountRequired = errors.New("smtp: 'default' account is required")

	// ErrAccountNotFound indicates the requested SMTP account was not found.
	ErrAccountNotFound = errors.New("smtp: account not found")
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

// SmtpAccountConfig defines the configuration for a single SMTP server account.
type SmtpAccountConfig struct {
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

// ApplyDefaults applies default settings for an individual SMTP account.
func (a *SmtpAccountConfig) ApplyDefaults() {
	if a.Port <= 0 {
		a.Port = 587
	}
	if a.Encryption == "" {
		a.Encryption = SmtpEncryptionAuto
	}
	if a.AuthType == "" {
		a.AuthType = SmtpAuthTypeAuto
	}
	if a.ConnectTimeout <= 0 {
		a.ConnectTimeout = 10 * time.Second
	}
	if a.SendTimeout <= 0 {
		a.SendTimeout = 30 * time.Second
	}
	if a.KeepAlive <= 0 {
		a.KeepAlive = 30 * time.Second
	}
	if a.Pool.MaxIdleConns <= 0 {
		a.Pool.MaxIdleConns = 5
	}
	if a.Pool.MaxOpenConns <= 0 {
		a.Pool.MaxOpenConns = 20
	}
	if a.Pool.IdleTimeout <= 0 {
		a.Pool.IdleTimeout = 60 * time.Second
	}
	if a.RateLimit.MaxRecipientsPerEmail <= 0 {
		a.RateLimit.MaxRecipientsPerEmail = 50
	}
}

// Validate validates the configuration of a single SMTP account.
func (a *SmtpAccountConfig) Validate(accountName ...string) error {
	prefix := "smtp"
	if len(accountName) > 0 && accountName[0] != "" {
		prefix = fmt.Sprintf("smtp account %q", accountName[0])
	}

	if strings.TrimSpace(a.Host) == "" {
		return fmt.Errorf("%s: host is required", prefix)
	}
	if a.Port <= 0 || a.Port > 65535 {
		return fmt.Errorf("%s: invalid port %d (must be between 1 and 65535)", prefix, a.Port)
	}
	if strings.TrimSpace(a.From) == "" {
		return fmt.Errorf("%s: from address is required", prefix)
	}
	if _, err := mail.ParseAddress(a.From); err != nil {
		return fmt.Errorf("%s: invalid from email address %q: %w", prefix, a.From, err)
	}
	if a.ReplyTo != "" {
		if _, err := mail.ParseAddress(a.ReplyTo); err != nil {
			return fmt.Errorf("%s: invalid reply_to email address %q: %w", prefix, a.ReplyTo, err)
		}
	}

	switch strings.ToUpper(string(a.Encryption)) {
	case "", "AUTO", "SSL", "TLS", "STARTTLS", "NONE":
	default:
		return fmt.Errorf("%s: unsupported encryption type %q", prefix, a.Encryption)
	}

	switch strings.ToUpper(string(a.AuthType)) {
	case "", "AUTO", "PLAIN", "LOGIN", "CRAM-MD5", "NONE":
	default:
		return fmt.Errorf("%s: unsupported auth_type %q", prefix, a.AuthType)
	}

	return nil
}

// SmtpConfig represents a collection of named SMTP accounts.
// The "default" account is mandatory.
type SmtpConfig map[string]SmtpAccountConfig

// ApplyDefaults applies default settings across all declared SMTP accounts.
func (c SmtpConfig) ApplyDefaults() {
	for name, acc := range c {
		acc.ApplyDefaults()
		c[name] = acc
	}
}

// Validate validates the entire multi-account SMTP configuration.
// It ensures that at least the 'default' account is declared and that each account is valid.
func (c SmtpConfig) Validate() error {
	if len(c) == 0 {
		return ErrDefaultAccountRequired
	}

	// Verify that 'default' account is present (case-insensitive check)
	defaultFound := false
	for name := range c {
		if strings.EqualFold(name, DefaultAccountName) {
			defaultFound = true
			break
		}
	}

	if !defaultFound {
		return ErrDefaultAccountRequired
	}

	c.ApplyDefaults()

	for name, acc := range c {
		if err := acc.Validate(name); err != nil {
			return err
		}
	}

	return nil
}

// GetAccount retrieves an SMTP account configuration by its identifier.
// If name is empty or "default", the default account is returned.
func (c SmtpConfig) GetAccount(name string) (SmtpAccountConfig, error) {
	if len(c) == 0 {
		return SmtpAccountConfig{}, ErrDefaultAccountRequired
	}

	trimmed := strings.TrimSpace(name)
	if trimmed == "" || strings.EqualFold(trimmed, DefaultAccountName) {
		for k, v := range c {
			if strings.EqualFold(k, DefaultAccountName) {
				return v, nil
			}
		}
		return SmtpAccountConfig{}, ErrDefaultAccountRequired
	}

	// Direct lookup
	if acc, ok := c[trimmed]; ok {
		return acc, nil
	}

	// Case-insensitive lookup fallback
	for k, v := range c {
		if strings.EqualFold(k, trimmed) {
			return v, nil
		}
	}

	return SmtpAccountConfig{}, fmt.Errorf("%w: %s", ErrAccountNotFound, trimmed)
}

// MustGetAccount retrieves an SMTP account by name or panics if not found.
func (c SmtpConfig) MustGetAccount(name string) SmtpAccountConfig {
	acc, err := c.GetAccount(name)
	if err != nil {
		panic(fmt.Sprintf("failed to get smtp account %q: %v", name, err))
	}
	return acc
}

// Default returns the mandatory default SMTP account configuration.
func (c SmtpConfig) Default() (SmtpAccountConfig, error) {
	return c.GetAccount(DefaultAccountName)
}

// HasAccount checks whether an SMTP account with the given name exists.
func (c SmtpConfig) HasAccount(name string) bool {
	_, err := c.GetAccount(name)
	return err == nil
}

// AccountNames returns a sorted slice of all configured account names.
func (c SmtpConfig) AccountNames() []string {
	names := make([]string, 0, len(c))
	for name := range c {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
