package config

import (
	"errors"
	"strings"
)

// AuthConfig defines the security and authentication configuration for HTTP/RPC requests.
type AuthConfig struct {
	Enabled    bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	SecretKey  string `mapstructure:"secret_key" json:"secret_key" yaml:"secret_key"`
	HeaderName string `mapstructure:"header_name" json:"header_name" yaml:"header_name"`

	// RatePerKeyPerMinute caps requests per authenticated key over a sliding
	// 60-second window. 0 disables per-key rate limiting. The default is 600.
	RatePerKeyPerMinute int `mapstructure:"rate_per_key_per_minute" json:"rate_per_key_per_minute" yaml:"rate_per_key_per_minute"`
}

// ApplyDefaults applies default settings for AuthConfig.
func (c *AuthConfig) ApplyDefaults() {
	if c.HeaderName == "" {
		c.HeaderName = "X-API-Key"
	}
}

// Validate validates the AuthConfig parameters.
func (c *AuthConfig) Validate() error {
	if c.Enabled {
		if strings.TrimSpace(c.SecretKey) == "" {
			return errors.New("auth: secret_key cannot be empty when authentication is enabled")
		}
	}
	return nil
}
