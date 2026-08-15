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
