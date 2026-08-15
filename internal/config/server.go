package config

import (
	"fmt"
	"time"
)

// ServerConfig defines the HTTP server configuration that serves metrics, health probes, and pprof.
type ServerConfig struct {
	Host         string        `mapstructure:"host" json:"host" yaml:"host"`
	Port         int           `mapstructure:"port" json:"port" yaml:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout" json:"idle_timeout" yaml:"idle_timeout"`
}

// ApplyDefaults applies safe default parameters for the HTTP server.
func (c *ServerConfig) ApplyDefaults() {
	if c.Host == "" {
		c.Host = "0.0.0.0"
	}
	if c.Port <= 0 {
		c.Port = 8080
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 10 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
}

// Validate validates the ServerConfig parameters.
func (c *ServerConfig) Validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("server: invalid port %d (must be between 1 and 65535)", c.Port)
	}
	return nil
}

// Address returns the combined host:port string for network listening.
func (c *ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
