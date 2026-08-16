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

	// TLS configuration. If Enabled is true and CertPath/KeyPath are non-empty,
	// the HTTP server uses ListenAndServeTLS. InsecureSkipVerify controls the
	// underlying tls.Config for downstream clients (NOT for this server's cert).
	Enabled           bool   `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	TLSEnabled        bool   `mapstructure:"tls_enabled" json:"tls_enabled" yaml:"tls_enabled"`
	TLSCertPath       string `mapstructure:"tls_cert_path" json:"tls_cert_path" yaml:"tls_cert_path"`
	TLSKeyPath        string `mapstructure:"tls_key_path" json:"tls_key_path" yaml:"tls_key_path"`
	TLSMinVersion     string `mapstructure:"tls_min_version" json:"tls_min_version" yaml:"tls_min_version"`
	CORSAllowedOrigin string `mapstructure:"cors_allowed_origin" json:"cors_allowed_origin" yaml:"cors_allowed_origin"`
	// LegacyAPIV1 enables the deprecated /api/v1/* route aliases. Default false.
	LegacyAPIV1 bool `mapstructure:"legacy_api_v1" json:"legacy_api_v1" yaml:"legacy_api_v1"`

	// TrustProxyHeaders makes the server honor X-Forwarded-For / X-Real-IP
	// for client IP reporting. Only enable when the server runs behind a
	// trusted reverse proxy that strips/rewrites these headers from outside
	// traffic. Default false (RemoteAddr is used).
	TrustProxyHeaders bool `mapstructure:"trust_proxy_headers" json:"trust_proxy_headers" yaml:"trust_proxy_headers"`
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
	if c.TLSMinVersion == "" {
		c.TLSMinVersion = "1.2"
	}
	// Default CORS to empty (no cross-origin) instead of "*" for security.
	if c.CORSAllowedOrigin == "" {
		c.CORSAllowedOrigin = ""
	}
}

// Validate validates the ServerConfig parameters.
func (c *ServerConfig) Validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("server: invalid port %d (must be between 1 and 65535)", c.Port)
	}
	if c.TLSEnabled {
		if c.TLSCertPath == "" || c.TLSKeyPath == "" {
			return fmt.Errorf("server: tls_enabled requires both tls_cert_path and tls_key_path")
		}
		if c.TLSCertPath == c.TLSKeyPath {
			return fmt.Errorf("server: tls_cert_path and tls_key_path must not be identical")
		}
	}
	return nil
}

// Address returns the combined host:port string for network listening.
func (c *ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
