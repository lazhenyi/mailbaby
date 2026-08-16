package config

import (
	"fmt"
	"time"
)

// GrpcConfig defines the gRPC server configuration for remote procedure calls.
type GrpcConfig struct {
	Enabled        bool          `mapstructure:"enabled" json:"enabled" yaml:"enabled"`
	Host           string        `mapstructure:"host" json:"host" yaml:"host"`
	Port           int           `mapstructure:"port" json:"port" yaml:"port"`
	MaxRecvMsgSize int           `mapstructure:"max_recv_msg_size" json:"max_recv_msg_size" yaml:"max_recv_msg_size"`
	MaxSendMsgSize int           `mapstructure:"max_send_msg_size" json:"max_send_msg_size" yaml:"max_send_msg_size"`
	Timeout        time.Duration `mapstructure:"timeout" json:"timeout" yaml:"timeout"`
	TLSEnabled     bool          `mapstructure:"tls_enabled" json:"tls_enabled" yaml:"tls_enabled"`
	TLSCertPath    string        `mapstructure:"tls_cert_path" json:"tls_cert_path" yaml:"tls_cert_path"`
	TLSKeyPath     string        `mapstructure:"tls_key_path" json:"tls_key_path" yaml:"tls_key_path"`
}

// ApplyDefaults applies safe default parameters for the gRPC server.
func (c *GrpcConfig) ApplyDefaults() {
	if c.Host == "" {
		c.Host = "0.0.0.0"
	}
	if c.Port <= 0 {
		c.Port = 8081
	}
	if c.MaxRecvMsgSize <= 0 {
		c.MaxRecvMsgSize = 16 * 1024 * 1024 // 16 MB default
	}
	if c.MaxSendMsgSize <= 0 {
		c.MaxSendMsgSize = 16 * 1024 * 1024 // 16 MB default
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
}

// Validate validates the GrpcConfig parameters.
func (c *GrpcConfig) Validate() error {
	if c.Enabled {
		if c.Port <= 0 || c.Port > 65535 {
			return fmt.Errorf("grpc: invalid port %d (must be between 1 and 65535)", c.Port)
		}
		if c.TLSEnabled {
			if c.TLSCertPath == "" || c.TLSKeyPath == "" {
				return fmt.Errorf("grpc: tls_enabled requires both tls_cert_path and tls_key_path")
			}
			if c.TLSCertPath == c.TLSKeyPath {
				return fmt.Errorf("grpc: tls_cert_path and tls_key_path must not be identical")
			}
		}
	}
	return nil
}

// Address returns the combined host:port string for network listening.
func (c *GrpcConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
