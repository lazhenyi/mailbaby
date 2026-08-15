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
	}
	return nil
}

// Address returns the combined host:port string for network listening.
func (c *GrpcConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
