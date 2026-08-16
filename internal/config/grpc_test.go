package config

import (
	"testing"
	"time"
)

func TestGrpcConfig_ApplyDefaults(t *testing.T) {
	var cfg GrpcConfig
	cfg.ApplyDefaults()

	if cfg.Host != "0.0.0.0" {
		t.Errorf("expected Host '0.0.0.0', got %q", cfg.Host)
	}
	if cfg.Port != 8081 {
		t.Errorf("expected Port 8081, got %d", cfg.Port)
	}
	if cfg.MaxRecvMsgSize != 16*1024*1024 {
		t.Errorf("expected MaxRecvMsgSize 16MB, got %d", cfg.MaxRecvMsgSize)
	}
	if cfg.MaxSendMsgSize != 16*1024*1024 {
		t.Errorf("expected MaxSendMsgSize 16MB, got %d", cfg.MaxSendMsgSize)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("expected Timeout 30s, got %v", cfg.Timeout)
	}
	if cfg.Address() != "0.0.0.0:8081" {
		t.Errorf("expected Address '0.0.0.0:8081', got %q", cfg.Address())
	}
}

func TestGrpcConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     GrpcConfig
		wantErr bool
	}{
		{
			name: "disabled with invalid port is ok",
			cfg: GrpcConfig{
				Enabled: false,
				Port:    0,
			},
			wantErr: false,
		},
		{
			name: "enabled with valid port is ok",
			cfg: GrpcConfig{
				Enabled: true,
				Port:    8081,
			},
			wantErr: false,
		},
		{
			name: "enabled with invalid port fails",
			cfg: GrpcConfig{
				Enabled: true,
				Port:    70000,
			},
			wantErr: true,
		},
		{
			name: "tls enabled without cert path fails",
			cfg: GrpcConfig{
				Enabled:    true,
				Port:       8081,
				TLSEnabled: true,
				TLSKeyPath: "/nonexistent/key.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled without key path fails",
			cfg: GrpcConfig{
				Enabled:     true,
				Port:        8081,
				TLSEnabled:  true,
				TLSCertPath: "/nonexistent/cert.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled with identical cert and key paths fails",
			cfg: GrpcConfig{
				Enabled:     true,
				Port:        8081,
				TLSEnabled:  true,
				TLSCertPath: "/same.pem",
				TLSKeyPath:  "/same.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled with both paths is ok",
			cfg: GrpcConfig{
				Enabled:     true,
				Port:        8081,
				TLSEnabled:  true,
				TLSCertPath: "/etc/ssl/cert.pem",
				TLSKeyPath:  "/etc/ssl/key.pem",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
