package config

import (
	"testing"
)

func TestServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ServerConfig
		wantErr bool
	}{
		{
			name: "valid port is ok",
			cfg: ServerConfig{
				Host: "0.0.0.0",
				Port: 8080,
			},
			wantErr: false,
		},
		{
			name: "port too low fails",
			cfg: ServerConfig{
				Port: 0,
			},
			wantErr: true,
		},
		{
			name: "port too high fails",
			cfg: ServerConfig{
				Port: 70000,
			},
			wantErr: true,
		},
		{
			name: "tls enabled without cert path fails",
			cfg: ServerConfig{
				Port:        8443,
				TLSEnabled:  true,
				TLSKeyPath:  "/tmp/key.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled without key path fails",
			cfg: ServerConfig{
				Port:        8443,
				TLSEnabled:  true,
				TLSCertPath: "/tmp/cert.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled with identical cert and key paths fails",
			cfg: ServerConfig{
				Port:        8443,
				TLSEnabled:  true,
				TLSCertPath: "/tmp/same.pem",
				TLSKeyPath:  "/tmp/same.pem",
			},
			wantErr: true,
		},
		{
			name: "tls enabled with both paths is ok",
			cfg: ServerConfig{
				Port:        8443,
				TLSEnabled:  true,
				TLSCertPath: "/tmp/cert.pem",
				TLSKeyPath:  "/tmp/key.pem",
			},
			wantErr: false,
		},
		{
			name: "tls disabled without cert/key is ok",
			cfg: ServerConfig{
				Port: 8080,
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
