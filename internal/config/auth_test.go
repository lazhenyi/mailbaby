package config

import (
	"testing"
)

func TestAuthConfig_ApplyDefaults(t *testing.T) {
	var cfg AuthConfig
	cfg.ApplyDefaults()

	if cfg.HeaderName != "X-API-Key" {
		t.Errorf("expected HeaderName to be 'X-API-Key', got %q", cfg.HeaderName)
	}
}

func TestAuthConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     AuthConfig
		wantErr bool
	}{
		{
			name: "disabled without secret key is valid",
			cfg: AuthConfig{
				Enabled:   false,
				SecretKey: "",
			},
			wantErr: false,
		},
		{
			name: "enabled with valid secret key is valid",
			cfg: AuthConfig{
				Enabled:   true,
				SecretKey: "supersecretkey123",
			},
			wantErr: false,
		},
		{
			name: "enabled with empty secret key returns error",
			cfg: AuthConfig{
				Enabled:   true,
				SecretKey: "   ",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.ApplyDefaults()
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
