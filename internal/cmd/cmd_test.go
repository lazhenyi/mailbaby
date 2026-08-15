package cmd

import (
	"context"
	"testing"
	"time"

	"mailbaby/internal/config"
	_ "mailbaby/internal/queue/driver/all"
)

func TestVersionCommand(t *testing.T) {
	if err := ExecuteArgs([]string{"version"}); err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	if err := ExecuteArgs([]string{"-v"}); err != nil {
		t.Fatalf("-v flag failed: %v", err)
	}

	info := GetVersionInfo()
	if info.Version == "" {
		t.Error("expected non-empty Version")
	}
	if info.GoVersion == "" {
		t.Error("expected non-empty GoVersion")
	}
}

func TestHelpCommand(t *testing.T) {
	if err := ExecuteArgs([]string{"--help"}); err != nil {
		t.Fatalf("--help flag failed: %v", err)
	}
	if err := ExecuteArgs([]string{"-h"}); err != nil {
		t.Fatalf("-h flag failed: %v", err)
	}
}

func TestCheckCommand(t *testing.T) {
	cfg := &config.Config{
		App: config.AppConfig{
			Name: "mailbaby",
			Env:  "test",
		},
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 1,
		},
		SMTP: config.SmtpConfig{
			"default": config.SmtpAccountConfig{
				Host:     "smtp.example.com",
				Port:     587,
				Username: "user",
				Password: "password",
				From:     "noreply@example.com",
			},
		},
	}

	if err := runCheck(cfg); err != nil {
		t.Fatalf("runCheck failed: %v", err)
	}
}

func TestAppLifecycle(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "127.0.0.1",
			Port: 0, // dynamic
		},
		Queue: config.QueueConfig{
			Driver:      config.DriverMemory,
			Concurrency: 2,
		},
		SMTP: config.SmtpConfig{
			"default": config.SmtpAccountConfig{
				Host:     "smtp.example.com",
				Port:     587,
				Username: "user",
				Password: "pass",
				From:     "test@example.com",
			},
		},
	}

	app, err := NewApp(cfg)
	if err != nil {
		t.Fatalf("failed to create App: %v", err)
	}

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("failed to start App: %v", err)
	}

	if !app.Engine().IsRunning() {
		t.Error("expected engine to be running")
	}

	if app.Queue() == nil || app.Sender() == nil || app.Server() == nil {
		t.Error("expected app components to be initialized")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := app.Shutdown(stopCtx); err != nil {
		t.Fatalf("failed to shutdown App: %v", err)
	}

	if app.Engine().IsRunning() {
		t.Error("expected engine not running after shutdown")
	}
}

func TestUnknownCommand(t *testing.T) {
	err := ExecuteArgs([]string{"invalid_command_xyz"})
	if err == nil {
		t.Error("expected error for unknown command, got nil")
	}
}
