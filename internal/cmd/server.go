package cmd

import (
	"context"
	"fmt"

	"mailbaby/internal/config"
	"mailbaby/internal/logger"
	"mailbaby/internal/queue"
)

// runServer executes the primary server command starting the daemon worker and HTTP server.
func runServer(cfg *config.Config) error {
	printBanner(cfg)

	app, err := NewApp(cfg)
	if err != nil {
		return fmt.Errorf("cmd: failed to initialize application: %w", err)
	}

	return app.Run(context.Background())
}

func printBanner(cfg *config.Config) {
	fmt.Println("==================================================")
	fmt.Println(" MailBaby - Message Queue Email Sending Service")
	fmt.Println("==================================================")
	fmt.Printf("[INFO] Service Name       : %s\n", cfg.App.Name)
	fmt.Printf("[INFO] Environment        : %s (debug=%v)\n", cfg.App.Env, cfg.App.Debug)
	fmt.Printf("[INFO] HTTP Server        : %s (metrics=%v, health=%v, pprof=%v)\n",
		cfg.Server.Address(), cfg.Metrics.Enabled, cfg.Observability.Health.Enabled, cfg.Observability.Pprof.Enabled)
	if cfg.GRPC.Enabled {
		fmt.Printf("[INFO] gRPC Server        : %s (enabled=true, max_recv=%dMB)\n",
			cfg.GRPC.Address(), cfg.GRPC.MaxRecvMsgSize/(1024*1024))
	} else {
		fmt.Printf("[INFO] gRPC Server        : disabled\n")
	}
	if cfg.Auth.Enabled {
		fmt.Printf("[INFO] Authentication     : enabled (header=%s, secret_configured=true)\n", cfg.Auth.HeaderName)
	} else {
		fmt.Printf("[INFO] Authentication     : disabled (open access)\n")
	}
	fmt.Printf("[INFO] Queue Driver       : %s (concurrency=%d, retries=%d)\n",
		cfg.Queue.Driver, cfg.Queue.Concurrency, cfg.Queue.MaxRetries)
	fmt.Printf("[INFO] Registered Drivers : %v\n", queue.GetRegisteredDrivers())

	if defaultAcc, err := cfg.SMTP.Default(); err == nil {
		fmt.Printf("[INFO] SMTP Accounts      : %d configured (default=%s:%d, from=%s)\n",
			len(cfg.SMTP), defaultAcc.Host, defaultAcc.Port, defaultAcc.From)
	} else {
		fmt.Printf("[INFO] SMTP Accounts      : %d configured\n", len(cfg.SMTP))
	}

	fmt.Printf("[INFO] Log Level          : %s (format=%s, output=%s, async=%v)\n",
		cfg.Log.Level, cfg.Log.Format, cfg.Log.Output, cfg.Log.Async)

	if cfg.Metrics.Enabled {
		fmt.Printf("[INFO] Metrics Exporter   : %s://%s%s (runtime=%v, queue=%v, smtp=%v)\n",
			cfg.Metrics.Provider, cfg.Server.Address(), cfg.Metrics.Path,
			cfg.Metrics.CollectRuntime, cfg.Metrics.CollectQueueStats, cfg.Metrics.CollectSmtpStats)
	} else {
		fmt.Printf("[INFO] Metrics Exporter   : disabled\n")
	}

	fmt.Printf("[INFO] Observability      : Tracing=%v (%s), Health=%v (live=%s, ready=%s), Pprof=%v (%s)\n",
		cfg.Observability.Tracing.Enabled, cfg.Observability.Tracing.Provider,
		cfg.Observability.Health.Enabled, cfg.Observability.Health.LivePath, cfg.Observability.Health.ReadyPath,
		cfg.Observability.Pprof.Enabled, cfg.Observability.Pprof.Path)
	fmt.Println("==================================================")
	logger.Get().Info("starting MailBaby application...")
}
