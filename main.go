package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"mailbaby/internal/config"
	"mailbaby/internal/handler"
	"mailbaby/internal/metrics"
	"mailbaby/internal/queue"
	_ "mailbaby/internal/queue/driver/all"
	"mailbaby/internal/sender"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println(" MailBaby - Message Queue Email Sending Service")
	fmt.Println("==================================================")

	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("[WARN] Failed to load %q (using defaults/env): %v\n", configPath, err)
		cfg, err = config.Load("")
		if err != nil {
			log.Fatalf("[FATAL] Could not initialize configuration: %v\n", err)
		}
	}

	fmt.Printf("[INFO] Service Name       : %s\n", cfg.App.Name)
	fmt.Printf("[INFO] Environment        : %s (debug=%v)\n", cfg.App.Env, cfg.App.Debug)
	fmt.Printf("[INFO] HTTP Server        : %s (metrics=%v, health=%v, pprof=%v)\n",
		cfg.Server.Address(), cfg.Metrics.Enabled, cfg.Observability.Health.Enabled, cfg.Observability.Pprof.Enabled)
	fmt.Printf("[INFO] Queue Driver       : %s (concurrency=%d, retries=%d)\n", cfg.Queue.Driver, cfg.Queue.Concurrency, cfg.Queue.MaxRetries)
	fmt.Printf("[INFO] Registered Drivers : %v\n", queue.GetRegisteredDrivers())
	if defaultAcc, err := cfg.SMTP.Default(); err == nil {
		fmt.Printf("[INFO] SMTP Accounts      : %d configured (default=%s:%d, from=%s)\n", len(cfg.SMTP), defaultAcc.Host, defaultAcc.Port, defaultAcc.From)
	} else {
		fmt.Printf("[INFO] SMTP Accounts      : %d configured\n", len(cfg.SMTP))
	}
	fmt.Printf("[INFO] Log Level          : %s (format=%s, output=%s, async=%v)\n", cfg.Log.Level, cfg.Log.Format, cfg.Log.Output, cfg.Log.Async)
	if cfg.Metrics.Enabled {
		fmt.Printf("[INFO] Metrics Exporter   : %s://%s%s (runtime=%v, queue=%v, smtp=%v)\n", cfg.Metrics.Provider, cfg.Server.Address(), cfg.Metrics.Path, cfg.Metrics.CollectRuntime, cfg.Metrics.CollectQueueStats, cfg.Metrics.CollectSmtpStats)
	} else {
		fmt.Printf("[INFO] Metrics Exporter   : disabled\n")
	}
	fmt.Printf("[INFO] Observability      : Tracing=%v (%s), Health=%v (live=%s, ready=%s), Pprof=%v (%s)\n",
		cfg.Observability.Tracing.Enabled, cfg.Observability.Tracing.Provider,
		cfg.Observability.Health.Enabled, cfg.Observability.Health.LivePath, cfg.Observability.Health.ReadyPath,
		cfg.Observability.Pprof.Enabled, cfg.Observability.Pprof.Path)

	// Initialize Queue subsystem
	q, err := queue.New(cfg)
	if err != nil {
		log.Printf("[WARN] Failed to initialize queue driver %q: %v\n", cfg.Queue.Driver, err)
	} else {
		defer q.Close()
		fmt.Printf("[INFO] Queue Initialized  : driver=%s, name=%s\n", q.Driver(), q.Name())
	}

	// Initialize Sender subsystem
	mailSender, err := sender.NewFromConfig(cfg)
	if err != nil {
		log.Printf("[WARN] Failed to initialize mail sender: %v\n", err)
	} else {
		defer mailSender.Close()
		fmt.Printf("[INFO] Sender Initialized : %d account(s) ready (%v)\n", len(mailSender.AccountNames()), mailSender.AccountNames())
	}

	// Initialize Metrics subsystem
	_, err = metrics.Init(cfg.Metrics)
	if err != nil {
		log.Printf("[WARN] Failed to initialize metrics: %v\n", err)
	} else {
		defer metrics.Close()
	}

	// Initialize Unified HTTP Server (Metrics, Health, Pprof)
	httpServer := handler.New(cfg)

	// Register readiness health probes
	httpServer.RegisterChecker("queue", func(ctx context.Context) error {
		if q == nil {
			return errors.New("queue driver not ready")
		}
		return nil
	})
	httpServer.RegisterChecker("smtp", func(ctx context.Context) error {
		if mailSender == nil {
			return errors.New("smtp sender not ready")
		}
		return nil
	})

	if err := httpServer.Start(context.Background()); err != nil {
		log.Printf("[WARN] Failed to start unified HTTP server: %v\n", err)
	} else {
		defer httpServer.Stop(context.Background())
		fmt.Printf("[INFO] HTTP Server Ready  : listening on http://%s\n", cfg.Server.Address())
	}

	fmt.Println("==================================================")
	fmt.Println("Configuration, Queue, Sender, Metrics, and Unified HTTP Server loaded successfully. Ready to start workers.")
}
