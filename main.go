package main

import (
	"fmt"
	"log"
	"os"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	_ "mailbaby/internal/queue/driver/all"
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
	fmt.Printf("[INFO] Queue Driver       : %s (concurrency=%d, retries=%d)\n", cfg.Queue.Driver, cfg.Queue.Concurrency, cfg.Queue.MaxRetries)
	fmt.Printf("[INFO] Registered Drivers : %v\n", queue.GetRegisteredDrivers())
	fmt.Printf("[INFO] SMTP Server        : %s:%d (from=%s, encryption=%s)\n", cfg.SMTP.Host, cfg.SMTP.Port, cfg.SMTP.From, cfg.SMTP.Encryption)
	fmt.Printf("[INFO] Log Level          : %s (format=%s, output=%s)\n", cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)

	q, err := queue.New(cfg)
	if err != nil {
		log.Printf("[WARN] Failed to initialize queue driver %q: %v\n", cfg.Queue.Driver, err)
	} else {
		defer q.Close()
		fmt.Printf("[INFO] Queue Initialized  : driver=%s, name=%s\n", q.Driver(), q.Name())
	}

	fmt.Println("==================================================")
	fmt.Println("Configuration and Queue loaded successfully. Ready to start workers.")
}

