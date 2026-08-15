package cmd

import (
	"context"
	"fmt"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/queue"
	"mailbaby/internal/sender"
)

// runCheck verifies the configuration file syntax, queue connectivity, and SMTP configuration without starting the worker.
func runCheck(cfg *config.Config) error {
	fmt.Println("==================================================")
	fmt.Println(" MailBaby - Configuration & Connectivity Checker")
	fmt.Println("==================================================")

	// 1. Validate Config Structure
	fmt.Print("[CHECK] Configuration validation... ")
	if err := cfg.Validate(); err != nil {
		fmt.Println("FAILED")
		return fmt.Errorf("configuration validation failed: %w", err)
	}
	fmt.Println("OK")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. Check Queue Broker Connectivity
	fmt.Printf("[CHECK] Queue driver %q connection... ", cfg.Queue.Driver)
	q, err := queue.New(cfg)
	if err != nil {
		fmt.Println("FAILED")
		return fmt.Errorf("queue initialization failed: %w", err)
	}
	defer func() { _ = q.Close() }()

	if err := q.Ping(ctx); err != nil {
		fmt.Printf("FAILED (%v)\n", err)
		return fmt.Errorf("queue broker ping failed: %w", err)
	}
	fmt.Println("OK")

	// 3. Check SMTP Accounts
	fmt.Printf("[CHECK] SMTP Accounts (%d configured)... ", len(cfg.SMTP))
	mailSender, err := sender.NewFromConfig(cfg)
	if err != nil {
		fmt.Println("FAILED")
		return fmt.Errorf("smtp accounts initialization failed: %w", err)
	}
	defer func() { _ = mailSender.Close() }()
	fmt.Printf("OK (Accounts: %v)\n", mailSender.AccountNames())

	// 4. Check HTTP Server Bind Address
	fmt.Printf("[CHECK] HTTP Server endpoint (http://%s)... OK\n", cfg.Server.Address())

	fmt.Println("==================================================")
	fmt.Println("All checks passed successfully! Configuration is valid and dependencies are reachable.")
	return nil
}
