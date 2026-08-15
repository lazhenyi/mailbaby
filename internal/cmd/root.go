package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"mailbaby/internal/config"
)

// Execute runs the root command with os.Args[1:].
func Execute() error {
	return ExecuteArgs(os.Args[1:])
}

// ExecuteArgs parses command line arguments and dispatches to the corresponding subcommand.
func ExecuteArgs(args []string) error {
	fs := flag.NewFlagSet("mailbaby", flag.ContinueOnError)

	var configPath string
	var showVersion bool
	var showHelp bool
	var debug bool
	var env string

	fs.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	fs.StringVar(&configPath, "c", "config.yaml", "Path to configuration file (shorthand)")
	fs.BoolVar(&showVersion, "version", false, "Show version and build metadata")
	fs.BoolVar(&showVersion, "v", false, "Show version (shorthand)")
	fs.BoolVar(&showHelp, "help", false, "Show help usage information")
	fs.BoolVar(&showHelp, "h", false, "Show help (shorthand)")
	fs.BoolVar(&debug, "debug", false, "Override debug mode to true")
	fs.BoolVar(&debug, "d", false, "Debug mode (shorthand)")
	fs.StringVar(&env, "env", "", "Override application environment (development, production, etc.)")
	fs.StringVar(&env, "e", "", "Environment (shorthand)")

	// Custom Usage function
	fs.Usage = func() {
		printUsage()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if showHelp {
		printUsage()
		return nil
	}

	if showVersion {
		return runVersion(nil)
	}

	remaining := fs.Args()
	subcommand := "server"
	subArgs := []string{}

	if len(remaining) > 0 {
		subcommand = remaining[0]
		subArgs = remaining[1:]
	}

	// Handle version subcommand
	if subcommand == "version" {
		return runVersion(subArgs)
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		// Fallback to default config if file is missing
		cfg, err = config.Load("")
		if err != nil {
			return fmt.Errorf("cmd: failed to load configuration: %w", err)
		}
	}

	if debug {
		cfg.App.Debug = true
	}
	if env != "" {
		cfg.App.Env = env
	}

	switch subcommand {
	case "server", "start", "run":
		return runServer(cfg)
	case "check", "validate", "test-config":
		return runCheck(cfg)
	case "send":
		return runSend(cfg, subArgs)
	default:
		fmt.Printf("Error: unknown subcommand %q\n\n", subcommand)
		printUsage()
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}
}

func printUsage() {
	fmt.Println("Usage: mailbaby [options] [command] [command-options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  server              Start the MailBaby email consumer daemon (default)")
	fmt.Println("  check               Validate configuration file and test external connectivity")
	fmt.Println("  send                Send an immediate test email message via CLI")
	fmt.Println("  version             Display build version and platform environment")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  -c, --config <path> Path to configuration file (default: config.yaml)")
	fmt.Println("  -e, --env <env>     Override environment (development, staging, production)")
	fmt.Println("  -d, --debug         Enable debug logging mode")
	fmt.Println("  -v, --version       Show version information")
	fmt.Println("  -h, --help          Show this help information")
}
