package main

import (
	"os"

	"mailbaby/internal/cmd"
	_ "mailbaby/internal/queue/driver/all"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
