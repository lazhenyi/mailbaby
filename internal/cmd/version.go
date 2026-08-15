package cmd

import (
	"fmt"
	"runtime"
)

// Build variables populated by ldflags during build time.
var (
	Version   = "1.0.0"
	Commit    = "dev"
	BuildDate = "unknown"
)

// VersionInfo encapsulates application version and build metadata.
type VersionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	Compiler  string `json:"compiler"`
	Platform  string `json:"platform"`
}

// GetVersionInfo returns current build and runtime metadata.
func GetVersionInfo() VersionInfo {
	return VersionInfo{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
		Compiler:  runtime.Compiler,
		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String formats the version info as a human-readable string.
func (v VersionInfo) String() string {
	return fmt.Sprintf("MailBaby %s (commit: %s, built: %s, %s, %s)",
		v.Version, v.Commit, v.BuildDate, v.GoVersion, v.Platform)
}

func runVersion(args []string) error {
	info := GetVersionInfo()
	fmt.Println(info.String())
	return nil
}
