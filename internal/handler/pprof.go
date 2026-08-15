package handler

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"

	"mailbaby/internal/config"
)

// MountPprof registers all Go runtime profiling endpoints under the configured path prefix.
func MountPprof(mux *http.ServeMux, cfg config.PprofConfig) {
	if !cfg.Enabled {
		return
	}

	cfg.ApplyDefaults()

	// Apply runtime profile rates
	if cfg.ProfileBlock || cfg.BlockRate > 0 {
		rate := cfg.BlockRate
		if rate <= 0 {
			rate = 1
		}
		runtime.SetBlockProfileRate(rate)
	}

	if cfg.ProfileMutex || cfg.MutexRate > 0 {
		rate := cfg.MutexRate
		if rate <= 0 {
			rate = 1
		}
		runtime.SetMutexProfileFraction(rate)
	}

	prefix := strings.TrimRight(cfg.Path, "/")
	if prefix == "" {
		prefix = "/debug/pprof"
	}

	// Mount standard pprof handlers
	mux.HandleFunc(prefix+"/", pprof.Index)
	mux.HandleFunc(prefix+"/cmdline", pprof.Cmdline)
	mux.HandleFunc(prefix+"/profile", pprof.Profile)
	mux.HandleFunc(prefix+"/symbol", pprof.Symbol)
	mux.HandleFunc(prefix+"/trace", pprof.Trace)

	// Named profiles
	for _, profileName := range []string{"goroutine", "heap", "threadcreate", "block", "mutex", "allocs"} {
		mux.Handle(prefix+"/"+profileName, pprof.Handler(profileName))
	}
}
