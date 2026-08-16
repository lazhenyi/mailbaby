package handler

import (
	"net"
	"net/http"
	"strings"
)

// clientAddr returns the best-effort client IP for logging/metrics. When
// trustProxy is false (the default), RemoteAddr is always returned so the
// server cannot be spoofed by client-supplied X-Forwarded-For headers. When
// trustProxy is true the first hop in X-Forwarded-For (or X-Real-IP) is
// preferred.
func clientAddr(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if first := strings.TrimSpace(strings.Split(xff, ",")[0]); first != "" {
				return first
			}
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}