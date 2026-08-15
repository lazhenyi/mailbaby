package handler

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"mailbaby/internal/config"
)

// AuthErrorResponse is the JSON structure returned on authentication failure.
type AuthErrorResponse struct {
	Code    int    `json:"code"`
	Error   string `json:"error"`
	Message string `json:"message"`
}

// AuthMiddleware enforces secret key authentication when enabled in configuration.
func AuthMiddleware(cfg config.AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			providedKey := extractToken(r, cfg.HeaderName)
			if providedKey == "" || subtle.ConstantTimeCompare([]byte(providedKey), []byte(cfg.SecretKey)) != 1 {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(AuthErrorResponse{
					Code:    http.StatusUnauthorized,
					Error:   "unauthorized",
					Message: "invalid or missing authentication token / secret key",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractToken extracts secret key/token from Authorization header, custom header or query string.
func extractToken(r *http.Request, headerName string) string {
	// 1. Authorization: Bearer <token>
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return strings.TrimSpace(parts[1])
		}
		if len(parts) == 1 && parts[0] != "" {
			return strings.TrimSpace(parts[0])
		}
	}

	// 2. Custom header (e.g. X-API-Key)
	if headerName != "" {
		if val := r.Header.Get(headerName); val != "" {
			return strings.TrimSpace(val)
		}
	}

	// 3. Fallback header X-API-Key
	if val := r.Header.Get("X-API-Key"); val != "" {
		return strings.TrimSpace(val)
	}

	// 4. Query param fallback (?api_key=xxx or ?token=xxx)
	if key := r.URL.Query().Get("api_key"); key != "" {
		return strings.TrimSpace(key)
	}
	if token := r.URL.Query().Get("token"); token != "" {
		return strings.TrimSpace(token)
	}

	return ""
}
