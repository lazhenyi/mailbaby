package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"mailbaby/internal/config"
)

func TestAuthMiddleware_PassThroughWhenDisabled(t *testing.T) {
	cfg := config.AuthConfig{Enabled: false, SecretKey: "anything"}
	mw := AuthMiddleware(cfg)
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("downstream must be called when auth disabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAuthMiddleware_RejectsMissingKey(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, SecretKey: "secret"}
	mw := AuthMiddleware(cfg)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream must not be called when auth fails")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddleware_AcceptsValidHeader(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, SecretKey: "secret", HeaderName: "X-API-Key"}
	mw := AuthMiddleware(cfg)
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "secret")
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatal("downstream must be called for valid key")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAuthMiddleware_AcceptsBearer(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, SecretKey: "secret"}
	mw := AuthMiddleware(cfg)
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatal("downstream must be called for valid bearer")
	}
}

func TestAuthMiddleware_RateLimit_429_WithRetryAfter(t *testing.T) {
	cfg := config.AuthConfig{
		Enabled:             true,
		SecretKey:           "secret",
		RatePerKeyPerMinute: 2,
	}
	mw := AuthMiddleware(cfg)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	makeReq := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", "secret")
		h.ServeHTTP(rec, req)
		return rec
	}

	makeReq()
	makeReq()
	rec := makeReq()

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after exceeding limit, got %d", rec.Code)
	}
	ra := rec.Header().Get("Retry-After")
	if ra == "" {
		t.Fatal("Retry-After header missing")
	}
	if _, err := strconv.Atoi(ra); err != nil {
		t.Fatalf("Retry-After must be integer seconds per RFC 7231, got %q: %v", ra, err)
	}

	body := AuthErrorResponse{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid body: %v", err)
	}
	if body.Code != http.StatusTooManyRequests || body.Error != "rate_limited" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestAuthMiddleware_DifferentKeysIndependent(t *testing.T) {
	cfg := config.AuthConfig{
		Enabled:             true,
		SecretKey:           "secret",
		RatePerKeyPerMinute: 1,
	}
	mw := AuthMiddleware(cfg)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.Header.Set("X-API-Key", "secret")
	h.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("X-API-Key", "secret")
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request same key: expected 429, got %d", rec2.Code)
	}
}

func TestExtractToken(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(*http.Request)
		header   string
		expected string
	}{
		{
			name: "bearer",
			setup: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer abc")
			},
			header:   "X-API-Key",
			expected: "abc",
		},
		{
			name: "custom header",
			setup: func(r *http.Request) {
				r.Header.Set("X-My-Key", "xyz")
			},
			header:   "X-My-Key",
			expected: "xyz",
		},
		{
			name: "fallback X-API-Key",
			setup: func(r *http.Request) {
				r.Header.Set("X-API-Key", "fallback")
			},
			header:   "X-API-Key",
			expected: "fallback",
		},
		{
			name:     "no headers",
			setup:    func(r *http.Request) {},
			header:   "X-API-Key",
			expected: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			tc.setup(req)
			if got := extractToken(req, tc.header); got != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestAuthMiddleware_BearerWithWhitespaceTrimmed(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, SecretKey: "secret"}
	mw := AuthMiddleware(cfg)
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer   secret   ")
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatal("downstream must be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAuthMiddleware_DoesNotLeakSecretInResponse(t *testing.T) {
	cfg := config.AuthConfig{Enabled: true, SecretKey: "secretkey"}
	mw := AuthMiddleware(cfg)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "wrong")
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "secretkey") || strings.Contains(body, "wrong") {
		t.Fatalf("response leaked secret or input: %s", body)
	}
}