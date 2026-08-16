package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// keyRateLimiter applies a sliding-window request cap per authenticated key.
// Key fingerprints (SHA-256 hex) are used so that raw secrets never live in
// the limiter's map. The default cap of 600/minute can be overridden via the
// RatePerKeyPerMinute field on AuthConfig.
type keyRateLimiter struct {
	mu       sync.Mutex
	cap      int
	window   time.Duration
	buckets  map[string]*keyBucket
	maxKeys  int
}

type keyBucket struct {
	windowStart time.Time
	count       int
}

func newKeyRateLimiter(perMinute int) *keyRateLimiter {
	if perMinute <= 0 {
		perMinute = 600
	}
	return &keyRateLimiter{
		cap:      perMinute,
		window:   time.Minute,
		buckets:  make(map[string]*keyBucket),
		maxKeys:  10000,
	}
}

func (r *keyRateLimiter) allow(rawKey string) (allowed bool, retryAfter time.Duration) {
	if rawKey == "" {
		return true, 0
	}
	fp := fingerprint(rawKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	b, ok := r.buckets[fp]
	if !ok || now.Sub(b.windowStart) >= r.window {
		if !ok && len(r.buckets) >= r.maxKeys {
			r.evictOldestLocked(now)
		}
		b = &keyBucket{windowStart: now, count: 0}
		r.buckets[fp] = b
	}

	if b.count >= r.cap {
		return false, b.windowStart.Add(r.window).Sub(now)
	}
	b.count++
	return true, 0
}

func (r *keyRateLimiter) evictOldestLocked(now time.Time) {
	var oldestFP string
	var oldestT time.Time
	first := true
	for fp, b := range r.buckets {
		if first || b.windowStart.Before(oldestT) {
			oldestFP = fp
			oldestT = b.windowStart
			first = false
		}
	}
	if oldestFP != "" && (now.Sub(oldestT) >= r.window || len(r.buckets) >= r.maxKeys) {
		delete(r.buckets, oldestFP)
	}
}

func fingerprint(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}