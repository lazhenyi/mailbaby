package handler

import (
	"testing"
	"time"
)

func TestKeyRateLimiter_BasicAllow(t *testing.T) {
	rl := newKeyRateLimiter(2)
	if ok, _ := rl.allow("k1"); !ok {
		t.Fatal("first request must be allowed")
	}
	if ok, _ := rl.allow("k1"); !ok {
		t.Fatal("second request must be allowed")
	}
	if ok, _ := rl.allow("k1"); ok {
		t.Fatal("third request must be denied at cap=2")
	}
	if ok, _ := rl.allow("k2"); !ok {
		t.Fatal("different key has its own bucket")
	}
}

func TestKeyRateLimiter_Fingerprint(t *testing.T) {
	a := fingerprint("secret-key")
	b := fingerprint("secret-key")
	if a != b {
		t.Fatalf("fingerprint not deterministic: %s != %s", a, b)
	}
	if a == fingerprint("other-key") {
		t.Fatal("different keys produce identical fingerprint")
	}
}

func TestKeyRateLimiter_EmptyKey(t *testing.T) {
	rl := newKeyRateLimiter(1)
	for i := 0; i < 5; i++ {
		if ok, _ := rl.allow(""); !ok {
			t.Fatal("empty key must always be allowed")
		}
	}
}

func TestKeyRateLimiter_RetryAfter(t *testing.T) {
	rl := newKeyRateLimiter(1)
	_, _ = rl.allow("k1")
	ok, ra := rl.allow("k1")
	if ok {
		t.Fatal("second request must be denied")
	}
	if ra <= 0 || ra > time.Minute+time.Second {
		t.Fatalf("unexpected retry-after: %s", ra)
	}
}

func TestKeyRateLimiter_DefaultsAndEviction(t *testing.T) {
	if rl := newKeyRateLimiter(0); rl.cap != 600 {
		t.Fatalf("expected default cap 600, got %d", rl.cap)
	}
	if rl := newKeyRateLimiter(-1); rl.cap != 600 {
		t.Fatalf("expected default cap 600 for negative input, got %d", rl.cap)
	}

	rl := newKeyRateLimiter(2)
	for i := 0; i < 5; i++ {
		rl.allow("same")
	}
	rl.evictOldestLocked(time.Now())
}