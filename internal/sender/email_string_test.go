package sender

import (
	"strings"
	"testing"
)

func TestEmailString_RedactsAttachmentData(t *testing.T) {
	secret := "PRIVATE-INVOICE-CONTENT-DO-NOT-LOG"
	e := NewEmail().
		SetFrom("sender@example.com").
		AddTo("user@example.com").
		SetSubject("Invoice").
		Attach("invoice.pdf", []byte(secret), "application/pdf").
		AttachInline("logo.png", "logo-img", []byte("PNG-BYTES"), "image/png")

	out := e.String()

	if strings.Contains(out, secret) {
		t.Fatalf("Email.String leaked attachment data: %s", out)
	}
	if !strings.Contains(out, `"filename":"invoice.pdf"`) {
		t.Fatalf("attachment filename missing: %s", out)
	}
	if !strings.Contains(out, `"size":`) {
		t.Fatalf("attachment size missing: %s", out)
	}
	if strings.Contains(out, "PNG-BYTES") {
		t.Fatalf("inline attachment data leaked: %s", out)
	}
}

func TestEmailString_NilSafe(t *testing.T) {
	var e *Email
	if out := e.String(); out == "" {
		t.Fatal("expected non-empty output for nil email")
	}
}