package sender

import (
	"strings"
	"testing"
)

func TestSanitizeHeaderValue_StripsCRLF(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain text", "hello world", "hello world"},
		{"cr injection", "hello\rworld", "helloworld"},
		{"lf injection", "hello\nworld", "helloworld"},
		{"crlf injection", "a\r\nbcc: malicious@evil.com", "abcc: malicious@evil.com"},
		{"empty", "", ""},
		{"only control chars", "\r\n\r\n", ""},
		{"mixed", "Subject: fake\r\nBcc: attacker", "Subject: fakeBcc: attacker"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeHeaderValue(tc.input)
			if got != tc.expected {
				t.Errorf("sanitizeHeaderValue(%q) = %q, want %q", tc.input, got, tc.expected)
			}
			if strings.ContainsAny(got, "\r\n") {
				t.Errorf("sanitizeHeaderValue(%q) still contains CR/LF: %q", tc.input, got)
			}
		})
	}
}