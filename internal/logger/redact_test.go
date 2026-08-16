package logger

import "testing"

func TestRedactSecrets_URL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "amqp creds",
			in:   "amqp://guest:secret@host:5672",
			want: "amqp://***:***@host:5672",
		},
		{
			name: "no creds",
			in:   "http://example.com/path",
			want: "http://example.com/path",
		},
		{
			name: "https creds",
			in:   "https://alice:passw0rd@example.com",
			want: "https://***:***@example.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactURL(tc.in)
			if got != tc.want {
				t.Fatalf("RedactURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRedactSecrets_AuthorizationHeader(t *testing.T) {
	in := `request failed: Authorization: Bearer abc123secret for /v1/email/send`
	out := RedactSecrets(in)
	if contains(out, "abc123secret") {
		t.Fatalf("secret leaked: %s", out)
	}
	if !contains(out, "Authorization") {
		t.Fatalf("header name missing: %s", out)
	}
}

func TestRedactSecrets_APIKeyHeader(t *testing.T) {
	in := "X-API-Key: sk_live_abcdef123456"
	out := RedactSecrets(in)
	if contains(out, "sk_live_abcdef123456") {
		t.Fatalf("api key leaked: %s", out)
	}
	if !contains(out, "X-API-Key") {
		t.Fatalf("header missing: %s", out)
	}
}

func TestRedactSecrets_SMTPLoginBlob(t *testing.T) {
	in := "smtp: server said 'AUTH LOGIN dXNlcg==' then '334 UGFzc3dvcmQ=='"
	out := RedactSecrets(in)
	if contains(out, "dXNlcg==") {
		t.Fatalf("login blob leaked: %s", out)
	}
	if contains(out, "UGFzc3dvcmQ==") {
		t.Fatalf("password blob leaked: %s", out)
	}
}

func TestRedactSecrets_Empty(t *testing.T) {
	if got := RedactSecrets(""); got != "" {
		t.Fatalf("expected empty for empty input, got %q", got)
	}
}

func TestRedactSecrets_NoSecrets(t *testing.T) {
	in := "normal log line without any credentials"
	if got := RedactSecrets(in); got != in {
		t.Fatalf("unmodified input changed: %q -> %q", in, got)
	}
}

func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}