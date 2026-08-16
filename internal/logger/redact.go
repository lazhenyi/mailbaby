package logger

import "regexp"

// urlCredsRe matches "scheme://user:password@host" URL userinfo sections.
// The password character class deliberately excludes "@" so an embedded
// ampersand that happens to live inside a password does not break URL parsing
// of the captured group.
var urlCredsRe = regexp.MustCompile(`(://)([^/@\s:]+):([^/@\s]+)@`)

// base64BlobRe matches base64-style blobs (>=16 chars of the base64 alphabet,
// optionally padded, or any length with `+`/`/`/trailing `=` characters) so
// credential blobs surfaced by SASL LOGIN / PLAIN exchanges can be replaced
// without producing false positives on common English words like
// "Authorization".
var base64BlobRe = regexp.MustCompile(`(?:[A-Za-z0-9+/]{16,}={0,2}|\b[A-Za-z0-9+/]{4,}=+\b)`)

// smtpChallengeRe matches SMTP server "334 <base64-prompt>" responses that
// precede credential submission in AUTH LOGIN/PLAIN.
var smtpChallengeRe = regexp.MustCompile(`(?i)(334\s+)([A-Za-z0-9+/=]{4,})`)

// smtpCredRe matches SMTP AUTH LOGIN/PLAIN credential lines that may end up
// in logs or traces when AUTH negotiation fails. We replace the base64 blob
// with "<redacted>".
var smtpCredRe = regexp.MustCompile(`(?i)(auth\s+(?:login|plain|cram-md5|xoauth2)\s+)(\S+)`)

// authorizationHeaderRe matches "Authorization: <scheme> <secret>" headers in
// captured request strings (e.g. from http transport dumps).
var authorizationHeaderRe = regexp.MustCompile(`(?i)(authorization:\s*)([A-Za-z]+\s+)?([^\s"']+)`)

// apiKeyHeaderRe matches X-API-Key / X-Api-Token headers in captured request
// strings. The value is replaced regardless of the header casing the caller
// used.
var apiKeyHeaderRe = regexp.MustCompile(`(?i)((?:x-api-key|x-api-token|x-auth-token):\s*)([^\s"',]+)`)

// RedactURL removes userinfo credentials embedded in URLs (e.g.
// amqp://guest:secret@host, https://user:pass@example.com), returning a
// sanitized string safe for logging.
func RedactURL(s string) string {
	return urlCredsRe.ReplaceAllString(s, "${1}***:***@")
}

// RedactSecrets replaces any credential-like fragment in s with a redacted
// placeholder. It is meant to be applied to error messages, captured request
// payloads, and field values that may transitively include user-supplied
// credentials.
func RedactSecrets(s string) string {
	if s == "" {
		return s
	}
	s = urlCredsRe.ReplaceAllString(s, "${1}***:***@")
	s = smtpCredRe.ReplaceAllString(s, "${1}<redacted>")
	s = smtpChallengeRe.ReplaceAllString(s, "${1}<redacted>")
	s = authorizationHeaderRe.ReplaceAllString(s, "${1}${2}<redacted>")
	s = apiKeyHeaderRe.ReplaceAllString(s, "${1}<redacted>")
	s = base64BlobRe.ReplaceAllString(s, "<redacted>")
	return s
}