package logger

import "regexp"

// urlCredsRe matches "scheme://user:password@host" URL userinfo sections.
var urlCredsRe = regexp.MustCompile(`(://)([^/@\s:]+):([^/@\s]+)@`)

// RedactURL removes userinfo credentials embedded in URLs (e.g.
// amqp://guest:secret@host, https://user:pass@example.com), returning a
// sanitized string safe for logging.
func RedactURL(s string) string {
	return urlCredsRe.ReplaceAllString(s, "${1}***:***@")
}
