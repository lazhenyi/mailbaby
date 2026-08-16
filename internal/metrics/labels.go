package metrics

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// maxLabelValueLen caps the length of any value used as a Prometheus label.
// Values longer than this are hashed to prevent label cardinality explosion
// (and to bound memory usage).
const maxLabelValueLen = 64

// maxAllowedAccountLabelValues bounds the number of distinct account label
// values to prevent cardinality attacks via the public-facing "account"
// parameter (which is accepted from API requests).
const maxAllowedAccountLabelValues = 100

// sanitizeLabel normalizes a metric label value. If the value exceeds
// maxLabelValueLen, it is replaced with a stable SHA-256 prefix so that
// metric cardinality stays bounded. Empty values are normalized to "default".
// Values containing characters that Prometheus forbids (\n, \r) are stripped.
func sanitizeLabel(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "default"
	}
	// Strip forbidden characters from Prometheus label values.
	v = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '"' || r == '\\' {
			return -1
		}
		return r
	}, v)
	if len(v) > maxLabelValueLen {
		sum := sha256.Sum256([]byte(v))
		return "h_" + hex.EncodeToString(sum[:8])
	}
	return v
}