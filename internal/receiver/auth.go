// Package receiver hosts the HTTP handlers for incoming webhooks and
// download-client script callbacks.
package receiver

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
)

// errAuthFailed is returned when neither HMAC nor bearer verification passes.
var errAuthFailed = errors.New("authentication failed")

// verifyHMACSHA256 compares the X-Webhook-Signature header to the HMAC of body
// keyed by secret. Header may be in raw hex or "sha256=<hex>" form. Returns
// nil on success, errAuthFailed otherwise. If secret is empty, verification
// is skipped (caller decides whether to require auth).
func verifyHMACSHA256(header, secret string, body []byte) error {
	if secret == "" {
		return nil
	}
	got := strings.TrimSpace(header)
	got = strings.TrimPrefix(got, "sha256=")
	if got == "" {
		return errAuthFailed
	}
	gotBytes, err := hex.DecodeString(got)
	if err != nil {
		return errAuthFailed
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	want := mac.Sum(nil)
	if !hmac.Equal(gotBytes, want) {
		return errAuthFailed
	}
	return nil
}

// verifyBearer compares the Authorization header to "Bearer <expected>".
// If expected is empty, verification is skipped.
func verifyBearer(header, expected string) error {
	if expected == "" {
		return nil
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return errAuthFailed
	}
	got := header[len(prefix):]
	if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
		return errAuthFailed
	}
	return nil
}

// writeError writes a minimal text response with the given status. We
// deliberately do not echo internal details — webhook senders are
// untrusted.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(msg))
}
