// Package azp implements the secure channel of the Azora client protocol
// (AZP): frame transports (raw stream and WebSocket), the Noise handshake
// and record layer that carry application messages end to end encrypted,
// and the message codec.
//
// It is a faithful port of the engine's internal/azp. The SDK uses the
// client half; the server half (Server, ServerConfig, KeyPair) is kept so
// that the SDK's tests can run an in-process protocol server without the
// engine binary (see internal/azptest).
package azp

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/crypto/blake2s"
)

// KeyID returns the short id of a public key: the first 4 bytes of
// BLAKE2s-256(pub) as 8 lowercase hex chars.
func KeyID(pub [32]byte) string {
	h := blake2s.Sum256(pub[:])
	return hex.EncodeToString(h[:4])
}

var fpEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// Fingerprint returns a human-checkable fingerprint of a public key:
// "azp1-" followed by the lowercase unpadded RFC 4648 base32 of the first 15
// bytes of BLAKE2s-256(pub), in 4-char groups joined by "-".
func Fingerprint(pub [32]byte) string {
	h := blake2s.Sum256(pub[:])
	s := fpEncoding.EncodeToString(h[:15]) // 24 chars
	var b strings.Builder
	b.WriteString("azp1")
	for i := 0; i < len(s); i += 4 {
		b.WriteByte('-')
		b.WriteString(s[i:min(i+4, len(s))])
	}
	return b.String()
}

// EncodePublicKey encodes a public key as unpadded base64url, the form used
// in connection strings and SDK configuration.
func EncodePublicKey(pub [32]byte) string {
	return base64.RawURLEncoding.EncodeToString(pub[:])
}

// DecodePublicKey parses a public key written by EncodePublicKey.
func DecodePublicKey(s string) ([32]byte, error) {
	var pub [32]byte
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return pub, fmt.Errorf("azp: decode public key: %w", err)
	}
	if len(b) != len(pub) {
		return pub, fmt.Errorf("azp: decode public key: %d bytes, want %d", len(b), len(pub))
	}
	copy(pub[:], b)
	return pub, nil
}
