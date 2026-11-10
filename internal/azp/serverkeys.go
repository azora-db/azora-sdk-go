package azp

import (
	"crypto/rand"
	"fmt"
	"time"

	"golang.org/x/crypto/curve25519"
)

// KeyPair is a server static X25519 key.
type KeyPair struct {
	// ID is a short id: 8 lowercase hex chars, the first 4 bytes of
	// BLAKE2s-256(Public). Clients send it in the clear so the server can
	// pick the key they pinned.
	ID      string
	Private [32]byte
	Public  [32]byte
	// CreatedAt orders keys: the newest non-retired key is the active one.
	CreatedAt time.Time
	// RetiredAt is set once the key is retired. Retired keys are kept for
	// an overlap window (handshakes naming them still succeed) but are
	// never chosen as active.
	RetiredAt *time.Time
}

// String describes the key without its private half, so a KeyPair can be
// logged or printed with %v safely.
func (k KeyPair) String() string {
	state := "active"
	if k.RetiredAt != nil {
		state = "retired"
	}
	return fmt.Sprintf("azp key %s (%s, %s, created %s)", k.ID, Fingerprint(k.Public), state,
		k.CreatedAt.Format(time.RFC3339))
}

// GoString is String: %#v must not dump the private key either.
func (k KeyPair) GoString() string { return k.String() }

// Retired reports whether the key has been retired.
func (k KeyPair) Retired() bool { return k.RetiredAt != nil }

// GenerateKey returns a new random X25519 key pair created at now.
func GenerateKey(now time.Time) (KeyPair, error) {
	var k KeyPair
	if _, err := rand.Read(k.Private[:]); err != nil {
		return KeyPair{}, fmt.Errorf("azp: generate key: %w", err)
	}
	pub, err := publicKey(k.Private)
	if err != nil {
		return KeyPair{}, fmt.Errorf("azp: generate key: %w", err)
	}
	k.Public = pub
	k.ID = KeyID(pub)
	k.CreatedAt = now.UTC().Round(0)
	return k, nil
}

func publicKey(priv [32]byte) ([32]byte, error) {
	var pub [32]byte
	b, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err != nil {
		return pub, err
	}
	copy(pub[:], b)
	return pub, nil
}

// StaticKeys is an in-memory KeySource holding one key: what a test
// server needs.
type StaticKeys struct{ Key KeyPair }

// Active returns the key.
func (s StaticKeys) Active() (KeyPair, bool) { return s.Key, true }

// Get returns the key when id names it.
func (s StaticKeys) Get(id string) (KeyPair, bool) { return s.Key, id == s.Key.ID }
