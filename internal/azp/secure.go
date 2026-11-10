package azp

import (
	"errors"
	"fmt"
	"time"

	"github.com/flynn/noise"
)

// Mode is the security mode of a channel, sent in the handshake preamble.
type Mode byte

const (
	// ModeIK is Noise_IK_25519_ChaChaPoly_BLAKE2s: the client knows (pins)
	// the server's static public key. One round trip.
	ModeIK Mode = 'K'
	// ModeXX is Noise_XX_25519_ChaChaPoly_BLAKE2s: the client learns the
	// server's static key during the handshake and must verify it.
	ModeXX Mode = 'X'
	// ModePlaintext carries messages unencrypted. For tests and loopback
	// only: both sides must allow it.
	ModePlaintext Mode = 'P'
)

// String returns the mode's name.
func (m Mode) String() string {
	switch m {
	case ModeIK:
		return "IK"
	case ModeXX:
		return "XX"
	case ModePlaintext:
		return "plaintext"
	}
	return fmt.Sprintf("Mode(%#x)", byte(m))
}

// Defaults for ClientConfig and ServerConfig fields left zero.
const (
	DefaultMaxMessage       = 16 << 20
	DefaultHandshakeTimeout = 10 * time.Second
)

var (
	// ErrHandshake is returned when the handshake fails: malformed or
	// tampered messages, a server key the client does not accept, a
	// prologue mismatch, a timeout or a connection lost mid-handshake.
	ErrHandshake = errors.New("azp: handshake failed")
	// ErrPlaintextRefused is returned when plaintext was requested but the
	// server (or the client) does not allow it.
	ErrPlaintextRefused = errors.New("azp: plaintext refused")
	// ErrUnknownKey is returned when the key id named by the client is not
	// in the server's key source.
	ErrUnknownKey = errors.New("azp: unknown server key")
	// ErrDecrypt is returned when a frame fails authentication: it was
	// tampered with, replayed, reordered, truncated or a frame is missing.
	// It is fatal: the Conn closes its transport.
	ErrDecrypt = errors.New("azp: frame failed authentication")
	// ErrMessageTooLarge is returned when a message exceeds MaxMessage.
	// Sending one fails without side effects; receiving one is fatal.
	ErrMessageTooLarge = errors.New("azp: message too large")
	// ErrClosed is returned by a Conn after Close.
	ErrClosed = errors.New("azp: connection closed")
	// ErrProtocol is returned when an authenticated peer breaks framing
	// rules (bad chunk flag, empty continuation chunk). It is fatal.
	ErrProtocol = errors.New("azp: protocol violation")
)

// Wire constants.
const (
	magic           = "AZP1"
	protocolVersion = 1
	preambleFixed   = len(magic) + 3 // magic | version | mode | kid length
	maxKIDLen       = 64
	tagSize         = 16
	// maxChunk is the most message bytes one frame carries: a Noise
	// message is at most FrameSize bytes, of which 16 are the tag and one
	// the chunk flag.
	maxChunk = FrameSize - tagSize - 1

	flagMore  byte = 0
	flagFinal byte = 1

	// First byte of the server's reply: 0 accepts, anything else is an
	// (unauthenticated) rejection.
	statusOK              byte = 0
	statusPlaintextRefuse byte = 1
	statusUnknownKey      byte = 2
	statusBadVersion      byte = 3
)

// Rekey thresholds, per direction, counted identically by both ends. Tests
// lower rekeyInterval; a Conn copies both when it is created.
var (
	rekeyInterval uint64 = 1 << 20
	rekeyBytes    uint64 = 1 << 30
)

var cipherSuite = noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)

// KeySource provides the server's static keys. StaticKeys satisfies it.
type KeySource interface {
	// Active returns the key used when the client names none (XX).
	Active() (KeyPair, bool)
	// Get returns the key the client named, including retired keys.
	Get(id string) (KeyPair, bool)
}

// ClientConfig configures Client. Exactly one of ServerKey (IK),
// VerifyServer alone (XX) or AllowPlaintext alone selects the mode; an
// encrypted mode always wins over plaintext.
type ClientConfig struct {
	// ServerKey pins the server's static public key and selects IK.
	ServerKey *[32]byte
	// KeyID is sent in the clear so the server picks the right key. With
	// ServerKey it defaults to KeyID(*ServerKey). With XX it is optional:
	// empty asks for the server's active key; set, the learned key must
	// have this id.
	KeyID string
	// VerifyServer checks the server's static key. Required for XX (nil
	// ServerKey): return an error to abort, for example on a fingerprint
	// mismatch. With IK it is an optional extra check.
	VerifyServer func(pub [32]byte) error
	// AllowPlaintext requests a plaintext channel; it applies only when
	// ServerKey and VerifyServer are both nil, and the server must allow
	// plaintext too.
	AllowPlaintext bool
	// MaxMessage bounds application messages sent and received (default
	// DefaultMaxMessage).
	MaxMessage int
	// Prologue is bound into the handshake (for example the database
	// name); it must equal the server's.
	Prologue []byte
}

// ServerConfig configures Server.
type ServerConfig struct {
	// Keys provides the static keys.
	Keys KeySource
	// AllowPlaintext lets clients request a plaintext channel.
	AllowPlaintext bool
	// MaxMessage bounds application messages (default DefaultMaxMessage).
	MaxMessage int
	// Prologue is bound into the handshake; it must equal the client's.
	Prologue []byte
	// HandshakeTimeout bounds the whole handshake (default
	// DefaultHandshakeTimeout).
	HandshakeTimeout time.Duration
}
