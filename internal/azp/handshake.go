package azp

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/flynn/noise"
)

// validKID reports whether kid is 0-64 chars of [0-9a-z].
func validKID(kid string) bool {
	if len(kid) > maxKIDLen {
		return false
	}
	for i := 0; i < len(kid); i++ {
		ch := kid[i]
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z') {
			return false
		}
	}
	return true
}

// buildPreamble returns the clear preamble that starts a handshake:
// magic, version, mode, key id length and key id.
func buildPreamble(mode Mode, kid string) []byte {
	p := make([]byte, 0, preambleFixed+len(kid)+96)
	p = append(p, magic...)
	p = append(p, protocolVersion, byte(mode), byte(len(kid)))
	return append(p, kid...)
}

// prologue binds the preamble and the configured prologue into the Noise
// handshake. The preamble is self-delimiting, so the concatenation is
// unambiguous.
func prologue(preamble, extra []byte) []byte {
	out := make([]byte, 0, len(preamble)+len(extra))
	return append(append(out, preamble...), extra...)
}

// hsErr returns an ErrHandshake with details.
func hsErr(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrHandshake}, args...)...)
}

// mode picks the handshake mode and key id a client config asks for.
func (cfg ClientConfig) mode() (Mode, string, error) {
	switch {
	case cfg.ServerKey != nil:
		kid := cfg.KeyID
		if kid == "" {
			kid = KeyID(*cfg.ServerKey)
		}
		if kid == "" || !validKID(kid) {
			return 0, "", errors.New("azp: client config: KeyID must be 1-64 chars of [0-9a-z]")
		}
		return ModeIK, kid, nil
	case cfg.VerifyServer != nil:
		if !validKID(cfg.KeyID) {
			return 0, "", errors.New("azp: client config: KeyID must be 0-64 chars of [0-9a-z]")
		}
		return ModeXX, cfg.KeyID, nil
	case cfg.AllowPlaintext:
		return ModePlaintext, "", nil
	}
	return 0, "", errors.New("azp: client config: set ServerKey (pinned key), VerifyServer (first contact) or AllowPlaintext")
}

// Client runs the client side of the handshake over t and returns the
// channel. ctx bounds the handshake. On error t is closed.
func Client(ctx context.Context, t Transport, cfg ClientConfig) (_ *Conn, err error) {
	defer func() {
		if err != nil {
			abortTransport(t)
		}
	}()
	mode, kid, err := cfg.mode()
	if err != nil {
		return nil, err
	}
	pre := buildPreamble(mode, kid)
	c := newConn(t, mode, cfg.MaxMessage)
	c.keyID = kid

	if mode == ModePlaintext {
		if err := t.WriteFrame(ctx, pre); err != nil {
			return nil, hsErr("send preamble: %w", err)
		}
		reply, err := readReply(ctx, t, mode)
		if err != nil {
			return nil, err
		}
		if len(reply) != 0 {
			return nil, hsErr("unexpected plaintext reply")
		}
		return c, nil
	}

	static, err := cipherSuite.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("azp: client key: %w", err)
	}
	ncfg := noise.Config{
		CipherSuite:   cipherSuite,
		Random:        rand.Reader,
		Pattern:       noise.HandshakeXX,
		Initiator:     true,
		Prologue:      prologue(pre, cfg.Prologue),
		StaticKeypair: static,
	}
	if mode == ModeIK {
		ncfg.Pattern = noise.HandshakeIK
		ncfg.PeerStatic = append([]byte(nil), cfg.ServerKey[:]...)
	}
	hs, err := noise.NewHandshakeState(ncfg)
	if err != nil {
		return nil, hsErr("init: %w", err)
	}
	first, _, _, err := hs.WriteMessage(pre, nil) // preamble | message 1
	if err != nil {
		return nil, hsErr("message 1: %w", err)
	}
	if err := t.WriteFrame(ctx, first); err != nil {
		return nil, hsErr("send message 1: %w", err)
	}
	reply, err := readReply(ctx, t, mode)
	if err != nil {
		return nil, err
	}
	payload, cs1, cs2, err := hs.ReadMessage(nil, reply)
	if err != nil {
		return nil, hsErr("message 2 failed authentication")
	}
	if len(payload) != 0 {
		return nil, hsErr("unexpected payload in message 2")
	}
	if mode == ModeIK {
		c.serverKey = *cfg.ServerKey
		if cs1 == nil || cs2 == nil {
			return nil, hsErr("IK did not complete")
		}
		if cfg.VerifyServer != nil {
			if err := cfg.VerifyServer(c.serverKey); err != nil {
				return nil, hsErr("server key rejected: %w", err)
			}
		}
	} else {
		ps := hs.PeerStatic()
		if len(ps) != 32 {
			return nil, hsErr("no server static key")
		}
		copy(c.serverKey[:], ps)
		if kid != "" && KeyID(c.serverKey) != kid {
			return nil, hsErr("server sent a key with another id than requested")
		}
		c.keyID = KeyID(c.serverKey)
		if err := cfg.VerifyServer(c.serverKey); err != nil {
			return nil, hsErr("server key rejected: %w", err)
		}
		var third []byte
		third, cs1, cs2, err = hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, hsErr("message 3: %w", err)
		}
		if cs1 == nil || cs2 == nil {
			return nil, hsErr("XX did not complete")
		}
		if err := t.WriteFrame(ctx, third); err != nil {
			return nil, hsErr("send message 3: %w", err)
		}
	}
	c.send.cs, c.recv.cs = cs1, cs2
	c.hh = append([]byte(nil), hs.ChannelBinding()...)
	return c, nil
}

// readReply reads the server's first frame and strips its status byte.
func readReply(ctx context.Context, t Transport, mode Mode) ([]byte, error) {
	reply, err := t.ReadFrame(ctx)
	if err != nil {
		return nil, hsErr("read reply: %w", err)
	}
	if len(reply) == 0 {
		return nil, hsErr("empty reply")
	}
	switch reply[0] {
	case statusOK:
		return reply[1:], nil
	case statusPlaintextRefuse:
		if mode == ModePlaintext {
			return nil, fmt.Errorf("%w by the server", ErrPlaintextRefused)
		}
	case statusUnknownKey:
		if mode != ModePlaintext {
			return nil, fmt.Errorf("%w: the server does not have the requested key", ErrUnknownKey)
		}
	case statusBadVersion:
		return nil, hsErr("the server does not support protocol version %d", protocolVersion)
	}
	return nil, hsErr("rejected by the server (status %d)", reply[0])
}
