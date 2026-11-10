package azp

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/flynn/noise"
)

// preamble is a parsed handshake preamble.
type preamble struct {
	mode Mode
	kid  string
	raw  []byte // the preamble bytes, bound into the prologue
	rest []byte // the first handshake message
}

// parsePreamble splits the client's first frame into its preamble and
// first Noise message. A non-zero status is the rejection to send back.
func parsePreamble(b []byte) (preamble, byte, error) {
	if len(b) < preambleFixed || string(b[:len(magic)]) != magic {
		return preamble{}, 0, hsErr("bad magic")
	}
	if b[4] != protocolVersion {
		return preamble{}, statusBadVersion, hsErr("unsupported version %d", b[4])
	}
	p := preamble{mode: Mode(b[5])}
	n := int(b[6])
	if len(b) < preambleFixed+n {
		return preamble{}, 0, hsErr("short preamble")
	}
	p.kid = string(b[preambleFixed : preambleFixed+n])
	if !validKID(p.kid) {
		return preamble{}, 0, hsErr("malformed key id")
	}
	p.raw = b[:preambleFixed+n]
	p.rest = b[preambleFixed+n:]
	switch p.mode {
	case ModeIK:
		if p.kid == "" {
			return preamble{}, 0, hsErr("IK without key id")
		}
	case ModeXX:
	case ModePlaintext:
		if p.kid != "" || len(p.rest) != 0 {
			return preamble{}, 0, hsErr("malformed plaintext preamble")
		}
	default:
		return preamble{}, 0, hsErr("unknown mode %#x", b[5])
	}
	return p, 0, nil
}

// Server runs the server side of the handshake over t and returns the
// channel. The handshake is bounded by cfg.HandshakeTimeout and ctx. On
// error t is closed; errors never contain key material.
func Server(ctx context.Context, t Transport, cfg ServerConfig) (_ *Conn, err error) {
	defer func() {
		if err != nil {
			abortTransport(t)
		}
	}()
	timeout := cfg.HandshakeTimeout
	if timeout <= 0 {
		timeout = DefaultHandshakeTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	reject := func(status byte) { _ = t.WriteFrame(ctx, []byte{status}) }

	first, err := t.ReadFrame(ctx)
	if err != nil {
		return nil, hsErr("read message 1: %w", err)
	}
	pre, status, err := parsePreamble(first)
	if err != nil {
		if status != 0 {
			reject(status)
		}
		return nil, err
	}
	c := newConn(t, pre.mode, cfg.MaxMessage)

	if pre.mode == ModePlaintext {
		if !cfg.AllowPlaintext {
			reject(statusPlaintextRefuse)
			return nil, fmt.Errorf("%w: plaintext is disabled", ErrPlaintextRefused)
		}
		if err := t.WriteFrame(ctx, []byte{statusOK}); err != nil {
			return nil, hsErr("send reply: %w", err)
		}
		return c, nil
	}

	if cfg.Keys == nil {
		return nil, errors.New("azp: server config: no key source")
	}
	var key KeyPair
	var ok bool
	if pre.kid == "" {
		key, ok = cfg.Keys.Active()
	} else {
		key, ok = cfg.Keys.Get(pre.kid)
	}
	if !ok {
		reject(statusUnknownKey)
		if pre.kid == "" {
			return nil, fmt.Errorf("%w: no active key", ErrUnknownKey)
		}
		return nil, fmt.Errorf("%w: %q", ErrUnknownKey, pre.kid)
	}
	priv := append([]byte(nil), key.Private[:]...)
	defer clear(priv) // the handshake state uses it until Server returns
	ncfg := noise.Config{
		CipherSuite:   cipherSuite,
		Random:        rand.Reader,
		Pattern:       noise.HandshakeXX,
		Prologue:      prologue(pre.raw, cfg.Prologue),
		StaticKeypair: noise.DHKey{Private: priv, Public: append([]byte(nil), key.Public[:]...)},
	}
	if pre.mode == ModeIK {
		ncfg.Pattern = noise.HandshakeIK
	}
	hs, err := noise.NewHandshakeState(ncfg)
	if err != nil {
		return nil, hsErr("init: %w", err)
	}
	payload, _, _, err := hs.ReadMessage(nil, pre.rest)
	if err != nil {
		return nil, hsErr("message 1 failed authentication")
	}
	if len(payload) != 0 {
		return nil, hsErr("unexpected payload in message 1")
	}
	reply, cs1, cs2, err := hs.WriteMessage([]byte{statusOK}, nil)
	if err != nil {
		return nil, hsErr("message 2: %w", err)
	}
	if err := t.WriteFrame(ctx, reply); err != nil {
		return nil, hsErr("send message 2: %w", err)
	}
	if pre.mode == ModeXX {
		third, err := t.ReadFrame(ctx)
		if err != nil {
			return nil, hsErr("read message 3: %w", err)
		}
		payload, cs1, cs2, err = hs.ReadMessage(nil, third)
		if err != nil {
			return nil, hsErr("message 3 failed authentication")
		}
		if len(payload) != 0 {
			return nil, hsErr("unexpected payload in message 3")
		}
	}
	if cs1 == nil || cs2 == nil {
		return nil, hsErr("handshake did not complete")
	}
	c.send.cs, c.recv.cs = cs2, cs1
	c.serverKey = key.Public
	c.keyID = key.ID
	c.hh = append([]byte(nil), hs.ChannelBinding()...)
	return c, nil
}
