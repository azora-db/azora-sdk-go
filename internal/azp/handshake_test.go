package azp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/stretchr/testify/require"
)

func TestWrongPinnedKey(t *testing.T) {
	_, a := newStaticKeys(t, 1)
	keysB, b := newStaticKeys(t, 1)

	t.Run("unknown key id", func(t *testing.T) {
		ct, st := pipePair(t)
		_, _, cerr, serr := handshake(t, ct, st, ClientConfig{ServerKey: &a[0].Public}, ServerConfig{Keys: keysB})
		require.ErrorIs(t, cerr, ErrUnknownKey)
		require.ErrorIs(t, serr, ErrUnknownKey)
		require.ErrorContains(t, serr, a[0].ID)
	})
	t.Run("server holds another key under the id", func(t *testing.T) {
		ct, st := pipePair(t)
		_, _, cerr, serr := handshake(t, ct, st, ClientConfig{ServerKey: &a[0].Public, KeyID: b[0].ID}, ServerConfig{Keys: keysB})
		require.ErrorIs(t, cerr, ErrHandshake)
		require.ErrorIs(t, serr, ErrHandshake)
		requireNoKeyMaterial(t, cerr, a[0], b[0])
		requireNoKeyMaterial(t, serr, a[0], b[0])
	})
	t.Run("IK VerifyServer rejects", func(t *testing.T) {
		ct, st := pipePair(t)
		boom := errors.New("not today")
		_, _, cerr, _ := handshake(t, ct, st, ClientConfig{ServerKey: &b[0].Public, VerifyServer: func([32]byte) error { return boom }},
			ServerConfig{Keys: keysB})
		require.ErrorIs(t, cerr, ErrHandshake)
		require.ErrorIs(t, cerr, boom)
	})
}

func requireNoKeyMaterial(t *testing.T, err error, keys ...KeyPair) {
	t.Helper()
	require.Error(t, err)
	for _, k := range keys {
		for _, s := range []string{hex.EncodeToString(k.Private[:]), base64.RawURLEncoding.EncodeToString(k.Private[:]),
			base64.StdEncoding.EncodeToString(k.Private[:]), hex.EncodeToString(k.Private[:8])} {
			require.NotContains(t, err.Error(), s)
		}
	}
}

func TestXXVerification(t *testing.T) {
	keys, ks := newStaticKeys(t, 2)
	old, active := ks[0], ks[1]

	t.Run("VerifyServer rejects", func(t *testing.T) {
		ct, st := pipePair(t)
		mismatch := pinFingerprint(Fingerprint(old.Public)) // pins the old key; the server serves the active one
		_, _, cerr, serr := handshake(t, ct, st, ClientConfig{VerifyServer: mismatch}, ServerConfig{Keys: keys})
		require.ErrorIs(t, cerr, ErrHandshake)
		require.ErrorContains(t, cerr, "server key rejected")
		require.ErrorIs(t, serr, ErrHandshake, "the client never sends message 3")
	})
	t.Run("VerifyServer sees the active key", func(t *testing.T) {
		var seen [32]byte
		cc, sc := connect(t, pipePair, ClientConfig{VerifyServer: func(pub [32]byte) error { seen = pub; return nil }},
			ServerConfig{Keys: keys})
		require.Equal(t, active.Public, seen)
		require.Equal(t, active.ID, cc.KeyID())
		require.Equal(t, active.ID, sc.KeyID())
	})
	t.Run("KeyID selects a key", func(t *testing.T) {
		cc, sc := connect(t, pipePair, ClientConfig{KeyID: old.ID, VerifyServer: pinFingerprint(Fingerprint(old.Public))},
			ServerConfig{Keys: keys})
		require.Equal(t, old.Public, cc.ServerKey())
		require.Equal(t, old.ID, sc.KeyID())
		exchange(t, cc, sc, []byte("hi"))
	})
	t.Run("unknown KeyID", func(t *testing.T) {
		ct, st := pipePair(t)
		_, _, cerr, serr := handshake(t, ct, st, ClientConfig{KeyID: "00000000", VerifyServer: func([32]byte) error { return nil }},
			ServerConfig{Keys: keys})
		require.ErrorIs(t, cerr, ErrUnknownKey)
		require.ErrorIs(t, serr, ErrUnknownKey)
	})
}

// rewriteFirst returns a client transport wrapper that rewrites the first
// frame (preamble and handshake message 1).
func rewriteFirst(f func(pre preamble) []byte) func(i int, p []byte, write func([]byte) error) error {
	return func(i int, p []byte, write func([]byte) error) error {
		if i == 0 {
			pre, _, err := parsePreamble(p)
			if err != nil {
				panic(err)
			}
			p = f(pre)
		}
		return write(p)
	}
}

func reframe(mode Mode, kid string, rest []byte) []byte {
	return append(buildPreamble(mode, kid), rest...)
}

func TestPreambleTampering(t *testing.T) {
	keys, ks := newStaticKeys(t, 2)
	old, active := ks[0], ks[1]
	ik := ClientConfig{ServerKey: &old.Public}
	xxActive := ClientConfig{KeyID: active.ID, VerifyServer: pinFingerprint(Fingerprint(active.Public))}
	cases := []struct {
		name    string
		ccfg    ClientConfig
		plain   bool // the server allows plaintext
		rewrite func(pre preamble) []byte
	}{
		{"IK kid swapped to another key", ik, false, func(pre preamble) []byte {
			return reframe(pre.mode, active.ID, pre.rest)
		}},
		// The server would pick the very same key: only the prologue
		// binding of the preamble catches this.
		{"XX kid stripped (same key)", xxActive, false, func(pre preamble) []byte {
			return reframe(pre.mode, "", pre.rest)
		}},
		{"IK mode changed to XX", ik, false, func(pre preamble) []byte {
			return reframe(ModeXX, pre.kid, pre.rest)
		}},
		{"XX mode changed to IK", ClientConfig{KeyID: old.ID, VerifyServer: pinFingerprint(Fingerprint(old.Public))}, false,
			func(pre preamble) []byte { return reframe(ModeIK, pre.kid, pre.rest) }},
		{"IK downgraded to plaintext", ik, true, func(pre preamble) []byte {
			return reframe(ModePlaintext, "", nil)
		}},
		{"XX downgraded to plaintext", xxActive, true, func(pre preamble) []byte {
			return reframe(ModePlaintext, "", nil)
		}},
		{"IK message bit flip", ik, false, func(pre preamble) []byte {
			rest := append([]byte(nil), pre.rest...)
			rest[40] ^= 1 // inside the encrypted client static key
			return reframe(pre.mode, pre.kid, rest)
		}},
		{"IK ephemeral bit flip", ik, false, func(pre preamble) []byte {
			rest := append([]byte(nil), pre.rest...)
			rest[3] ^= 1
			return reframe(pre.mode, pre.kid, rest)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m *mitm
			ct, st := mitmPair(pipePair, &m)(t)
			m.arm(rewriteFirst(tc.rewrite))
			_, _, cerr, _ := handshake(t, ct, st, tc.ccfg, ServerConfig{Keys: keys, AllowPlaintext: tc.plain})
			require.ErrorIs(t, cerr, ErrHandshake, "the client must not accept a tampered handshake")
		})
	}
}

func TestPrologueMismatch(t *testing.T) {
	for _, mode := range []Mode{ModeIK, ModeXX} {
		t.Run(mode.String(), func(t *testing.T) {
			ccfg, scfg := configs(t, mode)
			ccfg.Prologue, scfg.Prologue = []byte("orders"), []byte("orders")
			cc, sc := connect(t, pipePair, ccfg, scfg)
			exchange(t, cc, sc, []byte("same database"))

			scfg.Prologue = []byte("billing")
			ct, st := pipePair(t)
			_, _, cerr, serr := handshake(t, ct, st, ccfg, scfg)
			require.ErrorIs(t, cerr, ErrHandshake)
			require.ErrorIs(t, serr, ErrHandshake)

			scfg.Prologue = nil
			ct, st = pipePair(t)
			_, _, cerr, _ = handshake(t, ct, st, ccfg, scfg)
			require.ErrorIs(t, cerr, ErrHandshake)
		})
	}
}

func TestPlaintextPolicy(t *testing.T) {
	keys, ks := newStaticKeys(t, 1)

	ct, st := pipePair(t)
	_, _, cerr, serr := handshake(t, ct, st, ClientConfig{AllowPlaintext: true}, ServerConfig{Keys: keys})
	require.ErrorIs(t, cerr, ErrPlaintextRefused)
	require.ErrorIs(t, serr, ErrPlaintextRefused)

	// A pinned key always wins over AllowPlaintext.
	cc, sc := connect(t, pipePair, ClientConfig{ServerKey: &ks[0].Public, AllowPlaintext: true},
		ServerConfig{Keys: keys, AllowPlaintext: true})
	require.Equal(t, ModeIK, cc.Mode())
	require.Equal(t, ModeIK, sc.Mode())
	cc, sc = connect(t, pipePair, ClientConfig{VerifyServer: func([32]byte) error { return nil }, AllowPlaintext: true},
		ServerConfig{Keys: keys, AllowPlaintext: true})
	require.Equal(t, ModeXX, cc.Mode())
	require.Equal(t, ModeXX, sc.Mode())

	// Plaintext needs no keys.
	cc, sc = connect(t, pipePair, ClientConfig{AllowPlaintext: true}, ServerConfig{AllowPlaintext: true})
	require.Equal(t, ModePlaintext, cc.Mode())
	exchange(t, cc, sc, []byte("in the clear"))
}

func TestClientConfigErrors(t *testing.T) {
	pub := [32]byte{1}
	for name, cfg := range map[string]ClientConfig{
		"no mode":         {},
		"IK bad key id":   {ServerKey: &pub, KeyID: "ABC"},
		"XX bad key id":   {KeyID: "a/b", VerifyServer: func([32]byte) error { return nil }},
		"IK key id long":  {ServerKey: &pub, KeyID: string(bytes.Repeat([]byte("a"), 65))},
		"plaintext empty": {MaxMessage: 10},
	} {
		t.Run(name, func(t *testing.T) {
			ct, _ := pipePair(t)
			_, err := Client(testCtx(t), ct, cfg)
			require.ErrorContains(t, err, "client config")
			_, err = ct.ReadFrame(testCtx(t))
			require.Error(t, err, "the transport is closed on error")
		})
	}
	k, err := GenerateKey(t0)
	require.NoError(t, err)
	ct, st := pipePair(t)
	_, _, _, serr := handshake(t, ct, st, ClientConfig{ServerKey: &k.Public}, ServerConfig{})
	require.ErrorContains(t, serr, "no key source")

	// A low-order "public key" is refused by X25519 before anything is sent.
	ct, _ = pipePair(t)
	_, err = Client(testCtx(t), ct, ClientConfig{ServerKey: &pub})
	require.ErrorIs(t, err, ErrHandshake)
}

func TestRetiredKeysAndRemoval(t *testing.T) {
	keys, ks := newStaticKeys(t, 2)
	k1, k2 := ks[0], ks[1]
	retired := t0.Add(2 * time.Hour)
	k1.RetiredAt = &retired
	keys.keys[k1.ID] = k1
	scfg := ServerConfig{Keys: keys}

	// Clients pinning the retired key still connect.
	cc, sc := connect(t, pipePair, ClientConfig{ServerKey: &k1.Public}, scfg)
	require.Equal(t, k1.ID, sc.KeyID())
	exchange(t, cc, sc, []byte("overlap window"))

	// First contact gets the active key.
	cc, _ = connect(t, pipePair, ClientConfig{VerifyServer: pinFingerprint(Fingerprint(k2.Public))}, scfg)
	require.Equal(t, k2.ID, cc.KeyID())

	delete(keys.keys, k1.ID)
	ct, st := pipePair(t)
	_, _, cerr, serr := handshake(t, ct, st, ClientConfig{ServerKey: &k1.Public}, scfg)
	require.ErrorIs(t, cerr, ErrUnknownKey)
	require.ErrorIs(t, serr, ErrUnknownKey)
}

func TestHandshakeTimeout(t *testing.T) {
	keys, ks := newStaticKeys(t, 1)
	scfg := ServerConfig{Keys: keys, HandshakeTimeout: 150 * time.Millisecond}
	runServer := func(t *testing.T, st Transport) error {
		start := time.Now()
		_, err := Server(context.Background(), st, scfg)
		require.Less(t, time.Since(start), 5*time.Second)
		return err
	}

	t.Run("silent client", func(t *testing.T) {
		st, _ := rawPair(t, 0)
		err := runServer(t, st)
		require.ErrorIs(t, err, ErrHandshake)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("preamble then stall", func(t *testing.T) {
		st, raw := rawPair(t, 0)
		go func() {
			raw.Write([]byte{0, 0, 0, 104})
			raw.Write(buildPreamble(ModeIK, ks[0].ID)[:7])
		}()
		err := runServer(t, st)
		require.ErrorIs(t, err, ErrHandshake)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		_, err = raw.Write([]byte{1})
		require.Error(t, err, "the server closed the connection")
	})
	t.Run("XX client stalls before message 3", func(t *testing.T) {
		ct, st := pipePair(t)
		release := make(chan struct{})
		defer close(release)
		go Client(context.Background(), ct, ClientConfig{VerifyServer: func([32]byte) error { <-release; return nil }})
		err := runServer(t, st)
		require.ErrorIs(t, err, ErrHandshake)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("caller context", func(t *testing.T) {
		st, _ := rawPair(t, 0)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(30*time.Millisecond, cancel)
		_, err := Server(ctx, st, ServerConfig{Keys: keys})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func writeRawFrame(c net.Conn, p []byte) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(p)))
	c.Write(append(hdr[:], p...))
}

func readRawFrame(c net.Conn) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return nil, err
	}
	p := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	_, err := io.ReadFull(c, p)
	return p, err
}

func TestServerRejectsMalformedFirstFrames(t *testing.T) {
	keys, ks := newStaticKeys(t, 1)
	kid := ks[0].ID
	valid := func(mode Mode) []byte {
		hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: cipherSuite, Pattern: noise.HandshakeXX, Initiator: true,
			Prologue: buildPreamble(mode, "")})
		require.NoError(t, err)
		msg, _, _, err := hs.WriteMessage(buildPreamble(mode, ""), nil)
		require.NoError(t, err)
		return msg
	}
	cases := []struct {
		name   string
		frame  []byte
		reply  []byte // nil: the server answers nothing
		target error
	}{
		{"empty", []byte{}, nil, ErrHandshake},
		{"bad magic", []byte("HTTP/1.1 GET /"), nil, ErrHandshake},
		{"short", []byte("AZP1"), nil, ErrHandshake},
		{"bad version", []byte("AZP1\x02K\x00"), []byte{statusBadVersion}, ErrHandshake},
		{"unknown mode", []byte("AZP1\x01Q\x00"), nil, ErrHandshake},
		{"kid longer than frame", []byte("AZP1\x01K\x09abc"), nil, ErrHandshake},
		{"uppercase kid", reframe(ModeIK, "ABCDEF01", make([]byte, 96)), nil, ErrHandshake},
		{"kid with newline", reframe(ModeIK, "ab\ncd", make([]byte, 96)), nil, ErrHandshake},
		{"IK without kid", reframe(ModeIK, "", make([]byte, 96)), nil, ErrHandshake},
		{"IK short message", reframe(ModeIK, kid, make([]byte, 50)), nil, ErrHandshake},
		{"IK random message", reframe(ModeIK, kid, bytes.Repeat([]byte{7}, 96)), nil, ErrHandshake},
		{"XX short message", reframe(ModeXX, "", make([]byte, 31)), nil, ErrHandshake},
		{"XX with payload", append(valid(ModeXX), 'x'), nil, ErrHandshake},
		{"plaintext not allowed", reframe(ModePlaintext, "", nil), []byte{statusPlaintextRefuse}, ErrPlaintextRefused},
		{"plaintext with kid", reframe(ModePlaintext, kid, nil), nil, ErrHandshake},
		{"unknown kid", reframe(ModeIK, "0badc0de", make([]byte, 96)), []byte{statusUnknownKey}, ErrUnknownKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, raw := rawPair(t, 0)
			go writeRawFrame(raw, tc.frame)
			errc := make(chan error, 1)
			go func() {
				_, err := Server(testCtx(t), st, ServerConfig{Keys: keys})
				errc <- err
			}()
			reply, rerr := readRawFrame(raw)
			err := <-errc
			require.ErrorIs(t, err, tc.target)
			requireNoKeyMaterial(t, err, ks[0])
			if tc.reply == nil {
				require.ErrorIs(t, rerr, io.EOF, "no reply, connection closed")
			} else {
				require.NoError(t, rerr)
				require.Equal(t, tc.reply, reply)
				_, rerr = readRawFrame(raw)
				require.ErrorIs(t, rerr, io.EOF)
			}
		})
	}
	t.Run("valid XX first message is answered", func(t *testing.T) {
		st, raw := rawPair(t, 0)
		go writeRawFrame(raw, valid(ModeXX))
		errc := make(chan error, 1)
		go func() {
			_, err := Server(testCtx(t), st, ServerConfig{Keys: keys, HandshakeTimeout: time.Second})
			errc <- err
		}()
		reply, err := readRawFrame(raw)
		require.NoError(t, err)
		require.Equal(t, statusOK, reply[0])
		require.Len(t, reply, 1+32+48+16, "e, encrypted s, empty payload tag")
		raw.Close()
		require.ErrorIs(t, <-errc, ErrHandshake)
	})
}

func TestServerRefusesHugeFirstFrame(t *testing.T) {
	keys, _ := newStaticKeys(t, 1)
	st, raw := rawPair(t, 0)
	go raw.Write([]byte{0xff, 0xff, 0xff, 0xff})
	var before, after runtimeMem
	before.read()
	_, err := Server(testCtx(t), st, ServerConfig{Keys: keys})
	after.read()
	require.ErrorIs(t, err, ErrHandshake)
	require.ErrorIs(t, err, ErrFrameTooLarge)
	require.Less(t, after.total-before.total, uint64(1<<20))
}

func TestClientMapsServerReplies(t *testing.T) {
	pub := [32]byte{9}
	cases := []struct {
		name   string
		cfg    ClientConfig
		reply  []byte
		target error
	}{
		{"plaintext refused", ClientConfig{AllowPlaintext: true}, []byte{statusPlaintextRefuse}, ErrPlaintextRefused},
		{"plaintext refusal while IK", ClientConfig{ServerKey: &pub}, []byte{statusPlaintextRefuse}, ErrHandshake},
		{"unknown key", ClientConfig{ServerKey: &pub}, []byte{statusUnknownKey}, ErrUnknownKey},
		{"unknown key while plaintext", ClientConfig{AllowPlaintext: true}, []byte{statusUnknownKey}, ErrHandshake},
		{"bad version", ClientConfig{ServerKey: &pub}, []byte{statusBadVersion}, ErrHandshake},
		{"other status", ClientConfig{ServerKey: &pub}, []byte{99}, ErrHandshake},
		{"empty reply", ClientConfig{ServerKey: &pub}, []byte{}, ErrHandshake},
		{"plaintext with extra bytes", ClientConfig{AllowPlaintext: true}, []byte{0, 1}, ErrHandshake},
		{"IK garbage", ClientConfig{ServerKey: &pub}, append([]byte{0}, make([]byte, 48)...), ErrHandshake},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct, raw := rawPair(t, 0)
			go func() {
				if _, err := readRawFrame(raw); err == nil {
					writeRawFrame(raw, tc.reply)
				}
			}()
			_, err := Client(testCtx(t), ct, tc.cfg)
			require.ErrorIs(t, err, tc.target)
		})
	}
}
