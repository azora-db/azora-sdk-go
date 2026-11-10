package azp

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/stretchr/testify/require"
)

// staticKeys is an in-memory KeySource.
type staticKeys struct {
	active string
	keys   map[string]KeyPair
}

func (s staticKeys) Active() (KeyPair, bool) { k, ok := s.keys[s.active]; return k, ok }
func (s staticKeys) Get(id string) (KeyPair, bool) {
	k, ok := s.keys[id]
	return k, ok
}

func newStaticKeys(t testing.TB, n int) (staticKeys, []KeyPair) {
	t.Helper()
	s := staticKeys{keys: map[string]KeyPair{}}
	var out []KeyPair
	for i := 0; i < n; i++ {
		k, err := GenerateKey(t0.Add(time.Duration(i) * time.Second))
		require.NoError(t, err)
		s.keys[k.ID] = k
		s.active = k.ID
		out = append(out, k)
	}
	return s, out
}

func pinFingerprint(fp string) func([32]byte) error {
	return func(pub [32]byte) error {
		if got := Fingerprint(pub); got != fp {
			return fmt.Errorf("server fingerprint %s, want %s", got, fp)
		}
		return nil
	}
}

var modes = []Mode{ModeIK, ModeXX, ModePlaintext}

// configs returns matching client and server configs for mode.
func configs(t testing.TB, mode Mode) (ClientConfig, ServerConfig) {
	t.Helper()
	keys, ks := newStaticKeys(t, 1)
	k := ks[0]
	scfg := ServerConfig{Keys: keys, HandshakeTimeout: 10 * time.Second}
	var ccfg ClientConfig
	switch mode {
	case ModeIK:
		ccfg.ServerKey = &k.Public
	case ModeXX:
		ccfg.VerifyServer = pinFingerprint(Fingerprint(k.Public))
	case ModePlaintext:
		ccfg.AllowPlaintext = true
		scfg.AllowPlaintext = true
	}
	return ccfg, scfg
}

type hsResult struct {
	conn *Conn
	err  error
}

// handshake runs Client and Server concurrently.
func handshake(t testing.TB, ct, st Transport, ccfg ClientConfig, scfg ServerConfig) (cc, sc *Conn, cerr, serr error) {
	t.Helper()
	ctx := testCtx(t)
	ch := make(chan hsResult, 1)
	go func() {
		c, err := Server(ctx, st, scfg)
		ch <- hsResult{c, err}
	}()
	cc, cerr = Client(ctx, ct, ccfg)
	r := <-ch
	// Close both ends at once: a WebSocket close waits for the peer's reply.
	t.Cleanup(func() {
		if r.conn != nil {
			go r.conn.Close()
		}
		if cc != nil {
			cc.Close()
		}
	})
	return cc, r.conn, cerr, r.err
}

func connect(t testing.TB, pair pairFunc, ccfg ClientConfig, scfg ServerConfig) (*Conn, *Conn) {
	t.Helper()
	ct, st := pair(t)
	cc, sc, cerr, serr := handshake(t, ct, st, ccfg, scfg)
	require.NoError(t, cerr)
	require.NoError(t, serr)
	return cc, sc
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ seed
	}
	return b
}

// exchange sends msg from a to b and checks it arrives intact.
func exchange(t testing.TB, a, b *Conn, msg []byte) {
	t.Helper()
	ctx := testCtx(t)
	errc := make(chan error, 1)
	go func() { errc <- a.Send(ctx, msg) }()
	got, err := b.Recv(ctx)
	require.NoError(t, err)
	require.NoError(t, <-errc)
	require.Equal(t, len(msg), len(got))
	require.True(t, bytes.Equal(msg, got), "message of %d bytes corrupted", len(msg))
}

func TestRekey(t *testing.T) {
	saved := rekeyInterval
	rekeyInterval = 3
	t.Cleanup(func() { rekeyInterval = saved })

	for _, mode := range []Mode{ModeIK, ModeXX} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx := testCtx(t)
			var m *mitm
			ccfg, scfg := configs(t, mode)
			cc, sc := connect(t, mitmPair(tcpPair, &m), ccfg, scfg)
			require.Equal(t, uint64(3), cc.rekeyFrames)
			initial := sc.recv.cs.UnsafeKey()
			var captured [][]byte
			m.arm(func(i int, p []byte, write func([]byte) error) error {
				captured = append(captured, p)
				return write(p)
			})
			for i := 0; i < 100; i++ {
				n := i * 977
				if i%10 == 0 {
					n = 2*maxChunk + i
				}
				exchange(t, cc, sc, pattern(n, byte(i)))
				exchange(t, sc, cc, pattern(n, byte(i+1)))
			}
			require.Greater(t, cc.send.rekeys, uint64(40))
			require.Equal(t, cc.send.rekeys, sc.recv.rekeys)
			require.Equal(t, sc.send.rekeys, cc.recv.rekeys)
			require.Equal(t, cc.send.cs.UnsafeKey(), sc.recv.cs.UnsafeKey(), "both ends rekeyed in step")
			require.Equal(t, sc.send.cs.UnsafeKey(), cc.recv.cs.UnsafeKey())
			require.NotEqual(t, initial, sc.recv.cs.UnsafeKey())

			// Frame 0 was encrypted under the initial key with nonce 0: it
			// opens with that key, but not with the current key even at
			// the same nonce, so rewinding the counter would not help a
			// replay either.
			_, err := noise.UnsafeNewCipherState(cipherSuite, initial, 0).Decrypt(nil, nil, captured[0])
			require.NoError(t, err)
			_, err = noise.UnsafeNewCipherState(cipherSuite, sc.recv.cs.UnsafeKey(), 0).Decrypt(nil, nil, captured[0])
			require.Error(t, err)

			// And a replay on the wire is fatal.
			m.arm(func(i int, p []byte, write func([]byte) error) error { return write(captured[0]) })
			go cc.Send(ctx, []byte("ignored"))
			_, err = sc.Recv(ctx)
			require.ErrorIs(t, err, ErrDecrypt)
		})
	}
}

func TestRekeyByBytes(t *testing.T) {
	saved := rekeyBytes
	rekeyBytes = 200000
	t.Cleanup(func() { rekeyBytes = saved })
	ccfg, scfg := configs(t, ModeIK)
	cc, sc := connect(t, tcpPair, ccfg, scfg)
	for i := 0; i < 10; i++ {
		exchange(t, cc, sc, pattern(150000, byte(i)))
	}
	require.Equal(t, uint64(1<<20), cc.rekeyFrames)
	require.GreaterOrEqual(t, cc.send.rekeys, uint64(6))
	require.Equal(t, cc.send.rekeys, sc.recv.rekeys)
}

type runtimeMem struct{ total uint64 }

func (m *runtimeMem) read() {
	var s runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&s)
	m.total = s.TotalAlloc
}

func BenchmarkSecureRoundTrip(b *testing.B) {
	for _, mode := range []Mode{ModeIK, ModePlaintext} {
		for _, size := range []int{256, 64 << 10} {
			b.Run(fmt.Sprintf("%s/%dB", mode, size), func(b *testing.B) {
				ccfg, scfg := configs(b, mode)
				cc, sc := connect(b, pipePair, ccfg, scfg)
				ctx := context.Background()
				go func() { // echo
					for {
						msg, err := sc.Recv(ctx)
						if err != nil || sc.Send(ctx, msg) != nil {
							return
						}
					}
				}()
				msg := pattern(size, 1)
				b.SetBytes(int64(2 * size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := cc.Send(ctx, msg); err != nil {
						b.Fatal(err)
					}
					if _, err := cc.Recv(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkHandshake(b *testing.B) {
	for _, mode := range []Mode{ModeIK, ModeXX} {
		b.Run(mode.String(), func(b *testing.B) {
			ccfg, scfg := configs(b, mode)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ct, st := newMemPair()
				done := make(chan error, 1)
				go func() {
					_, err := Server(context.Background(), st, scfg)
					done <- err
				}()
				if _, err := Client(context.Background(), ct, ccfg); err != nil {
					b.Fatal(err)
				}
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
