package azp

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// mitm wraps a Transport and, once armed, hands every written frame to
// onWrite, which decides what reaches the peer.
type mitm struct {
	Transport
	mu      sync.Mutex
	armed   bool
	n       int
	onWrite func(i int, p []byte, write func([]byte) error) error
}

func (m *mitm) arm(f func(i int, p []byte, write func([]byte) error) error) {
	m.mu.Lock()
	m.armed, m.n, m.onWrite = true, 0, f
	m.mu.Unlock()
}

func (m *mitm) WriteFrame(ctx context.Context, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.armed {
		return m.Transport.WriteFrame(ctx, p)
	}
	i := m.n
	m.n++
	cp := append([]byte(nil), p...)
	return m.onWrite(i, cp, func(b []byte) error { return m.Transport.WriteFrame(ctx, b) })
}

func mitmPair(pair pairFunc, m **mitm) pairFunc {
	return func(t testing.TB) (Transport, Transport) {
		ct, st := pair(t)
		*m = &mitm{Transport: ct}
		return *m, st
	}
}

func TestTamperingIsFatal(t *testing.T) {
	const k = 2 // the attacked frame
	attacks := map[string]struct {
		good int // messages delivered before the failure
		f    func(i int, p []byte, write func([]byte) error) error
	}{
		"flip bit": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p[rand.IntN(len(p))] ^= 1 << rand.IntN(8)
			}
			return write(p)
		}},
		"flip tag bit": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p[len(p)-1] ^= 0x80
			}
			return write(p)
		}},
		"replay": {k + 1, func(i int, p []byte, write func([]byte) error) error {
			if err := write(p); err != nil || i != k {
				return err
			}
			return write(p)
		}},
		"replay older": {k + 1, func() func(int, []byte, func([]byte) error) error {
			var first []byte
			return func(i int, p []byte, write func([]byte) error) error {
				if i == 0 {
					first = p
				}
				if err := write(p); err != nil || i != k {
					return err
				}
				return write(first)
			}
		}()},
		"drop": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				return nil
			}
			return write(p)
		}},
		"reorder": {k, func() func(int, []byte, func([]byte) error) error {
			var held []byte
			return func(i int, p []byte, write func([]byte) error) error {
				switch i {
				case k:
					held = p
					return nil
				case k + 1:
					if err := write(p); err != nil {
						return err
					}
					return write(held)
				}
				return write(p)
			}
		}()},
		"truncate by one": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p = p[:len(p)-1]
			}
			return write(p)
		}},
		"truncate below tag": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p = p[:10]
			}
			return write(p)
		}},
		"empty frame": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p = nil
			}
			return write(p)
		}},
		"extend": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				p = append(p, 0)
			}
			return write(p)
		}},
		"inject": {k, func(i int, p []byte, write func([]byte) error) error {
			if i == k {
				junk := make([]byte, len(p))
				for j := range junk {
					junk[j] = byte(rand.IntN(256))
				}
				if err := write(junk); err != nil {
					return err
				}
			}
			return write(p)
		}},
	}
	for _, mode := range []Mode{ModeIK, ModeXX} {
		for name, a := range attacks {
			t.Run(mode.String()+"/"+name, func(t *testing.T) {
				ctx := testCtx(t)
				var m *mitm
				ccfg, scfg := configs(t, mode)
				cc, sc := connect(t, mitmPair(tcpPair, &m), ccfg, scfg)
				m.arm(a.f)
				go func() {
					for i := 0; i < 6; i++ {
						if cc.Send(ctx, []byte(fmt.Sprintf("message %d", i))) != nil {
							return
						}
					}
				}()
				got := 0
				var err error
				for {
					var b []byte
					if b, err = sc.Recv(ctx); err != nil {
						break
					}
					require.Equal(t, fmt.Sprintf("message %d", got), string(b))
					got++
				}
				require.ErrorIs(t, err, ErrDecrypt)
				require.Equal(t, a.good, got)

				// The Conn is unusable and its transport closed.
				_, err = sc.Recv(ctx)
				require.ErrorIs(t, err, ErrDecrypt)
				require.ErrorIs(t, sc.Send(ctx, []byte("x")), ErrDecrypt)
				_, err = cc.Recv(ctx)
				require.Error(t, err, "the peer sees the connection close")
			})
		}
	}
}

func TestTamperingOnEveryTransport(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ctx := testCtx(t)
			var m *mitm
			ccfg, scfg := configs(t, ModeIK)
			// Attack the server-to-client direction this time.
			ct, st := tr.pair(t)
			m = &mitm{Transport: st}
			cc, sc, cerr, serr := handshake(t, ct, m, ccfg, scfg)
			require.NoError(t, cerr)
			require.NoError(t, serr)
			m.arm(func(i int, p []byte, write func([]byte) error) error {
				if i == 1 { // the second chunk of the first message
					p[20] ^= 4
				}
				return write(p)
			})
			go sc.Send(ctx, pattern(200000, 9))
			_, err := cc.Recv(ctx)
			require.ErrorIs(t, err, ErrDecrypt, "no partial message is ever returned")
			_, err = sc.Recv(ctx)
			require.Error(t, err)
		})
	}
}
