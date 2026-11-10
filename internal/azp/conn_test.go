package azp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRoundTripSizes(t *testing.T) {
	const maxMsg = 1 << 20
	for _, tr := range transports {
		for _, mode := range modes {
			t.Run(tr.name+"/"+mode.String(), func(t *testing.T) {
				ccfg, scfg := configs(t, mode)
				ccfg.MaxMessage, scfg.MaxMessage = maxMsg, maxMsg
				cc, sc := connect(t, tr.pair, ccfg, scfg)
				for i, n := range []int{0, 1, maxChunk - 1, maxChunk, maxChunk + 1, 65519, 65520, 2 * maxChunk, 200000, maxMsg} {
					exchange(t, cc, sc, pattern(n, byte(i)))
					exchange(t, sc, cc, pattern(n, byte(i+100)))
				}
				ctx := testCtx(t)
				require.ErrorIs(t, cc.Send(ctx, make([]byte, maxMsg+1)), ErrMessageTooLarge)
				require.ErrorIs(t, sc.Send(ctx, make([]byte, maxMsg+1)), ErrMessageTooLarge)
				// Refusing to send leaves the channel usable.
				exchange(t, cc, sc, []byte("still alive"))
				exchange(t, sc, cc, []byte("still alive"))
			})
		}
	}
}

func TestDefaultMaxMessage(t *testing.T) {
	ccfg, scfg := configs(t, ModeIK)
	cc, _ := connect(t, pipePair, ccfg, scfg)
	require.ErrorIs(t, cc.Send(testCtx(t), make([]byte, DefaultMaxMessage+1)), ErrMessageTooLarge)
}

func TestReceiverEnforcesMaxMessage(t *testing.T) {
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			ctx := testCtx(t)
			ccfg, scfg := configs(t, mode)
			ccfg.MaxMessage, scfg.MaxMessage = 1<<20, 100000
			cc, sc := connect(t, tcpPair, ccfg, scfg)
			exchange(t, cc, sc, pattern(100000, 1))
			go cc.Send(ctx, pattern(100001, 2))
			_, err := sc.Recv(ctx)
			require.ErrorIs(t, err, ErrMessageTooLarge)
			_, err = sc.Recv(ctx)
			require.ErrorIs(t, err, ErrMessageTooLarge, "fatal and sticky")
			_, err = cc.Recv(ctx)
			require.Error(t, err, "the receiver closed the connection")
		})
	}
}

func TestReassemblyNeverAllocatesPastMaxMessage(t *testing.T) {
	b := appendCapped(nil, make([]byte, maxChunk), 100000)
	require.LessOrEqual(t, cap(b), 100000)
	b = appendCapped(b, make([]byte, 100000-maxChunk), 100000)
	require.Equal(t, 100000, len(b))
	require.Equal(t, 100000, cap(b))
}

func TestConcurrentSends(t *testing.T) {
	const senders, perSender = 8, 30
	for _, tr := range transports {
		for _, mode := range modes {
			t.Run(tr.name+"/"+mode.String(), func(t *testing.T) {
				ctx := testCtx(t)
				ccfg, scfg := configs(t, mode)
				cc, sc := connect(t, tr.pair, ccfg, scfg)
				size := func(g, i int) int {
					if (g+i)%7 == 0 {
						return 70000 + g*1000 + i // several frames
					}
					return 8 + (g*131+i*17)%3000
				}
				var wg sync.WaitGroup
				errc := make(chan error, senders)
				for g := 0; g < senders; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < perSender; i++ {
							msg := pattern(size(g, i), byte(g))
							binary.BigEndian.PutUint32(msg[0:], uint32(g))
							binary.BigEndian.PutUint32(msg[4:], uint32(i))
							if err := cc.Send(ctx, msg); err != nil {
								errc <- err
								return
							}
						}
					}()
				}
				next := make([]int, senders)
				for n := 0; n < senders*perSender; n++ {
					got, err := sc.Recv(ctx)
					require.NoError(t, err)
					g, i := int(binary.BigEndian.Uint32(got)), int(binary.BigEndian.Uint32(got[4:]))
					require.Less(t, g, senders)
					require.Equal(t, next[g], i, "messages of one sender arrive in order")
					next[g]++
					want := pattern(size(g, i), byte(g))
					copy(want, got[:8])
					require.True(t, bytes.Equal(want, got), "message %d/%d interleaved or corrupted", g, i)
				}
				wg.Wait()
				close(errc)
				for err := range errc {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestBidirectionalStreaming(t *testing.T) {
	const n = 10000
	for _, tr := range transports {
		for _, mode := range modes {
			t.Run(tr.name+"/"+mode.String(), func(t *testing.T) {
				ctx := testCtx(t)
				ccfg, scfg := configs(t, mode)
				cc, sc := connect(t, tr.pair, ccfg, scfg)
				stream := func(a, b *Conn, seed byte) error {
					errc := make(chan error, 1)
					go func() {
						for i := 0; i < n; i++ {
							msg := pattern(4+i%200, seed)
							binary.BigEndian.PutUint32(msg, uint32(i))
							if err := a.Send(ctx, msg); err != nil {
								errc <- err
								return
							}
						}
						errc <- nil
					}()
					for i := 0; i < n; i++ {
						got, err := b.Recv(ctx)
						if err != nil {
							return err
						}
						want := pattern(4+i%200, seed)
						binary.BigEndian.PutUint32(want, uint32(i))
						if !bytes.Equal(want, got) {
							return fmt.Errorf("message %d corrupted", i)
						}
					}
					return <-errc
				}
				var wg sync.WaitGroup
				var e1, e2 error
				wg.Add(2)
				go func() { defer wg.Done(); e1 = stream(cc, sc, 1) }()
				go func() { defer wg.Done(); e2 = stream(sc, cc, 2) }()
				wg.Wait()
				require.NoError(t, e1)
				require.NoError(t, e2)
			})
		}
	}
}

func TestConnAccessors(t *testing.T) {
	for _, tr := range transports {
		for _, mode := range modes {
			t.Run(tr.name+"/"+mode.String(), func(t *testing.T) {
				ccfg, scfg := configs(t, mode)
				cc, sc := connect(t, tr.pair, ccfg, scfg)
				require.Equal(t, mode, cc.Mode())
				require.Equal(t, mode, sc.Mode())
				require.NotEmpty(t, cc.RemoteAddr())
				require.NotEmpty(t, sc.RemoteAddr())
				if mode == ModePlaintext {
					require.Nil(t, cc.HandshakeHash())
					require.Nil(t, sc.HandshakeHash())
					require.Equal(t, [32]byte{}, cc.ServerKey())
					require.Equal(t, [32]byte{}, sc.ServerKey())
					require.Empty(t, cc.KeyID())
					return
				}
				k, _ := scfg.Keys.Active()
				require.Equal(t, k.Public, cc.ServerKey())
				require.Equal(t, k.Public, sc.ServerKey())
				require.Equal(t, k.ID, cc.KeyID())
				require.Equal(t, k.ID, sc.KeyID())
				require.Len(t, cc.HandshakeHash(), 32)
				require.Equal(t, cc.HandshakeHash(), sc.HandshakeHash(), "channel binding agrees")
				hh := cc.HandshakeHash()
				hh[0] ^= 1
				require.NotEqual(t, hh, cc.HandshakeHash(), "HandshakeHash returns a copy")
			})
		}
	}
	require.Equal(t, "IK", ModeIK.String())
	require.Equal(t, "XX", ModeXX.String())
	require.Equal(t, "plaintext", ModePlaintext.String())
	require.Equal(t, "Mode(0x5a)", Mode('Z').String())
}

func TestHandshakeHashUniquePerConnection(t *testing.T) {
	for _, mode := range []Mode{ModeIK, ModeXX} {
		t.Run(mode.String(), func(t *testing.T) {
			ccfg, scfg := configs(t, mode)
			a1, b1 := connect(t, pipePair, ccfg, scfg)
			a2, b2 := connect(t, pipePair, ccfg, scfg)
			require.Equal(t, a1.ServerKey(), a2.ServerKey(), "same server key")
			require.NotEqual(t, a1.HandshakeHash(), a2.HandshakeHash())
			require.Equal(t, a1.HandshakeHash(), b1.HandshakeHash())
			require.Equal(t, a2.HandshakeHash(), b2.HandshakeHash())
			require.NotEqual(t, a1.send.cs.UnsafeKey(), a2.send.cs.UnsafeKey(), "fresh ephemeral keys per connection")
			require.NotEqual(t, a1.send.cs.UnsafeKey(), a1.recv.cs.UnsafeKey(), "one key per direction")
		})
	}
}

func TestCloseSemantics(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ctx := testCtx(t)
			ccfg, scfg := configs(t, ModeIK)
			cc, sc := connect(t, tr.pair, ccfg, scfg)

			// Close unblocks a pending Recv with ErrClosed.
			errc := make(chan error, 1)
			go func() {
				_, err := sc.Recv(ctx)
				errc <- err
			}()
			// And the peer sees a clean EOF.
			peer := make(chan error, 1)
			go func() {
				_, err := cc.Recv(ctx)
				peer <- err
			}()
			time.Sleep(20 * time.Millisecond)
			require.NoError(t, sc.Close())
			require.ErrorIs(t, <-errc, ErrClosed)
			require.ErrorIs(t, <-peer, io.EOF)
			require.ErrorIs(t, sc.Send(ctx, []byte("x")), ErrClosed)
			_, err := sc.Recv(ctx)
			require.ErrorIs(t, err, ErrClosed)
			sc.Close() // idempotent
		})
	}
}

func TestRecvTimeoutIsNotFatal(t *testing.T) {
	for _, tr := range transports[:2] { // a WebSocket closes when a read's context ends
		t.Run(tr.name, func(t *testing.T) {
			ccfg, scfg := configs(t, ModeIK)
			cc, sc := connect(t, tr.pair, ccfg, scfg)
			for i := 0; i < 3; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				_, err := sc.Recv(ctx)
				cancel()
				require.ErrorIs(t, err, context.DeadlineExceeded)
				exchange(t, cc, sc, []byte("after timeout"))
				exchange(t, sc, cc, []byte("both ways"))
			}
		})
	}
}

func TestStreamCutMidMessage(t *testing.T) {
	ctx := testCtx(t)
	var m *mitm
	ccfg, scfg := configs(t, ModeIK)
	cc, sc := connect(t, mitmPair(tcpPair, &m), ccfg, scfg)
	m.arm(func(i int, p []byte, write func([]byte) error) error {
		if i == 1 {
			m.Transport.Close()
			return errors.New("cut")
		}
		return write(p)
	})
	go cc.Send(ctx, pattern(200000, 3))
	_, err := sc.Recv(ctx)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestPartialMessageSurvivesRecvTimeout(t *testing.T) {
	var m *mitm
	ccfg, scfg := configs(t, ModeXX)
	cc, sc := connect(t, mitmPair(tcpPair, &m), ccfg, scfg)
	release := make(chan struct{})
	m.arm(func(i int, p []byte, write func([]byte) error) error {
		if i == 2 {
			<-release
		}
		return write(p)
	})
	msg := pattern(3*maxChunk, 5)
	errc := make(chan error, 1)
	go func() { errc <- cc.Send(testCtx(t), msg) }()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err := sc.Recv(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(release)
	got, err := sc.Recv(testCtx(t))
	require.NoError(t, err)
	require.True(t, bytes.Equal(msg, got))
	require.NoError(t, <-errc)
}

func TestAuthenticatedProtocolViolations(t *testing.T) {
	cases := map[string][]byte{
		"bad flag":           {7, 'x'},
		"empty continuation": {flagMore},
	}
	for name, plain := range cases {
		for _, mode := range []Mode{ModeIK, ModePlaintext} {
			t.Run(name+"/"+mode.String(), func(t *testing.T) {
				ctx := testCtx(t)
				ccfg, scfg := configs(t, mode)
				cc, sc := connect(t, tcpPair, ccfg, scfg)
				frame := plain
				if cc.send.cs != nil {
					var err error
					frame, err = cc.send.cs.Encrypt(nil, nil, plain)
					require.NoError(t, err)
				}
				require.NoError(t, cc.t.WriteFrame(ctx, frame))
				_, err := sc.Recv(ctx)
				require.ErrorIs(t, err, ErrProtocol)
			})
		}
	}
}
