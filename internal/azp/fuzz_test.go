package azp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flynn/noise"
)

// readerConn is a net.Conn reading from a fixed byte slice.
type readerConn struct {
	r *bytes.Reader
}

func (c *readerConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *readerConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *readerConn) Close() error                     { return nil }
func (c *readerConn) LocalAddr() net.Addr              { return nil }
func (c *readerConn) RemoteAddr() net.Addr             { return nil }
func (c *readerConn) SetDeadline(time.Time) error      { return nil }
func (c *readerConn) SetReadDeadline(time.Time) error  { return nil }
func (c *readerConn) SetWriteDeadline(time.Time) error { return nil }

func FuzzStreamTransport(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 3, 'a', 'b', 'c', 0, 0, 0, 1, 'x'})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 4, 1, 1, 2})
	f.Add([]byte{0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		const max = 1024
		tr := NewStreamTransport(&readerConn{r: bytes.NewReader(data)}, max)
		consumed := 0
		for {
			p, err := tr.ReadFrame(context.Background())
			if err != nil {
				if consumed == len(data) && !errors.Is(err, io.EOF) {
					t.Fatalf("clean end reported as %v", err)
				}
				if _, again := tr.ReadFrame(context.Background()); again == nil {
					t.Fatal("errors are sticky")
				}
				return
			}
			if len(p) > max {
				t.Fatalf("frame of %d bytes over the %d limit", len(p), max)
			}
			if !bytes.Equal(p, data[consumed+4:consumed+4+len(p)]) {
				t.Fatal("frame payload differs from the input")
			}
			consumed += 4 + len(p)
		}
	})
}

// memTransport is an in-memory Transport fed from a channel.
type memTransport struct {
	in        chan []byte
	out       chan []byte
	closed    chan struct{} // shared by both ends
	closeOnce *sync.Once
}

func newMemPair() (*memTransport, *memTransport) {
	ab, ba := make(chan []byte, 64), make(chan []byte, 64)
	done, once := make(chan struct{}), new(sync.Once)
	return &memTransport{in: ba, out: ab, closed: done, closeOnce: once},
		&memTransport{in: ab, out: ba, closed: done, closeOnce: once}
}

func (m *memTransport) ReadFrame(ctx context.Context) ([]byte, error) {
	select {
	case p, ok := <-m.in:
		if !ok {
			return nil, io.EOF
		}
		return append([]byte(nil), p...), nil
	case <-m.closed:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *memTransport) WriteFrame(ctx context.Context, p []byte) error {
	select {
	case <-m.closed:
		return net.ErrClosed
	default:
	}
	select {
	case m.out <- append([]byte(nil), p...):
		return nil
	case <-m.closed:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *memTransport) Close() error {
	m.closeOnce.Do(func() { close(m.closed) })
	return nil
}

func (m *memTransport) RemoteAddr() string { return "mem" }

// sinkTransport replays fixed frames, then reports EOF; writes vanish.
type sinkTransport struct{ frames [][]byte }

func (s *sinkTransport) ReadFrame(context.Context) ([]byte, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	p := s.frames[0]
	s.frames = s.frames[1:]
	return append([]byte(nil), p...), nil
}
func (s *sinkTransport) WriteFrame(context.Context, []byte) error { return nil }
func (s *sinkTransport) Close() error                             { return nil }
func (s *sinkTransport) RemoteAddr() string                       { return "sink" }

func FuzzServerHandshake(f *testing.F) {
	keys, ks := newStaticKeys(f, 1)
	k := ks[0]
	// Seeds: real first messages of each mode. A genuine IK message 1
	// completes the server side (IK is one round trip); no mutation of it
	// may.
	var genuineIK []byte
	for _, mode := range []Mode{ModeIK, ModeXX} {
		pre := buildPreamble(mode, k.ID)
		cfg := noise.Config{CipherSuite: cipherSuite, Pattern: noise.HandshakeXX, Initiator: true, Prologue: pre}
		if mode == ModeIK {
			static, _ := cipherSuite.GenerateKeypair(nil)
			cfg.Pattern, cfg.StaticKeypair, cfg.PeerStatic = noise.HandshakeIK, static, k.Public[:]
		}
		hs, err := noise.NewHandshakeState(cfg)
		if err != nil {
			f.Fatal(err)
		}
		msg, _, _, err := hs.WriteMessage(append([]byte(nil), pre...), nil)
		if err != nil {
			f.Fatal(err)
		}
		if mode == ModeIK {
			genuineIK = msg
		}
		f.Add(msg, make([]byte, 64))
	}
	f.Add(buildPreamble(ModePlaintext, ""), []byte{})
	f.Add([]byte("AZP1\x02K\x00"), []byte{})
	f.Add([]byte("AZP1\x01X\x00"), make([]byte, 80))
	f.Add([]byte{}, []byte{})
	f.Fuzz(func(t *testing.T, first, third []byte) {
		tr := &sinkTransport{frames: [][]byte{first, third}}
		c, err := Server(context.Background(), tr, ServerConfig{Keys: keys, HandshakeTimeout: time.Second})
		if err == nil && !bytes.Equal(first, genuineIK) {
			c.Close()
			t.Fatalf("handshake succeeded from fuzzed input (mode %v)", c.Mode())
		}
		if err != nil {
			requireNoKeyMaterial(t, err, k)
		}
	})
}

// FuzzRecvChunks feeds fuzzed frames to the receiving side of an
// established channel: encrypted (after a real handshake) and plaintext
// (which exercises reassembly with attacker-chosen chunks).
func FuzzRecvChunks(f *testing.F) {
	keys, ks := newStaticKeys(f, 1)
	frames := func(chunks ...[]byte) []byte {
		var b []byte
		for _, c := range chunks {
			b = binary.BigEndian.AppendUint16(b, uint16(len(c)))
			b = append(b, c...)
		}
		return b
	}
	f.Add(false, frames([]byte{flagFinal, 'h', 'i'}))
	f.Add(false, frames(append([]byte{flagMore}, make([]byte, 100)...), []byte{flagFinal, 1}))
	f.Add(false, frames([]byte{flagMore}, []byte{2}))
	f.Add(false, frames([]byte{}, []byte{flagFinal}))
	f.Add(true, frames(make([]byte, 17), make([]byte, 40)))
	f.Add(true, frames([]byte{1, 2, 3}))
	f.Fuzz(func(t *testing.T, encrypted bool, data []byte) {
		ct, st := newMemPair()
		ccfg := ClientConfig{AllowPlaintext: true}
		scfg := ServerConfig{Keys: keys, AllowPlaintext: true, MaxMessage: 3000}
		if encrypted {
			ccfg = ClientConfig{ServerKey: &ks[0].Public}
		}
		ch := make(chan *Conn, 1)
		go func() {
			c, _ := Server(context.Background(), st, scfg)
			ch <- c
		}()
		cc, err := Client(context.Background(), ct, ccfg)
		sc := <-ch
		if err != nil || sc == nil {
			t.Fatalf("handshake: %v", err)
		}
		defer cc.Close()
		// Write the fuzzed frames straight into the server's inbox, then end
		// the stream.
		go func() {
			for len(data) >= 2 {
				n := int(binary.BigEndian.Uint16(data))
				data = data[2:]
				n = min(n, len(data))
				select {
				case ct.out <- data[:n]:
				case <-ct.closed: // the server gave up
					return
				}
				data = data[n:]
			}
			close(ct.out)
		}()
		for i := 0; ; i++ {
			msg, err := sc.Recv(context.Background())
			if err != nil {
				if encrypted && i == 0 && !errors.Is(err, ErrDecrypt) && !errors.Is(err, io.EOF) {
					t.Fatalf("forged ciphertext gave %v", err)
				}
				break
			}
			if encrypted {
				t.Fatal("a forged frame decrypted")
			}
			if len(msg) > scfg.MaxMessage {
				t.Fatalf("message of %d bytes over MaxMessage", len(msg))
			}
		}
		if _, err := sc.Recv(context.Background()); err == nil {
			t.Fatal("errors are sticky")
		}
	})
}
