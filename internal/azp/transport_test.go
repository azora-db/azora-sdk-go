package azp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// pairFunc returns two connected transports: client side, server side.
type pairFunc func(t testing.TB) (Transport, Transport)

func pipePair(t testing.TB) (Transport, Transport) {
	a, b := net.Pipe()
	ct, st := NewStreamTransport(a, 0), NewStreamTransport(b, 0)
	t.Cleanup(func() { ct.Close(); st.Close() })
	return ct, st
}

func tcpPair(t testing.TB) (Transport, Transport) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	b, ok := <-accepted
	require.True(t, ok)
	ct, st := NewStreamTransport(a, 0), NewStreamTransport(b, 0)
	t.Cleanup(func() { ct.Close(); st.Close() })
	return ct, st
}

func wsPair(t testing.TB) (Transport, Transport) {
	return wsPairLimit(t, 0)
}

func wsPairLimit(t testing.TB, maxFrame int) (Transport, Transport) {
	accepted := make(chan Transport, 1)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- NewWSTransport(c, r.RemoteAddr, maxFrame)
		<-done
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	ct := NewWSTransport(c, srv.Listener.Addr().String(), maxFrame)
	st := <-accepted
	t.Cleanup(func() {
		c.CloseNow()
		close(done)
		srv.Close()
	})
	return ct, st
}

var transports = []struct {
	name string
	pair pairFunc
}{{"pipe", pipePair}, {"tcp", tcpPair}, {"ws", wsPair}}

func testCtx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestTransportRoundTrip(t *testing.T) {
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			ctx := testCtx(t)
			a, b := tr.pair(t)
			require.NotEmpty(t, a.RemoteAddr())
			require.NotEmpty(t, b.RemoteAddr())
			sizes := []int{0, 1, 3, 4, 4096, 4097, 65534, 65535}
			go func() {
				for _, n := range sizes {
					if err := a.WriteFrame(ctx, bytes.Repeat([]byte{byte(n)}, n)); err != nil {
						return
					}
				}
			}()
			for _, n := range sizes {
				p, err := b.ReadFrame(ctx)
				require.NoError(t, err)
				require.Equal(t, n, len(p))
				require.True(t, bytes.Equal(bytes.Repeat([]byte{byte(n)}, n), p))
			}
			require.ErrorIs(t, a.WriteFrame(ctx, make([]byte, FrameSize+1)), ErrFrameTooLarge)

			// Both directions at once.
			errc := make(chan error, 1)
			go func() {
				for i := 0; i < 100; i++ {
					if err := b.WriteFrame(ctx, []byte{byte(i)}); err != nil {
						errc <- err
						return
					}
				}
				errc <- nil
			}()
			go func() {
				for i := 0; i < 100; i++ {
					_ = a.WriteFrame(ctx, []byte{byte(i), 1})
				}
			}()
			for i := 0; i < 100; i++ {
				p, err := a.ReadFrame(ctx)
				require.NoError(t, err)
				require.Equal(t, []byte{byte(i)}, p)
				p, err = b.ReadFrame(ctx)
				require.NoError(t, err)
				require.Equal(t, []byte{byte(i), 1}, p)
			}
			require.NoError(t, <-errc)

			go a.Close() // a WebSocket close waits for the peer's reply
			_, err := b.ReadFrame(ctx)
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

// rawPair is a stream transport over one end of a pipe, the other end raw.
func rawPair(t testing.TB, maxFrame int) (Transport, net.Conn) {
	a, b := net.Pipe()
	tr := NewStreamTransport(a, maxFrame)
	t.Cleanup(func() { tr.Close(); b.Close() })
	return tr, b
}

func TestStreamTransportRefusesHugeHeaderWithoutAllocating(t *testing.T) {
	ctx := testCtx(t)
	tr, raw := rawPair(t, 1<<20)
	go raw.Write([]byte{0xff, 0xff, 0xff, 0xff, 1, 2, 3})

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := tr.ReadFrame(ctx)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, ErrFrameTooLarge)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10), "must not allocate the claimed 4 GiB (or the 1 MiB limit)")
	_, err2 := tr.ReadFrame(ctx)
	require.ErrorIs(t, err2, ErrFrameTooLarge, "the stream lost sync: the error is sticky")

	// Exactly at the limit is fine; one over is not.
	tr2, raw2 := rawPair(t, 10)
	go func() {
		raw2.Write([]byte{0, 0, 0, 10})
		raw2.Write(make([]byte, 10))
		raw2.Write([]byte{0, 0, 0, 11})
	}()
	p, err := tr2.ReadFrame(ctx)
	require.NoError(t, err)
	require.Len(t, p, 10)
	_, err = tr2.ReadFrame(ctx)
	require.ErrorIs(t, err, ErrFrameTooLarge)
}

func TestStreamTransportEOF(t *testing.T) {
	ctx := testCtx(t)
	tr, raw := rawPair(t, 0)
	go func() {
		raw.Write([]byte{0, 0, 0, 5, 1, 2})
		raw.Close()
	}()
	_, err := tr.ReadFrame(ctx)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "a frame cut short is not a clean EOF")

	tr, raw = rawPair(t, 0)
	go func() {
		raw.Write([]byte{0, 0})
		raw.Close()
	}()
	_, err = tr.ReadFrame(ctx)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	tr, raw = rawPair(t, 0)
	raw.Close()
	_, err = tr.ReadFrame(ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestStreamTransportContext(t *testing.T) {
	tr, raw := rawPair(t, 0)

	// A deadline with nothing read leaves the transport usable.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	start := time.Now()
	_, err := tr.ReadFrame(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)

	// So does a cancellation.
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	_, err = tr.ReadFrame(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// An already-ended context does not touch the stream.
	_, err = tr.ReadFrame(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, tr.WriteFrame(ctx, []byte("x")), context.Canceled)

	go raw.Write([]byte{0, 0, 0, 2, 'h', 'i'})
	p, err := tr.ReadFrame(context.Background())
	require.NoError(t, err)
	require.Equal(t, "hi", string(p))

	// A deadline mid-frame breaks the stream for good.
	go raw.Write([]byte{0, 0, 0, 4, 'a'})
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = tr.ReadFrame(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	go raw.Write([]byte("bcd"))
	_, err = tr.ReadFrame(context.Background())
	require.Error(t, err, "lost frame sync is permanent")

	// A blocked write honours its context too.
	tr2, _ := rawPair(t, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = tr2.WriteFrame(ctx, []byte("nobody reads"))
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStreamTransportCloseUnblocksReader(t *testing.T) {
	tr, _ := rawPair(t, 0)
	errc := make(chan error, 1)
	go func() {
		_, err := tr.ReadFrame(context.Background())
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, tr.Close())
	select {
	case err := <-errc:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ReadFrame still blocked after Close")
	}
}

func TestWSTransportLimitsAndTypes(t *testing.T) {
	ctx := testCtx(t)

	// The read limit is enforced by the receiving side.
	a, b := wsPairLimit(t, 100)
	big := make([]byte, 101)
	require.ErrorIs(t, a.WriteFrame(ctx, big), ErrFrameTooLarge, "the sender refuses too")
	go func() {
		wa := a.(*wsTransport)
		_ = wa.c.Write(ctx, websocket.MessageBinary, big)
	}()
	_, err := b.ReadFrame(ctx)
	require.ErrorIs(t, err, ErrFrameTooLarge)

	// Text messages are refused.
	a, b = wsPair(t)
	go func() {
		wa := a.(*wsTransport)
		_ = wa.c.Write(ctx, websocket.MessageText, []byte("hello"))
	}()
	_, err = b.ReadFrame(ctx)
	require.ErrorContains(t, err, "text")

	// A context deadline is reported as such.
	a, _ = wsPair(t)
	dctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err = a.ReadFrame(dctx)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
}

func TestStreamFrameEncoding(t *testing.T) {
	// The wire format is a 4-byte big-endian length, then the payload.
	ctx := testCtx(t)
	tr, raw := rawPair(t, 0)
	payload := bytes.Repeat([]byte("z"), 5000) // the writev path
	go func() {
		_ = tr.WriteFrame(ctx, []byte("abc"))
		_ = tr.WriteFrame(ctx, payload)
	}()
	buf := make([]byte, 7)
	_, err := io.ReadFull(raw, buf)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 3, 'a', 'b', 'c'}, buf)
	hdr := make([]byte, 4)
	_, err = io.ReadFull(raw, hdr)
	require.NoError(t, err)
	require.Equal(t, uint32(5000), binary.BigEndian.Uint32(hdr))
	got := make([]byte, 5000)
	_, err = io.ReadFull(raw, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
