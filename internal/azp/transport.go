package azp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// FrameSize is the largest frame a Conn reads or writes: a Noise message
// (65535 bytes). Transports carrying a Conn need maxFrame >= FrameSize; 0
// selects it.
const FrameSize = 65535

// ErrFrameTooLarge is returned for a frame longer than the transport's
// limit. A stream transport refuses it from the length header alone,
// without reading or allocating the payload.
var ErrFrameTooLarge = errors.New("azp: frame too large")

// Transport moves whole frames. Implementations must allow one concurrent
// reader and one concurrent writer. ReadFrame returns a slice the caller
// owns; WriteFrame does not keep p after it returns.
type Transport interface {
	// ReadFrame returns the next frame. A context that ends before any
	// byte of a frame arrived leaves the transport usable on stream
	// transports (a WebSocket closes); anything else is permanent.
	ReadFrame(ctx context.Context) ([]byte, error)
	// WriteFrame writes p as one frame.
	WriteFrame(ctx context.Context, p []byte) error
	// Close closes the underlying connection, unblocking pending calls.
	Close() error
	// RemoteAddr describes the peer, for logs and rate limits.
	RemoteAddr() string
}

// NewStreamTransport frames a byte stream (TCP, Unix socket, net.Pipe):
// each frame is a 4-byte big-endian length followed by the payload. Frames
// longer than maxFrame (FrameSize if <= 0) are refused. Context deadlines
// and cancellation are applied with SetReadDeadline/SetWriteDeadline.
func NewStreamTransport(c net.Conn, maxFrame int) Transport {
	if maxFrame <= 0 {
		maxFrame = FrameSize
	}
	return &streamTransport{c: c, max: maxFrame}
}

type streamTransport struct {
	c   net.Conn
	max int

	rmu  sync.Mutex
	rerr error // sticky: the stream lost frame sync or ended
	rhdr [4]byte

	wmu  sync.Mutex
	werr error
	wbuf []byte
}

// smallFrame frames up to this size are copied behind their header and
// written with one Write; larger ones use writev.
const smallFrame = 4 << 10

func (t *streamTransport) ReadFrame(ctx context.Context) ([]byte, error) {
	t.rmu.Lock()
	defer t.rmu.Unlock()
	if t.rerr != nil {
		return nil, t.rerr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var p []byte
	consumed := false
	err := withDeadline(ctx, t.c.SetReadDeadline, func() error {
		n, err := io.ReadFull(t.c, t.rhdr[:])
		consumed = n > 0
		if err != nil {
			return err
		}
		size := binary.BigEndian.Uint32(t.rhdr[:])
		if uint64(size) > uint64(t.max) {
			return fmt.Errorf("%w: %d bytes (limit %d)", ErrFrameTooLarge, size, t.max)
		}
		p = make([]byte, size)
		if _, err := io.ReadFull(t.c, p); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		return nil
	})
	if err == nil {
		return p, nil
	}
	err = ctxCause(ctx, err)
	if !consumed && isCtxErr(err) {
		return nil, err // nothing read: the next ReadFrame starts cleanly
	}
	if errors.Is(err, io.EOF) {
		t.rerr = io.EOF // clean end between frames
	} else {
		t.rerr = fmt.Errorf("azp: read frame: %w", err)
	}
	return nil, t.rerr
}

func (t *streamTransport) WriteFrame(ctx context.Context, p []byte) error {
	if len(p) > t.max {
		return fmt.Errorf("%w: %d bytes (limit %d)", ErrFrameTooLarge, len(p), t.max)
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.werr != nil {
		return t.werr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(p)))
	var n int64
	err := withDeadline(ctx, t.c.SetWriteDeadline, func() error {
		var err error
		if len(p) <= smallFrame {
			t.wbuf = append(append(t.wbuf[:0], hdr[:]...), p...)
			var m int
			m, err = t.c.Write(t.wbuf)
			n = int64(m)
		} else {
			bufs := net.Buffers{hdr[:], p}
			n, err = bufs.WriteTo(t.c)
		}
		return err
	})
	if err == nil {
		return nil
	}
	err = ctxCause(ctx, err)
	if n == 0 && isCtxErr(err) {
		return err
	}
	t.werr = fmt.Errorf("azp: write frame: %w", err)
	return t.werr
}

func (t *streamTransport) Close() error { return t.c.Close() }

func (t *streamTransport) RemoteAddr() string {
	if a := t.c.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
}

// withDeadline runs fn with the connection deadline set from ctx, and
// moves the deadline into the past if ctx is cancelled meanwhile.
func withDeadline(ctx context.Context, set func(time.Time) error, fn func() error) error {
	dl, _ := ctx.Deadline() // zero: no deadline
	// Setting a deadline fails only on a closed connection (net.Pipe also
	// fails once the peer closed); fn then reports the real state (EOF).
	_ = set(dl)
	if ctx.Done() == nil {
		return fn()
	}
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = set(time.Unix(1, 0))
		close(fired)
	})
	err := fn()
	if !stop() {
		<-fired // the deadline is reset by the next call
	}
	return err
}

// ctxCause reports a deadline hit because of ctx as the context's error.
func ctxCause(ctx context.Context, err error) error {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if ctxEnded(ctx) {
		return context.DeadlineExceeded
	}
	return err
}

// ctxEnded reports whether ctx is done or past its deadline (a connection
// deadline set from ctx can fire just before ctx's own timer).
func ctxEnded(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	dl, ok := ctx.Deadline()
	return ok && !time.Now().Before(dl)
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// NewWSTransport carries frames as binary WebSocket messages. remote
// describes the peer (the HTTP request's remote address on a server).
// Messages longer than maxFrame (FrameSize if <= 0) are refused. A context
// ending during ReadFrame or WriteFrame closes the WebSocket (a property of
// the WebSocket library). A normal closure by the peer reads as io.EOF.
func NewWSTransport(c *websocket.Conn, remote string, maxFrame int) Transport {
	if maxFrame <= 0 {
		maxFrame = FrameSize
	}
	c.SetReadLimit(int64(maxFrame))
	return &wsTransport{c: c, remote: remote, max: maxFrame}
}

type wsTransport struct {
	c      *websocket.Conn
	remote string
	max    int
}

func (t *wsTransport) ReadFrame(ctx context.Context) ([]byte, error) {
	typ, p, err := t.c.Read(ctx)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		switch websocket.CloseStatus(err) {
		case websocket.StatusNormalClosure, websocket.StatusGoingAway:
			return nil, io.EOF
		}
		if errors.Is(err, websocket.ErrMessageTooBig) {
			return nil, fmt.Errorf("%w: %w", ErrFrameTooLarge, err)
		}
		return nil, fmt.Errorf("azp: read frame: %w", err)
	}
	if typ != websocket.MessageBinary {
		// Close in the background: the close handshake waits for the peer.
		go t.c.Close(websocket.StatusUnsupportedData, "binary messages only")
		return nil, errors.New("azp: read frame: text WebSocket message")
	}
	return p, nil
}

func (t *wsTransport) WriteFrame(ctx context.Context, p []byte) error {
	if len(p) > t.max {
		return fmt.Errorf("%w: %d bytes (limit %d)", ErrFrameTooLarge, len(p), t.max)
	}
	if err := t.c.Write(ctx, websocket.MessageBinary, p); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return fmt.Errorf("azp: write frame: %w", err)
	}
	return nil
}

// Close runs the WebSocket close handshake, which waits (up to 5s) for the
// peer's reply.
func (t *wsTransport) Close() error { return t.c.Close(websocket.StatusNormalClosure, "") }

// abort closes without the close handshake: a Conn uses it after a fatal
// error, when the peer may be hostile or gone.
func (t *wsTransport) abort() error { return t.c.CloseNow() }

// aborter is implemented by transports with a faster close than Close.
type aborter interface{ abort() error }

// abortTransport closes t without waiting for the peer if it can.
func abortTransport(t Transport) error {
	if a, ok := t.(aborter); ok {
		return a.abort()
	}
	return t.Close()
}

func (t *wsTransport) RemoteAddr() string { return t.remote }
