package azp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/flynn/noise"
)

// Conn is an established secure channel carrying whole application
// messages. Send may be called from several goroutines; Recv from one at a
// time.
type Conn struct {
	t         Transport
	mode      Mode
	serverKey [32]byte
	keyID     string
	hh        []byte
	maxMsg    int

	rekeyFrames, rekeyBytes uint64

	sendMu sync.Mutex
	send   direction
	pbuf   []byte // flag + chunk
	cbuf   []byte // ciphertext

	recvMu sync.Mutex
	recv   direction
	rbuf   []byte // message being reassembled
	inMsg  bool

	errMu     sync.Mutex
	err       error // sticky: the Conn is unusable
	closeOnce sync.Once
	closeErr  error
}

// direction is one cipher state with its rekey counters.
type direction struct {
	cs     *noise.CipherState // nil in plaintext
	frames uint64             // since the last rekey
	bytes  uint64
	rekeys uint64
}

// account counts a frame of n bytes in direction d and rekeys its cipher
// when a threshold is reached (both ends count the same frames).
func (c *Conn) account(d *direction, n int) {
	d.frames++
	d.bytes += uint64(n)
	if d.frames >= c.rekeyFrames || d.bytes >= c.rekeyBytes {
		d.cs.Rekey()
		d.frames, d.bytes = 0, 0
		d.rekeys++
	}
}

// newConn returns a Conn over t; the handshake sets its cipher states.
func newConn(t Transport, mode Mode, maxMsg int) *Conn {
	if maxMsg <= 0 {
		maxMsg = DefaultMaxMessage
	}
	return &Conn{t: t, mode: mode, maxMsg: maxMsg, rekeyFrames: rekeyInterval, rekeyBytes: rekeyBytes}
}

// loadErr returns the sticky error that made the Conn unusable, or nil.
func (c *Conn) loadErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

// fail makes the Conn unusable with err and closes the transport.
func (c *Conn) fail(err error) error {
	c.errMu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errMu.Unlock()
	c.closeTransport(true)
	return err
}

// closeTransport closes the transport once; abort skips a graceful close
// (a WebSocket close handshake).
func (c *Conn) closeTransport(abort bool) error {
	c.closeOnce.Do(func() {
		if abort {
			c.closeErr = abortTransport(c.t)
		} else {
			c.closeErr = c.t.Close()
		}
	})
	return c.closeErr
}

// Send sends msg as one message, split into frames of at most FrameSize
// bytes. Concurrent calls are serialized. A message over MaxMessage is
// refused (ErrMessageTooLarge) and the Conn stays usable; any other
// failure, including ctx ending mid-write, closes the Conn, since the peer
// could not decrypt what follows.
func (c *Conn) Send(ctx context.Context, msg []byte) error {
	if len(msg) > c.maxMsg {
		return fmt.Errorf("%w: %d bytes (limit %d)", ErrMessageTooLarge, len(msg), c.maxMsg)
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err := c.loadErr(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		n := min(len(msg), maxChunk)
		flag := flagMore
		if n == len(msg) {
			flag = flagFinal
		}
		c.pbuf = append(append(c.pbuf[:0], flag), msg[:n]...)
		frame := c.pbuf
		if c.send.cs != nil {
			out, err := c.send.cs.Encrypt(c.cbuf[:0], nil, c.pbuf)
			if err != nil {
				return c.fail(fmt.Errorf("azp: encrypt: %w", err))
			}
			c.cbuf = out
			frame = out
			c.account(&c.send, len(out))
		}
		if err := c.t.WriteFrame(ctx, frame); err != nil {
			if e := c.loadErr(); e != nil {
				return e
			}
			return c.fail(err)
		}
		msg = msg[n:]
		if flag == flagFinal {
			return nil
		}
	}
}

// Recv returns the next message. Call it from one goroutine at a time. If
// ctx ends before a frame starts arriving, Recv returns ctx's error and the
// Conn stays usable (on stream transports; a WebSocket closes). A frame
// that fails authentication returns ErrDecrypt and closes the Conn. A
// clean close by the peer returns io.EOF.
func (c *Conn) Recv(ctx context.Context) ([]byte, error) {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	if err := c.loadErr(); err != nil {
		return nil, err
	}
	for {
		frame, err := c.t.ReadFrame(ctx)
		if err != nil {
			if e := c.loadErr(); e != nil {
				return nil, e
			}
			if isCtxErr(err) && ctxEnded(ctx) {
				return nil, err // a partial message stays buffered
			}
			if errors.Is(err, io.EOF) && c.inMsg {
				err = io.ErrUnexpectedEOF
			}
			return nil, c.fail(err)
		}
		plain := frame
		if c.recv.cs != nil {
			if len(frame) < tagSize+1 || len(frame) > FrameSize {
				return nil, c.fail(ErrDecrypt)
			}
			plain, err = c.recv.cs.Decrypt(frame[:0], nil, frame)
			if err != nil {
				return nil, c.fail(ErrDecrypt)
			}
			c.account(&c.recv, len(frame))
		} else if len(frame) < 1 || len(frame) > maxChunk+1 {
			return nil, c.fail(fmt.Errorf("%w: frame of %d bytes", ErrProtocol, len(frame)))
		}
		flag, data := plain[0], plain[1:]
		switch {
		case flag != flagMore && flag != flagFinal:
			return nil, c.fail(fmt.Errorf("%w: chunk flag %d", ErrProtocol, flag))
		case flag == flagMore && len(data) == 0:
			return nil, c.fail(fmt.Errorf("%w: empty continuation chunk", ErrProtocol))
		case len(c.rbuf)+len(data) > c.maxMsg:
			return nil, c.fail(fmt.Errorf("%w: incoming message over %d bytes", ErrMessageTooLarge, c.maxMsg))
		case flag == flagFinal && !c.inMsg:
			return data, nil // single-frame message: no copy
		}
		c.rbuf = appendCapped(c.rbuf, data, c.maxMsg)
		c.inMsg = true
		if flag == flagFinal {
			msg := c.rbuf
			c.rbuf, c.inMsg = nil, false
			return msg, nil
		}
	}
}

// appendCapped appends data to b, never growing b's capacity beyond limit
// (len(b)+len(data) <= limit is checked by the caller).
func appendCapped(b, data []byte, limit int) []byte {
	need := len(b) + len(data)
	if need > cap(b) {
		nc := min(max(2*cap(b), need, 2*FrameSize), limit)
		nb := make([]byte, len(b), nc)
		copy(nb, b)
		b = nb
	}
	return append(b, data...)
}

// Close closes the channel and its transport. Pending and later Send and
// Recv calls return ErrClosed.
func (c *Conn) Close() error {
	c.errMu.Lock()
	if c.err == nil {
		c.err = ErrClosed
	}
	c.errMu.Unlock()
	return c.closeTransport(false)
}

// Mode returns the channel's security mode.
func (c *Conn) Mode() Mode { return c.mode }

// ServerKey returns the server static public key the channel is bound to
// (zero in plaintext).
func (c *Conn) ServerKey() [32]byte { return c.serverKey }

// KeyID returns the id of ServerKey ("" in plaintext).
func (c *Conn) KeyID() string { return c.keyID }

// HandshakeHash returns the Noise handshake hash, equal on both ends and
// unique per connection: a channel binding value for authentication at the
// application layer. It is nil in plaintext.
func (c *Conn) HandshakeHash() []byte {
	if c.hh == nil {
		return nil
	}
	return append([]byte(nil), c.hh...)
}

// RemoteAddr returns the transport's remote address.
func (c *Conn) RemoteAddr() string { return c.t.RemoteAddr() }
