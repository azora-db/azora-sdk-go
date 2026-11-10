package azora

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/azora-db/azora-go/internal/azp"
)

// conn is one established session.
type conn struct {
	db      *DB
	l       *link
	az      *azp.Conn
	abort   func()
	welcome Welcome
	hb      time.Duration
	started time.Time

	ctx    context.Context // ends when the connection dies
	cancel context.CancelCauseFunc
	ctrl   chan azp.Msg // pongs and pings, written by writeLoop

	lastRecv atomic.Int64
	authGen  atomic.Uint64
	credGen  uint64       // the DB's credGen this session's credential belongs to; guarded by db.authMu
	goAway   atomic.Int64 // reconnect delay hint (ns), 0 = none
	inflight atomic.Int32 // requests waiting for an answer

	// Rate limiting: once the server asked a request to retry later, the
	// connection's requests go through the lane one at a time, each after
	// the server's hint, until one passes with nobody else waiting.
	lane         chan struct{} // capacity 1: held by the request allowed to send
	limitedUntil atomic.Int64  // unix nanoseconds; 0: not rate-limited
	limitHint    atomic.Int64  // the server's last retry hint, in nanoseconds
	laneWaiters  atomic.Int32

	mu    sync.Mutex
	calls map[uint32]chan azp.Msg
	pings map[uint64]chan error
	wg    sync.WaitGroup
}

var (
	errHeartbeat = errors.New("azora: no traffic from the server for three heartbeat intervals")
	errGoAway    = errors.New("azora: server going away")
)

// credential returns the credential for Hello, refreshing it through
// CredentialFunc when set.
func (db *DB) credential(ctx context.Context) (string, error) {
	if f := db.cfg.CredentialFunc; f != nil {
		cred, err := f(ctx)
		if err != nil {
			return "", fmt.Errorf("azora: CredentialFunc: %w", err)
		}
		if cred == "" {
			return "", errors.New("azora: CredentialFunc returned an empty credential")
		}
		db.mu.Lock()
		db.cred = cred
		db.mu.Unlock()
		return cred, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.cred, nil
}

// dial opens a transport for link l, runs the handshake and Hello.
func (db *DB) dial(ctx context.Context, l *link) (_ *conn, err error) {
	if d := db.cfg.RequestTimeout; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	var t azp.Transport
	var abort func()
	db.secMu.Lock()
	sec, wsURL := db.sec, db.wsURL
	db.secMu.Unlock()
	if wsURL != "" {
		ws, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: db.http, Subprotocols: []string{subprotocol}})
		if err != nil {
			return nil, fmt.Errorf("azora: websocket dial: %w", err)
		}
		if ws.Subprotocol() != subprotocol {
			ws.CloseNow()
			return nil, fmt.Errorf("azora: websocket: server did not accept subprotocol %s", subprotocol)
		}
		t = azp.NewWSTransport(ws, wsURL, 0)
		abort = func() { ws.CloseNow() }
	} else {
		nc, err := db.cfg.Dial(ctx, "tcp", db.cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("azora: dial: %w", err)
		}
		if tc, ok := nc.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		t = azp.NewStreamTransport(nc, 0)
		abort = func() { nc.Close() }
	}
	az, err := azp.Client(ctx, t, sec) // closes t on error
	if err != nil {
		if db.discovered && ctx.Err() == nil {
			// The key was read from the server; it may have been rotated
			// since. The next attempt uses whatever the server names now.
			if derr := db.applyDiscovery(ctx); derr != nil {
				db.log.Debug("discovery after a failed handshake", "err", derr)
			}
		}
		return nil, fmt.Errorf("azora: %w", err)
	}
	defer func() {
		if err != nil {
			abort()
		}
	}()
	db.authMu.Lock()
	credGen := db.credGen
	db.authMu.Unlock()
	cred, err := db.credential(ctx)
	if err != nil {
		return nil, err
	}
	b, err := azp.Encode(&azp.Hello{Version: azp.ProtocolVersion, Database: db.cfg.Database, Credential: cred, Client: db.cfg.ClientName})
	if err != nil {
		return nil, err
	}
	if err := az.Send(ctx, b); err != nil {
		return nil, fmt.Errorf("azora: send hello: %w", err)
	}
	var w *azp.Welcome
	for w == nil {
		raw, err := az.Recv(ctx)
		if err != nil {
			return nil, fmt.Errorf("azora: read welcome: %w", err)
		}
		m, err := azp.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("azora: %w", err)
		}
		switch x := m.(type) {
		case *azp.Welcome:
			w = x
		case *azp.Error:
			return nil, fromWire(x)
		case *azp.Ping, *azp.Pong:
		default:
			return nil, fmt.Errorf("azora: unexpected message type %d before welcome", m.Type())
		}
	}
	cn := &conn{db: db, l: l, az: az, abort: abort, ctrl: make(chan azp.Msg, 64), calls: map[uint32]chan azp.Msg{},
		pings: map[uint64]chan error{}, started: time.Now(), credGen: credGen, lane: make(chan struct{}, 1)}
	cn.hb = time.Duration(w.HeartbeatMS) * time.Millisecond
	if cn.hb <= 0 {
		cn.hb = 15 * time.Second
	}
	cn.welcome = Welcome{SessionID: w.SessionID, ServerTime: time.UnixMilli(w.ServerTimeMS), Heartbeat: cn.hb,
		MaxMessage: int(w.MaxMessage), Principal: w.Principal, Scopes: w.Scopes, MaxSubscriptions: int(w.MaxSubscriptions),
		Mode: az.Mode().String()}
	if az.Mode() != azp.ModePlaintext {
		k := az.ServerKey()
		cn.welcome.ServerKey, cn.welcome.Fingerprint = azp.EncodePublicKey(k), azp.Fingerprint(k)
	}
	cn.ctx, cn.cancel = context.WithCancelCause(db.ctx)
	context.AfterFunc(cn.ctx, abort)
	cn.lastRecv.Store(time.Now().UnixNano())
	cn.wg.Add(3)
	go cn.readLoop()
	go cn.writeLoop()
	go cn.heartbeat()
	return cn, nil
}

// supervise follows the connection cn installed on link l (nil: none
// yet), then reconnects (and installs new connections) until Close.
// first, when set, is closed after the first connection attempt.
func (db *DB) supervise(l *link, cn *conn, first chan<- struct{}) {
	defer db.wg.Done()
	attempted := func() {
		if first != nil {
			close(first)
			first = nil
		}
	}
	defer attempted()
	attempt := 0
	for {
		if cn != nil {
			<-cn.ctx.Done()
			cause := context.Cause(cn.ctx)
			cn.wg.Wait()
			db.uninstall(cn, cause)
			if db.ctx.Err() != nil {
				return
			}
			// A connection that died young does not reset the backoff, so
			// a server that accepts and drops cannot cause a reconnect storm.
			if time.Since(cn.started) < 2*time.Second {
				attempt++
			} else {
				attempt = 0
			}
			delay := db.backoff(attempt)
			if ga := time.Duration(cn.goAway.Load()); ga > 0 {
				delay = ga + db.backoff(0)
			}
			cn = nil
			if sleepCtx(db.ctx, delay) != nil {
				return
			}
		}
		next, err := db.dial(db.ctx, l)
		if err != nil {
			if db.ctx.Err() != nil {
				return
			}
			db.connectFailures.Add(1)
			db.mu.Lock()
			l.lastErr = err
			db.broadcastLocked() // waiters may fail fast on a permanent error
			db.mu.Unlock()
			db.log.Debug("connect failed", "conn", l.idx, "attempt", attempt, "err", err)
			db.reportError(err)
			attempted()
			attempt++
			if sleepCtx(db.ctx, db.backoff(attempt)) != nil {
				return
			}
			continue
		}
		db.install(next)
		attempted()
		cn = next
	}
}

// install makes cn the current connection of its link.
func (db *DB) install(cn *conn) {
	db.mu.Lock()
	cn.l.cur, cn.l.lastErr = cn, nil
	db.broadcastLocked()
	db.mu.Unlock()
	db.connects.Add(1)
	db.log.Debug("connected", "conn", cn.l.idx, "session", cn.welcome.SessionID, "mode", cn.welcome.Mode)
	if h := db.cfg.Hooks.OnConnect; h != nil {
		h(cn.welcome)
	}
}

// uninstall forgets cn after it died of cause.
func (db *DB) uninstall(cn *conn, cause error) {
	db.mu.Lock()
	if cn.l.cur == cn {
		cn.l.cur = nil
	}
	db.broadcastLocked()
	db.mu.Unlock()
	if db.ctx.Err() != nil {
		cause = ErrClosed
	}
	db.disconnects.Add(1)
	db.log.Debug("disconnected", "conn", cn.l.idx, "session", cn.welcome.SessionID, "err", cause)
	if h := db.cfg.Hooks.OnDisconnect; h != nil {
		h(cause)
	}
}

// detach stops handing cn to new requests (GoAway).
func (db *DB) detach(cn *conn) {
	db.mu.Lock()
	if cn.l.cur == cn {
		cn.l.cur = nil
		db.broadcastLocked()
	}
	db.mu.Unlock()
}

// waitConn returns a usable connection, waiting for one: link l's when l
// is set, else the least loaded of the pool. With failFast, a permanent
// connection error (credential refused, ...) is returned instead of
// waiting.
func (db *DB) waitConn(ctx context.Context, l *link, failFast bool) (*conn, error) {
	for {
		db.mu.Lock()
		if db.closed {
			db.mu.Unlock()
			return nil, ErrClosed
		}
		cn, last := db.pickLocked(l)
		ch := db.changed
		db.mu.Unlock()
		if cn != nil {
			return cn, nil
		}
		if failFast && permanent(last) {
			return nil, last
		}
		select {
		case <-ch:
		case <-ctx.Done():
			if last != nil {
				return nil, fmt.Errorf("%w (not connected: %v)", ctx.Err(), last)
			}
			return nil, ctx.Err()
		}
	}
}

// fail kills the connection with err as its cause.
func (cn *conn) fail(err error) { cn.cancel(err) }

// readLoop decodes messages and hands them to DB.handle until the
// connection dies.
func (cn *conn) readLoop() {
	defer cn.wg.Done()
	for {
		raw, err := cn.az.Recv(cn.ctx)
		if err != nil {
			cn.fail(fmt.Errorf("azora: read: %w", err))
			return
		}
		cn.lastRecv.Store(time.Now().UnixNano())
		m, err := azp.Decode(raw)
		if err != nil {
			cn.fail(fmt.Errorf("azora: %w", err))
			return
		}
		cn.db.handle(cn, m, len(raw))
	}
}

// writeLoop sends control messages (pings, pongs, unsubscribes) queued
// with sendCtrl.
func (cn *conn) writeLoop() {
	defer cn.wg.Done()
	for {
		select {
		case <-cn.ctx.Done():
			return
		case m := <-cn.ctrl:
			if err := cn.send(m); err != nil {
				return
			}
		}
	}
}

// sendCtrl queues a control message; it is dropped when the queue is full
// (pings and pongs are best effort).
func (cn *conn) sendCtrl(m azp.Msg) {
	select {
	case cn.ctrl <- m:
	default:
	}
}

// heartbeat pings a silent server and declares the connection dead after
// three silent intervals.
func (cn *conn) heartbeat() {
	defer cn.wg.Done()
	iv := cn.hb
	t := time.NewTicker(max(iv/8, 20*time.Millisecond))
	defer t.Stop()
	var lastPing time.Time
	for {
		select {
		case <-cn.ctx.Done():
			return
		case now := <-t.C:
			silent := now.Sub(time.Unix(0, cn.lastRecv.Load()))
			if silent > 3*iv {
				cn.fail(errHeartbeat)
				return
			}
			if silent > iv && now.Sub(lastPing) > iv/2 {
				lastPing = now
				cn.sendCtrl(&azp.Ping{Nonce: cn.db.nonce.Add(1)})
			}
		}
	}
}

// send writes m. A failed write kills the connection (the channel cannot
// recover from a partial frame) and returns a *lostError.
func (cn *conn) send(m azp.Msg) error {
	b, err := azp.Encode(m)
	if err != nil {
		return fmt.Errorf("azora: %w", err)
	}
	if err := context.Cause(cn.ctx); err != nil {
		return &lostError{sent: false, err: err}
	}
	if err := cn.az.Send(cn.ctx, b); err != nil {
		if errors.Is(err, azp.ErrMessageTooLarge) {
			return &Error{Code: CodeResourceLimit, Message: err.Error()}
		}
		cn.fail(fmt.Errorf("azora: write: %w", err))
		return &lostError{sent: true, err: err}
	}
	return nil
}

// roundTrip sends m (whose request id is id) and waits for its answer.
func (cn *conn) roundTrip(ctx context.Context, id uint32, m azp.Msg) (azp.Msg, error) {
	ch := make(chan azp.Msg, 1)
	cn.mu.Lock()
	if err := context.Cause(cn.ctx); err != nil {
		cn.mu.Unlock()
		return nil, &lostError{err: err}
	}
	cn.calls[id] = ch
	cn.mu.Unlock()
	cn.inflight.Add(1)
	defer func() {
		cn.inflight.Add(-1)
		cn.mu.Lock()
		delete(cn.calls, id)
		cn.mu.Unlock()
	}()
	if err := cn.send(m); err != nil {
		return nil, err
	}
	select {
	case r := <-ch:
		return r, nil
	case <-cn.ctx.Done():
		select {
		case r := <-ch:
			return r, nil
		default:
		}
		return nil, &lostError{sent: true, err: context.Cause(cn.ctx)}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// limited records that the server asked to retry after d.
func (cn *conn) limited(d time.Duration) {
	cn.limitHint.Store(int64(d))
	cn.limitedUntil.Store(time.Now().Add(d).UnixNano())
}

// enterLane waits for the connection's retry lane. It fails with a
// *lostError when the connection dies first.
func (cn *conn) enterLane(ctx context.Context) error {
	cn.laneWaiters.Add(1)
	defer cn.laneWaiters.Add(-1)
	select {
	case cn.lane <- struct{}{}:
		return nil
	case <-cn.ctx.Done():
		return &lostError{err: context.Cause(cn.ctx)}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// leaveLane hands the lane on. After a request that passed, the next one
// waits one more hint (about the time the server takes to allow another
// request); with nobody waiting the connection is no longer paced.
func (cn *conn) leaveLane(passed bool) {
	if passed {
		if cn.laneWaiters.Load() > 0 {
			cn.limitedUntil.Store(time.Now().UnixNano() + cn.limitHint.Load())
		} else {
			cn.limitedUntil.Store(0)
		}
	}
	<-cn.lane
}

// deliver hands an answer to the roundTrip waiting for request id.
func (cn *conn) deliver(id uint32, m azp.Msg) {
	cn.mu.Lock()
	ch := cn.calls[id]
	cn.mu.Unlock()
	if ch != nil {
		select {
		case ch <- m:
		default:
		}
	}
}

// ping sends a Ping and waits for its Pong.
func (cn *conn) ping(ctx context.Context) error {
	nonce := cn.db.nonce.Add(1)
	ch := make(chan error, 1)
	cn.mu.Lock()
	cn.pings[nonce] = ch
	cn.mu.Unlock()
	defer func() {
		cn.mu.Lock()
		delete(cn.pings, nonce)
		cn.mu.Unlock()
	}()
	if err := cn.send(&azp.Ping{Nonce: nonce}); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-cn.ctx.Done():
		return &lostError{sent: true, err: context.Cause(cn.ctx)}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pong wakes the ping waiting for nonce.
func (cn *conn) pong(nonce uint64) {
	cn.mu.Lock()
	ch := cn.pings[nonce]
	delete(cn.pings, nonce)
	cn.mu.Unlock()
	if ch != nil {
		ch <- nil
	}
}

// failPings ends every waiting ping with err: the server answers a
// rate-limited Ping with a session error that names no request.
func (cn *conn) failPings(err error) {
	cn.mu.Lock()
	for nonce, ch := range cn.pings {
		delete(cn.pings, nonce)
		ch <- err
	}
	cn.mu.Unlock()
}

// handle dispatches one message from the reader goroutine. It never blocks
// on the network.
func (db *DB) handle(cn *conn, m azp.Msg, size int) {
	switch x := m.(type) {
	case *azp.Ping:
		cn.sendCtrl(&azp.Pong{Nonce: x.Nonce})
	case *azp.Pong:
		cn.pong(x.Nonce)
	case *azp.Welcome:
	case *azp.Result:
		cn.deliver(x.ReqID, x)
	case *azp.OK:
		cn.deliver(x.ReqID, x)
	case *azp.Subscribed:
		cn.deliver(x.ReqID, x)
	case *azp.Verified:
		cn.deliver(x.ReqID, x)
	case *azp.Signed:
		cn.deliver(x.ReqID, x)
	case *azp.Error:
		if x.ReqID != 0 {
			cn.deliver(x.ReqID, x)
			return
		}
		err := fromWire(x)
		db.log.Debug("session error", "conn", cn.l.idx, "err", err)
		if !x.Fatal && x.Code == CodeUnauthenticated && db.cfg.CredentialFunc != nil {
			gen := cn.authGen.Load()
			cn.wg.Add(1)
			go func() {
				defer cn.wg.Done()
				if err := db.reauth(cn.ctx, cn, gen); err != nil && cn.ctx.Err() == nil {
					db.log.Warn("credential refresh failed", "err", err)
					db.reportError(err)
				}
			}()
			return
		}
		if !x.Fatal && x.RetryAfterMS > 0 {
			cn.failPings(err)
		}
		db.reportError(err)
		if x.Fatal {
			cn.fail(err)
		}
	case *azp.Shape:
		db.shapes.put(x)
	case *azp.Batch:
		if s := db.route(x.SubID); s != nil {
			s.onBatch(x, size)
		}
	case *azp.SubEnd:
		if s := db.route(x.SubID); s != nil {
			s.onEnd(x)
		}
	case *azp.GoAway:
		d := time.Duration(x.RetryAfterMS) * time.Millisecond
		cn.goAway.Store(int64(max(d, time.Millisecond)))
		db.log.Debug("server going away", "conn", cn.l.idx, "reason", x.Reason, "retry_after", d)
		db.detach(cn)
		// Let answers in flight arrive, then leave.
		cn.wg.Add(1)
		go func() {
			defer cn.wg.Done()
			deadline := time.Now().Add(time.Second)
			for cn.inflight.Load() > 0 && time.Now().Before(deadline) && cn.ctx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			cn.fail(errGoAway)
		}()
	}
}

// route returns the subscription using wire id, or nil.
func (db *DB) route(wire uint32) *Subscription {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.routes[wire]
}
