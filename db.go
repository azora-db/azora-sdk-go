package azora

import (
	"context"
	"errors"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// DB is a client of one database of an Azora server. It keeps its
// connections alive in the background (heartbeats, reconnects with
// backoff) and multiplexes requests and subscriptions over them. Every
// method is safe for concurrent use.
type DB struct {
	cfg  Config
	log  *slog.Logger
	http *http.Client

	// secMu guards the handshake settings and the WebSocket address: with
	// a discovered server key both are read again from the server when a
	// handshake fails (the key may have been rotated).
	secMu      sync.Mutex
	sec        azp.ClientConfig
	wsURL      string   // the realtime WebSocket endpoint ("" over TCP)
	wsBase     string   // its address without the path, when discovery may move it
	discovered bool     // sec came from the discovery document
	features   []string // the discovery document's features (when discovered)

	ctx    context.Context // canceled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup // supervisors and subscription goroutines

	reqID  atomic.Uint32
	wireID atomic.Uint32
	nonce  atomic.Uint64
	rr     atomic.Uint32 // where the search for the least loaded link starts

	// mu guards the fields below and every link's cur and lastErr.
	mu      sync.Mutex
	links   []*link       // fixed once Open returns
	changed chan struct{} // closed and replaced when a link's connection or closed changes
	closed  bool
	cred    string
	routes  map[uint32]*Subscription // wire subscription id -> subscription
	subs    map[*Subscription]struct{}

	authMu  sync.Mutex
	credGen uint64 // counts credentials fetched to answer an expiry; guarded by authMu
	shapes  shapeCache

	storageOnce sync.Once
	storage     *Storage

	connects, disconnects, connectFailures   atomic.Uint64
	retries, rateLimited, resyncs, refreshes atomic.Uint64
}

// link is one slot of the pool: a connection that is replaced by a new
// one whenever it dies.
type link struct {
	db      *DB
	idx     int
	cur     *conn // guarded by db.mu
	lastErr error // why the last attempt failed (nil once connected); guarded by db.mu
}

// Stats are the client's counters.
type Stats struct {
	// Connected reports at least one live connection; Connections counts
	// them and PoolSize is how many the DB keeps.
	Connected   bool
	Connections int
	PoolSize    int
	// Connects counts established sessions; Disconnects the ones that
	// ended; ConnectFailures failed attempts.
	Connects, Disconnects, ConnectFailures uint64
	// Retries counts requests sent again after their connection dropped.
	Retries uint64
	// RateLimited counts requests the server asked to retry later.
	RateLimited uint64
	// Resyncs counts subscriptions that took a new snapshot.
	Resyncs uint64
	// CredentialRefreshes counts credentials sent with Auth.
	CredentialRefreshes uint64
	// Subscriptions is the number of open subscriptions (live tables and
	// watchers included).
	Subscriptions int
	// InFlight is the number of requests waiting for an answer.
	InFlight int
}

// Connect opens a DB from a connection string (see ParseURL for the
// formats) and options:
//
//	db, err := azora.Connect(ctx, "azora://db.example.com:7480/shop?key="+serverKey,
//		azora.WithAPIKey(os.Getenv("AZORA_API_KEY")))
//
// It returns once the first connection is established (or failed).
func Connect(ctx context.Context, url string, opts ...Option) (*DB, error) {
	cfg, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	for _, o := range opts {
		o(&cfg)
	}
	return Open(ctx, cfg)
}

// Open validates cfg, establishes the first connection (returning its
// error) and then keeps the connections alive in the background until
// Close. With a pool, the other connections are opened before Open
// returns too, but a failure of one of those is not an error: it is
// retried in the background like any lost connection.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	cfg, sec, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	db := &DB{cfg: cfg, sec: sec, wsURL: cfg.WebSocketURL, log: cfg.Logger.With("component", "azora-go", "db", cfg.Database),
		changed: make(chan struct{}), cred: cfg.Credential, routes: map[uint32]*Subscription{}, subs: map[*Subscription]struct{}{},
		shapes: shapeCache{m: map[shapeKey]*shapeInfo{}, changed: make(chan struct{})}}
	db.http = cfg.HTTPClient
	if db.http == nil {
		db.http = &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: cfg.Dial,
			ForceAttemptHTTP2: false, TLSHandshakeTimeout: 10 * time.Second, MaxIdleConnsPerHost: 16,
			IdleConnTimeout: 90 * time.Second}}
	}
	for i := 0; i < cfg.PoolSize; i++ {
		db.links = append(db.links, &link{db: db, idx: i})
	}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	if cfg.needsDiscovery() {
		db.discovered = true
		db.wsBase = strings.TrimSuffix(cfg.WebSocketURL, DefaultWebSocketPath)
		if err := db.applyDiscovery(ctx); err != nil {
			db.cancel()
			db.closeIdle()
			return nil, err
		}
	}
	cn, err := db.dial(ctx, db.links[0])
	if err != nil {
		db.cancel()
		db.closeIdle()
		return nil, err
	}
	db.install(cn)
	db.wg.Add(1)
	go db.supervise(db.links[0], cn, nil)
	var first []chan struct{}
	for _, l := range db.links[1:] {
		ch := make(chan struct{})
		first = append(first, ch)
		db.wg.Add(1)
		go db.supervise(l, nil, ch)
	}
	for _, ch := range first {
		select {
		case <-ch:
		case <-ctx.Done():
			db.Close()
			return nil, ctx.Err()
		}
	}
	return db, nil
}

// Close closes the connections and every subscription. Calls in progress
// and later calls return ErrClosed. It waits for the client's goroutines.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.broadcastLocked()
	db.mu.Unlock()
	db.cancel()
	db.wg.Wait()
	db.closeIdle()
	return nil
}

// closeIdle drops the idle connections of the HTTP client the DB built
// for itself (a caller's client is left alone).
func (db *DB) closeIdle() {
	if db.cfg.HTTPClient == nil {
		db.http.CloseIdleConnections()
	}
}

// Database returns the name of the database the DB is bound to.
func (db *DB) Database() string { return db.cfg.Database }

// Welcome returns the Welcome of a current session (the first pooled
// connection that is up); ok is false while disconnected.
func (db *DB) Welcome() (w Welcome, ok bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, l := range db.links {
		if l.cur != nil && l.cur.ctx.Err() == nil {
			return l.cur.welcome, true
		}
	}
	return Welcome{}, false
}

// Stats returns the client's counters.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	st := Stats{PoolSize: len(db.links), Subscriptions: len(db.subs)}
	for _, l := range db.links {
		if l.cur != nil && l.cur.ctx.Err() == nil {
			st.Connections++
			st.InFlight += int(l.cur.inflight.Load())
		}
	}
	db.mu.Unlock()
	st.Connected = st.Connections > 0
	st.Connects, st.Disconnects, st.ConnectFailures = db.connects.Load(), db.disconnects.Load(), db.connectFailures.Load()
	st.Retries, st.RateLimited, st.Resyncs, st.CredentialRefreshes = db.retries.Load(), db.rateLimited.Load(), db.resyncs.Load(), db.refreshes.Load()
	return st
}

// Ping sends a protocol ping on a live connection (waiting for one like
// any request) and returns the round trip time. It works with every
// credential, restricted tokens included, and runs no SQL.
func (db *DB) Ping(ctx context.Context) (time.Duration, error) {
	ctx, cancel := db.opContext(ctx)
	defer cancel()
	for {
		cn, err := db.waitConn(ctx, nil, true)
		if err != nil {
			return 0, db.finalErr(err)
		}
		start := time.Now()
		err = cn.ping(ctx)
		if err == nil {
			return time.Since(start), nil
		}
		var le *lostError
		var se *Error
		switch {
		case errors.As(err, &le):
			db.retries.Add(1)
		case errors.As(err, &se) && se.RetryAfter > 0:
			db.rateLimited.Add(1)
			if err := sleepCtx(ctx, se.RetryAfter); err != nil {
				return 0, db.finalErr(err)
			}
		default:
			return 0, db.finalErr(err)
		}
	}
}

// broadcastLocked wakes everyone waiting for a change of a connection or
// of closed.
func (db *DB) broadcastLocked() {
	close(db.changed)
	db.changed = make(chan struct{})
}

// reportError calls Hooks.OnError, if set.
func (db *DB) reportError(err error) {
	if db.cfg.Hooks.OnError != nil {
		db.cfg.Hooks.OnError(err)
	}
}

// backoff returns the delay before reconnect attempt n (n >= 0):
// exponential, capped, with full jitter.
func (db *DB) backoff(n int) time.Duration {
	d := db.cfg.ReconnectMin
	for i := 0; i < n && d < db.cfg.ReconnectMax; i++ {
		d *= 2
	}
	d = min(d, db.cfg.ReconnectMax)
	return time.Duration(mrand.Int64N(int64(d)) + 1)
}

// liveLocked returns the link's connection if it is usable.
func (l *link) liveLocked() *conn {
	if cn := l.cur; cn != nil && cn.ctx.Err() == nil {
		return cn
	}
	return nil
}

// pickLocked returns a usable connection: l's when l is set, else the
// one with the fewest requests in flight (ties go round-robin). Without
// one it returns why a link last failed to connect.
func (db *DB) pickLocked(l *link) (*conn, error) {
	if l == nil && len(db.links) == 1 {
		l = db.links[0]
	}
	if l != nil {
		return l.liveLocked(), l.lastErr
	}
	n := len(db.links)
	start := int(db.rr.Add(1) % uint32(n))
	var best *conn
	var last error
	load := int32(math.MaxInt32)
	for i := 0; i < n; i++ {
		k := db.links[(start+i)%n]
		cn := k.liveLocked()
		if cn == nil {
			if k.lastErr != nil {
				last = k.lastErr
			}
			continue
		}
		if v := cn.inflight.Load(); v < load {
			best, load = cn, v
		}
	}
	return best, last
}

// sleepCtx sleeps for d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
