package azora

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// Subscription is a live feed of a subscription's rows. It survives
// reconnects: it resumes from the last checkpoint it received, skipping
// changes it already has, and takes a new snapshot (an Update with Reset)
// only when the server can no longer resume it. Next is for one consumer;
// the other methods are safe for concurrent use.
type Subscription struct {
	db   *DB
	link *link // the pooled connection slot the subscription lives on
	spec Spec
	id   uint32
	tmpl azp.Subscribe

	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	endCh     chan *azp.SubEnd
	resyncReq chan struct{}
	nextMu    sync.Mutex

	mu      sync.Mutex
	wire    uint32 // current wire id (0: none)
	cn      *conn
	active  bool
	started bool // subscribed at least once
	stateCh chan struct{}
	signal  chan struct{}
	err     error

	// Stream state. Changes are staged until a checkpoint completes them.
	needSnapshot  bool
	rcvCP         uint64 // highest checkpoint received
	rcvEpoch      int64
	threshold     uint64 // changes with Seq <= threshold were already received
	partial       []Change
	pReset, pSnap bool
	partialBytes  int64
	ready         []staged
	readyBytes    int64
	delivered     uint64 // checkpoint of the last update returned by Next
	deliverEpoch  int64
	ackPending    uint64
	notifyQ       [][]string
	pv            *VerifyResult // server checksum waiting for delivery up to its checkpoint
}

// staged is a complete step of changes ending at a checkpoint, waiting for
// Next.
type staged struct {
	changes         []Change
	cp              uint64
	epoch           int64
	reset, snapshot bool
	bytes           int64
}

var subIDs atomic.Uint32

// Subscribe starts a subscription and waits until the server confirms it
// (while disconnected, it waits for the connection until ctx ends). Errors
// such as permission_denied or an unknown table are returned here.
//
// Build the Spec with TableSpec, RowSpec, DatabaseSpec, FilesSpec or
// NotifySpec. Subscribe is the precise, low-level stream; Live and Watch
// build ready-made views on it.
func (db *DB) Subscribe(ctx context.Context, spec Spec) (*Subscription, error) {
	if spec.Kind < KindDatabase || spec.Kind > KindNotify {
		return nil, fmt.Errorf("azora: unknown subscription kind %d", spec.Kind)
	}
	if spec.Ops > OpsAll {
		return nil, fmt.Errorf("azora: bad ops mask %d", spec.Ops)
	}
	key, err := normalizeAll(spec.Key)
	if err != nil {
		return nil, fmt.Errorf("azora: subscription key: %w", err)
	}
	where, err := spec.Where.wire(0)
	if err != nil {
		return nil, err
	}
	s := &Subscription{db: db, link: db.links[0], spec: spec, id: subIDs.Add(1), done: make(chan struct{}), endCh: make(chan *azp.SubEnd, 4),
		resyncReq: make(chan struct{}, 1), stateCh: make(chan struct{}), signal: make(chan struct{}, 1)}
	s.tmpl = azp.Subscribe{Kind: uint8(spec.Kind), Table: spec.Table, Key: key, Where: where, Ops: uint8(spec.Ops),
		NoSnapshot: spec.NoSnapshot, Bucket: spec.Bucket, PathPrefix: spec.PathPrefix, Tables: spec.Tables}
	if spec.Kind != KindNotify {
		switch {
		case spec.Since > 0:
			s.rcvCP, s.rcvEpoch, s.threshold = spec.Since, spec.Epoch, spec.Since
			s.delivered, s.deliverEpoch = spec.Since, spec.Epoch
		case !spec.NoSnapshot:
			s.needSnapshot = true
		}
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrClosed
	}
	db.subs[s] = struct{}{}
	db.wg.Add(1) // under mu: Close waits only after it set closed
	db.mu.Unlock()
	s.ctx, s.cancel = context.WithCancel(db.ctx)
	first := make(chan error, 1)
	go s.run(first)
	octx, cancel := db.opContext(ctx)
	defer cancel()
	select {
	case err := <-first:
		if err != nil {
			s.Close()
			return nil, db.finalErr(err)
		}
		return s, nil
	case <-octx.Done():
		s.Close()
		return nil, db.finalErr(octx.Err())
	}
}

// ID identifies the subscription within the DB (OnResync reports it).
func (s *Subscription) ID() uint32 { return s.id }

// Spec returns the subscription's spec.
func (s *Subscription) Spec() Spec { return s.spec }

// Checkpoint returns the checkpoint and epoch of the last update Next
// returned: store them to resume in another process (Spec.Since/Epoch).
func (s *Subscription) Checkpoint() (uint64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered, s.deliverEpoch
}

// Buffered returns the size of the messages received and not yet returned
// by Next. The server stops sending a subscription about 4 MiB past the
// last update the application consumed, so it stays bounded.
func (s *Subscription) Buffered() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.partialBytes + s.readyBytes
}

// Close ends the subscription. Next returns ErrClosed afterwards.
func (s *Subscription) Close() error {
	s.cancel()
	<-s.done
	s.db.mu.Lock()
	delete(s.db.subs, s)
	s.db.mu.Unlock()
	return nil
}

// Resync drops the resume point and asks for a new snapshot: the next
// update has Reset set. Use it after Verify reports a mismatch.
func (s *Subscription) Resync() {
	select {
	case s.resyncReq <- struct{}{}:
	default:
	}
}

// broadcastLocked wakes waitActive callers after a state change.
func (s *Subscription) broadcastLocked() {
	close(s.stateCh)
	s.stateCh = make(chan struct{})
}

// signalLocked wakes Next.
func (s *Subscription) signalLocked() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

// fail ends the subscription with err (the first error wins).
func (s *Subscription) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		if errors.Is(err, ErrClosed) {
			s.ready, s.readyBytes, s.partial, s.partialBytes, s.notifyQ = nil, 0, nil, 0, nil
		}
	}
	s.active = false
	s.broadcastLocked()
	s.signalLocked()
	s.mu.Unlock()
}

// begin prepares a Subscribe for a new incarnation on cn.
func (s *Subscription) begin(cn *conn, wire uint32) *azp.Subscribe {
	m := s.tmpl
	m.SubID = wire
	s.mu.Lock()
	s.partial, s.pReset, s.pSnap, s.partialBytes = nil, false, false, 0 // incomplete tail: sent again
	s.wire, s.cn, s.active = wire, cn, false
	if s.spec.Kind != KindNotify {
		switch {
		case s.needSnapshot && s.spec.NoSnapshot:
			// Resume point lost without a snapshot: the first update
			// carries Reset to mark the gap.
			m.Since, m.Epoch, m.NoSnapshot = 0, 0, true
			s.threshold, s.pReset = 0, s.started
		case s.needSnapshot:
			m.Since, m.Epoch, m.NoSnapshot = 0, 0, false
			s.threshold = 0
		case s.rcvCP > 0:
			m.Since, m.Epoch = s.rcvCP, s.rcvEpoch
			s.threshold = s.rcvCP
		default: // NoSnapshot, no checkpoint yet
			m.Since, m.Epoch, m.NoSnapshot = 0, 0, true
		}
	}
	s.mu.Unlock()
	s.db.mu.Lock()
	s.db.routes[wire] = s
	s.db.mu.Unlock()
	return &m
}

// setActive marks incarnation wire as subscribed. A notify subscription
// that resubscribes reports "*": it may have missed signals meanwhile.
func (s *Subscription) setActive(wire uint32) {
	s.mu.Lock()
	if s.wire == wire {
		s.active = true
		if s.started && s.spec.Kind == KindNotify {
			s.notifyQ = append(s.notifyQ, []string{"*"})
			s.signalLocked()
		}
		s.started = true
		s.broadcastLocked()
	}
	s.mu.Unlock()
}

// unroute ends incarnation wire: its late messages are ignored.
func (s *Subscription) unroute(wire uint32) {
	s.db.mu.Lock()
	if s.db.routes[wire] == s {
		delete(s.db.routes, wire)
	}
	s.db.mu.Unlock()
	s.mu.Lock()
	if s.wire == wire {
		s.wire, s.active = 0, false // late batches of this incarnation are dropped
		s.broadcastLocked()
	}
	s.mu.Unlock()
}

// resync forgets the resume point so the next incarnation takes a new
// snapshot; false when the application handles resyncs itself.
func (s *Subscription) resync(reason string) bool {
	if s.spec.ManualResync {
		return false
	}
	s.mu.Lock()
	s.ready, s.readyBytes, s.partial, s.partialBytes = nil, 0, nil, 0
	s.pReset, s.pSnap = false, false
	s.rcvCP, s.threshold, s.needSnapshot, s.pv = 0, 0, true, nil
	s.mu.Unlock()
	s.db.resyncs.Add(1)
	s.db.log.Debug("subscription resync", "sub", s.id, "reason", reason)
	if h := s.db.cfg.Hooks.OnResync; h != nil {
		h(s.id, reason)
	}
	return true
}

// sendUnsubscribe tells the server, best effort, without waiting.
func (s *Subscription) sendUnsubscribe(cn *conn, wire uint32) {
	t := time.NewTimer(100 * time.Millisecond)
	defer t.Stop()
	select {
	case cn.ctrl <- &azp.Unsubscribe{ReqID: s.db.nextReqID(), SubID: wire}:
	case <-cn.ctx.Done():
	case <-t.C:
	}
}

// newWire returns a fresh, non-zero wire subscription id.
func (db *DB) newWire() uint32 {
	for {
		if id := db.wireID.Add(1); id != 0 {
			return id
		}
	}
}

// run keeps the subscription subscribed on the current connection.
func (s *Subscription) run(first chan<- error) {
	defer s.db.wg.Done()
	defer close(s.done)
	reported := false
	report := func(err error) {
		if !reported {
			reported = true
			first <- err
		}
	}
	fail := func(err error) {
		s.fail(err)
		report(err)
	}
	backoff, refreshed := 0, 0
	for {
		cn, err := s.db.waitConn(s.ctx, s.link, false)
		if err != nil {
			fail(ErrClosed)
			return
		}
		wire := s.db.newWire()
		msg := s.begin(cn, wire)
		gen := cn.authGen.Load()
		msg.ReqID = s.db.nextReqID()
		m, err := cn.roundTrip(s.ctx, msg.ReqID, msg)
		if err != nil {
			s.unroute(wire)
			var le *lostError
			switch {
			case s.ctx.Err() != nil:
				fail(ErrClosed)
				return
			case errors.As(err, &le):
				continue
			}
			fail(err)
			return
		}
		switch x := m.(type) {
		case *azp.Subscribed:
			backoff, refreshed = 0, 0
			s.setActive(wire)
			report(nil)
		case *azp.Error:
			s.unroute(wire)
			if x.RetryAfterMS > 0 {
				s.db.rateLimited.Add(1)
				if sleepCtx(s.ctx, time.Duration(x.RetryAfterMS)*time.Millisecond) != nil {
					fail(ErrClosed)
					return
				}
				continue
			}
			switch x.Code {
			case CodeGone:
				if !s.resync(x.Message) {
					fail(fmt.Errorf("%w: %s", ErrResync, x.Message))
					return
				}
				continue
			case CodeUnauthenticated:
				if s.db.cfg.CredentialFunc != nil && refreshed < 2 {
					refreshed++
					if err := s.db.reauth(s.ctx, cn, gen); err == nil {
						continue
					}
				}
			case CodeUnavailable, CodeInternal:
				// Transient (database reopening, ...): retry with backoff.
				// The first Subscribe is bounded by its caller's context.
				backoff = min(backoff+1, 16)
				if sleepCtx(s.ctx, s.db.backoff(backoff)) != nil {
					fail(ErrClosed)
					return
				}
				continue
			}
			fail(fromWire(x))
			return
		default:
			s.unroute(wire)
			cn.fail(fmt.Errorf("azora: unexpected answer type %d to subscribe", m.Type()))
			continue
		}
		if !s.wait(cn, wire, &backoff) {
			return
		}
	}
}

// wait follows an active incarnation until it ends; false ends run.
func (s *Subscription) wait(cn *conn, wire uint32, backoff *int) bool {
	for {
		select {
		case <-cn.ctx.Done():
			s.unroute(wire)
			return true
		case end := <-s.endCh:
			if end.SubID != wire {
				continue // a previous incarnation's
			}
			s.unroute(wire)
			switch end.Code {
			case CodeGone:
				if !s.resync(end.Message) {
					s.fail(fmt.Errorf("%w: %s", ErrResync, end.Message))
					return false
				}
			case CodeUnavailable:
				// The database was closed or reopened: resume.
				*backoff = 0
			case CodeInvalid, CodePermissionDenied, CodeNotFound:
				s.fail(&Error{Code: end.Code, Message: end.Message})
				return false
			default:
				*backoff = min(*backoff+1, 16)
				if sleepCtx(s.ctx, s.db.backoff(*backoff)) != nil {
					s.fail(ErrClosed)
					return false
				}
			}
			return true
		case <-s.resyncReq:
			s.sendUnsubscribe(cn, wire)
			s.unroute(wire)
			s.resync("requested")
			return true
		case <-s.ctx.Done():
			s.sendUnsubscribe(cn, wire)
			s.unroute(wire)
			s.fail(ErrClosed)
			return false
		}
	}
}
