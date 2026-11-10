package azora

import (
	"context"
	"errors"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// policy says whether a request may be sent again after its connection
// was lost.
type policy uint8

const (
	// retryable requests are sent again after a lost connection (reads,
	// subscribes, writes with a mutation id).
	retryable policy = iota
	// once requests are not: losing the connection after sending one
	// returns ErrUnknownOutcome.
	once
)

// callOpts tune call.
type callOpts struct {
	policy    policy
	noRefresh bool  // an unauthenticated answer is final (Auth itself)
	link      *link // send on this link's connection (nil: any)
}

// opContext bounds an operation by RequestTimeout (when ctx has no
// deadline) and by the DB's lifetime.
func (db *DB) opContext(ctx context.Context) (context.Context, context.CancelFunc) {
	cancel1 := context.CancelFunc(func() {})
	if _, ok := ctx.Deadline(); !ok && db.cfg.RequestTimeout > 0 {
		ctx, cancel1 = context.WithTimeout(ctx, db.cfg.RequestTimeout)
	}
	ctx, cancel2 := context.WithCancel(ctx)
	stop := context.AfterFunc(db.ctx, cancel2)
	return ctx, func() { stop(); cancel2(); cancel1() }
}

// finalErr maps errors caused by Close to ErrClosed.
func (db *DB) finalErr(err error) error {
	if db.ctx.Err() != nil {
		return ErrClosed
	}
	return err
}

// nextReqID returns a fresh, non-zero request id.
func (db *DB) nextReqID() uint32 {
	for {
		if id := db.reqID.Add(1); id != 0 {
			return id
		}
	}
}

// call sends a request built by build (given its request id) until it gets
// an answer: through reconnects, rate limits and credential refreshes.
//
// A rate-limited request is sent again after the server's hint. So that
// many concurrent callers do not all come back at the same instant (the
// server counts refused requests and ends a session that keeps sending
// them), a connection that was told to slow down sends one request at a
// time, spaced by the hint, until the backlog is gone.
func (db *DB) call(ctx context.Context, o callOpts, build func(id uint32) azp.Msg) (azp.Msg, error) {
	ctx, cancel := db.opContext(ctx)
	defer cancel()
	refreshed, unavailable := 0, 0
	// While the server rate-limits a connection its requests take turns
	// (see conn.lane): held is the connection whose lane this call holds.
	var held *conn
	leave := func(passed bool) {
		if held != nil {
			held.leaveLane(passed)
			held = nil
		}
	}
	defer leave(false)
	for {
		cn, err := db.waitConn(ctx, o.link, true)
		if err != nil {
			return nil, db.finalErr(err)
		}
		if held != cn {
			leave(false)
			if cn.limitedUntil.Load() != 0 {
				if err := cn.enterLane(ctx); err != nil {
					var le *lostError
					if errors.As(err, &le) {
						continue
					}
					return nil, db.finalErr(err)
				}
				held = cn
			}
		}
		if held != nil {
			if d := time.Until(time.Unix(0, cn.limitedUntil.Load())); d > 0 {
				if err := sleepCtx(ctx, d); err != nil {
					return nil, db.finalErr(err)
				}
			}
		}
		gen := cn.authGen.Load()
		id := db.nextReqID()
		m, err := cn.roundTrip(ctx, id, build(id))
		if err != nil {
			var le *lostError
			if errors.As(err, &le) {
				if o.policy == once && le.sent {
					return nil, db.finalErr(ErrUnknownOutcome)
				}
				db.retries.Add(1)
				continue
			}
			return nil, db.finalErr(err)
		}
		e, ok := m.(*azp.Error)
		if !ok {
			leave(true)
			return m, nil
		}
		if e.RetryAfterMS > 0 {
			db.rateLimited.Add(1)
			cn.limited(time.Duration(e.RetryAfterMS) * time.Millisecond)
			continue // through the lane, after the hint
		}
		leave(true)
		switch e.Code {
		case CodeUnauthenticated:
			if !o.noRefresh && db.cfg.CredentialFunc != nil && refreshed < 2 {
				refreshed++
				if err := db.reauth(ctx, cn, gen); err != nil {
					return nil, db.finalErr(err)
				}
				continue
			}
		case CodeUnavailable:
			if unavailable < 5 {
				unavailable++
				if err := sleepCtx(ctx, db.backoff(unavailable)); err != nil {
					return nil, db.finalErr(err)
				}
				continue
			}
		}
		return nil, fromWire(e)
	}
}

// reauth sends a fresh credential with Auth on cn, unless another refresh
// happened on cn since gen was observed. The credential comes from
// CredentialFunc, or from the refresh another pooled connection just made
// for the same expiry.
func (db *DB) reauth(ctx context.Context, cn *conn, gen uint64) error {
	db.authMu.Lock()
	defer db.authMu.Unlock()
	if cn.authGen.Load() != gen {
		return nil
	}
	// A credential another connection fetched for this expiry is reused;
	// if the server refuses it too, a fresh one is fetched after all.
	shared := cn.credGen < db.credGen
	var cred string
	if shared {
		db.mu.Lock()
		cred = db.cred
		db.mu.Unlock()
	} else {
		var err error
		if cred, err = db.credential(ctx); err != nil {
			return err
		}
		db.credGen++
	}
	for {
		id := db.nextReqID()
		m, err := cn.roundTrip(ctx, id, &azp.Auth{ReqID: id, Credential: cred})
		if err != nil {
			var le *lostError
			if errors.As(err, &le) {
				return nil // the next connection sends the new credential in Hello
			}
			return err
		}
		if e, ok := m.(*azp.Error); ok {
			if e.RetryAfterMS > 0 {
				if err := sleepCtx(ctx, time.Duration(e.RetryAfterMS)*time.Millisecond); err != nil {
					return err
				}
				continue
			}
			if e.Code == CodeUnauthenticated && shared {
				shared = false
				if cred, err = db.credential(ctx); err != nil {
					return err
				}
				db.credGen++
				continue
			}
			return fromWire(e)
		}
		db.refreshes.Add(1)
		cn.credGen = db.credGen
		cn.authGen.Add(1)
		return nil
	}
}

// SetCredential replaces the credential: it is sent to the server now
// (Auth, on every pooled connection) and used for later connections. With
// CredentialFunc set, the next reconnect asks CredentialFunc again.
//
// If the new credential grants exactly what the old one did, open
// subscriptions continue; otherwise the server ends them and they take a
// new snapshot under the new credential.
func (db *DB) SetCredential(ctx context.Context, cred string) error {
	if cred == "" {
		return errors.New("azora: empty credential")
	}
	db.mu.Lock()
	db.cred = cred
	db.mu.Unlock()
	errs := make(chan error, len(db.links))
	for _, l := range db.links {
		go func() {
			_, err := db.call(ctx, callOpts{noRefresh: true, link: l}, func(id uint32) azp.Msg { return &azp.Auth{ReqID: id, Credential: cred} })
			errs <- err
		}()
	}
	var first error
	for range db.links {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	if first == nil {
		db.refreshes.Add(1)
	}
	return first
}
