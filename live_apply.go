package azora

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"time"
)

// goTracked runs fn on a goroutine Close waits for; false once the DB is
// closed.
func (db *DB) goTracked(fn func()) bool {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return false
	}
	db.wg.Add(1)
	db.mu.Unlock()
	go func() {
		defer db.wg.Done()
		fn()
	}()
	return true
}

// verifyReq is a Verify call waiting for the apply loop, which owns the
// subscription's stream.
type verifyReq struct {
	ctx  context.Context
	done chan verifyResp
}

type verifyResp struct {
	res VerifyResult
	err error
}

// run is the apply loop: it takes updates from the subscription, applies
// them and tells the listeners, and serves Verify calls in between.
func (lt *LiveTable[T]) run() {
	defer close(lt.done)
	for {
		lt.imu.Lock()
		reqs := lt.verifies
		lt.verifies = nil
		lt.imu.Unlock()
		for _, r := range reqs {
			if !lt.serveVerify(r) {
				return
			}
		}
		nctx, cancel := context.WithCancel(lt.ctx)
		lt.imu.Lock()
		if len(lt.verifies) > 0 {
			cancel()
		}
		lt.interrupt = cancel
		lt.imu.Unlock()
		u, err := lt.sub.Next(nctx)
		lt.imu.Lock()
		lt.interrupt = nil
		lt.imu.Unlock()
		interrupted := err != nil && nctx.Err() != nil && lt.ctx.Err() == nil
		cancel()
		if interrupted {
			continue // a Verify call is waiting
		}
		if err != nil {
			lt.fail(err)
			return
		}
		if err := lt.step(u); err != nil {
			lt.fail(err)
			return
		}
	}
}

// fail records why the view stopped.
func (lt *LiveTable[T]) fail(err error) {
	if lt.ctx.Err() != nil || errors.Is(err, context.Canceled) {
		err = ErrClosed
	}
	lt.mu.Lock()
	if lt.err == nil {
		lt.err = err
	}
	lt.mu.Unlock()
	lt.imu.Lock()
	reqs := lt.verifies
	lt.verifies = nil
	lt.imu.Unlock()
	for _, r := range reqs {
		r.done <- verifyResp{err: err}
	}
}

// step applies one update and notifies the listeners.
func (lt *LiveTable[T]) step(u Update) error {
	lt.notifyMu.Lock()
	defer lt.notifyMu.Unlock()
	ev, err := lt.apply(u)
	if err != nil {
		return err
	}
	lt.lmu.Lock()
	ls := lt.listeners
	lt.lmu.Unlock()
	for _, l := range ls {
		lt.lmu.Lock()
		stopped := l.stopped
		lt.lmu.Unlock()
		if !stopped {
			l.fn(ev)
		}
	}
	return nil
}

// apply applies u to the rows under the write lock and returns what
// changed, decoded.
func (lt *LiveTable[T]) apply(u Update) (LiveUpdate[T], error) {
	ev := LiveUpdate[T]{Reset: u.Reset, Checkpoint: u.Checkpoint, Epoch: u.Epoch}
	if len(u.Changes) > 0 {
		ev.Changes = make([]LiveChange[T], 0, len(u.Changes))
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if u.Reset {
		lt.rows, lt.sum = make(map[string]*liveRow[T], len(u.Changes)), 0
		lt.order = nil
	}
	for i := range u.Changes {
		c := &u.Changes[i]
		if c.Columns != nil && (lt.bind == nil || !sameStrings(lt.cols, c.Columns)) {
			b, err := lt.plan.bind(c.Columns)
			if err != nil {
				return ev, fmt.Errorf("azora: live %s: %w", c.Table, err)
			}
			lt.bind, lt.cols, lt.pk = b, c.Columns, c.PK
		}
		k := encodeKey(c.Key)
		old := lt.rows[k]
		lc := LiveChange[T]{Op: c.Op, Key: c.Key}
		if old != nil {
			lc.Old = old.val
		}
		switch c.Op {
		case OpInsert, OpUpdate:
			if c.New == nil {
				continue
			}
			if lt.bind == nil {
				return ev, fmt.Errorf("azora: live %s: the server sent rows before the table's columns", c.Table)
			}
			row := &liveRow[T]{key: c.Key, raw: c.New, hash: RowHash(c.Table, c.Key, c.New)}
			if err := lt.bind.decode(reflect.ValueOf(&row.val).Elem(), c.New); err != nil {
				return ev, fmt.Errorf("azora: live %s: %w", c.Table, err)
			}
			if old != nil {
				lt.sum -= old.hash
			}
			lt.sum += row.hash
			lt.rows[k] = row
			lc.New = row.val
		case OpDelete:
			if old == nil {
				continue
			}
			lt.sum -= old.hash
			delete(lt.rows, k)
		default:
			continue
		}
		lt.order = nil
		ev.Changes = append(ev.Changes, lc)
	}
	lt.cp, lt.epoch = u.Checkpoint, u.Epoch
	lt.version++
	if !lt.isReady {
		lt.isReady = true
		close(lt.ready)
	}
	return ev, nil
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) > 0 && &a[0] == &b[0] {
		return true
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// OnUpdate calls fn after every step applied to the view, on the view's
// goroutine: fn sees the view already updated, and the next step waits
// until fn returns, so a slow fn slows the view (and, through the
// subscription's flow control, the server's stream) instead of losing
// changes. If the view already holds its first snapshot, fn is first
// called with the current rows as a Reset update, so a listener never
// misses a change between reading the rows and subscribing.
//
// The returned function removes the listener; it may be called from fn.
// Do not call OnUpdate from fn.
func (lt *LiveTable[T]) OnUpdate(fn func(LiveUpdate[T])) (stop func()) {
	l := &liveListener[T]{fn: fn}
	lt.notifyMu.Lock()
	lt.mu.RLock()
	ready := lt.isReady
	lt.mu.RUnlock()
	if ready {
		rows := lt.ordered()
		ev := LiveUpdate[T]{Reset: true, Changes: make([]LiveChange[T], len(rows))}
		for i, r := range rows {
			ev.Changes[i] = LiveChange[T]{Op: OpInsert, Key: r.key, New: r.val}
		}
		ev.Checkpoint, ev.Epoch = lt.Checkpoint()
		fn(ev)
	}
	lt.lmu.Lock()
	lt.listeners = append(append([]*liveListener[T](nil), lt.listeners...), l)
	lt.lmu.Unlock()
	lt.notifyMu.Unlock()
	return func() {
		lt.lmu.Lock()
		defer lt.lmu.Unlock()
		if l.stopped {
			return
		}
		l.stopped = true
		ls := make([]*liveListener[T], 0, len(lt.listeners))
		for _, x := range lt.listeners {
			if x != l {
				ls = append(ls, x)
			}
		}
		lt.listeners = ls
	}
}

// Updates iterates over the steps applied to the view, starting with the
// current rows as a Reset update (see OnUpdate). The view applies at most
// one step ahead of the loop body and then waits for it, so nothing is
// lost. The iteration ends when the loop breaks, or with a non-nil error
// when ctx ends or the view stops (see Err).
//
//	for u, err := range orders.Updates(ctx) {
//		if err != nil { break }
//		for _, c := range u.Changes { ... }
//	}
func (lt *LiveTable[T]) Updates(ctx context.Context) iter.Seq2[LiveUpdate[T], error] {
	return func(yield func(LiveUpdate[T], error) bool) {
		// One slot: the first update is delivered by OnUpdate itself,
		// before this goroutine starts receiving.
		ch := make(chan LiveUpdate[T], 1)
		quit := make(chan struct{})
		stop := lt.OnUpdate(func(u LiveUpdate[T]) {
			select {
			case ch <- u:
			case <-quit:
			case <-lt.ctx.Done():
			}
		})
		defer stop()
		defer close(quit)
		for {
			select {
			case u := <-ch:
				if !yield(u, nil) {
					return
				}
			case <-ctx.Done():
				yield(LiveUpdate[T]{}, ctx.Err())
				return
			case <-lt.done:
				select {
				case u := <-ch:
					if !yield(u, nil) {
						return
					}
				default:
				}
				yield(LiveUpdate[T]{}, lt.Err())
				return
			}
		}
	}
}

// Verify compares the view's checksum with the server's checksum of the
// subscription's rows at the same checkpoint. When the server is ahead
// it first applies the changes up to the server's checkpoint, so the
// comparison is exact even while the table is being written.
// VerifyResult.Match false means the view and the database differ, which
// only a bug or corruption can cause: call Resync.
func (lt *LiveTable[T]) Verify(ctx context.Context) (VerifyResult, error) {
	ctx, cancel := lt.sub.db.opContext(ctx)
	defer cancel()
	r := &verifyReq{ctx: ctx, done: make(chan verifyResp, 1)}
	lt.imu.Lock()
	lt.verifies = append(lt.verifies, r)
	if lt.interrupt != nil {
		lt.interrupt()
	}
	lt.imu.Unlock()
	select {
	case resp := <-r.done:
		return resp.res, resp.err
	case <-lt.done:
		select {
		case resp := <-r.done:
			return resp.res, resp.err
		default:
		}
		return VerifyResult{}, lt.Err()
	case <-ctx.Done():
		return VerifyResult{}, lt.sub.db.finalErr(ctx.Err())
	}
}

// serveVerify answers one Verify call from the apply loop; false means
// the view failed meanwhile.
func (lt *LiveTable[T]) serveVerify(r *verifyReq) bool {
	for {
		if r.ctx.Err() != nil {
			r.done <- verifyResp{err: r.ctx.Err()}
			return true
		}
		res, err := lt.sub.Verify(r.ctx, lt.Checksum)
		if errors.Is(err, ErrRetry) {
			u, err := lt.sub.Next(r.ctx)
			if err != nil {
				if r.ctx.Err() != nil && lt.ctx.Err() == nil {
					r.done <- verifyResp{err: r.ctx.Err()}
					return true
				}
				r.done <- verifyResp{err: err}
				lt.fail(err)
				return false
			}
			if err := lt.step(u); err != nil {
				r.done <- verifyResp{err: err}
				lt.fail(err)
				return false
			}
			continue
		}
		if err == nil && !res.Match {
			lt.mu.Lock()
			lt.mismatches++
			lt.mu.Unlock()
		}
		r.done <- verifyResp{res: res, err: err}
		return true
	}
}

// autoVerify runs LiveOptions.VerifyEvery.
func (lt *LiveTable[T]) autoVerify() {
	t := time.NewTicker(lt.opts.VerifyEvery)
	defer t.Stop()
	for {
		select {
		case <-lt.done:
			return
		case <-lt.ctx.Done():
			return
		case <-t.C:
		}
		if !lt.Ready() {
			continue
		}
		res, err := lt.Verify(lt.ctx)
		if err != nil || res.Match {
			continue
		}
		if h := lt.opts.OnMismatch; h != nil {
			h(res)
		}
		lt.Resync()
	}
}
