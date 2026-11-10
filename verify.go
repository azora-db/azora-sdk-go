package azora

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// VerifyResult compares the server's checksum of a subscription's rows
// with the application's.
type VerifyResult struct {
	// Match reports equal row counts and sums.
	Match bool
	// Checkpoint and Epoch are where the server computed its checksum.
	Checkpoint uint64
	Epoch      int64
	ServerRows int64
	ServerSum  uint64
	LocalRows  int64
	LocalSum   uint64
}

// Verify compares the server's checksum of the subscription's rows with
// local's (for example Replica.Checksum). Call it from the goroutine that
// calls Next, after applying the last update: local must describe exactly
// what Next delivered so far.
//
// The server computes its checksum at its current checkpoint. When that
// is the checkpoint the application is at, or nothing the subscription
// covers changed in between, Verify compares at once. Otherwise it
// returns ErrRetry and remembers the server's checksum: the next update
// from Next stops exactly at that checkpoint, and the following Verify
// compares against it without asking the server again:
//
//	for {
//		u, _ := sub.Next(ctx)
//		replica.Apply(u)
//		res, err := sub.Verify(ctx, replica.Checksum)
//		if errors.Is(err, client.ErrRetry) {
//			continue
//		}
//		...
//	}
func (s *Subscription) Verify(ctx context.Context, local func() (rows int64, sum uint64)) (VerifyResult, error) {
	if s.spec.Kind == KindNotify {
		return VerifyResult{}, errors.New("azora: notify subscriptions have no rows to verify")
	}
	ctx, cancel := s.db.opContext(ctx)
	defer cancel()
	s.mu.Lock()
	if pv := s.pv; pv != nil && pv.Checkpoint == s.delivered && pv.Epoch == s.deliverEpoch {
		res := *pv
		s.pv = nil
		s.mu.Unlock()
		return compareSums(res, local), nil
	}
	s.mu.Unlock()
	for attempt := 0; ; attempt++ {
		if attempt > 20 {
			return VerifyResult{}, fmt.Errorf("azora: verify: the subscription keeps restarting")
		}
		cn, wire, err := s.waitActive(ctx)
		if err != nil {
			return VerifyResult{}, s.db.finalErr(err)
		}
		gen := cn.authGen.Load()
		id := s.db.nextReqID()
		m, err := cn.roundTrip(ctx, id, &azp.Verify{ReqID: id, SubID: wire})
		if err != nil {
			var le *lostError
			if errors.As(err, &le) {
				continue
			}
			return VerifyResult{}, s.db.finalErr(err)
		}
		switch x := m.(type) {
		case *azp.Error:
			switch {
			case x.RetryAfterMS > 0:
				s.db.rateLimited.Add(1)
				if err := sleepCtx(ctx, time.Duration(x.RetryAfterMS)*time.Millisecond); err != nil {
					return VerifyResult{}, s.db.finalErr(err)
				}
				continue
			case x.Code == CodeNotFound: // the incarnation ended meanwhile
				if err := sleepCtx(ctx, 10*time.Millisecond); err != nil {
					return VerifyResult{}, s.db.finalErr(err)
				}
				continue
			case x.Code == CodeUnauthenticated && s.db.cfg.CredentialFunc != nil && attempt < 3:
				if err := s.db.reauth(ctx, cn, gen); err != nil {
					return VerifyResult{}, err
				}
				continue
			}
			return VerifyResult{}, fromWire(x)
		case *azp.Verified:
			res := VerifyResult{Checkpoint: x.Checkpoint, Epoch: x.Epoch, ServerRows: x.Rows, ServerSum: x.Sum}
			s.mu.Lock()
			if s.wire != wire {
				s.mu.Unlock()
				continue
			}
			d, de := s.delivered, s.deliverEpoch
			switch {
			case x.Epoch != de || s.resetPendingLocked() || d == 0:
				s.mu.Unlock()
				return res, ErrRetry
			case x.Checkpoint == d:
				s.mu.Unlock()
				return compareSums(res, local), nil
			case x.Checkpoint < d:
				s.mu.Unlock()
				continue
			case s.rcvCP >= x.Checkpoint && !s.changesInLocked(d, x.Checkpoint):
				// Nothing this subscription covers changed in (d, V]: the
				// application's state at d is the state at V.
				s.mu.Unlock()
				return compareSums(res, local), nil
			}
			pv := res
			s.pv = &pv
			s.mu.Unlock()
			return res, ErrRetry
		default:
			return VerifyResult{}, fmt.Errorf("azora: unexpected answer type %d to verify", m.Type())
		}
	}
}

// compareSums fills the local figures of res and whether they match.
func compareSums(res VerifyResult, local func() (int64, uint64)) VerifyResult {
	res.LocalRows, res.LocalSum = local()
	res.Match = res.LocalRows == res.ServerRows && res.LocalSum == res.ServerSum
	return res
}

// resetPendingLocked reports whether a new snapshot is pending: the
// application's state is about to be replaced.
func (s *Subscription) resetPendingLocked() bool {
	if s.needSnapshot || s.pReset {
		return true
	}
	for _, r := range s.ready {
		if r.reset {
			return true
		}
	}
	return false
}

// changesInLocked reports staged changes with lo < Seq <= hi.
func (s *Subscription) changesInLocked(lo, hi uint64) bool {
	for _, r := range s.ready {
		for _, c := range r.changes {
			if c.Seq > lo && c.Seq <= hi {
				return true
			}
		}
	}
	for _, c := range s.partial {
		if c.Seq > lo && c.Seq <= hi {
			return true
		}
	}
	return false
}

// waitActive waits until the subscription is subscribed on a live
// connection.
func (s *Subscription) waitActive(ctx context.Context) (*conn, uint32, error) {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return nil, 0, err
		}
		cn, wire, active, ch := s.cn, s.wire, s.active, s.stateCh
		s.mu.Unlock()
		if active && cn != nil && cn.ctx.Err() == nil {
			return cn, wire, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
}
