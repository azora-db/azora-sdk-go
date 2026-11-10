package azora

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// onBatch stages a batch (reader goroutine).
func (s *Subscription) onBatch(b *azp.Batch, size int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.SubID != s.wire || s.err != nil {
		return
	}
	if s.spec.Kind == KindNotify {
		if len(b.Tables) > 0 {
			if len(s.notifyQ) >= 1024 {
				s.notifyQ = [][]string{{"*"}}
			} else {
				s.notifyQ = append(s.notifyQ, b.Tables)
			}
			s.signalLocked()
		}
		return
	}
	if b.Reset {
		// A new snapshot replaces everything not yet delivered.
		s.ready, s.readyBytes, s.partial, s.partialBytes = nil, 0, nil, 0
		s.pReset = true
	}
	if b.Snapshot {
		s.pSnap = true
	}
	thr, top := s.threshold, s.threshold
	for i := range b.Changes {
		w := &b.Changes[i]
		if w.Seq != 0 && w.Seq <= thr {
			continue // sent again after a server-side resume
		}
		s.partial = append(s.partial, Change{Seq: w.Seq, Table: w.Table, Op: Op(w.Op), Key: w.Key, New: w.New, Old: w.Old, tv: w.TV})
		top = max(top, w.Seq)
	}
	s.threshold = top
	s.partialBytes += int64(size)
	if b.Checkpoint == 0 {
		return
	}
	if b.Checkpoint <= s.rcvCP && len(s.partial) == 0 && !s.pReset {
		s.partialBytes = 0
		return
	}
	s.ready = append(s.ready, staged{changes: s.partial, cp: b.Checkpoint, epoch: b.Epoch, reset: s.pReset, snapshot: s.pSnap, bytes: s.partialBytes})
	s.readyBytes += s.partialBytes
	s.partial, s.pReset, s.pSnap, s.partialBytes = nil, false, false, 0
	s.rcvCP, s.rcvEpoch = b.Checkpoint, b.Epoch
	s.threshold = max(s.threshold, b.Checkpoint)
	s.needSnapshot = false
	s.signalLocked()
}

// onEnd passes a SubEnd of the current incarnation to run (reader
// goroutine).
func (s *Subscription) onEnd(x *azp.SubEnd) {
	s.mu.Lock()
	cur := s.wire
	s.mu.Unlock()
	if x.SubID != cur {
		return
	}
	select {
	case s.endCh <- x:
	default:
	}
}

// Next returns the next update, waiting for one. Updates are consistent
// steps ending at a checkpoint. Calling Next again acknowledges the
// previous update (the application has applied it), which lets the server
// send more: an application that stops calling Next stops the stream
// (bounded buffering) instead of growing memory.
func (s *Subscription) Next(ctx context.Context) (Update, error) {
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	s.flushAck()
	for {
		s.mu.Lock()
		if errors.Is(s.err, ErrClosed) {
			s.mu.Unlock()
			return Update{}, ErrClosed
		}
		if len(s.notifyQ) > 0 {
			t := s.notifyQ[0]
			s.notifyQ = s.notifyQ[1:]
			s.mu.Unlock()
			return Update{Tables: t}, nil
		}
		if len(s.ready) > 0 {
			u := s.takeLocked()
			s.mu.Unlock()
			s.resolve(ctx, &u)
			return u, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return Update{}, err
		}
		s.mu.Unlock()
		select {
		case <-s.signal:
		case <-ctx.Done():
			return Update{}, ctx.Err()
		}
	}
}

// flushAck acknowledges the last delivered checkpoint.
func (s *Subscription) flushAck() {
	s.mu.Lock()
	cp, cn, wire, active := s.ackPending, s.cn, s.wire, s.active
	s.ackPending = 0
	s.mu.Unlock()
	if cp > 0 && active && cn != nil {
		cn.send(&azp.Ack{SubID: wire, Checkpoint: cp}) // a lost connection resumes anyway
	}
}

// takeLocked builds the next update from staged checkpoints: every
// complete step merged, stopping at a pending Verify checkpoint (splitting
// a step there if needed) and at snapshots.
func (s *Subscription) takeLocked() Update {
	if s.pv != nil && (s.pv.Epoch != s.ready[0].epoch || s.pv.Checkpoint <= s.delivered) {
		s.pv = nil
	}
	limit := uint64(math.MaxUint64)
	if s.pv != nil {
		limit = s.pv.Checkpoint
	}
	var u Update
	n := 0
	var bytes int64
	if f := s.ready[0]; f.reset || f.snapshot {
		u = Update{Reset: f.reset, Snapshot: f.snapshot, Changes: f.changes, Checkpoint: f.cp, Epoch: f.epoch}
		bytes, n = f.bytes, 1
		if s.pv != nil && s.pv.Checkpoint != f.cp {
			s.pv = nil
		}
	} else {
		for n < len(s.ready) {
			r := s.ready[n]
			if r.reset || r.snapshot || r.cp > limit {
				break
			}
			if n == 0 {
				u.Changes = r.changes
			} else {
				u.Changes = append(u.Changes, r.changes...)
			}
			u.Checkpoint, u.Epoch = r.cp, r.epoch
			bytes += r.bytes
			n++
		}
		if u.Checkpoint < limit && n < len(s.ready) {
			if r := &s.ready[n]; !r.reset && !r.snapshot && r.cp > limit {
				k := 0
				for k < len(r.changes) && r.changes[k].Seq <= limit {
					k++
				}
				u.Changes = append(u.Changes[:len(u.Changes):len(u.Changes)], r.changes[:k]...)
				r.changes = r.changes[k:]
				u.Checkpoint, u.Epoch = limit, r.epoch
			}
		}
	}
	for i := 0; i < n; i++ {
		s.ready[i] = staged{}
	}
	s.ready = s.ready[n:]
	if len(s.ready) == 0 {
		s.ready = nil
	}
	s.readyBytes = max(0, s.readyBytes-bytes)
	if len(s.ready) == 0 {
		s.readyBytes = 0
	}
	s.delivered, s.deliverEpoch, s.ackPending = u.Checkpoint, u.Epoch, u.Checkpoint
	return u
}

// resolve fills Columns and PK from the shapes the server sent.
func (s *Subscription) resolve(ctx context.Context, u *Update) {
	var lastTable string
	var lastTV int64
	var last *shapeInfo
	for i := range u.Changes {
		ch := &u.Changes[i]
		if last == nil || ch.Table != lastTable || ch.tv != lastTV {
			last = s.db.shapes.wait(ctx, ch.Table, ch.tv, 2*time.Second)
			lastTable, lastTV = ch.Table, ch.tv
		}
		if last != nil {
			ch.Columns, ch.PK = last.cols, last.pk
		}
	}
}
