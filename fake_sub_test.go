package azora_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
)

// subScript hands the subscription messages a client sends to the test,
// which plays the server's part by hand.
type subScript struct {
	subs     chan subReq
	acks     chan *azp.Ack
	unsubs   chan *azp.Unsubscribe
	verifies chan verifyMsg
}

type subReq struct {
	ss *azptest.Session
	m  *azp.Subscribe
}

type verifyMsg struct {
	ss *azptest.Session
	m  *azp.Verify
}

func newSubScript(srv *azptest.Server) *subScript {
	sc := &subScript{subs: make(chan subReq, 64), acks: make(chan *azp.Ack, 64), unsubs: make(chan *azp.Unsubscribe, 64),
		verifies: make(chan verifyMsg, 64)}
	srv.Handle(sc.handler(srv))
	return sc
}

// handler returns the server handler that feeds the script.
func (sc *subScript) handler(*azptest.Server) azptest.Handler {
	return func(ss *azptest.Session, m azp.Msg) {
		switch x := m.(type) {
		case *azp.Subscribe:
			sc.subs <- subReq{ss, x}
		case *azp.Ack:
			sc.acks <- x
		case *azp.Unsubscribe:
			sc.unsubs <- x
			ss.Send(&azp.OK{ReqID: x.ReqID})
		case *azp.Verify:
			sc.verifies <- verifyMsg{ss, x}
		default:
			echo(ss, m)
		}
	}
}

// expect returns the next Subscribe the server received.
func (sc *subScript) expect(t testing.TB) subReq {
	t.Helper()
	select {
	case r := <-sc.subs:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("no Subscribe arrived")
		return subReq{}
	}
}

// accept confirms a Subscribe.
func (r subReq) accept() { r.ss.Send(&azp.Subscribed{ReqID: r.m.ReqID, SubID: r.m.SubID}) }

// batch sends changes for the subscription.
func (r subReq) batch(b azp.Batch) {
	b.SubID = r.m.SubID
	r.ss.Send(&b)
}

func (sc *subScript) expectAck(t testing.TB) *azp.Ack {
	t.Helper()
	select {
	case a := <-sc.acks:
		return a
	case <-time.After(10 * time.Second):
		t.Fatal("no Ack arrived")
		return nil
	}
}

// subscribe starts db.Subscribe and plays the server's confirmation.
func (sc *subScript) subscribe(t testing.TB, db *azora.DB, spec azora.Spec) (*azora.Subscription, subReq) {
	t.Helper()
	type res struct {
		sub *azora.Subscription
		err error
	}
	done := make(chan res, 1)
	go func() {
		sub, err := db.Subscribe(ctx(t), spec)
		done <- res{sub, err}
	}()
	r := sc.expect(t)
	r.accept()
	out := <-done
	require.NoError(t, out.err)
	t.Cleanup(func() { out.sub.Close() })
	return out.sub, r
}

func ins(seq uint64, id int64, v string) azp.Change {
	return azp.Change{Seq: seq, Table: "t", TV: 1, Op: azp.OpInsert, Key: []any{id}, New: []any{id, v}}
}

var shapeT = &azp.Shape{Table: "t", TV: 1, Columns: []string{"id", "v"}, PK: []string{"id"}}

// seqs lists the sequence numbers of an update's changes.
func seqs(u azora.Update) []uint64 {
	out := make([]uint64, len(u.Changes))
	for i, c := range u.Changes {
		out[i] = c.Seq
	}
	return out
}

// TestFakeStagingDedupeAck plays a subscription's whole life: snapshot in
// several batches, live changes staged until their checkpoint, changes
// sent twice, acknowledgements, and a resume after a dropped connection.
func TestFakeStagingDedupeAck(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	sub, r := sc.subscribe(t, db, azora.TableSpec("t", azora.Gt("id", 0)).On(azora.OpsAll))
	require.Equal(t, azp.KindTable, r.m.Kind)
	require.Equal(t, "t", r.m.Table)
	require.Equal(t, &azp.Filter{Op: ">", Col: "id", Val: int64(0)}, r.m.Where)
	require.Zero(t, r.m.Since)
	require.False(t, r.m.NoSnapshot)
	require.Equal(t, azora.KindTable, sub.Spec().Kind)
	require.NotZero(t, sub.ID())

	// The snapshot arrives in two batches; only the second completes it.
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Changes: []azp.Change{ins(0, 1, "a"), ins(0, 2, "b")}})
	c, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err := sub.Next(c)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "nothing is delivered before a checkpoint")
	require.Positive(t, sub.Buffered())
	r.batch(azp.Batch{Snapshot: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 3, "c")}})
	u := next(t, sub, 5*time.Second)
	require.True(t, u.Reset)
	require.True(t, u.Snapshot)
	require.Len(t, u.Changes, 3)
	require.Equal(t, uint64(10), u.Checkpoint)
	require.Equal(t, int64(5), u.Epoch)
	require.Equal(t, []string{"id", "v"}, u.Changes[0].Columns)
	require.Equal(t, []string{"id"}, u.Changes[0].PK)
	require.Equal(t, azora.OpInsert, u.Changes[0].Op)
	cp, ep := sub.Checkpoint()
	require.Equal(t, uint64(10), cp)
	require.Equal(t, int64(5), ep)
	require.Zero(t, sub.Buffered())

	// Live changes: staged until a checkpoint, merged when several
	// checkpoints are waiting, and the previous update is acknowledged by
	// the next call to Next.
	r.batch(azp.Batch{Changes: []azp.Change{ins(11, 4, "d")}})
	r.batch(azp.Batch{Checkpoint: 12, Epoch: 5, Changes: []azp.Change{ins(12, 5, "e")}})
	r.batch(azp.Batch{Checkpoint: 13, Epoch: 5, Changes: []azp.Change{ins(13, 6, "f")}})
	var got []uint64
	for cp != 13 {
		u = next(t, sub, 5*time.Second)
		require.False(t, u.Reset)
		got = append(got, seqs(u)...)
		cp = u.Checkpoint
	}
	require.Equal(t, []uint64{11, 12, 13}, got)
	require.Equal(t, uint64(10), sc.expectAck(t).Checkpoint, "Next acknowledged the snapshot")

	// Changes the server sends again are skipped by sequence number; an
	// idle checkpoint is delivered without changes.
	r.batch(azp.Batch{Checkpoint: 14, Epoch: 5, Changes: []azp.Change{ins(12, 5, "e"), ins(13, 6, "f"), ins(14, 7, "g")}})
	u = next(t, sub, 5*time.Second)
	require.Equal(t, []uint64{14}, seqs(u))
	r.batch(azp.Batch{Checkpoint: 14, Epoch: 5}) // nothing new: dropped
	r.batch(azp.Batch{Checkpoint: 20, Epoch: 5})
	u = next(t, sub, 5*time.Second)
	require.Empty(t, u.Changes)
	require.Equal(t, uint64(20), u.Checkpoint)

	// A partial step, then the connection drops: the subscription resumes
	// from its last checkpoint, the partial data is dropped and what the
	// server replays is delivered exactly once.
	r.batch(azp.Batch{Changes: []azp.Change{ins(21, 8, "h")}})
	eventually(t, 5*time.Second, func() bool { return sub.Buffered() > 0 }, "the partial batch arrived")
	r.ss.Drop()
	r2 := sc.expect(t)
	require.Equal(t, uint64(20), r2.m.Since)
	require.Equal(t, int64(5), r2.m.Epoch)
	require.NotEqual(t, r.m.SubID, r2.m.SubID, "every incarnation has its own wire id")
	r2.accept()
	r.batch(azp.Batch{Checkpoint: 99, Epoch: 5, Changes: []azp.Change{ins(98, 1, "stale")}}) // the dead incarnation
	r2.batch(azp.Batch{Checkpoint: 22, Epoch: 5, Changes: []azp.Change{ins(20, 7, "dup"), ins(21, 8, "h"), ins(22, 9, "i")}})
	u = next(t, sub, 5*time.Second)
	require.Equal(t, []uint64{21, 22}, seqs(u))
	require.Equal(t, uint64(22), u.Checkpoint)

	// Close tells the server and ends Next.
	require.NoError(t, sub.Close())
	select {
	case un := <-sc.unsubs:
		require.Equal(t, r2.m.SubID, un.SubID)
	case <-time.After(5 * time.Second):
		t.Fatal("no Unsubscribe arrived")
	}
	_, err = sub.Next(ctx(t))
	require.ErrorIs(t, err, azora.ErrClosed)
	require.Zero(t, db.Stats().Subscriptions)
}

func TestFakeResumeFromStoredCheckpoint(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	sub, r := sc.subscribe(t, db, azora.RowSpec("t", 7).ResumeFrom(40, 5))
	require.Equal(t, azp.KindRow, r.m.Kind)
	require.Equal(t, []any{int64(7)}, r.m.Key)
	require.Equal(t, uint64(40), r.m.Since)
	require.Equal(t, int64(5), r.m.Epoch)
	cp, ep := sub.Checkpoint()
	require.Equal(t, uint64(40), cp)
	require.Equal(t, int64(5), ep)
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Checkpoint: 42, Epoch: 5, Changes: []azp.Change{ins(40, 7, "old"), ins(41, 7, "new")}})
	u := next(t, sub, 5*time.Second)
	require.Equal(t, []uint64{41}, seqs(u), "changes up to the stored checkpoint are not delivered again")
	require.False(t, u.Reset)

	// Without a snapshot and without a checkpoint yet.
	_, r = sc.subscribe(t, db, azora.DatabaseSpec().WithoutSnapshot())
	require.Equal(t, azp.KindDatabase, r.m.Kind)
	require.True(t, r.m.NoSnapshot)
	// Files subscriptions carry their bucket, prefix and object id.
	_, r = sc.subscribe(t, db, azora.FilesSpec().InBucket("avatars").WithPathPrefix("u/").Object("obj_1"))
	require.Equal(t, azp.KindFiles, r.m.Kind)
	require.Equal(t, "avatars", r.m.Bucket)
	require.Equal(t, "u/", r.m.PathPrefix)
	require.Equal(t, []any{"obj_1"}, r.m.Key)
}

func TestFakeResyncOnGone(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	var resyncs atomic.Int32
	var reason atomic.Value
	db := fakeDB(t, srv, azora.WithHooks(azora.Hooks{OnResync: func(id uint32, why string) {
		reason.Store(why)
		resyncs.Add(1)
	}}))
	sub, r := sc.subscribe(t, db, azora.TableSpec("t"))
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 1, "a")}})
	require.True(t, next(t, sub, 5*time.Second).Reset)

	// The server can no longer resume the subscription: it takes a new
	// snapshot by itself, and the next update says to drop the old rows.
	r.batch(azp.Batch{Checkpoint: 11, Epoch: 5, Changes: []azp.Change{ins(11, 2, "undelivered")}})
	r.ss.Send(&azp.SubEnd{SubID: r.m.SubID, Code: azp.CodeGone, Message: "table t changed"})
	r2 := sc.expect(t)
	require.Zero(t, r2.m.Since, "a new snapshot is requested")
	require.Zero(t, r2.m.Epoch)
	r2.accept()
	r2.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 30, Epoch: 6, Changes: []azp.Change{ins(0, 1, "a2"), ins(0, 2, "b2")}})
	var u azora.Update
	for !u.Reset {
		u = next(t, sub, 5*time.Second)
	}
	require.True(t, u.Snapshot)
	require.Len(t, u.Changes, 2)
	require.Equal(t, int64(6), u.Epoch)
	require.Equal(t, int32(1), resyncs.Load())
	require.Equal(t, "table t changed", reason.Load())
	require.Equal(t, uint64(1), db.Stats().Resyncs)

	// A resume refused as gone after a reconnect resyncs the same way.
	r2.ss.Drop()
	r3 := sc.expect(t)
	require.Equal(t, uint64(30), r3.m.Since)
	r3.ss.Send(&azp.Error{ReqID: r3.m.ReqID, Code: azp.CodeGone, Message: "pruned"})
	r4 := sc.expect(t)
	require.Zero(t, r4.m.Since)
	r4.accept()
	r4.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 50, Epoch: 6})
	u = next(t, sub, 5*time.Second)
	require.True(t, u.Reset)
	require.Empty(t, u.Changes)
	require.Equal(t, int32(2), resyncs.Load())

	// Resync on request.
	sub.Resync()
	r5 := sc.expect(t)
	require.Zero(t, r5.m.Since)
	r5.accept()
	r5.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 60, Epoch: 6})
	require.True(t, next(t, sub, 5*time.Second).Reset)
	require.Equal(t, "requested", reason.Load())
}

func TestFakeManualResyncAndSubEnd(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)

	// ManualResync: the application decides what to do about a lost
	// resume point.
	sub, r := sc.subscribe(t, db, azora.TableSpec("t").Manual())
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5})
	next(t, sub, 5*time.Second)
	r.ss.Send(&azp.SubEnd{SubID: r.m.SubID, Code: azp.CodeGone, Message: "pruned"})
	_, err := sub.Next(ctx(t))
	require.ErrorIs(t, err, azora.ErrResync)
	require.ErrorContains(t, err, "pruned")

	// "unavailable" resumes from the last checkpoint.
	sub, r = sc.subscribe(t, db, azora.TableSpec("t"))
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5})
	next(t, sub, 5*time.Second)
	r.ss.Send(&azp.SubEnd{SubID: r.m.SubID, Code: azp.CodeUnavailable, Message: "database reopening"})
	r2 := sc.expect(t)
	require.Equal(t, uint64(10), r2.m.Since)
	r2.accept()

	// "invalid" ends the subscription: what was delivered stays, then
	// Next reports the error.
	r2.batch(azp.Batch{Checkpoint: 11, Epoch: 5, Changes: []azp.Change{ins(11, 1, "a")}})
	eventually(t, 5*time.Second, func() bool { return sub.Buffered() > 0 }, "the batch arrived")
	r2.ss.Send(&azp.SubEnd{SubID: r2.m.SubID, Code: azp.CodeInvalid, Message: "the filter names a dropped column"})
	u := next(t, sub, 5*time.Second)
	require.Equal(t, []uint64{11}, seqs(u))
	_, err = sub.Next(ctx(t))
	require.True(t, azora.IsInvalid(err), "%v", err)
	require.ErrorContains(t, err, "dropped column")
}

func TestFakeSubscribeErrors(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	answer := func(e *azp.Error) error {
		done := make(chan error, 1)
		go func() {
			_, err := db.Subscribe(ctx(t), azora.TableSpec("t"))
			done <- err
		}()
		r := sc.expect(t)
		e.ReqID = r.m.ReqID
		r.ss.Send(e)
		return <-done
	}
	err := answer(&azp.Error{Code: azp.CodePermissionDenied, Message: "the token does not cover table t"})
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	err = answer(&azp.Error{Code: azp.CodeNotFound, Message: "table t is not published"})
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.Zero(t, db.Stats().Subscriptions, "a refused subscription leaves nothing behind")

	// Rate limits and a database that is reopening are retried.
	done := make(chan error, 1)
	go func() {
		sub, err := db.Subscribe(ctx(t), azora.TableSpec("t"))
		if err == nil {
			sub.Close()
		}
		done <- err
	}()
	r := sc.expect(t)
	r.ss.Send(&azp.Error{ReqID: r.m.ReqID, Code: azp.CodeResourceLimit, Message: "rate", RetryAfterMS: 5})
	r = sc.expect(t)
	r.ss.Send(&azp.Error{ReqID: r.m.ReqID, Code: azp.CodeUnavailable, Message: "reopening"})
	r = sc.expect(t)
	r.accept()
	require.NoError(t, <-done)

	// The caller's context bounds a Subscribe nobody answers.
	c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = db.Subscribe(c, azora.TableSpec("t"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	sc.expect(t)
	require.Zero(t, db.Stats().Subscriptions)
}

func TestFakeNotifyAndWatch(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	type res struct {
		w   *azora.Watcher
		err error
	}
	done := make(chan res, 1)
	go func() {
		w, err := db.Watch(ctx(t), "orders", "items")
		done <- res{w, err}
	}()
	r := sc.expect(t)
	require.Equal(t, azp.KindNotify, r.m.Kind)
	require.Equal(t, []string{"orders", "items"}, r.m.Tables)
	r.accept()
	out := <-done
	require.NoError(t, out.err)
	w := out.w

	r.batch(azp.Batch{Tables: []string{"orders"}, Checkpoint: 5})
	n, err := w.Next(ctx(t))
	require.NoError(t, err)
	require.Equal(t, azora.Notice{Tables: []string{"orders"}}, n)
	require.True(t, n.Has("ORDERS"))
	require.False(t, n.Has("items"))

	// After a reconnect signals may have been missed: one notice says so.
	r.ss.Drop()
	r2 := sc.expect(t)
	require.Zero(t, r2.m.Since, "notify subscriptions do not resume")
	r2.accept()
	n, err = w.Next(ctx(t))
	require.NoError(t, err)
	require.True(t, n.Everything)
	require.True(t, n.Has("anything"))

	// The iterator form, ended by Close.
	r2.batch(azp.Batch{Tables: []string{"items"}})
	r2.batch(azp.Batch{Tables: []string{"orders", "items"}})
	var seen [][]string
	var last error
	for n, err := range w.Notices(ctx(t)) {
		if err != nil {
			last = err
			break
		}
		seen = append(seen, n.Tables)
		if len(seen) == 2 {
			go w.Close()
		}
	}
	require.Equal(t, [][]string{{"items"}, {"orders", "items"}}, seen)
	require.ErrorIs(t, last, azora.ErrClosed)
}

func TestFakeVerify(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	sub, r := sc.subscribe(t, db, azora.TableSpec("t"))
	rep := azora.NewReplica()
	answer := func(v azp.Verified) {
		t.Helper()
		select {
		case m := <-sc.verifies:
			v.ReqID, v.SubID = m.m.ReqID, m.m.SubID
			m.ss.Send(&v)
		case <-time.After(5 * time.Second):
			t.Error("no Verify arrived")
		}
	}
	sumOf := func(rows ...[]any) (sum uint64) {
		for _, row := range rows {
			sum += azora.RowHash("t", row[:1], row)
		}
		return sum
	}
	row1, row2, row3 := []any{int64(1), "a"}, []any{int64(2), "b"}, []any{int64(3), "c"}

	// Before any update there is nothing to compare.
	go answer(azp.Verified{Epoch: 5, Checkpoint: 10, Rows: 1, Sum: sumOf(row1)})
	_, err := sub.Verify(ctx(t), rep.Checksum)
	require.ErrorIs(t, err, azora.ErrRetry)

	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 1, "a")}})
	rep.Apply(next(t, sub, 5*time.Second))

	// Same checkpoint: compared at once.
	go answer(azp.Verified{Epoch: 5, Checkpoint: 10, Rows: 1, Sum: sumOf(row1)})
	res, err := sub.Verify(ctx(t), rep.Checksum)
	require.NoError(t, err)
	require.True(t, res.Match)
	require.Equal(t, azora.VerifyResult{Match: true, Checkpoint: 10, Epoch: 5, ServerRows: 1, ServerSum: sumOf(row1), LocalRows: 1, LocalSum: sumOf(row1)}, res)

	// The server is ahead: its checksum is remembered, the next update
	// stops exactly at its checkpoint even though more has arrived, and
	// the following Verify compares without asking again.
	r.batch(azp.Batch{Checkpoint: 13, Epoch: 5, Changes: []azp.Change{ins(11, 2, "b"), ins(13, 3, "c")}})
	eventually(t, 5*time.Second, func() bool { return sub.Buffered() > 0 }, "the batch arrived")
	go answer(azp.Verified{Epoch: 5, Checkpoint: 12, Rows: 2, Sum: sumOf(row1, row2)})
	pending, err := sub.Verify(ctx(t), rep.Checksum)
	require.ErrorIs(t, err, azora.ErrRetry)
	require.Equal(t, uint64(12), pending.Checkpoint)
	u := next(t, sub, 5*time.Second)
	require.Equal(t, uint64(12), u.Checkpoint, "the update stops at the verified checkpoint")
	require.Equal(t, []uint64{11}, seqs(u))
	rep.Apply(u)
	res, err = sub.Verify(ctx(t), rep.Checksum)
	require.NoError(t, err)
	require.True(t, res.Match)
	require.Equal(t, uint64(12), res.Checkpoint)
	rep.Apply(next(t, sub, 5*time.Second))
	require.Equal(t, 3, rep.Len("t"))

	// A copy that diverged is reported.
	go answer(azp.Verified{Epoch: 5, Checkpoint: 13, Rows: 3, Sum: sumOf(row1, row2, row3) + 1})
	res, err = sub.Verify(ctx(t), rep.Checksum)
	require.NoError(t, err)
	require.False(t, res.Match)
	require.Equal(t, int64(3), res.LocalRows)

	// The server ahead with nothing of ours in between: same rows.
	r.batch(azp.Batch{Checkpoint: 20, Epoch: 5})
	eventually(t, 5*time.Second, func() bool { return sub.Buffered() > 0 }, "the checkpoint arrived")
	go answer(azp.Verified{Epoch: 5, Checkpoint: 18, Rows: 3, Sum: sumOf(row1, row2, row3)})
	res, err = sub.Verify(ctx(t), rep.Checksum)
	require.NoError(t, err)
	require.True(t, res.Match)

	// Errors of the server are returned; a notify subscription has no rows.
	go func() {
		m := <-sc.verifies
		m.ss.Send(&azp.Error{ReqID: m.m.ReqID, Code: azp.CodePermissionDenied, Message: "no"})
	}()
	_, err = sub.Verify(ctx(t), rep.Checksum)
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	nsub, _ := sc.subscribe(t, db, azora.NotifySpec())
	_, err = nsub.Verify(ctx(t), rep.Checksum)
	require.ErrorContains(t, err, "notify")
}
