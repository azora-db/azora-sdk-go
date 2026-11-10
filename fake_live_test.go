package azora_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
)

type item struct {
	ID int64
	V  string
}

// liveOn starts a typed live view against the scripted server.
func liveOn[T any](t testing.TB, sc *subScript, db *azora.DB, spec azora.Spec, opts ...azora.LiveOptions) (*azora.LiveTable[T], subReq) {
	t.Helper()
	type res struct {
		lt  *azora.LiveTable[T]
		err error
	}
	done := make(chan res, 1)
	go func() {
		lt, err := azora.Live[T](ctx(t), db, spec, opts...)
		done <- res{lt, err}
	}()
	r := sc.expect(t)
	r.accept()
	out := <-done
	require.NoError(t, out.err)
	t.Cleanup(func() { out.lt.Close() })
	return out.lt, r
}

func upd(seq uint64, id int64, old, v string) azp.Change {
	return azp.Change{Seq: seq, Table: "t", TV: 1, Op: azp.OpUpdate, Key: []any{id}, New: []any{id, v}, Old: []any{id, old}}
}

func del(seq uint64, id int64, old string) azp.Change {
	return azp.Change{Seq: seq, Table: "t", TV: 1, Op: azp.OpDelete, Key: []any{id}, Old: []any{id, old}}
}

func TestFakeLiveTable(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	lt, r := liveOn[item](t, sc, db, azora.TableSpec("t"))
	require.False(t, lt.Ready())
	require.Zero(t, lt.Len())
	require.Empty(t, lt.Rows())
	c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	require.ErrorIs(t, lt.Wait(c), context.DeadlineExceeded)
	cancel()

	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 3, "c"), ins(0, 1, "a"), ins(0, 2, "b")}})
	require.NoError(t, lt.Wait(ctx(t)))
	require.True(t, lt.Ready())
	require.Equal(t, []item{{1, "a"}, {2, "b"}, {3, "c"}}, lt.Rows(), "rows come in primary key order")
	require.Equal(t, 3, lt.Len())
	got, ok := lt.Get(2)
	require.True(t, ok)
	require.Equal(t, item{2, "b"}, got)
	_, ok = lt.Get(9)
	require.False(t, ok)
	cols, pk := lt.Columns()
	require.Equal(t, []string{"id", "v"}, cols)
	require.Equal(t, []string{"id"}, pk)
	cp, ep := lt.Checkpoint()
	require.Equal(t, uint64(10), cp)
	require.Equal(t, int64(5), ep)
	require.Equal(t, uint64(1), lt.Version())

	// A listener first gets the current rows, then every step, with the
	// old and new row of each change.
	var mu sync.Mutex
	var steps []azora.LiveUpdate[item]
	stop := lt.OnUpdate(func(u azora.LiveUpdate[item]) {
		mu.Lock()
		steps = append(steps, u)
		mu.Unlock()
	})
	r.batch(azp.Batch{Checkpoint: 13, Epoch: 5, Changes: []azp.Change{ins(11, 4, "d"), upd(12, 2, "b", "B"), del(13, 1, "a")}})
	eventually(t, 5*time.Second, func() bool { return lt.Version() == 2 }, "the step was applied")
	require.Equal(t, []item{{2, "B"}, {3, "c"}, {4, "d"}}, lt.Rows())
	mu.Lock()
	require.Len(t, steps, 2)
	require.True(t, steps[0].Reset)
	require.Len(t, steps[0].Changes, 3)
	require.Equal(t, item{1, "a"}, steps[0].Changes[0].New)
	require.Equal(t, uint64(10), steps[0].Checkpoint)
	require.False(t, steps[1].Reset)
	require.Equal(t, []azora.LiveChange[item]{
		{Op: azora.OpInsert, Key: []any{int64(4)}, New: item{4, "d"}},
		{Op: azora.OpUpdate, Key: []any{int64(2)}, New: item{2, "B"}, Old: item{2, "b"}},
		{Op: azora.OpDelete, Key: []any{int64(1)}, Old: item{1, "a"}},
	}, steps[1].Changes)
	require.Equal(t, uint64(13), steps[1].Checkpoint)
	mu.Unlock()
	stop()
	stop() // twice is fine

	// The keys and rows of All, and the checksum the server would compute.
	var keys []any
	for k, row := range lt.All() {
		keys = append(keys, k[0])
		require.Equal(t, k[0], row.ID)
	}
	require.Equal(t, []any{int64(2), int64(3), int64(4)}, keys)
	n, sum := lt.Checksum()
	require.Equal(t, int64(3), n)
	want := azora.RowHash("t", []any{int64(2)}, []any{int64(2), "B"}) + azora.RowHash("t", []any{int64(3)}, []any{int64(3), "c"}) +
		azora.RowHash("t", []any{int64(4)}, []any{int64(4), "d"})
	require.Equal(t, want, sum)

	// Verify asks the server and compares; a mismatch is counted.
	go func() {
		m := <-sc.verifies
		m.ss.Send(&azp.Verified{ReqID: m.m.ReqID, SubID: m.m.SubID, Epoch: 5, Checkpoint: 13, Rows: 3, Sum: want})
		m = <-sc.verifies
		m.ss.Send(&azp.Verified{ReqID: m.m.ReqID, SubID: m.m.SubID, Epoch: 5, Checkpoint: 13, Rows: 3, Sum: want + 1})
	}()
	res, err := lt.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)
	res, err = lt.Verify(ctx(t))
	require.NoError(t, err)
	require.False(t, res.Match)
	require.Equal(t, uint64(1), lt.Mismatches())

	// The server ahead of the view: Verify applies the pending step
	// first, then compares at the server's checkpoint.
	want2 := want + azora.RowHash("t", []any{int64(5)}, []any{int64(5), "e"})
	go func() {
		m := <-sc.verifies
		m.ss.Send(&azp.Verified{ReqID: m.m.ReqID, SubID: m.m.SubID, Epoch: 5, Checkpoint: 14, Rows: 4, Sum: want2})
		r.batch(azp.Batch{Checkpoint: 14, Epoch: 5, Changes: []azp.Change{ins(14, 5, "e")}})
	}()
	res, err = lt.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)
	require.Equal(t, uint64(14), res.Checkpoint)
	require.Equal(t, 4, lt.Len())

	// A lost resume point replaces the rows in one step.
	r.ss.Send(&azp.SubEnd{SubID: r.m.SubID, Code: azp.CodeGone, Message: "schema"})
	r2 := sc.expect(t)
	r2.accept()
	require.Equal(t, 4, lt.Len(), "the old rows stay until the new snapshot is complete")
	r2.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 30, Epoch: 6, Changes: []azp.Change{ins(0, 7, "g")}})
	eventually(t, 5*time.Second, func() bool { return lt.Len() == 1 }, "the new snapshot replaced the rows")
	require.Equal(t, []item{{7, "g"}}, lt.Rows())

	require.Nil(t, lt.Err())
	require.NoError(t, lt.Close())
	require.ErrorIs(t, lt.Err(), azora.ErrClosed)
	<-lt.Done()
	require.Equal(t, []item{{7, "g"}}, lt.Rows(), "the rows stay readable")
	_, err = lt.Verify(ctx(t))
	require.ErrorIs(t, err, azora.ErrClosed)
	require.ErrorIs(t, lt.Wait(ctx(t)), nil, "a view that was ready stays so")
}

func TestFakeLiveUpdatesIterator(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	lt, r := liveOn[azora.Record](t, sc, db, azora.RowSpec("t", 1))
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 1, "a")}})
	require.NoError(t, lt.Wait(ctx(t)))
	require.Equal(t, []azora.Record{{"id": int64(1), "v": "a"}}, lt.Rows())

	go func() {
		for i := uint64(11); i <= 15; i++ {
			r.batch(azp.Batch{Checkpoint: i, Epoch: 5, Changes: []azp.Change{upd(i, 1, "x", "v"+string(rune('0'+i-10)))}})
			time.Sleep(2 * time.Millisecond)
		}
	}()
	var vals []string
	for u, err := range lt.Updates(ctx(t)) {
		require.NoError(t, err)
		for _, c := range u.Changes {
			vals = append(vals, c.New["v"].(string))
		}
		if u.Checkpoint == 15 {
			break
		}
	}
	require.Equal(t, []string{"a", "v1", "v2", "v3", "v4", "v5"}, vals, "the current rows, then every change, none lost")

	// The iteration ends with the context, or with the view.
	c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var last error
	for _, err := range lt.Updates(c) {
		last = err
	}
	require.ErrorIs(t, last, context.DeadlineExceeded)
	go func() {
		time.Sleep(20 * time.Millisecond)
		lt.Close()
	}()
	for _, err := range lt.Updates(ctx(t)) {
		last = err
	}
	require.ErrorIs(t, last, azora.ErrClosed)
}

func TestFakeLiveAutoVerifyResyncs(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)
	var reported atomic.Int32
	lt, r := liveOn[item](t, sc, db, azora.TableSpec("t"), azora.LiveOptions{VerifyEvery: 20 * time.Millisecond,
		OnMismatch: func(res azora.VerifyResult) { reported.Add(1) }})
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 1, "a")}})
	require.NoError(t, lt.Wait(ctx(t)))
	// The server reports another checksum than the view's: the view
	// reports the mismatch and takes a new snapshot.
	m := <-sc.verifies
	m.ss.Send(&azp.Verified{ReqID: m.m.ReqID, SubID: m.m.SubID, Epoch: 5, Checkpoint: 10, Rows: 2, Sum: 42})
	r2 := sc.expect(t)
	require.Zero(t, r2.m.Since)
	r2.accept()
	r2.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 20, Epoch: 5, Changes: []azp.Change{ins(0, 1, "a"), ins(0, 2, "b")}})
	eventually(t, 5*time.Second, func() bool { return lt.Len() == 2 }, "resynchronized")
	require.Equal(t, int32(1), reported.Load())
	require.Equal(t, uint64(1), lt.Mismatches())
}

func TestFakeLiveErrors(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	db := fakeDB(t, srv)

	// Specs a live view cannot be built on.
	for _, spec := range []azora.Spec{azora.DatabaseSpec(), azora.NotifySpec(), azora.TableSpec("t").On(azora.OpsInsert),
		azora.TableSpec("t").WithoutSnapshot(), azora.TableSpec("t").ResumeFrom(5, 1), azora.TableSpec("t").Manual()} {
		_, err := azora.Live[item](ctx(t), db, spec)
		require.Error(t, err, "%+v", spec)
	}
	_, err := azora.Live[int](ctx(t), db, azora.TableSpec("t"))
	require.ErrorContains(t, err, "struct")
	_, err = azora.Live[map[int]string](ctx(t), db, azora.TableSpec("t"))
	require.Error(t, err)

	// A row that does not fit T stops the view with a clear error.
	type strict struct {
		ID int8
		V  string
	}
	lt, r := liveOn[strict](t, sc, db, azora.TableSpec("t"))
	r.ss.Send(shapeT)
	r.batch(azp.Batch{Snapshot: true, Reset: true, Checkpoint: 10, Epoch: 5, Changes: []azp.Change{ins(0, 1000, "a")}})
	err = lt.Wait(ctx(t))
	require.ErrorContains(t, err, "overflows int8")
	require.ErrorContains(t, err, `column "id"`)
	require.Equal(t, err, lt.Err())
	<-lt.Done()

	// The server ending the subscription stops the view with its error.
	lt2, r2 := liveOn[item](t, sc, db, azora.TableSpec("t"))
	r2.ss.Send(&azp.SubEnd{SubID: r2.m.SubID, Code: azp.CodePermissionDenied, Message: "the token lost access"})
	<-lt2.Done()
	require.True(t, azora.IsPermissionDenied(lt2.Err()), "%v", lt2.Err())
	require.True(t, azora.IsPermissionDenied(lt2.Wait(ctx(t))))

	// Closing the DB stops every view.
	lt3, _ := liveOn[item](t, sc, db, azora.TableSpec("t"))
	db.Close()
	<-lt3.Done()
	require.ErrorIs(t, lt3.Err(), azora.ErrClosed)
	requireNoLeaks(t)
}

func TestFakePool(t *testing.T) {
	srv := azptest.New(t)
	sc := newSubScript(srv)
	var auths atomic.Int32
	var mu sync.Mutex
	perSession := map[int]int{}
	inner := sc.handler(srv)
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		switch m.(type) {
		case *azp.Query:
			mu.Lock()
			perSession[ss.N]++
			mu.Unlock()
			time.Sleep(time.Millisecond) // requests overlap
		case *azp.Auth:
			auths.Add(1)
		}
		inner(ss, m)
	})
	var connects atomic.Int32
	db := fakeDB(t, srv, azora.WithPool(4), azora.WithHooks(azora.Hooks{OnConnect: func(azora.Welcome) { connects.Add(1) }}))
	st := db.Stats()
	require.Equal(t, 4, st.PoolSize)
	require.Equal(t, 4, st.Connections, "every pooled connection is up when Connect returns")
	require.Equal(t, int32(4), connects.Load())
	require.Equal(t, 4, srv.Accepted())

	run := func(n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := db.Query(ctx(t), "SELECT 1")
				require.NoError(t, err)
			}()
		}
		wg.Wait()
	}
	run(200)
	mu.Lock()
	require.Len(t, perSession, 4, "requests are spread over every connection: %v", perSession)
	for n, c := range perSession {
		require.Greater(t, c, 20, "session %d got %d of 200", n, c)
	}
	mu.Unlock()

	// Subscriptions live on the first connection.
	_, r := sc.subscribe(t, db, azora.TableSpec("t"))
	require.Equal(t, 1, r.ss.N)

	// SetCredential reaches every connection.
	require.NoError(t, db.SetCredential(ctx(t), "next"))
	require.Equal(t, int32(4), auths.Load())

	// Losing connections does not fail requests: they use the others, and
	// the pool heals.
	for _, ss := range srv.Sessions() {
		if ss.N == 2 || ss.N == 3 {
			ss.Drop()
		}
	}
	run(100)
	eventually(t, 5*time.Second, func() bool { return db.Stats().Connections == 4 }, "the pool is whole again")
	require.Equal(t, 6, srv.Accepted())
	require.Equal(t, "next", srv.Hellos()[5].Credential, "new connections use the new credential")
}
