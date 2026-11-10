package azora_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/devengine"
)

// Integration tests: live views, tokens and restarts against the real
// engine.

type liveOrder struct {
	ID         int64
	CustomerID *int64
	Status     string
	Total      float64
	Note       []byte
}

// liveEquals reports whether the view's rows are exactly the query's.
func liveEquals(t testing.TB, db *azora.DB, lt *azora.LiveTable[liveOrder], query string, args ...any) bool {
	want, err := azora.All[liveOrder](ctx(t), db, query, args...)
	require.NoError(t, err)
	got := lt.Rows()
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Status != want[i].Status || got[i].Total != want[i].Total {
			return false
		}
	}
	return true
}

func TestEngineLiveTable(t *testing.T) {
	f := engine(t)
	db := f.connect(t, azora.WithPool(3))
	seed(t, db, "t_live", 20)

	all, err := azora.Live[liveOrder](ctx(t), db, azora.TableSpec("t_live"))
	require.NoError(t, err)
	defer all.Close()
	paid, err := azora.Live[liveOrder](ctx(t), db, azora.TableSpec("t_live", *azora.MustParseFilter("status = 'paid' AND total >= 100")))
	require.NoError(t, err)
	defer paid.Close()
	one, err := db.Live(ctx(t), azora.RowSpec("t_live", 7))
	require.NoError(t, err)
	defer one.Close()
	require.NoError(t, all.Wait(ctx(t)))
	require.NoError(t, paid.Wait(ctx(t)))
	require.NoError(t, one.Wait(ctx(t)))

	require.Equal(t, 20, all.Len())
	rows := all.Rows()
	require.Equal(t, int64(1), rows[0].ID)
	require.Equal(t, int64(20), rows[19].ID)
	require.Equal(t, int64(1), *rows[0].CustomerID)
	require.Equal(t, []byte{1}, rows[0].Note)
	require.Equal(t, 6, paid.Len(), "ids 10, 12, ... 20")
	rec, ok := one.Get(7)
	require.True(t, ok)
	require.Equal(t, azora.Record{"id": int64(7), "customer_id": int64(1), "status": "new", "total": 70.0, "note": []byte{7}}, rec)
	require.Equal(t, 1, one.Len())

	// Typed change events, in the order of the database.
	var mu sync.Mutex
	var events []azora.LiveChange[liveOrder]
	stop := all.OnUpdate(func(u azora.LiveUpdate[liveOrder]) {
		if u.Reset {
			return // the current rows, delivered on registration
		}
		mu.Lock()
		events = append(events, u.Changes...)
		mu.Unlock()
	})
	defer stop()
	mustExec(t, db, "UPDATE t_live SET status = 'paid', total = 500 WHERE id = 7")
	mustExec(t, db, "DELETE FROM t_live WHERE id = 20")
	mustExec(t, db, "INSERT INTO t_live (id, status, total) VALUES (21, 'new', 1)")
	eventually(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(events) == 3 }, "three changes")
	mu.Lock()
	require.Equal(t, azora.OpUpdate, events[0].Op)
	require.Equal(t, "new", events[0].Old.Status)
	require.Equal(t, "paid", events[0].New.Status)
	require.Equal(t, 500.0, events[0].New.Total)
	require.Equal(t, azora.OpDelete, events[1].Op)
	require.Equal(t, int64(20), events[1].Old.ID)
	require.Equal(t, azora.OpInsert, events[2].Op)
	require.Nil(t, events[2].New.CustomerID, "NULL scans to a nil pointer")
	mu.Unlock()
	eventually(t, 10*time.Second, func() bool { r, _ := one.Get(7); return r["status"] == "paid" }, "the row view follows")
	eventually(t, 10*time.Second, func() bool { _, ok := paid.Get(7); return ok && paid.Len() == 6 }, "7 moved into the filter, 20 left")

	// Many concurrent writers: the views converge to the database and
	// their checksums match the server's, also while writes continue.
	var wg sync.WaitGroup
	var stopWriters atomic.Bool
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stopWriters.Load(); i++ {
				id := 100 + w*1000 + i%40
				var err error
				switch i % 4 {
				case 0, 1:
					_, err = db.Exec(ctx(t), "INSERT OR REPLACE INTO t_live (id, customer_id, status, total) VALUES (?, ?, ?, ?)", id, w, "paid", float64(i))
				case 2:
					_, err = db.Exec(ctx(t), "UPDATE t_live SET status = 'new', total = total + 1 WHERE id = ?", id-1)
				case 3:
					_, err = db.Batch(ctx(t), azora.Stmt("DELETE FROM t_live WHERE id = ?", id-3),
						azora.Stmt("UPDATE t_live SET total = 1000 WHERE customer_id = ? AND total < 5", w))
				}
				if err != nil {
					t.Errorf("writer: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		res, err := all.Verify(ctx(t))
		require.NoError(t, err)
		require.True(t, res.Match, "while writing: %+v", res)
		res, err = paid.Verify(ctx(t))
		require.NoError(t, err)
		require.True(t, res.Match, "filtered, while writing: %+v", res)
		time.Sleep(20 * time.Millisecond)
	}
	stopWriters.Store(true)
	wg.Wait()
	eventually(t, 20*time.Second, func() bool { return liveEquals(t, db, all, "SELECT * FROM t_live ORDER BY id") }, "the view converges")
	eventually(t, 20*time.Second, func() bool {
		return liveEquals(t, db, paid, "SELECT * FROM t_live WHERE status = 'paid' AND total >= 100 ORDER BY id")
	}, "the filtered view converges")
	for _, lt := range []*azora.LiveTable[liveOrder]{all, paid} {
		res, err := lt.Verify(ctx(t))
		require.NoError(t, err)
		require.True(t, res.Match, "%+v", res)
		n, _ := lt.Checksum()
		require.Equal(t, res.ServerRows, n)
		require.Zero(t, lt.Mismatches())
	}
	require.Greater(t, all.Version(), uint64(10))
	require.Zero(t, db.Stats().Resyncs)

	// Resync on request: a new snapshot replaces the rows; the content is
	// the same.
	before := all.Version()
	all.Resync()
	eventually(t, 10*time.Second, func() bool { return all.Version() > before }, "resynchronized")
	require.True(t, liveEquals(t, db, all, "SELECT * FROM t_live ORDER BY id"))
	require.Equal(t, uint64(1), db.Stats().Resyncs)

	// Periodic verification finds nothing to fix.
	checked, err := azora.Live[liveOrder](ctx(t), db, azora.TableSpec("t_live"), azora.LiveOptions{VerifyEvery: 30 * time.Millisecond,
		OnMismatch: func(res azora.VerifyResult) { t.Errorf("mismatch: %+v", res) }})
	require.NoError(t, err)
	defer checked.Close()
	require.NoError(t, checked.Wait(ctx(t)))
	mustExec(t, db, "UPDATE t_live SET total = total + 1")
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, checked.Mismatches())
	require.Equal(t, all.Len(), checked.Len())
}

func TestEngineTokens(t *testing.T) {
	f := engine(t)
	admin := f.connect(t)
	clearTable(t, admin, "t_token")
	clearTable(t, admin, "t_ops")
	for i := 1; i <= 6; i++ {
		mustExec(t, admin, "INSERT INTO t_token (id, customer_id, status) VALUES (?, ?, 'new')", i, i%2) // odd ids: customer 1
	}

	// An unrestricted read token: SQL reads work, writes do not.
	tok := f.connectURL(t, f.URL(testDB), azora.WithToken(f.Token))
	w, _ := tok.Welcome()
	require.Equal(t, "user:1", w.Principal)
	require.Equal(t, []string{"read"}, w.Scopes)
	n, err := azora.Scalar[int](ctx(t), tok, "SELECT COUNT(*) FROM t_token")
	require.NoError(t, err)
	require.Equal(t, 6, n)
	_, err = tok.Exec(ctx(t), "DELETE FROM t_token")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)

	// A token pinned to a table and a filter: no SQL at all, subscriptions
	// narrowed by the filter whatever the client asks for.
	rt := f.connectURL(t, f.WebSocketURL(testDB), azora.WithToken(f.Restricted))
	_, err = rt.Query(ctx(t), "SELECT 1")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	_, err = rt.Exec(ctx(t), "DELETE FROM t_token")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	_, err = rt.Ping(ctx(t))
	require.NoError(t, err, "Ping needs no SQL")
	_, err = rt.Subscribe(ctx(t), azora.TableSpec("t_ops"))
	require.True(t, azora.IsPermissionDenied(err), "another table: %v", err)
	_, err = rt.Subscribe(ctx(t), azora.DatabaseSpec())
	require.True(t, azora.IsPermissionDenied(err), "database-wide: %v", err)
	_, err = rt.Watch(ctx(t), "t_token")
	require.True(t, azora.IsPermissionDenied(err), "notices on a filtered table: %v", err)

	mine, err := azora.Live[liveOrder](ctx(t), rt, azora.TableSpec("t_token"))
	require.NoError(t, err)
	defer mine.Close()
	require.NoError(t, mine.Wait(ctx(t)))
	ids := func() (out []int64) {
		for _, o := range mine.Rows() {
			out = append(out, o.ID)
		}
		return out
	}
	require.Equal(t, []int64{1, 3, 5}, ids(), "only customer 1's rows")
	// The client's own filter is added to the pinned one, never replaces it.
	narrowed, err := azora.Live[liveOrder](ctx(t), rt, azora.TableSpec("t_token", azora.Or(azora.Gt("id", 2), azora.Eq("customer_id", 0))))
	require.NoError(t, err)
	defer narrowed.Close()
	require.NoError(t, narrowed.Wait(ctx(t)))
	require.Equal(t, 2, narrowed.Len(), "ids 3 and 5")
	// A row of another customer is invisible, also by key.
	other, err := rt.Live(ctx(t), azora.RowSpec("t_token", 2))
	require.NoError(t, err)
	defer other.Close()
	require.NoError(t, other.Wait(ctx(t)))
	require.Zero(t, other.Len())

	mustExec(t, admin, "INSERT INTO t_token (id, customer_id, status) VALUES (7, 1, 'new'), (8, 0, 'new')")
	mustExec(t, admin, "UPDATE t_token SET customer_id = 0 WHERE id = 1") // leaves the token's view
	mustExec(t, admin, "UPDATE t_token SET customer_id = 1 WHERE id = 2") // enters it
	eventually(t, 10*time.Second, func() bool { return len(ids()) == 4 && ids()[0] == 2 }, "the pinned filter follows the rows: %v", ids())
	require.Equal(t, []int64{2, 3, 5, 7}, ids())
	eventually(t, 10*time.Second, func() bool { return other.Len() == 1 }, "row 2 became visible")
	res, err := mine.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)

	// Signed URLs over the realtime connection work with a token.
	st := f.connect(t, azora.WithStorageURL(f.HTTPURL())).Storage()
	obj, err := st.Upload(ctx(t), stringsReader("token file"), azora.UploadOptions{Name: "t.txt"})
	require.NoError(t, err)
	link, exp, err := tok.SignURL(ctx(t), obj.ID, time.Minute)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Minute), exp, 5*time.Second)
	require.Equal(t, "token file", httpGet(t, link))
	// The HTTP API takes API keys only.
	_, err = f.connectURL(t, f.URL(testDB), azora.WithToken(f.Token), azora.WithStorageURL(f.HTTPURL())).Storage().Stat(ctx(t), obj.ID)
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
	require.NoError(t, st.Delete(ctx(t), obj.ID))
}

// A short-lived token expires while connected: the client fetches a new
// one and carries on without reconnecting, and its subscriptions continue.
func TestEngineTokenRefresh(t *testing.T) {
	f := newEngine(t, nil)
	admin := f.connect(t)
	seed(t, admin, "orders", 2)
	require.NoError(t, f.Stop())
	short, err := f.CreateToken(testDB, devengine.Token{Sub: "user:9", Scopes: []string{"read"}, TTL: 3 * time.Second})
	require.NoError(t, err)
	long, err := f.CreateToken(testDB, devengine.Token{Sub: "user:9", Scopes: []string{"read"}, TTL: time.Hour})
	require.NoError(t, err)
	require.NoError(t, f.Serve())

	var calls atomic.Int32
	var sessionErrs atomic.Int32
	db := f.connectURL(t, f.URL(testDB), azora.WithPool(2), azora.WithCredentialFunc(func(context.Context) (string, error) {
		if calls.Add(1) <= 2 { // the two pooled connections' Hello
			return short, nil
		}
		return long, nil
	}), azora.WithHooks(azora.Hooks{OnError: func(error) { sessionErrs.Add(1) }}))
	lt, err := azora.Live[liveOrder](ctx(t), db, azora.TableSpec("orders"))
	require.NoError(t, err)
	defer lt.Close()
	require.NoError(t, lt.Wait(ctx(t)))
	require.Equal(t, 2, lt.Len())

	// Wait for the expiry: the server announces it and waits for Auth.
	eventually(t, 15*time.Second, func() bool { return db.Stats().CredentialRefreshes >= 2 }, "both connections refreshed")
	require.Equal(t, int32(3), calls.Load(), "one fetch serves both pooled connections")
	st := db.Stats()
	require.Equal(t, uint64(2), st.Connects, "no reconnect")
	require.Zero(t, st.Disconnects)
	require.Zero(t, st.Resyncs, "the same grant: subscriptions continue")

	// Requests and the subscription work under the new token.
	n, err := azora.Scalar[int](ctx(t), db, "SELECT COUNT(*) FROM orders")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	mustExec(t, admin, "INSERT INTO orders (id, status) VALUES (3, 'after')")
	eventually(t, 10*time.Second, func() bool { return lt.Len() == 3 }, "changes still arrive")

	// Without a way to refresh, an expired token ends in an error.
	plain, err := azora.Connect(ctx(t), f.URL(testDB), azora.WithToken(short), azora.WithLogger(quiet))
	if err == nil {
		defer plain.Close()
		_, err = plain.Query(ctx(t), "SELECT 1")
	}
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
}

// The server process is killed and started again on the same data root
// and ports while writers keep going: every subscription resumes from
// its checkpoint, nothing is lost, nothing is delivered twice, and no
// new snapshot is needed.
func TestEngineRestartResume(t *testing.T) {
	f := newEngine(t, nil)
	var disconnects atomic.Int32
	opts := []azora.Option{azora.WithReconnectBackoff(10*time.Millisecond, 200*time.Millisecond),
		azora.WithHooks(azora.Hooks{OnDisconnect: func(error) { disconnects.Add(1) },
			OnResync: func(id uint32, reason string) { t.Logf("resync %d: %s", id, reason) }})}
	db := f.connect(t, opts...)
	ws := f.connectURL(t, f.WebSocketURL(testDB), append(opts, azora.WithAPIKey(f.Key), azora.WithPool(2))...)
	seed(t, db, "orders", 10)

	sub, err := db.Subscribe(ctx(t), azora.TableSpec("orders"))
	require.NoError(t, err)
	defer sub.Close()
	whole, err := ws.Subscribe(ctx(t), azora.DatabaseSpec())
	require.NoError(t, err)
	defer whole.Close()
	lt, err := azora.Live[liveOrder](ctx(t), ws, azora.TableSpec("orders", azora.Eq("status", "paid")))
	require.NoError(t, err)
	defer lt.Close()
	watch, err := db.Watch(ctx(t), "orders")
	require.NoError(t, err)
	defer watch.Close()
	k, kw := consume(sub), consume(whole)
	require.NoError(t, lt.Wait(ctx(t)))

	// A writer that never stops: every insert has its own id, so a write
	// applied twice would fail and a lost one would leave a gap.
	var written atomic.Int64
	var stopWriter atomic.Bool
	writerDone := make(chan error, 1)
	go func() {
		for i := 1000; !stopWriter.Load(); i++ {
			status := "paid"
			if i%2 == 1 {
				status = "new"
			}
			c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := db.Exec(c, "INSERT INTO orders (id, customer_id, status, total) VALUES (?, 1, ?, ?)", i, status, float64(i))
			cancel()
			if err != nil {
				writerDone <- err
				return
			}
			written.Add(1)
		}
		writerDone <- nil
	}()
	eventually(t, 10*time.Second, func() bool { return written.Load() > 50 }, "writes before the crash")

	for round := 0; round < 2; round++ {
		before := written.Load()
		if round == 0 {
			require.NoError(t, f.Kill()) // a crash
		} else {
			require.NoError(t, f.Stop()) // a clean shutdown (GoAway)
		}
		time.Sleep(150 * time.Millisecond)
		require.NoError(t, f.Serve())
		eventually(t, 20*time.Second, func() bool { return written.Load() > before+50 }, "writes continue after restart %d", round)
	}
	stopWriter.Store(true)
	require.NoError(t, <-writerDone, "every write got exactly one answer")

	// Every write is there exactly once.
	n, err := azora.Scalar[int64](ctx(t), db, "SELECT COUNT(*) FROM orders WHERE id >= 1000")
	require.NoError(t, err)
	require.Equal(t, written.Load(), n)
	gaps, err := azora.Scalar[int64](ctx(t), db, "SELECT MAX(id) - MIN(id) + 1 - COUNT(*) FROM orders WHERE id >= 1000")
	require.NoError(t, err)
	require.Zero(t, gaps)

	// The subscriptions caught up without a new snapshot.
	converge(t, db, k.r, "orders", "SELECT * FROM orders")
	converge(t, db, kw.r, "orders", "SELECT * FROM orders")
	eventually(t, 20*time.Second, func() bool { return liveEquals(t, db, lt, "SELECT * FROM orders WHERE status = 'paid' ORDER BY id") }, "the live view converges")
	k.stop(t)
	kw.stop(t)
	require.Equal(t, int64(1), k.resets.Load(), "only the initial snapshot: resumed from the checkpoint")
	require.Equal(t, int64(1), kw.resets.Load())
	require.Zero(t, db.Stats().Resyncs)
	require.Zero(t, ws.Stats().Resyncs)
	require.Equal(t, int64(10)+written.Load(), k.changes.Load(), "each change was delivered exactly once")
	require.True(t, verifySub(t, sub, k.r).Match)
	res, err := lt.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)
	require.GreaterOrEqual(t, disconnects.Load(), int32(4))
	require.GreaterOrEqual(t, db.Stats().Connects, uint64(3))
	require.Equal(t, 2, ws.Stats().Connections, "the pool is whole again")

	// The watcher reported that it may have missed notices.
	sawEverything := false
	for i := 0; i < 1000 && !sawEverything; i++ {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		n, err := watch.Next(c)
		cancel()
		if err != nil {
			break
		}
		sawEverything = n.Everything
	}
	require.True(t, sawEverything)
}

func TestEngineCloseNoLeaks(t *testing.T) {
	f := engine(t)
	db, err := azora.Connect(ctx(t), f.URL(testDB), azora.WithAPIKey(f.Key), azora.WithPool(3), azora.WithStorageURL(f.HTTPURL()))
	require.NoError(t, err)
	clearTable(t, db, "t_ops")
	sub, err := db.Subscribe(ctx(t), azora.TableSpec("t_ops"))
	require.NoError(t, err)
	lt, err := db.Live(ctx(t), azora.TableSpec("t_ops"), azora.LiveOptions{VerifyEvery: 10 * time.Millisecond})
	require.NoError(t, err)
	w, err := db.Watch(ctx(t))
	require.NoError(t, err)
	_, err = db.Storage().List(ctx(t), azora.ListOptions{Limit: 1})
	require.NoError(t, err)

	// Operations in flight when Close is called end with ErrClosed (or
	// succeed): none hangs.
	var wg sync.WaitGroup
	results := make(chan error, 64)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				_, err := db.Exec(context.Background(), "INSERT INTO t_ops (customer_id, status) VALUES (?, 'x')", i)
				if err == nil {
					_, err = db.Query(context.Background(), "SELECT COUNT(*) FROM t_ops")
				}
				if err != nil {
					results <- err
					return
				}
			}
		}()
	}
	nextErr := make(chan error, 3)
	go func() { _, err := w.Next(context.Background()); nextErr <- err }()
	go func() {
		for {
			if _, err := sub.Next(context.Background()); err != nil {
				nextErr <- err
				return
			}
		}
	}()
	go func() { <-lt.Done(); nextErr <- lt.Err() }()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, db.Close())
	wg.Wait()
	close(results)
	for err := range results {
		require.ErrorIs(t, err, azora.ErrClosed)
	}
	for i := 0; i < 3; i++ {
		select {
		case err := <-nextErr:
			if err != nil { // the watcher may have delivered a notice first
				require.ErrorIs(t, err, azora.ErrClosed)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a subscription did not end with the DB")
		}
	}
	require.ErrorIs(t, lt.Err(), azora.ErrClosed)
	require.False(t, db.Stats().Connected)
	requireNoLeaks(t)
}
