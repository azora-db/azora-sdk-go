package azora_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

// Integration tests: subscriptions against the real engine.

// consumer applies a subscription's updates to a replica in the
// background and checks what the stream promises: sequence numbers only
// grow (nothing delivered twice) and checkpoints never go back.
type consumer struct {
	sub     *azora.Subscription
	r       *azora.Replica
	applied atomic.Uint64
	resets  atomic.Int64
	changes atomic.Int64
	mu      sync.Mutex
	err     error
	seqErr  error
	lastSeq uint64
	cancel  context.CancelFunc
	done    chan struct{}
}

func consume(sub *azora.Subscription) *consumer {
	c, cancel := context.WithCancel(context.Background())
	k := &consumer{sub: sub, r: azora.NewReplica(), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(k.done)
		for {
			u, err := sub.Next(c)
			if err != nil {
				if c.Err() == nil {
					k.mu.Lock()
					k.err = err
					k.mu.Unlock()
				}
				return
			}
			k.check(u)
			k.r.Apply(u)
			k.changes.Add(int64(len(u.Changes)))
			if u.Reset {
				k.resets.Add(1)
			}
			k.applied.Store(u.Checkpoint)
		}
	}()
	return k
}

func (k *consumer) check(u azora.Update) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if u.Reset {
		k.lastSeq = 0
	}
	if !u.Reset && u.Checkpoint < k.applied.Load() && k.seqErr == nil {
		k.seqErr = fmt.Errorf("checkpoint %d delivered after %d", u.Checkpoint, k.applied.Load())
	}
	inUpdate := map[uint64]bool{}
	for _, c := range u.Changes {
		if c.Seq == 0 {
			continue
		}
		// Equal only within one update: an update split into a delete and
		// an insert by a filter.
		if (c.Seq < k.lastSeq || (c.Seq == k.lastSeq && !inUpdate[c.Seq])) && k.seqErr == nil {
			k.seqErr = fmt.Errorf("seq %d delivered after %d", c.Seq, k.lastSeq)
		}
		k.lastSeq = c.Seq
		inUpdate[c.Seq] = true
	}
}

// stop ends the consumer and reports what it found.
func (k *consumer) stop(t testing.TB) {
	t.Helper()
	k.cancel()
	<-k.done
	k.mu.Lock()
	defer k.mu.Unlock()
	require.NoError(t, k.err)
	require.NoError(t, k.seqErr)
}

// replicaEquals reports whether the replica's rows of table are exactly
// the rows of query (whose first column is the primary key).
func replicaEquals(t testing.TB, db *azora.DB, r *azora.Replica, table, query string, args ...any) (bool, string) {
	rows, err := db.Query(ctx(t), query, args...)
	require.NoError(t, err)
	if rows.Len() != r.Len(table) {
		return false, fmt.Sprintf("%d rows in the replica of %s, %d in the database", r.Len(table), table, rows.Len())
	}
	for _, row := range rows.Values() {
		got, ok := r.Get(table, row[0])
		if !ok {
			return false, fmt.Sprintf("row %v missing from the replica", row[0])
		}
		if !reflect.DeepEqual(row, got) {
			return false, fmt.Sprintf("row %v: replica %v, database %v", row[0], got, row)
		}
	}
	return true, ""
}

// converge waits until the replica equals the query's rows.
func converge(t testing.TB, db *azora.DB, r *azora.Replica, table, query string, args ...any) {
	t.Helper()
	var why string
	deadline := time.Now().Add(20 * time.Second)
	for {
		var ok bool
		if ok, why = replicaEquals(t, db, r, table, query, args...); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the replica did not converge: %s", why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// verifySub runs the Verify protocol by hand (applying updates on
// ErrRetry) for a subscription nobody else consumes.
func verifySub(t testing.TB, sub *azora.Subscription, r *azora.Replica) azora.VerifyResult {
	t.Helper()
	for i := 0; i < 200; i++ {
		res, err := sub.Verify(ctx(t), r.Checksum)
		if errors.Is(err, azora.ErrRetry) {
			r.Apply(next(t, sub, 20*time.Second))
			continue
		}
		require.NoError(t, err)
		return res
	}
	t.Fatal("verify never settled")
	return azora.VerifyResult{}
}

func seed(t testing.TB, db *azora.DB, table string, n int) {
	t.Helper()
	clearTable(t, db, table)
	b := db.NewBatch()
	for i := 1; i <= n; i++ {
		status := "new"
		if i%2 == 0 {
			status = "paid"
		}
		b.Add("INSERT INTO "+table+" (id, customer_id, status, total, note) VALUES (?, ?, ?, ?, ?)", i, i%3, status, float64(i)*10, []byte{byte(i)})
	}
	_, err := b.Run(ctx(t))
	require.NoError(t, err)
}

func TestEngineSubscribeTable(t *testing.T) {
	f := engine(t)
	for name, url := range map[string]string{"tcp": f.URL(testDB), "ws": f.WebSocketURL(testDB)} {
		t.Run(name, func(t *testing.T) {
			db := f.connectURL(t, url, azora.WithAPIKey(f.Key))
			seed(t, db, "t_table", 6)
			all, err := db.Subscribe(ctx(t), azora.TableSpec("t_table"))
			require.NoError(t, err)
			defer all.Close()
			paid, err := db.Subscribe(ctx(t), azora.TableSpec("t_table", azora.Eq("status", "paid"), azora.Gt("total", 25)))
			require.NoError(t, err)
			defer paid.Close()
			require.Equal(t, 2, db.Stats().Subscriptions)

			// The snapshot: the rows as they are, with their columns.
			u := next(t, all, 10*time.Second)
			require.True(t, u.Reset)
			require.True(t, u.Snapshot)
			require.Len(t, u.Changes, 6)
			require.Positive(t, u.Checkpoint)
			require.NotZero(t, u.Epoch)
			c := u.Changes[0]
			require.Equal(t, azora.OpInsert, c.Op)
			require.Equal(t, "t_table", c.Table)
			require.Equal(t, []string{"id", "customer_id", "status", "total", "note"}, c.Columns)
			require.Equal(t, []string{"id"}, c.PK)
			require.Equal(t, []any{int64(1)}, c.Key)
			require.Equal(t, []any{int64(1), int64(1), "new", 10.0, []byte{1}}, c.New)
			require.Zero(t, c.Seq)
			ra := azora.NewReplica()
			ra.Apply(u)
			rp := azora.NewReplica()
			u = next(t, paid, 10*time.Second)
			require.Len(t, u.Changes, 2, "rows 4 and 6 are paid and over 25")
			rp.Apply(u)

			// Live changes: an insert, an update with the old row, a delete.
			mustExec(t, db, "INSERT INTO t_table (id, customer_id, status, total) VALUES (7, 1, 'new', 70)")
			u = next(t, all, 10*time.Second)
			require.False(t, u.Reset)
			require.Len(t, u.Changes, 1)
			require.Equal(t, azora.OpInsert, u.Changes[0].Op)
			require.Equal(t, []any{int64(7), int64(1), "new", 70.0, nil}, u.Changes[0].New)
			require.Nil(t, u.Changes[0].Old)
			require.Positive(t, u.Changes[0].Seq)
			ra.Apply(u)
			mustExec(t, db, "UPDATE t_table SET status = 'paid' WHERE id = 7")
			u = next(t, all, 10*time.Second)
			require.Equal(t, azora.OpUpdate, u.Changes[0].Op)
			require.Equal(t, "new", u.Changes[0].Old[2])
			require.Equal(t, "paid", u.Changes[0].New[2])
			ra.Apply(u)
			// For the filtered subscription the update moved the row in: an
			// insert. Moving it out again is a delete.
			u = next(t, paid, 10*time.Second)
			require.Len(t, u.Changes, 1)
			require.Equal(t, azora.OpInsert, u.Changes[0].Op)
			require.Equal(t, int64(7), u.Changes[0].Key[0])
			rp.Apply(u)
			mustExec(t, db, "UPDATE t_table SET total = 5 WHERE id = 7")
			u = next(t, paid, 10*time.Second)
			require.Equal(t, azora.OpDelete, u.Changes[0].Op)
			rp.Apply(u)
			ra.Apply(next(t, all, 10*time.Second))
			mustExec(t, db, "DELETE FROM t_table WHERE id = 1")
			u = next(t, all, 10*time.Second)
			require.Equal(t, azora.OpDelete, u.Changes[0].Op)
			require.Equal(t, []any{int64(1)}, u.Changes[0].Key)
			require.Nil(t, u.Changes[0].New)
			require.Equal(t, "new", u.Changes[0].Old[2])
			ra.Apply(u)

			// A transaction arrives as one step, in order.
			_, err = db.Batch(ctx(t),
				azora.Stmt("INSERT INTO t_table (id, status, total) VALUES (8, 'paid', 80)"),
				azora.Stmt("UPDATE t_table SET total = total + 1 WHERE id IN (2, 4)"),
				azora.Stmt("DELETE FROM t_table WHERE id = 3"))
			require.NoError(t, err)
			u = next(t, all, 10*time.Second)
			require.Len(t, u.Changes, 4, "one consistent step for the whole transaction")
			ops := []azora.Op{u.Changes[0].Op, u.Changes[1].Op, u.Changes[2].Op, u.Changes[3].Op}
			require.Equal(t, []azora.Op{azora.OpInsert, azora.OpUpdate, azora.OpUpdate, azora.OpDelete}, ops)
			require.Less(t, u.Changes[0].Seq, u.Changes[3].Seq)
			ra.Apply(u)
			rp.Apply(next(t, paid, 10*time.Second))

			// The copies equal the database, and the server agrees.
			ok, why := replicaEquals(t, db, ra, "t_table", "SELECT * FROM t_table")
			require.True(t, ok, why)
			ok, why = replicaEquals(t, db, rp, "t_table", "SELECT * FROM t_table WHERE status = 'paid' AND total > 25")
			require.True(t, ok, why)
			res := verifySub(t, all, ra)
			require.True(t, res.Match, "%+v", res)
			require.Equal(t, int64(ra.Len("t_table")), res.ServerRows)
			require.True(t, verifySub(t, paid, rp).Match)
			cols, pk := ra.Columns("t_table")
			require.Equal(t, []string{"id", "customer_id", "status", "total", "note"}, cols)
			require.Equal(t, []string{"id"}, pk)
		})
	}
}

func TestEngineSubscribeKinds(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	seed(t, db, "t_row", 3)
	seed(t, db, "t_db", 2)

	row, err := db.Subscribe(ctx(t), azora.RowSpec("t_row", 2))
	require.NoError(t, err)
	defer row.Close()
	inserts, err := db.Subscribe(ctx(t), azora.TableSpec("t_row").On(azora.OpsInsert).WithoutSnapshot())
	require.NoError(t, err)
	defer inserts.Close()
	notify, err := db.Subscribe(ctx(t), azora.NotifySpec("plain")) // not published: notices work anyway
	require.NoError(t, err)
	defer notify.Close()
	whole, err := db.Subscribe(ctx(t), azora.DatabaseSpec())
	require.NoError(t, err)
	defer whole.Close()

	u := next(t, row, 10*time.Second)
	require.True(t, u.Reset)
	require.Len(t, u.Changes, 1)
	require.Equal(t, int64(2), u.Changes[0].Key[0])
	rr := azora.NewReplica()
	rr.Apply(u)

	// The database subscription covers every published table.
	kd := consume(whole)
	eventually(t, 10*time.Second, func() bool { return kd.r.Len("t_db") == 2 && kd.r.Len("t_row") == 3 }, "the database snapshot")

	mustExec(t, db, "INSERT INTO plain (v) VALUES ('hello')")
	u = next(t, notify, 10*time.Second)
	require.Equal(t, []string{"plain"}, u.Tables)
	require.Empty(t, u.Changes)
	require.Zero(t, u.Checkpoint)

	mustExec(t, db, "UPDATE t_row SET status = 'shipped' WHERE id IN (1, 2)")
	mustExec(t, db, "INSERT INTO t_row (id, status) VALUES (50, 'x')")
	mustExec(t, db, "UPDATE t_db SET total = total + 1")
	mustExec(t, db, "DELETE FROM t_row WHERE id = 3")

	// The row subscription sees its row only.
	for rr.Len("t_row") != 1 || func() bool { r, _ := rr.Get("t_row", 2); return r[2] != "shipped" }() {
		rr.Apply(next(t, row, 10*time.Second))
	}
	ok, why := replicaEquals(t, db, rr, "t_row", "SELECT * FROM t_row WHERE id = 2")
	require.True(t, ok, why)
	require.True(t, verifySub(t, row, rr).Match)
	// Deleting the row empties the subscription.
	mustExec(t, db, "DELETE FROM t_row WHERE id = 2")
	for rr.Len("t_row") != 0 {
		rr.Apply(next(t, row, 10*time.Second))
	}

	// Inserts only, no snapshot.
	u = next(t, inserts, 10*time.Second)
	for len(u.Changes) == 0 {
		u = next(t, inserts, 10*time.Second)
	}
	require.False(t, u.Reset)
	require.Len(t, u.Changes, 1)
	require.Equal(t, azora.OpInsert, u.Changes[0].Op)
	require.Equal(t, int64(50), u.Changes[0].Key[0])

	converge(t, db, kd.r, "t_row", "SELECT * FROM t_row")
	converge(t, db, kd.r, "t_db", "SELECT * FROM t_db")
	kd.stop(t)
	require.Equal(t, int64(1), kd.resets.Load())
	require.True(t, verifySub(t, whole, kd.r).Match)
	require.Contains(t, kd.r.Tables(), "t_db")

	// Watch: the convenient form of notify.
	w, err := db.Watch(ctx(t))
	require.NoError(t, err)
	defer w.Close()
	mustExec(t, db, "INSERT INTO plain (v) VALUES ('again')")
	n, err := w.Next(ctx(t))
	require.NoError(t, err)
	require.True(t, n.Has("plain"), "%+v", n)
	require.False(t, n.Everything)
}

func TestEngineSubscribeErrorsAndResume(t *testing.T) {
	f := engine(t)
	var mu sync.Mutex
	var resyncs []uint32
	db := f.connect(t, azora.WithHooks(azora.Hooks{OnResync: func(id uint32, reason string) {
		mu.Lock()
		resyncs = append(resyncs, id)
		mu.Unlock()
	}}))
	seed(t, db, "t_resume", 3)

	_, err := db.Subscribe(ctx(t), azora.TableSpec("missing"))
	require.Error(t, err)
	_, err = db.Subscribe(ctx(t), azora.TableSpec("plain"))
	require.Error(t, err, "the table is not published")
	_, err = db.Subscribe(ctx(t), azora.TableSpec("t_resume", azora.Eq("nope", 1)))
	require.True(t, azora.IsInvalid(err), "%v", err)
	_, err = db.Subscribe(ctx(t), azora.TableSpec("t_resume", azora.Filter{Op: "like", Col: "status", Val: "x"}))
	require.True(t, azora.IsInvalid(err), "%v", err)
	_, err = db.Subscribe(ctx(t), azora.RowSpec("t_resume"))
	require.Error(t, err, "a row subscription needs its key")
	require.Zero(t, db.Stats().Subscriptions)

	// Resuming from a stored checkpoint: only later changes arrive.
	sub, err := db.Subscribe(ctx(t), azora.TableSpec("t_resume"))
	require.NoError(t, err)
	u := next(t, sub, 10*time.Second)
	cp, epoch := sub.Checkpoint()
	require.Equal(t, u.Checkpoint, cp)
	require.NoError(t, sub.Close())
	mustExec(t, db, "INSERT INTO t_resume (id, status) VALUES (30, 'later')")
	sub, err = db.Subscribe(ctx(t), azora.TableSpec("t_resume").ResumeFrom(cp, epoch))
	require.NoError(t, err)
	u = next(t, sub, 10*time.Second)
	for len(u.Changes) == 0 {
		u = next(t, sub, 10*time.Second)
	}
	require.False(t, u.Reset)
	require.Len(t, u.Changes, 1)
	require.Equal(t, int64(30), u.Changes[0].Key[0])
	require.NoError(t, sub.Close())

	// A resume point from another epoch is gone: a new snapshot, or
	// ErrResync with ManualResync.
	sub, err = db.Subscribe(ctx(t), azora.TableSpec("t_resume").ResumeFrom(cp, epoch+1))
	require.NoError(t, err)
	defer sub.Close()
	u = next(t, sub, 10*time.Second)
	require.True(t, u.Reset && u.Snapshot)
	require.Len(t, u.Changes, 4)
	mu.Lock()
	require.Equal(t, []uint32{sub.ID()}, resyncs)
	mu.Unlock()
	require.Equal(t, uint64(1), db.Stats().Resyncs)
	_, err = db.Subscribe(ctx(t), azora.TableSpec("t_resume").ResumeFrom(cp, epoch+1).Manual())
	require.ErrorIs(t, err, azora.ErrResync)
}

func TestEngineSchemaChangeResync(t *testing.T) {
	f := engine(t)
	resynced := make(chan string, 4)
	db := f.connect(t, azora.WithHooks(azora.Hooks{OnResync: func(id uint32, reason string) { resynced <- reason }}))
	mustExec(t, db, "DROP TABLE IF EXISTS t_schema_x")
	seed(t, db, "t_schema", 2)
	// The column may be left over from an earlier run of the test.
	if _, err := db.Query(ctx(t), "SELECT extra FROM t_schema LIMIT 1"); err == nil {
		mustExec(t, db, "ALTER TABLE t_schema DROP COLUMN extra")
	}
	sub, err := db.Subscribe(ctx(t), azora.TableSpec("t_schema"))
	require.NoError(t, err)
	defer sub.Close()
	r := azora.NewReplica()
	r.Apply(next(t, sub, 10*time.Second))
	lt, err := azora.Live[azora.Record](ctx(t), db, azora.TableSpec("t_schema"))
	require.NoError(t, err)
	defer lt.Close()
	require.NoError(t, lt.Wait(ctx(t)))

	mustExec(t, db, "INSERT INTO t_schema (id, status) VALUES (5, 'before')")
	mustExec(t, db, "ALTER TABLE t_schema ADD COLUMN extra TEXT")
	mustExec(t, db, "UPDATE t_schema SET extra = 'n' || id")
	var reset bool
	for !reset {
		u := next(t, sub, 10*time.Second)
		reset = u.Reset
		r.Apply(u)
	}
	select {
	case reason := <-resynced:
		require.NotEmpty(t, reason)
	case <-time.After(10 * time.Second):
		t.Fatal("OnResync not called")
	}
	for {
		if ok, _ := replicaEquals(t, db, r, "t_schema", "SELECT * FROM t_schema"); ok {
			break
		}
		r.Apply(next(t, sub, 10*time.Second))
	}
	cols, _ := r.Columns("t_schema")
	require.Equal(t, []string{"id", "customer_id", "status", "total", "note", "extra"}, cols)
	require.True(t, verifySub(t, sub, r).Match)

	// The live view followed the schema change too.
	eventually(t, 10*time.Second, func() bool {
		row, ok := lt.Get(5)
		return ok && row["extra"] == "n5"
	}, "the live view has the new column")
	res, err := lt.Verify(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Match, "%+v", res)
	mustExec(t, db, "ALTER TABLE t_schema DROP COLUMN extra")
}

// The filter grammar parsed by the client must mean what SQLite means:
// for each expression the rows a filtered subscription delivers are
// compared with the rows of SELECT ... WHERE <the same expression>.
func TestEngineFilterMatchesSQL(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	clearTable(t, db, "t_filter")
	rows := [][]any{
		{1, 1, "paid", 10.0, []byte{1}}, {2, 2, "new", 150.5, []byte{0xff, 0x00}}, {3, nil, "paid", 100.0, nil},
		{4, 1, nil, nil, []byte{}}, {5, 42, "pending", -5.0, []byte("abc")}, {6, 7, "PAID", 100, nil},
		{7, 0, "", 1e10, nil}, {8, 3, "pa", 99.99, []byte{1, 2}}, {9, 42, "it's", 0.0, nil}, {10, -1, "new", 3, nil},
	}
	for _, r := range rows {
		mustExec(t, db, "INSERT INTO t_filter (id, customer_id, status, total, note) VALUES (?, ?, ?, ?, ?)", r...)
	}
	// filter text -> the same predicate in SQL, when it is spelled differently.
	exprs := map[string]string{
		"status = 'paid'":                          "",
		"status != 'paid'":                         "",
		"status <> 'paid' AND total > 0":           "",
		"total >= 100":                             "",
		"total < 100.0":                            "",
		"total <= 99.99 OR customer_id = 42":       "",
		"customer_id IN (1, 42, 7)":                "",
		"customer_id NOT IN (1, 42)":               "",
		"customer_id IS NULL":                      "",
		"status IS NOT NULL AND total IS NOT NULL": "",
		// NOT is two-valued: a comparison with NULL is false, so its
		// negation is true (SQL would leave it NULL, and skip the row).
		"NOT status = 'paid'":                      "NOT coalesce(status = 'paid', 0)",
		"NOT (status = 'paid' OR total > 100)":     "NOT (coalesce(status = 'paid', 0) OR coalesce(total > 100, 0))",
		"NOT customer_id IN (1, 42)":               "NOT coalesce(customer_id IN (1, 42), 0)",
		"NOT status IS NULL":                       "",
		"status ^= 'pa'":                           "substr(status, 1, 2) = 'pa'",
		"status ^= ''":                             "status IS NOT NULL",
		"status = 'it''s'":                         "",
		"note = x'ff00'":                           "",
		"note > x'01'":                             "",
		"total = 100":                              "",
		"total == 100.0 AND status = 'paid'":       "",
		"id = TRUE OR id = 2":                      "id = 1 OR id = 2",
		"status = 'paid' AND (total >= 100 OR customer_id IS NOT NULL) AND id IN (1, 2, 3)": "",
		"\"status\" = 'new' OR \"total\" < 0":                                                "",
		"customer_id = '1'":                                                                  "typeof(customer_id) = 'text' AND customer_id = '1'", // no type conversion: never matches
		"status > 'new'":                                                                     "",
		"status = ''":                                                                        "",
		"total > 9999999999":                                                                 "",
		"customer_id >= -1 AND customer_id < 1":                                              "",
	}
	for expr, sql := range exprs {
		if sql == "" {
			sql = expr
		}
		flt, err := azora.ParseFilter(expr)
		require.NoError(t, err, expr)
		want, err := azora.All[int64](ctx(t), db, "SELECT id FROM t_filter WHERE "+sql+" ORDER BY id")
		require.NoError(t, err, sql)
		sub, err := db.Subscribe(ctx(t), azora.TableSpec("t_filter", *flt))
		require.NoError(t, err, expr)
		u := next(t, sub, 10*time.Second)
		got := make([]int64, 0, len(u.Changes))
		for _, c := range u.Changes {
			got = append(got, c.Key[0].(int64))
		}
		require.ElementsMatch(t, want, got, "filter %q against WHERE %s", expr, sql)
		// The engine's own parser reads what Filter.String writes.
		sub2, err := db.Subscribe(ctx(t), azora.TableSpec("t_filter", *azora.MustParseFilter(flt.String())))
		require.NoError(t, err, flt.String())
		require.Len(t, next(t, sub2, 10*time.Second).Changes, len(want), flt.String())
		sub.Close()
		sub2.Close()
	}
	require.Zero(t, db.Stats().Subscriptions)
}

func TestEngineSlowConsumerIsBounded(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	clearTable(t, db, "t_slow")
	sub, err := db.Subscribe(ctx(t), azora.TableSpec("t_slow"))
	require.NoError(t, err)
	defer sub.Close()
	r := azora.NewReplica()
	r.Apply(next(t, sub, 10*time.Second))
	// The application stops calling Next while about 12 MiB of changes
	// are written: the server stops sending a few MiB past the last
	// acknowledged checkpoint instead of filling the client's memory.
	pad := make([]byte, 60_000)
	for i := 1; i <= 200; i++ {
		mustExec(t, db, "INSERT INTO t_slow (id, note) VALUES (?, ?)", i, pad)
	}
	time.Sleep(500 * time.Millisecond)
	held := sub.Buffered()
	require.Less(t, held, int64(9<<20), "buffered %d bytes", held)
	t.Logf("buffered while stalled: %.1f MiB of about 12 MiB written", float64(held)/(1<<20))
	// Reading again delivers everything.
	for r.Len("t_slow") < 200 {
		r.Apply(next(t, sub, 20*time.Second))
	}
	require.True(t, verifySub(t, sub, r).Match)
	clearTable(t, db, "t_slow")
}
