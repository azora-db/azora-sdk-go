package azora_test

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/devengine"
)

// Integration tests: requests against the real engine.

func TestEngineSecurityModes(t *testing.T) {
	f := engine(t)
	tcp := "azora://" + f.RTAddr + "/" + testDB
	ws := "azora+ws://" + f.HTTPAddr + "/rt/v1?db=" + testDB
	plainWS := "ws://" + f.HTTPAddr + "?db=" + testDB // ws://, default path
	for name, c := range map[string]struct{ url, mode string }{
		"IK tcp":        {tcp + "?key=" + f.ServerKey, "IK"},
		"XX tcp":        {tcp + "?fp=" + f.Fingerprint, "XX"},
		"IK+fp tcp":     {tcp + "?key=" + f.ServerKey + "&fp=" + f.Fingerprint, "IK"},
		"plaintext tcp": {tcp + "?plaintext=1", "plaintext"},
		"IK ws":         {ws + "&key=" + f.ServerKey, "IK"},
		"XX ws":         {ws + "&fp=" + f.Fingerprint, "XX"},
		"plaintext ws":  {ws + "&plaintext=1", "plaintext"},
		"IK plain ws":   {plainWS + "&key=" + f.ServerKey, "IK"},
	} {
		t.Run(name, func(t *testing.T) {
			db := f.connectURL(t, c.url, azora.WithAPIKey(f.Key))
			w, ok := db.Welcome()
			require.True(t, ok)
			require.Equal(t, c.mode, w.Mode)
			require.Equal(t, "key:app", w.Principal)
			require.ElementsMatch(t, []string{"read", "write", "ddl"}, w.Scopes)
			require.Equal(t, 15*time.Second, w.Heartbeat)
			require.Positive(t, w.MaxSubscriptions)
			require.NotEmpty(t, w.SessionID)
			require.WithinDuration(t, time.Now(), w.ServerTime, time.Minute)
			if c.mode != "plaintext" {
				require.Equal(t, f.ServerKey, w.ServerKey)
				require.Equal(t, f.Fingerprint, w.Fingerprint)
			}
			n, err := azora.Scalar[int](ctx(t), db, "SELECT 40 + 2")
			require.NoError(t, err)
			require.Equal(t, 42, n)
			d, err := db.Ping(ctx(t))
			require.NoError(t, err)
			require.Positive(t, d)
		})
	}

	// The server proves it holds the pinned key; another key or
	// fingerprint fails the handshake before any credential is sent.
	_, err := azora.Connect(ctx(t), tcp+"?key=B6N8vBQgk8i3VdwbEOhstCY3StFqqFPtC9_AsrhtHHw", azora.WithAPIKey(f.Key))
	require.ErrorIs(t, err, azora.ErrUnknownKey)
	_, err = azora.Connect(ctx(t), tcp+"?fp=azp1-7uih-nrqh-26rp-caj3-az5k-zdaw", azora.WithAPIKey(f.Key))
	require.ErrorIs(t, err, azora.ErrHandshake)

	// A bad credential and an unknown database get the same answer.
	_, err = azora.Connect(ctx(t), f.URL(testDB), azora.WithAPIKey("azk_wrong"))
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
	_, err2 := azora.Connect(ctx(t), f.URL("nowhere"), azora.WithAPIKey(f.Key))
	require.True(t, azora.IsUnauthenticated(err2), "%v", err2)
	require.Equal(t, err.Error(), err2.Error(), "a client cannot probe which databases exist")
}

func TestEngineValueTypes(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	mustExec(t, db, "DROP TABLE IF EXISTS vals")
	mustExec(t, db, "CREATE TABLE vals (id INTEGER PRIMARY KEY, i INTEGER, f REAL, s TEXT, b BLOB, ok INTEGER, at TEXT, any)")
	now := time.Date(2026, 11, 10, 15, 4, 5, 123456789, time.FixedZone("x", 3600))
	blob := make([]byte, 5000)
	for i := range blob {
		blob[i] = byte(i)
	}
	unicode := "héllo, 世界 👋 \x00 'quoted' \"double\" \n\t"
	rows := [][]any{
		{1, int64(math.MaxInt64), math.MaxFloat64, unicode, blob, true, now, nil},
		{2, int64(math.MinInt64), -math.SmallestNonzeroFloat64, "", []byte{}, false, now.UTC(), int64(7)},
		{3, 0, 0.1 + 0.2, strings.Repeat("x", 100_000), []byte{0}, nil, nil, "text"},
		{4, nil, nil, nil, nil, nil, nil, 2.5},
		{5, uint32(4_000_000_000), float32(1.5), myStr("named"), []byte(nil), int8(1), nil, []byte("blob")},
	}
	for _, r := range rows {
		mustExec(t, db, "INSERT INTO vals VALUES (?, ?, ?, ?, ?, ?, ?, ?)", r...)
	}
	got, err := db.Query(ctx(t), "SELECT * FROM vals ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, []string{"id", "i", "f", "s", "b", "ok", "at", "any"}, got.Columns())
	v := got.Values()
	require.Len(t, v, 5)
	// Raw values are exactly SQLite's: integers, floats, text, blobs, NULL.
	require.Equal(t, []any{int64(1), int64(math.MaxInt64), math.MaxFloat64, unicode, blob, int64(1)}, v[0][:6])
	require.Nil(t, v[0][7])
	require.Equal(t, []any{int64(2), int64(math.MinInt64), -math.SmallestNonzeroFloat64, "", []byte{}, int64(0)}, v[1][:6])
	require.Equal(t, int64(7), v[1][7])
	require.Equal(t, 0.1+0.2, v[2][2])
	require.Len(t, v[2][3], 100_000)
	require.Equal(t, []any{int64(4), nil, nil, nil, nil, nil, nil, 2.5}, v[3])
	require.Equal(t, []any{int64(5), int64(4_000_000_000), 1.5, "named", nil, int64(1), nil, []byte("blob")}, v[4])

	// Typed access: struct scanning with NULLs, bools and times.
	type val struct {
		ID  int
		I   *int64
		F   *float64
		S   *string
		B   []byte
		OK  *bool
		At  *time.Time
		Any any
	}
	vals, err := azora.All[val](ctx(t), db, "SELECT * FROM vals ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), *vals[0].I)
	require.Equal(t, unicode, *vals[0].S)
	require.Equal(t, blob, vals[0].B)
	require.True(t, *vals[0].OK)
	require.True(t, now.Equal(*vals[0].At), "a time.Time argument reads back as the same instant: %v", vals[0].At)
	require.True(t, now.Equal(*vals[1].At))
	require.False(t, *vals[1].OK)
	require.NotNil(t, vals[1].B, "an empty blob is not NULL")
	require.Empty(t, vals[1].B)
	require.Nil(t, vals[3].I)
	require.Nil(t, vals[3].F)
	require.Nil(t, vals[3].S)
	require.Nil(t, vals[3].B)
	require.Nil(t, vals[3].OK)
	require.Nil(t, vals[3].At)
	require.Equal(t, 2.5, vals[3].Any)
	require.Nil(t, vals[4].B, "a nil []byte argument is NULL")

	// The server evaluates arguments with SQLite's types.
	var typ string
	require.NoError(t, db.Get(ctx(t), &typ, "SELECT typeof(?)", int8(3)))
	require.Equal(t, "integer", typ)
	require.NoError(t, db.Get(ctx(t), &typ, "SELECT typeof(?)", []byte{}))
	require.Equal(t, "blob", typ)
	require.NoError(t, db.Get(ctx(t), &typ, "SELECT typeof(?)", true))
	require.Equal(t, "integer", typ)
	var at string
	require.NoError(t, db.Get(ctx(t), &at, "SELECT at FROM vals WHERE id = 2"))
	require.Equal(t, "2026-11-10 14:04:05.123456789+00:00", at, "times are stored the way the engine's SQLite driver writes them")

	// A JSON column, through the wrapper and through a struct tag.
	mustExec(t, db, "UPDATE vals SET any = ? WHERE id = 1", azora.JSON[map[string]int]{V: map[string]int{"a": 1}})
	var js azora.JSON[map[string]int]
	require.NoError(t, db.Get(ctx(t), &js, "SELECT any FROM vals WHERE id = 1"))
	require.Equal(t, map[string]int{"a": 1}, js.V)
	type tagged struct {
		Any map[string]int `db:"any,json"`
	}
	tg, err := azora.One[tagged](ctx(t), db, "SELECT any FROM vals WHERE id = 1")
	require.NoError(t, err)
	require.Equal(t, 1, tg.Any["a"])
	mustExec(t, db, "DROP TABLE vals")
}

func TestEngineLargeResults(t *testing.T) {
	f := engine(t)
	for _, url := range []string{f.URL(testDB), f.WebSocketURL(testDB), "azora://" + f.RTAddr + "/" + testDB + "?plaintext=1"} {
		db := f.connectURL(t, url, azora.WithAPIKey(f.Key))
		// A result of about 2 MiB spans many 64 KiB frames.
		rows, err := db.Query(ctx(t), `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 8000)
			SELECT x, printf('%0250d', x) AS pad FROM c`)
		require.NoError(t, err)
		require.Equal(t, 8000, rows.Len())
		require.False(t, rows.Truncated())
		for i, r := range rows.Values() {
			require.Equal(t, int64(i+1), r[0])
			require.Len(t, r[1], 250)
		}
		// One value larger than a megabyte, both ways.
		big := make([]byte, 3<<20)
		for i := range big {
			big[i] = byte(i * 31)
		}
		got, err := azora.Scalar[[]byte](ctx(t), db, "SELECT ?", big)
		require.NoError(t, err)
		require.Equal(t, big, got)
	}

	db := f.connect(t)
	// The server's row limit, and a caller's.
	rows, err := db.Query(ctx(t), "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10050) SELECT x FROM c")
	require.NoError(t, err)
	require.Equal(t, azora.MaxQueryRows, rows.Len())
	require.True(t, rows.Truncated())
	rows, err = db.QueryN(ctx(t), 5, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10) SELECT x FROM c")
	require.NoError(t, err)
	require.Equal(t, 5, rows.Len())
	require.True(t, rows.Truncated())
	rows, err = db.QueryN(ctx(t), 10, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10) SELECT x FROM c")
	require.NoError(t, err)
	require.False(t, rows.Truncated())
	// Select and All refuse to hand out a silently incomplete slice.
	var xs []int
	err = db.Select(ctx(t), &xs, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10050) SELECT x FROM c")
	require.ErrorContains(t, err, "more than 10000 rows")
	_, err = azora.All[int](ctx(t), db, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10050) SELECT x FROM c")
	require.ErrorContains(t, err, "more than 10000 rows")
	// A message over the server's limit is refused without losing the
	// connection.
	_, err = db.Query(ctx(t), "SELECT length(?)", make([]byte, 17<<20))
	require.True(t, azora.IsCode(err, azora.CodeResourceLimit), "%v", err)
	_, err = db.Ping(ctx(t))
	require.NoError(t, err)
	require.Equal(t, uint64(1), db.Stats().Connects)
}

func TestEngineSelectGet(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	clearTable(t, db, "orders")
	for i := 1; i <= 3; i++ {
		mustExec(t, db, "INSERT INTO orders (id, customer_id, status, total, note) VALUES (?, ?, ?, ?, ?)", i, 10+i, "new", float64(i)*1.5, []byte{byte(i)})
	}
	type order struct {
		ID         int64
		CustomerID int64
		Status     string
		Total      float64
		Note       []byte
	}
	var orders []order
	require.NoError(t, db.Select(ctx(t), &orders, "SELECT * FROM orders ORDER BY id"))
	require.Equal(t, []order{{1, 11, "new", 1.5, []byte{1}}, {2, 12, "new", 3, []byte{2}}, {3, 13, "new", 4.5, []byte{3}}}, orders)
	var one order
	require.NoError(t, db.Get(ctx(t), &one, "SELECT * FROM orders WHERE id = ?", 2))
	require.Equal(t, orders[1], one)
	require.ErrorIs(t, db.Get(ctx(t), &one, "SELECT * FROM orders WHERE id = ?", 99), azora.ErrNoRows)
	var n int
	require.NoError(t, db.Get(ctx(t), &n, "SELECT COUNT(*) FROM orders"))
	require.Equal(t, 3, n)
	rec := azora.Record{}
	require.NoError(t, db.Get(ctx(t), &rec, "SELECT id, status FROM orders WHERE id = 1"))
	require.Equal(t, azora.Record{"id": int64(1), "status": "new"}, rec)

	o, err := azora.One[order](ctx(t), db, "SELECT * FROM orders WHERE id = ?", 3)
	require.NoError(t, err)
	require.Equal(t, orders[2], o)
	_, err = azora.One[order](ctx(t), db, "SELECT * FROM orders WHERE id = 0")
	require.ErrorIs(t, err, azora.ErrNoRows)
	total, err := azora.Scalar[float64](ctx(t), db, "SELECT SUM(total) FROM orders")
	require.NoError(t, err)
	require.Equal(t, 9.0, total)
	ids, err := azora.All[int64](ctx(t), db, "SELECT id FROM orders ORDER BY id DESC")
	require.NoError(t, err)
	require.Equal(t, []int64{3, 2, 1}, ids)

	// Cursor access.
	rows, err := db.Query(ctx(t), "SELECT id, status, total FROM orders ORDER BY id")
	require.NoError(t, err)
	var sum float64
	for rows.Next() {
		var id int
		var status string
		var total float64
		require.NoError(t, rows.Scan(&id, &status, &total))
		sum += total
	}
	require.Equal(t, 9.0, sum)
	// Introspection and other read statements.
	cols, err := azora.All[azora.Record](ctx(t), db, "PRAGMA table_info(orders)")
	require.NoError(t, err)
	require.Len(t, cols, 5)
	require.Equal(t, "id", cols[0]["name"])
	v, err := azora.All[int](ctx(t), db, "VALUES (1), (2)")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, v)
}

func TestEngineExecBatchExactlyOnce(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	mustExec(t, db, "DROP TABLE IF EXISTS acct")
	mustExec(t, db, "CREATE TABLE acct (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL CHECK (balance >= 0))")
	res := mustExec(t, db, "INSERT INTO acct (balance) VALUES (?), (?)", 100, 50)
	require.Equal(t, azora.Result{RowsAffected: 2, LastInsertID: 2}, res)

	// A batch is one transaction: every statement applies.
	res, err := db.Batch(ctx(t),
		azora.Stmt("UPDATE acct SET balance = balance - ? WHERE id = 1", 30),
		azora.Stmt("UPDATE acct SET balance = balance + ? WHERE id = 2", 30),
		azora.Stmt("INSERT INTO acct (balance) VALUES (?)", 7))
	require.NoError(t, err)
	require.Equal(t, int64(3), res.RowsAffected, "the rows changed by all statements together")
	require.Equal(t, int64(3), res.LastInsertID)
	require.False(t, res.Replayed)
	balances := func() []int {
		b, err := azora.All[int](ctx(t), db, "SELECT balance FROM acct ORDER BY id")
		require.NoError(t, err)
		return b
	}
	require.Equal(t, []int{70, 80, 7}, balances())

	// ... or none does: the second statement violates the CHECK.
	_, err = db.Batch(ctx(t),
		azora.Stmt("UPDATE acct SET balance = balance + 1000 WHERE id = 2"),
		azora.Stmt("UPDATE acct SET balance = balance - 1000 WHERE id = 1"))
	require.True(t, azora.IsInvalid(err), "%v", err)
	require.Equal(t, []int{70, 80, 7}, balances(), "the batch left nothing behind")

	// The same mutation id applies once; the retry gets the first answer.
	id := azora.NewMutationID()
	first, err := db.ExecMutation(ctx(t), id, "UPDATE acct SET balance = balance + 5 WHERE id = 1")
	require.NoError(t, err)
	require.Equal(t, azora.Result{RowsAffected: 1, LastInsertID: 3}, first)
	again, err := db.ExecMutation(ctx(t), id, "UPDATE acct SET balance = balance + 5 WHERE id = 1")
	require.NoError(t, err)
	require.True(t, again.Replayed)
	require.Equal(t, first.RowsAffected, again.RowsAffected)
	// Even from another connection, and whatever the statement says now.
	other := f.connect(t)
	again, err = other.ExecMutation(ctx(t), id, "DELETE FROM acct")
	require.NoError(t, err)
	require.True(t, again.Replayed)
	require.Equal(t, []int{75, 80, 7}, balances())

	bid := "transfer-0001"
	b := db.NewBatch().MutationID(bid).
		Add("UPDATE acct SET balance = balance - 10 WHERE id = 1").
		Add("UPDATE acct SET balance = balance + 10 WHERE id = 3")
	res, err = b.Run(ctx(t))
	require.NoError(t, err)
	require.False(t, res.Replayed)
	res, err = b.Run(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Replayed)
	res, err = db.BatchMutation(ctx(t), bid, azora.Stmt("DELETE FROM acct"))
	require.NoError(t, err)
	require.True(t, res.Replayed)
	require.Equal(t, []int{65, 80, 17}, balances())

	// A failed write is not remembered: the id can be used again.
	fid := azora.NewMutationID()
	_, err = db.ExecMutation(ctx(t), fid, "UPDATE acct SET balance = -1 WHERE id = 1")
	require.True(t, azora.IsInvalid(err), "%v", err)
	res, err = db.ExecMutation(ctx(t), fid, "UPDATE acct SET balance = 1 WHERE id = 1")
	require.NoError(t, err)
	require.False(t, res.Replayed)
	mustExec(t, db, "DROP TABLE acct")
}

func TestEngineDDLAndErrorCodes(t *testing.T) {
	f := engine(t)
	db := f.connect(t)
	mustExec(t, db, "DROP TABLE IF EXISTS ddl_t")
	res := mustExec(t, db, "CREATE TABLE ddl_t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
	require.False(t, res.Replayed)
	mustExec(t, db, "ALTER TABLE ddl_t ADD COLUMN extra INTEGER")
	mustExec(t, db, "/* a comment */ CREATE INDEX ddl_i ON ddl_t (extra)")
	mustExec(t, db, "INSERT INTO ddl_t (v, extra) VALUES ('a', 1)")

	code := func(err error) string {
		var e *azora.Error
		require.ErrorAs(t, err, &e)
		return e.Code
	}
	// SQL the server runs and SQLite refuses: invalid, with SQLite's message.
	_, err := db.Query(ctx(t), "SELECT * FROM nowhere")
	require.Equal(t, azora.CodeInvalid, code(err))
	require.ErrorContains(t, err, "no such table: nowhere")
	_, err = db.Query(ctx(t), "SELECT nope FROM ddl_t")
	require.Equal(t, azora.CodeInvalid, code(err))
	_, err = db.Query(ctx(t), "SELECT (")
	require.Equal(t, azora.CodeInvalid, code(err))
	_, err = db.Exec(ctx(t), "INSERT INTO ddl_t (v) VALUES ('a')")
	require.Equal(t, azora.CodeInvalid, code(err), "a constraint violation")
	require.ErrorIs(t, err, azora.ErrInvalid)
	_, err = db.Exec(ctx(t), "INSERT INTO ddl_t (v) VALUES (?, ?)", 1)
	require.Equal(t, azora.CodeInvalid, code(err))
	// Statements the protocol does not let a client send: permission_denied.
	_, err = db.Query(ctx(t), "SELEC 1")
	require.Equal(t, azora.CodePermissionDenied, code(err))
	_, err = db.Query(ctx(t), "DELETE FROM ddl_t")
	require.Equal(t, azora.CodePermissionDenied, code(err), "a write sent as a query")
	_, err = db.Exec(ctx(t), "SELECT 1")
	require.Equal(t, azora.CodePermissionDenied, code(err), "a read sent as a write")
	for _, q := range []string{"ATTACH DATABASE '/tmp/x' AS x", "VACUUM", "BEGIN", "PRAGMA journal_mode = DELETE"} {
		_, err = db.Exec(ctx(t), q)
		require.Equal(t, azora.CodePermissionDenied, code(err), q)
	}
	_, err = db.Query(ctx(t), "SELECT 1; SELECT 2")
	require.Error(t, err, "one statement per request")
	// Schema changes run alone.
	_, err = db.Batch(ctx(t), azora.Stmt("CREATE TABLE ddl_u (a)"), azora.Stmt("INSERT INTO ddl_t (v) VALUES ('b')"))
	require.Equal(t, azora.CodeInvalid, code(err))
	_, err = db.ExecMutation(ctx(t), "ddl-1", "CREATE TABLE ddl_u (a)")
	require.Equal(t, azora.CodeInvalid, code(err))

	// Scopes.
	reader := f.connectURL(t, f.URL(testDB), azora.WithAPIKey(f.ReadKey))
	_, err = reader.Query(ctx(t), "SELECT * FROM ddl_t")
	require.NoError(t, err)
	_, err = reader.Exec(ctx(t), "INSERT INTO ddl_t (v) VALUES ('c')")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	require.ErrorIs(t, err, azora.ErrPermissionDenied)
	_, err = reader.Exec(ctx(t), "DROP TABLE ddl_t")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	writer := f.connectURL(t, f.URL(testDB), azora.WithAPIKey(f.WriteKey))
	_, err = writer.Exec(ctx(t), "INSERT INTO ddl_t (v) VALUES ('c')")
	require.NoError(t, err)
	_, err = writer.Query(ctx(t), "SELECT 1")
	require.True(t, azora.IsPermissionDenied(err), "write does not imply read: %v", err)
	_, err = writer.Exec(ctx(t), "DROP TABLE ddl_t")
	require.True(t, azora.IsPermissionDenied(err), "schema changes need the ddl scope: %v", err)

	// After all these errors the connections are still the first ones.
	require.Equal(t, uint64(1), db.Stats().Connects)
	mustExec(t, db, "DROP INDEX ddl_i")
	mustExec(t, db, "DROP TABLE ddl_t")
}

func TestEngineConcurrencyAndPool(t *testing.T) {
	f := engine(t)
	for name, opts := range map[string][]azora.Option{"single": nil, "pool": {azora.WithPool(4)}} {
		t.Run(name, func(t *testing.T) {
			db := f.connect(t, opts...)
			clearTable(t, db, "t_conc")
			const workers, each = 32, 25
			var wg sync.WaitGroup
			var failed atomic.Int32
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < each; i++ {
						id := w*each + i + 1
						if _, err := db.Exec(ctx(t), "INSERT INTO t_conc (id, customer_id, status) VALUES (?, ?, ?)", id, w, "s"); err != nil {
							t.Errorf("exec: %v", err)
							failed.Add(1)
							return
						}
						got, err := azora.Scalar[int](ctx(t), db, "SELECT customer_id FROM t_conc WHERE id = ?", id)
						if err != nil || got != w {
							t.Errorf("query: got %d, %v", got, err)
							failed.Add(1)
							return
						}
					}
				}()
			}
			wg.Wait()
			require.Zero(t, failed.Load())
			n, err := azora.Scalar[int](ctx(t), db, "SELECT COUNT(*) FROM t_conc")
			require.NoError(t, err)
			require.Equal(t, workers*each, n)
			st := db.Stats()
			require.Equal(t, st.PoolSize, st.Connections)
			require.Equal(t, uint64(st.PoolSize), st.Connects)
			require.Zero(t, st.Retries)
			require.Zero(t, st.InFlight)
		})
	}
}

func TestEngineRateLimitAndPlaintextRefused(t *testing.T) {
	// A server with a low request rate and without plaintext.
	f := newEngine(t, map[string]map[string]any{"realtime": {"message_rate": 50, "message_burst": 10, "allow_plaintext": false}})
	_, err := azora.Connect(ctx(t), "azora://"+f.RTAddr+"/"+testDB+"?plaintext=1", azora.WithAPIKey(f.Key))
	require.ErrorIs(t, err, azora.ErrPlaintextRefused)

	db := f.connect(t)
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Query(ctx(t), "SELECT 1")
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	st := db.Stats()
	t.Logf("60 concurrent queries at 50/s: %d rate-limit answers", st.RateLimited)
	require.Positive(t, st.RateLimited, "requests over the rate were told to wait, and waited")
	require.Less(t, st.RateLimited, uint64(200), "callers take turns instead of retrying all at once")
	require.Equal(t, uint64(1), st.Connects, "the session survived")
	// Pings are rate-limited too and survive it.
	for i := 0; i < 20; i++ {
		_, err := db.Ping(ctx(t))
		require.NoError(t, err)
	}
}

// Requests made while the server is down wait for it, and a write sent
// before the crash applies exactly once.
func TestEngineRestartRequests(t *testing.T) {
	f := newEngine(t, nil)
	db := f.connect(t, azora.WithReconnectBackoff(10*time.Millisecond, 200*time.Millisecond))
	mustExec(t, db, "INSERT INTO orders (id, status) VALUES (1, 'a')")
	require.NoError(t, f.Kill())

	type out struct {
		res azora.Result
		err error
	}
	results := make(chan out, 3)
	for i := 2; i <= 4; i++ {
		go func() {
			res, err := db.Exec(ctx(t), "INSERT INTO orders (id, status) VALUES (?, 'down')", i)
			results <- out{res, err}
		}()
	}
	eventually(t, 10*time.Second, func() bool { return !db.Stats().Connected }, "the client noticed the server is gone")
	c, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err := db.Query(c, "SELECT 1")
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "a request waits for the connection until its context ends")
	require.ErrorContains(t, err, "not connected")

	require.NoError(t, f.Serve())
	for i := 0; i < 3; i++ {
		o := <-results
		require.NoError(t, o.err)
		require.Equal(t, int64(1), o.res.RowsAffected)
	}
	n, err := azora.Scalar[int](ctx(t), db, "SELECT COUNT(*) FROM orders")
	require.NoError(t, err)
	require.Equal(t, 4, n)
	st := db.Stats()
	require.GreaterOrEqual(t, st.Connects, uint64(2))
	require.Positive(t, st.ConnectFailures)
	_ = devengine.EnvBin
}
