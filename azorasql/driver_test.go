package azorasql_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/azorasql"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
	"github.com/azora-db/azora-go/internal/devengine"
)

func bg(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestParseDSN(t *testing.T) {
	const key = "B6N8vBQgk8i3VdwbEOhstCY3StFqqFPtC9_AsrhtHHw"
	cfg, batch, err := azorasql.ParseDSN("azora://db.example.com:7481/shop?key=" + key + "&api_key=azk_x&pool=4&tx=batch&timeout=5s&name=billing%2F2")
	require.NoError(t, err)
	require.True(t, batch)
	require.Equal(t, "db.example.com:7481", cfg.Addr)
	require.Equal(t, "shop", cfg.Database)
	require.Equal(t, key, cfg.ServerKey)
	require.Equal(t, "azk_x", cfg.Credential)
	require.Equal(t, 4, cfg.PoolSize)
	require.Equal(t, 5*time.Second, cfg.RequestTimeout)
	require.Equal(t, "billing/2", cfg.ClientName)

	cfg, batch, err = azorasql.ParseDSN("wss://edge.example.com/rt/v1?db=shop&fp=azp1-7uih-nrqh-26rp-caj3-az5k-zdaw&token=azt1.x")
	require.NoError(t, err)
	require.False(t, batch)
	require.Equal(t, "wss://edge.example.com/rt/v1", cfg.WebSocketURL)
	require.Equal(t, "azt1.x", cfg.Credential)

	t.Setenv(azorasql.EnvAPIKey, "azk_env")
	cfg, _, err = azorasql.ParseDSN("azora://h/shop?plaintext=1")
	require.NoError(t, err)
	require.Equal(t, "azk_env", cfg.Credential)
	t.Setenv(azorasql.EnvAPIKey, "")
	os.Unsetenv(azorasql.EnvAPIKey)

	for dsn, msg := range map[string]string{
		"azora://h/shop?plaintext=1":                          "no credential",
		"azora://h/shop?plaintext=1&api_key=a&token=b":        "not both",
		"azora://h/shop?plaintext=1&api_key=a&api_key=b":      "given 2 times",
		"azora://h/shop?plaintext=1&api_key=a&pool=0":         "pool",
		"azora://h/shop?plaintext=1&api_key=a&pool=x":         "pool",
		"azora://h/shop?plaintext=1&api_key=a&tx=interactive": "tx=",
		"azora://h/shop?plaintext=1&api_key=a&timeout=soon":   "timeout",
		"azora://h/shop?plaintext=1&api_key=a&unknown=1":      "unknown parameter",
		"azora://h/shop?api_key=a":                            "ServerKey",
		"postgres://h/shop?api_key=a":                         "unsupported scheme",
		"azora://h/shop?plaintext=1&api_key=a&%zz":            "parse dsn",
	} {
		_, _, err := azorasql.ParseDSN(dsn)
		require.ErrorContains(t, err, msg, dsn)
		// database/sql hands the DSN to the driver in Open, so a bad one
		// fails there rather than at the first query.
		_, err = sql.Open("azora", dsn)
		require.ErrorContains(t, err, msg, dsn)
	}
	_, err = (&azorasql.Driver{}).OpenConnector("azora://h/shop")
	require.Error(t, err)
	_, err = (&azorasql.Driver{}).Open("azora://h/shop")
	require.Error(t, err)
}

// The driver against the scripted server: what database/sql sends and
// how faults surface.
func TestDriverOnFakeServer(t *testing.T) {
	srv := azptest.New(t)
	var mu sync.Mutex
	var queries []azp.Query
	var txs []azp.Tx
	var dropped bool
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		mu.Lock()
		defer mu.Unlock()
		switch x := m.(type) {
		case *azp.Query:
			queries = append(queries, *x)
			if x.SQL == "truncated" {
				ss.Send(&azp.Result{ReqID: x.ReqID, Columns: []string{"n"}, Rows: [][]any{{int64(1)}}, Truncated: true})
				return
			}
			ss.Send(&azp.Result{ReqID: x.ReqID, Columns: []string{"id", "name", "at"}, Rows: [][]any{
				{int64(1), "a", "2026-11-10 09:30:00"}, {int64(2), nil, nil}}})
		case *azp.Exec:
			ss.Send(&azp.Result{ReqID: x.ReqID, RowsAffected: 3, LastInsertID: 9})
		case *azp.Tx:
			txs = append(txs, *x)
			if !dropped {
				dropped = true
				ss.Drop() // the commit's answer is lost once
				return
			}
			ss.Send(&azp.Result{ReqID: x.ReqID, RowsAffected: int64(len(x.Statements)), Replayed: true})
		}
	})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg, err := azora.ParseURL(srv.URL("app"))
	require.NoError(t, err)
	cfg.Credential, cfg.Logger = "azk_test", quiet
	cfg.ReconnectMin, cfg.ReconnectMax = time.Millisecond, 10*time.Millisecond

	db := sql.OpenDB(azorasql.NewConnector(cfg, azorasql.WithBatchTransactions()))
	defer db.Close()
	require.NoError(t, db.PingContext(bg(t)))

	// Query: arguments of any type azora accepts, NULLs, times.
	rows, err := db.QueryContext(bg(t), "SELECT id, name, at FROM t WHERE a = ? AND b = ? AND c = ?", int8(5), true, []byte{1})
	require.NoError(t, err)
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "at"}, cols)
	type rec struct {
		id   int
		name sql.NullString
		at   azorasql.Time
	}
	var got []rec
	for rows.Next() {
		var r rec
		require.NoError(t, rows.Scan(&r.id, &r.name, &r.at))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, got, 2)
	require.Equal(t, "a", got[0].name.String)
	require.True(t, got[0].at.Valid)
	require.Equal(t, time.Date(2026, 11, 10, 9, 30, 0, 0, time.UTC), got[0].at.Time)
	require.False(t, got[1].name.Valid)
	require.False(t, got[1].at.Valid)
	mu.Lock()
	require.Equal(t, []any{int64(5), true, []byte{1}}, queries[0].Args)
	mu.Unlock()
	v, err := azorasql.Time{Time: got[0].at.Time, Valid: true}.Value()
	require.NoError(t, err)
	require.Equal(t, got[0].at.Time, v)
	v, err = azorasql.Time{}.Value()
	require.NoError(t, err)
	require.Nil(t, v)

	var id int
	require.NoError(t, db.QueryRowContext(bg(t), "one").Scan(&id, new(any), new(any)))
	require.Equal(t, 1, id)
	_, err = db.QueryContext(bg(t), "truncated")
	require.ErrorIs(t, err, azorasql.ErrTruncated)
	_, err = db.QueryContext(bg(t), "SELECT :a", sql.Named("a", 1))
	require.ErrorContains(t, err, "named parameters")
	_, err = db.ExecContext(bg(t), "INSERT INTO t VALUES (?)", struct{}{})
	require.ErrorContains(t, err, "unsupported")

	// Exec and prepared statements.
	res, err := db.ExecContext(bg(t), "UPDATE t SET a = ?", 1)
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	last, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(9), last)
	st, err := db.PrepareContext(bg(t), "UPDATE t SET a = ? WHERE id = ?")
	require.NoError(t, err)
	_, err = st.ExecContext(bg(t), 1, 2)
	require.NoError(t, err)
	_, err = st.Exec(3, 4)
	require.NoError(t, err)
	qs, err := db.Prepare("SELECT ?")
	require.NoError(t, err)
	r2, err := qs.Query(1)
	require.NoError(t, err)
	require.NoError(t, r2.Close())
	require.NoError(t, st.Close())
	require.NoError(t, qs.Close())

	// A buffered transaction: nothing is sent before Commit, results are
	// pending, queries are refused; Commit is one batch, applied once even
	// though its first answer was lost.
	tx, err := db.BeginTx(bg(t), nil)
	require.NoError(t, err)
	res, err = tx.ExecContext(bg(t), "UPDATE acct SET b = b - ? WHERE id = ?", 10, 1)
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.ErrorIs(t, err, azorasql.ErrPendingResult)
	_, err = res.LastInsertId()
	require.ErrorIs(t, err, azorasql.ErrPendingResult)
	_, err = tx.ExecContext(bg(t), "UPDATE acct SET b = b + ? WHERE id = ?", 10, 2)
	require.NoError(t, err)
	_, err = tx.QueryContext(bg(t), "SELECT 1")
	require.ErrorIs(t, err, azorasql.ErrQueryInTransaction)
	mu.Lock()
	require.Empty(t, txs, "nothing was sent before Commit")
	mu.Unlock()
	require.NoError(t, tx.Commit())
	mu.Lock()
	require.Len(t, txs, 2, "the commit was sent again after its answer was lost")
	require.Equal(t, txs[0].MutationID, txs[1].MutationID)
	require.NotEmpty(t, txs[0].MutationID)
	require.Equal(t, []azp.Statement{{SQL: "UPDATE acct SET b = b - ? WHERE id = ?", Args: []any{int64(10), int64(1)}},
		{SQL: "UPDATE acct SET b = b + ? WHERE id = ?", Args: []any{int64(10), int64(2)}}}, txs[1].Statements)
	txs = nil
	mu.Unlock()
	require.ErrorIs(t, tx.Commit(), sql.ErrTxDone)

	// Rollback and an empty transaction send nothing.
	tx, err = db.BeginTx(bg(t), nil)
	require.NoError(t, err)
	_, err = tx.Exec("DELETE FROM acct")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	tx, err = db.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	mu.Lock()
	require.Empty(t, txs)
	mu.Unlock()
	_, err = db.BeginTx(bg(t), &sql.TxOptions{Isolation: sql.LevelSerializable})
	require.ErrorContains(t, err, "isolation")
	_, err = db.BeginTx(bg(t), &sql.TxOptions{ReadOnly: true})
	require.ErrorContains(t, err, "read-only")
	// The connection is usable again after a transaction.
	_, err = db.QueryContext(bg(t), "SELECT 1")
	require.NoError(t, err)

	// Without the option, Begin says why it cannot work.
	plain := sql.OpenDB(azorasql.NewConnector(cfg))
	defer plain.Close()
	_, err = plain.BeginTx(bg(t), nil)
	require.ErrorIs(t, err, azorasql.ErrNoTransactions)

	// Server errors come through as *azora.Error.
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if x, ok := m.(*azp.Query); ok {
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeInvalid, Message: "no such table: t"})
		}
	})
	_, err = db.QueryContext(bg(t), "SELECT * FROM t")
	var ae *azora.Error
	require.ErrorAs(t, err, &ae)
	require.True(t, azora.IsInvalid(err))

	// Closing the *sql.DB closes the azora.DB the connector opened.
	require.NoError(t, db.Close())
	_, err = db.QueryContext(bg(t), "SELECT 1")
	require.Error(t, err)
}

// The driver against the real engine, through a DSN.
func TestDriverOnEngine(t *testing.T) {
	bin, err := devengine.FindBinary()
	if err != nil {
		t.Skipf("integration test skipped: %v", err)
	}
	e, err := devengine.New(devengine.Options{Bin: bin, Dir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, e.CreateDB("shop"))
	key, err := e.CreateKey("shop", "app", "read", "write", "ddl")
	require.NoError(t, err)
	require.NoError(t, e.Serve())
	t.Cleanup(func() { e.Stop() })

	db, err := sql.Open("azora", e.URL("shop")+"&api_key="+key+"&pool=2&tx=batch")
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PingContext(bg(t)))
	db.SetMaxOpenConns(8)

	_, err = db.ExecContext(bg(t), "CREATE TABLE acct (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER NOT NULL CHECK (balance >= 0), opened TEXT, active INTEGER)")
	require.NoError(t, err)
	opened := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	res, err := db.ExecContext(bg(t), "INSERT INTO acct (owner, balance, opened, active) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
		"ann", 100, opened, true, "bob", 50, nil, false)
	require.NoError(t, err)
	n, _ := res.RowsAffected()
	require.Equal(t, int64(2), n)
	last, _ := res.LastInsertId()
	require.Equal(t, int64(2), last)

	rows, err := db.QueryContext(bg(t), "SELECT id, owner, balance, opened, active FROM acct ORDER BY id")
	require.NoError(t, err)
	type acct struct {
		id      int64
		owner   string
		balance int
		opened  azorasql.Time
		active  bool
	}
	var got []acct
	for rows.Next() {
		var a acct
		require.NoError(t, rows.Scan(&a.id, &a.owner, &a.balance, &a.opened, &a.active))
		got = append(got, a)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 2)
	require.Equal(t, "ann", got[0].owner)
	require.True(t, got[0].active)
	require.True(t, opened.Equal(got[0].opened.Time))
	require.False(t, got[1].opened.Valid)
	require.False(t, got[1].active)

	var balance int
	require.NoError(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = ?", "ann").Scan(&balance))
	require.Equal(t, 100, balance)
	require.ErrorIs(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = ?", "nobody").Scan(&balance), sql.ErrNoRows)

	// A buffered transaction commits as one batch...
	tx, err := db.BeginTx(bg(t), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(bg(t), "UPDATE acct SET balance = balance - 30 WHERE owner = 'ann'")
	require.NoError(t, err)
	_, err = tx.ExecContext(bg(t), "UPDATE acct SET balance = balance + 30 WHERE owner = 'bob'")
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = 'ann'").Scan(&balance))
	require.Equal(t, 100, balance, "nothing runs before Commit")
	require.NoError(t, tx.Commit())
	require.NoError(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = 'ann'").Scan(&balance))
	require.Equal(t, 70, balance)
	// ... or not at all: the error surfaces at Commit.
	tx, err = db.BeginTx(bg(t), nil)
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE acct SET balance = balance + 1000 WHERE owner = 'bob'")
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE acct SET balance = balance - 1000 WHERE owner = 'ann'")
	require.NoError(t, err)
	err = tx.Commit()
	require.True(t, azora.IsInvalid(err), "%v", err)
	require.NoError(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = 'bob'").Scan(&balance))
	require.Equal(t, 80, balance)
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("DELETE FROM acct")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	// Many goroutines through database/sql's pool.
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := db.ExecContext(bg(t), "UPDATE acct SET balance = balance + 1 WHERE owner = 'ann'"); err != nil {
					t.Error(err)
					return
				}
				var b int
				if err := db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = 'ann'").Scan(&b); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, db.QueryRowContext(bg(t), "SELECT balance FROM acct WHERE owner = 'ann'").Scan(&balance))
	require.Equal(t, 70+16*20, balance)

	// SQL errors keep their code.
	_, err = db.QueryContext(bg(t), "SELECT * FROM nowhere")
	require.True(t, azora.IsInvalid(err), "%v", err)
	require.False(t, errors.Is(err, sql.ErrConnDone))

	// Sharing an azora.DB the application already has.
	adb, err := azora.Connect(bg(t), e.URL("shop"), azora.WithAPIKey(key))
	require.NoError(t, err)
	defer adb.Close()
	shared := azorasql.OpenDB(adb)
	var count int
	require.NoError(t, shared.QueryRowContext(bg(t), "SELECT COUNT(*) FROM acct").Scan(&count))
	require.Equal(t, 2, count)
	_, err = shared.Begin()
	require.ErrorIs(t, err, azorasql.ErrNoTransactions)
	require.NoError(t, shared.Close())
	_, err = adb.Ping(bg(t))
	require.NoError(t, err, "closing the *sql.DB leaves the shared azora.DB open")

	c := azorasql.NewConnector(azora.Config{Addr: e.RTAddr, Database: "shop", ServerKey: e.ServerKey, Credential: key})
	inner, err := c.DB(bg(t))
	require.NoError(t, err)
	_, err = inner.Ping(bg(t))
	require.NoError(t, err)
	require.NoError(t, c.Close())
	_, err = c.Connect(bg(t))
	require.ErrorIs(t, err, azora.ErrClosed)
}
