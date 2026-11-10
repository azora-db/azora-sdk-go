package azorasql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"

	azora "github.com/azora-db/azora-go"
)

// conn is a database/sql connection: a handle on the shared azora.DB,
// plus the buffer of the transaction in progress, if any. database/sql
// uses a conn from one goroutine at a time.
type conn struct {
	c  *Connector
	db *azora.DB
	tx *batchTx
}

var (
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
)

// Close releases the handle. The shared connections stay open until the
// *sql.DB is closed.
func (c *conn) Close() error {
	c.tx = nil
	return nil
}

// ResetSession drops a transaction buffer left behind before the
// connection is reused.
func (c *conn) ResetSession(context.Context) error {
	c.tx = nil
	return nil
}

// Ping checks the server with a protocol ping.
func (c *conn) Ping(ctx context.Context) error {
	_, err := c.db.Ping(ctx)
	return err
}

// CheckNamedValue converts a parameter with azora.Normalize, so every
// type the SDK accepts works here too. Named parameters are refused.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		return errors.New("azorasql: named parameters are not supported: use ? placeholders")
	}
	v, err := azora.Normalize(nv.Value)
	if err != nil {
		return err
	}
	nv.Value = v
	return nil
}

func values(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// QueryContext runs a read.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.tx != nil {
		return nil, ErrQueryInTransaction
	}
	r, err := c.db.Query(ctx, query, values(args)...)
	if err != nil {
		return nil, err
	}
	if r.Truncated() {
		return nil, ErrTruncated
	}
	return &rows{cols: r.Columns(), vals: r.Values()}, nil
}

// ExecContext runs a write, or buffers it inside a transaction.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.tx != nil {
		c.tx.stmts = append(c.tx.stmts, azora.Stmt(query, values(args)...))
		return pendingResult{}, nil
	}
	res, err := c.db.Exec(ctx, query, values(args)...)
	if err != nil {
		return nil, err
	}
	return result{res}, nil
}

// Prepare returns a statement that remembers its SQL; the server is not
// contacted.
func (c *conn) Prepare(query string) (driver.Stmt, error) { return &stmt{c: c, query: query}, nil }

// PrepareContext is Prepare.
func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

// Begin starts a buffered transaction (see BeginTx).
func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }

// BeginTx starts a buffered transaction when the connector allows them
// (tx=batch); otherwise it fails with ErrNoTransactions. Isolation
// levels and read-only transactions do not apply to a buffer and are
// refused.
func (c *conn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.c.batch {
		return nil, ErrNoTransactions
	}
	if opts.Isolation != 0 || opts.ReadOnly {
		return nil, errors.New("azorasql: a buffered transaction has no isolation level and cannot be read-only")
	}
	if c.tx != nil {
		return nil, errors.New("azorasql: a transaction is already in progress on this connection")
	}
	c.tx = &batchTx{c: c, mutationID: azora.NewMutationID()}
	return c.tx, nil
}

// batchTx buffers the statements of a transaction until Commit.
type batchTx struct {
	c          *conn
	mutationID string // fixed at Begin, so a Commit retried by the SDK applies once
	stmts      []azora.Statement
}

// Commit sends the buffered statements as one all-or-nothing batch. A
// transaction without statements commits without a request.
func (t *batchTx) Commit() error {
	if t.c.tx != t {
		return errors.New("azorasql: the transaction is no longer active")
	}
	t.c.tx = nil
	if len(t.stmts) == 0 {
		return nil
	}
	// database/sql gives Commit no context: the DB's request timeout
	// bounds it.
	_, err := t.c.db.BatchMutation(context.Background(), t.mutationID, t.stmts...)
	return err
}

// Rollback discards the buffered statements: nothing was sent.
func (t *batchTx) Rollback() error {
	if t.c.tx == t {
		t.c.tx = nil
	}
	return nil
}

// stmt is a prepared statement: its SQL, run through the connection.
type stmt struct {
	c     *conn
	query string
}

var (
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) Close() error { return nil }

// NumInput returns -1: the driver does not parse SQL to count
// placeholders; the server checks the arguments.
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.c.ExecContext(context.Background(), s.query, named(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.c.QueryContext(context.Background(), s.query, named(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

// result is the outcome of a statement that ran.
type result struct{ res azora.Result }

func (r result) LastInsertId() (int64, error) { return r.res.LastInsertID, nil }
func (r result) RowsAffected() (int64, error) { return r.res.RowsAffected, nil }

// pendingResult is the outcome of a statement buffered in a transaction.
type pendingResult struct{}

func (pendingResult) LastInsertId() (int64, error) { return 0, ErrPendingResult }
func (pendingResult) RowsAffected() (int64, error) { return 0, ErrPendingResult }

// rows iterates over a result held in memory.
type rows struct {
	cols []string
	vals [][]any
	i    int
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { r.vals = nil; return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	row := r.vals[r.i]
	r.i++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		} else {
			dest[i] = nil
		}
	}
	return nil
}
