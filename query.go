package azora

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// MaxQueryRows is the most rows the server returns for one query; a
// result cut there has Rows.Truncated set. Page with LIMIT and a key
// condition to read more.
const MaxQueryRows = 10_000

// MaxMutationIDLen is the longest mutation id the server accepts, in
// bytes.
const MaxMutationIDLen = 128

// Statement is one SQL statement with its arguments. Make one with Stmt.
type Statement struct {
	SQL  string
	Args []any
}

// Stmt returns a Statement, for Batch:
//
//	db.Batch(ctx,
//		azora.Stmt("UPDATE accounts SET balance = balance - ? WHERE id = ?", 10, from),
//		azora.Stmt("UPDATE accounts SET balance = balance + ? WHERE id = ?", 10, to))
func Stmt(sql string, args ...any) Statement { return Statement{SQL: sql, Args: args} }

// Result is the outcome of Exec and Batch.
type Result struct {
	// RowsAffected is the number of rows the statement changed (for a
	// batch: by all its statements together).
	RowsAffected int64
	// LastInsertID is the rowid of the last row inserted.
	LastInsertID int64
	// Replayed reports a write answered from its first run (the same
	// mutation id had already been applied, for example before a
	// reconnect): it did not run again.
	Replayed bool
}

// Query runs a read and returns its rows (at most MaxQueryRows;
// Rows.Truncated reports a cut). It is sent again on a new connection if
// its connection drops, and retried when the server rate-limits it.
//
// Arguments bind to ? placeholders and are converted with Normalize. One
// statement per call: SELECT, WITH, VALUES, EXPLAIN and read-only
// introspection pragmas.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	return db.QueryN(ctx, 0, sql, args...)
}

// QueryN is Query with a row limit (0 = MaxQueryRows): the server stops
// after maxRows rows and Rows.Truncated reports whether more existed.
func (db *DB) QueryN(ctx context.Context, maxRows int, sql string, args ...any) (*Rows, error) {
	a, err := normalizeAll(args)
	if err != nil {
		return nil, err
	}
	if maxRows < 0 || maxRows > 1<<31 {
		return nil, fmt.Errorf("azora: maxRows %d out of range", maxRows)
	}
	m, err := db.call(ctx, callOpts{}, func(id uint32) azp.Msg {
		return &azp.Query{ReqID: id, SQL: sql, Args: a, MaxRows: uint32(maxRows)}
	})
	if err != nil {
		return nil, err
	}
	r, ok := m.(*azp.Result)
	if !ok {
		return nil, fmt.Errorf("azora: unexpected answer type %d", m.Type())
	}
	return &Rows{cols: r.Columns, rows: r.Rows, truncated: r.Truncated, cur: -1}, nil
}

// Exec runs a write (INSERT, UPDATE, DELETE, REPLACE) or a schema change.
// A write carries a random mutation id and is sent again with the same id
// after a lost connection, so it applies exactly once even when the
// answer was lost. Schema changes (CREATE, ALTER, DROP; they need the ddl
// scope) cannot carry a mutation id: if the connection drops after one
// was sent, Exec returns ErrUnknownOutcome.
//
// A context canceled after the statement was sent leaves its outcome
// unknown: use ExecMutation with an id you keep to find out by retrying.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	if isDDL(sql) {
		return db.write(ctx, "", []Statement{{SQL: sql, Args: args}}, true)
	}
	return db.write(ctx, NewMutationID(), []Statement{{SQL: sql, Args: args}}, true)
}

// ExecMutation is Exec with a caller-chosen mutation id (1 to 128 bytes):
// an application that persists the id with a queued write can resend it
// after a restart and still apply it exactly once. The server remembers
// ids for realtime.mutation_window (24h by default); a retry within it
// returns the first result with Result.Replayed set.
func (db *DB) ExecMutation(ctx context.Context, mutationID, sql string, args ...any) (Result, error) {
	if err := checkMutationID(mutationID); err != nil {
		return Result{}, err
	}
	return db.write(ctx, mutationID, []Statement{{SQL: sql, Args: args}}, true)
}

// Batch runs statements in one write transaction: all of them apply, or
// none does. It is sent as a single request (an AZP Tx), so it is not an
// interactive transaction: statements cannot depend on results read
// inside the batch, only on what SQL itself can express. Like Exec it
// applies exactly once across reconnects. Schema changes are not allowed
// in a batch.
func (db *DB) Batch(ctx context.Context, stmts ...Statement) (Result, error) {
	return db.write(ctx, NewMutationID(), stmts, false)
}

// BatchMutation is Batch with a caller-chosen mutation id (see
// ExecMutation).
func (db *DB) BatchMutation(ctx context.Context, mutationID string, stmts ...Statement) (Result, error) {
	if err := checkMutationID(mutationID); err != nil {
		return Result{}, err
	}
	return db.write(ctx, mutationID, stmts, false)
}

func checkMutationID(id string) error {
	if id == "" {
		return errors.New("azora: empty mutation id")
	}
	if len(id) > MaxMutationIDLen {
		return fmt.Errorf("azora: mutation id of %d bytes (at most %d)", len(id), MaxMutationIDLen)
	}
	return nil
}

// write sends statements as one Exec (single) or a Tx, idempotent when mid
// is set.
func (db *DB) write(ctx context.Context, mid string, stmts []Statement, single bool) (Result, error) {
	if len(stmts) == 0 {
		return Result{}, errors.New("azora: no statements")
	}
	wire := make([]azp.Statement, len(stmts))
	for i, s := range stmts {
		a, err := normalizeAll(s.Args)
		if err != nil {
			return Result{}, fmt.Errorf("statement %d: %w", i+1, err)
		}
		wire[i] = azp.Statement{SQL: s.SQL, Args: a}
	}
	o := callOpts{}
	if mid == "" {
		o.policy = once
	}
	m, err := db.call(ctx, o, func(id uint32) azp.Msg {
		if single {
			return &azp.Exec{ReqID: id, MutationID: mid, SQL: wire[0].SQL, Args: wire[0].Args}
		}
		return &azp.Tx{ReqID: id, MutationID: mid, Statements: wire}
	})
	if err != nil {
		return Result{}, err
	}
	r, ok := m.(*azp.Result)
	if !ok {
		return Result{}, fmt.Errorf("azora: unexpected answer type %d", m.Type())
	}
	return Result{RowsAffected: r.RowsAffected, LastInsertID: r.LastInsertID, Replayed: r.Replayed}, nil
}

// BatchBuilder collects the statements of a batch. Make one with
// DB.NewBatch; it is not safe for concurrent use.
type BatchBuilder struct {
	db    *DB
	mid   string
	stmts []Statement
}

// NewBatch starts a batch to fill with Add and send with Run:
//
//	res, err := db.NewBatch().
//		Add("INSERT INTO orders (id, total) VALUES (?, ?)", id, total).
//		Add("UPDATE stock SET qty = qty - 1 WHERE sku = ?", sku).
//		Run(ctx)
func (db *DB) NewBatch() *BatchBuilder { return &BatchBuilder{db: db} }

// Add appends a statement.
func (b *BatchBuilder) Add(sql string, args ...any) *BatchBuilder {
	b.stmts = append(b.stmts, Statement{SQL: sql, Args: args})
	return b
}

// MutationID sets the batch's mutation id (see ExecMutation); without it
// Run picks a random one.
func (b *BatchBuilder) MutationID(id string) *BatchBuilder { b.mid = id; return b }

// Len returns the number of statements added.
func (b *BatchBuilder) Len() int { return len(b.stmts) }

// Run sends the batch as one all-or-nothing transaction (see DB.Batch).
// The builder can be run again: without MutationID that is a new write.
func (b *BatchBuilder) Run(ctx context.Context) (Result, error) {
	if b.mid != "" {
		return b.db.BatchMutation(ctx, b.mid, b.stmts...)
	}
	return b.db.Batch(ctx, b.stmts...)
}

// SignURL returns a signed download URL for a stored object, valid for ttl
// (0 = the server's default), and its expiry. It works over the realtime
// connection with any credential that has the read scope, client tokens
// included; Storage.SignURL does the same over HTTP with more options.
func (db *DB) SignURL(ctx context.Context, objectID string, ttl time.Duration) (string, time.Time, error) {
	if ttl < 0 || ttl > (1<<32-1)*time.Second {
		return "", time.Time{}, fmt.Errorf("azora: ttl %v out of range", ttl)
	}
	secs := uint32((ttl + time.Second - 1) / time.Second)
	m, err := db.call(ctx, callOpts{}, func(id uint32) azp.Msg { return &azp.Sign{ReqID: id, ObjectID: objectID, TTLSeconds: secs} })
	if err != nil {
		return "", time.Time{}, err
	}
	s, ok := m.(*azp.Signed)
	if !ok {
		return "", time.Time{}, fmt.Errorf("azora: unexpected answer type %d", m.Type())
	}
	return s.URL, time.UnixMilli(s.ExpiresAtMS), nil
}

// NewMutationID returns a random mutation id for ExecMutation and
// BatchMutation.
func NewMutationID() string {
	var b [16]byte
	rand.Read(b[:])
	return "m-" + hex.EncodeToString(b[:])
}

// isDDL mirrors the server's test: the first keyword (after whitespace and
// comments) is CREATE, ALTER or DROP.
func isDDL(q string) bool {
	i := 0
	for i < len(q) {
		switch {
		case q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r' || q[i] == ';':
			i++
		case len(q)-i >= 2 && q[i:i+2] == "--":
			for i < len(q) && q[i] != '\n' {
				i++
			}
		case len(q)-i >= 2 && q[i:i+2] == "/*":
			j := i + 2
			for j+1 < len(q) && q[j:j+2] != "*/" {
				j++
			}
			if j+1 >= len(q) {
				return false
			}
			i = j + 2
		default:
			j := i
			for j < len(q) && (q[j]|0x20 >= 'a' && q[j]|0x20 <= 'z') {
				j++
			}
			switch kw := q[i:j]; len(kw) {
			case 4:
				return eqFold(kw, "drop")
			case 5:
				return eqFold(kw, "alter")
			case 6:
				return eqFold(kw, "create")
			}
			return false
		}
	}
	return false
}

// eqFold reports whether ASCII a equals lowercase b, ignoring a's case
// (len(a) == len(b) is checked by the caller).
func eqFold(a, b string) bool {
	for i := 0; i < len(a); i++ {
		if a[i]|0x20 != b[i] {
			return false
		}
	}
	return true
}
