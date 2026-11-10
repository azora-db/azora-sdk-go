// Package azorasql is a database/sql driver for Azora DB, built on the
// azora package:
//
//	import _ "github.com/azora-db/azora-go/azorasql"
//
//	db, err := sql.Open("azora", os.Getenv("AZORA_URL")) // the credential comes from AZORA_TOKEN
//	rows, err := db.QueryContext(ctx, "SELECT id, total FROM orders WHERE status = ?", "paid")
//
// It gives code written against database/sql (and the libraries built on
// it) reads and writes over the encrypted realtime connection, with the
// SDK's reconnects, retries and exactly-once writes. Realtime features
// (subscriptions, live tables, storage) are in the azora package; use
// OpenDB to share one connection between both.
//
// # Data source names
//
// A DSN is an azora connection string (see azora.ParseURL) with these
// extra query parameters:
//
//	token=...         the credential: an API key or a client token
//	                  (api_key= is accepted too). Without it, the
//	                  AZORA_TOKEN environment variable is used.
//	pool=N            N protocol connections (azora.WithPool)
//	tx=batch          enable buffered transactions (see below)
//	timeout=10s       azora.WithRequestTimeout
//	name=billing/2    azora.WithClientName
//
// A DSN holds a credential: keep it out of logs. NewConnector takes an
// azora.Config instead, for credentials that come from a function.
//
// # What differs from other drivers
//
// The protocol has no sessions: every statement is a request of its own,
// and all driver connections of a *sql.DB share the SDK's protocol
// connections (so sql.DB's pool settings do not matter; pool=N does).
//
//   - Transactions. The protocol has no interactive transactions: a
//     server cannot hold the database's writer open between requests of a
//     client. By default Begin fails with ErrNoTransactions. With
//     tx=batch, a transaction buffers its Exec statements and sends them
//     at Commit as one all-or-nothing batch (azora.DB.Batch), applied
//     exactly once. Inside such a transaction nothing has run yet, so
//     Exec returns a Result whose RowsAffected and LastInsertId fail with
//     ErrPendingResult, Query fails with ErrQueryInTransaction (it could
//     not see the transaction's own writes), a statement's error (a
//     constraint, bad SQL) surfaces at Commit, and Rollback only discards
//     the buffer. Code that reads inside a transaction to decide what to
//     write cannot use this mode.
//   - Prepared statements are kept on the client; Prepare does not
//     contact the server.
//   - Parameters are positional (?). Named parameters are refused.
//   - Results are complete in memory, at most 10,000 rows per query; a
//     result cut there fails with ErrTruncated instead of silently
//     missing rows. Page with LIMIT.
//   - Values are SQLite's: integers, floats, text, blobs and NULL. A
//     time.Time parameter is stored as text; scan it back into a string,
//     or into Time (database/sql does not parse text into *time.Time by
//     itself). Booleans are stored as 1 and 0 and scan into bool.
//   - Schema changes (CREATE, ALTER, DROP) need a key with the ddl scope
//     and cannot run inside a transaction.
package azorasql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	azora "github.com/azora-db/azora-go"
)

func init() { sql.Register(DriverName, &Driver{}) }

// DriverName is the name the driver registers with database/sql.
const DriverName = "azora"

// EnvAPIKey names the environment variable read for a credential when
// the DSN has none.
const EnvAPIKey = "AZORA_API_KEY"

var (
	// ErrNoTransactions is returned by Begin unless buffered transactions
	// were enabled (tx=batch): the protocol has no interactive
	// transactions.
	ErrNoTransactions = errors.New("azorasql: interactive transactions are not supported by the protocol: use azora.DB.Batch, or the tx=batch DSN parameter for buffered transactions")
	// ErrQueryInTransaction is returned by a query inside a buffered
	// transaction: its writes have not run yet, so the query could not
	// see them.
	ErrQueryInTransaction = errors.New("azorasql: queries are not available inside a buffered transaction: its statements run at Commit")
	// ErrPendingResult is returned by RowsAffected and LastInsertId of an
	// Exec inside a buffered transaction: the statement runs at Commit.
	ErrPendingResult = errors.New("azorasql: the statement runs at Commit: its result is not available")
	// ErrTruncated is returned for a result the server cut at its row
	// limit.
	ErrTruncated = fmt.Errorf("azorasql: the query returned more than %d rows: page it with LIMIT", azora.MaxQueryRows)
)

// Driver is the database/sql driver. It is registered as "azora".
type Driver struct{}

// Open returns a connection for dsn. database/sql calls OpenConnector
// instead, so that every connection of a *sql.DB shares one azora.DB.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector parses dsn (see the package documentation).
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, batch, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg, batch: batch, owned: true}, nil
}

// ParseDSN splits a data source name into the azora.Config it describes
// and whether buffered transactions are enabled.
func ParseDSN(dsn string) (cfg azora.Config, batchTx bool, err error) {
	base, rawQuery, _ := strings.Cut(strings.TrimSpace(dsn), "?")
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return cfg, false, fmt.Errorf("azorasql: parse dsn: %w", err)
	}
	take := func(name string) (string, error) {
		vs := q[name]
		delete(q, name)
		switch len(vs) {
		case 0:
			return "", nil
		case 1:
			return vs[0], nil
		}
		return "", fmt.Errorf("azorasql: parse dsn: parameter %q given %d times", name, len(vs))
	}
	var apiKey, token, pool, tx, timeout, name string
	for _, p := range []struct {
		name string
		dst  *string
	}{{"api_key", &apiKey}, {"token", &token}, {"pool", &pool}, {"tx", &tx}, {"timeout", &timeout}, {"name", &name}} {
		if *p.dst, err = take(p.name); err != nil {
			return cfg, false, err
		}
	}
	rest := base
	if len(q) > 0 {
		rest += "?" + q.Encode()
	}
	if cfg, err = azora.ParseURL(rest); err != nil {
		return cfg, false, err
	}
	switch {
	case apiKey != "" && token != "":
		return cfg, false, errors.New("azorasql: parse dsn: give api_key or token, not both")
	case apiKey != "":
		cfg.Credential = apiKey
	case token != "":
		cfg.Credential = token
	default:
		if cfg.Credential = os.Getenv(azora.EnvToken); cfg.Credential == "" {
			cfg.Credential = os.Getenv(EnvAPIKey)
		}
	}
	if cfg.Credential == "" {
		return cfg, false, fmt.Errorf("azorasql: parse dsn: no credential: add token= to the DSN, or set %s", azora.EnvToken)
	}
	if pool != "" {
		n, err := strconv.Atoi(pool)
		if err != nil || n < 1 || n > azora.MaxPoolSize {
			return cfg, false, fmt.Errorf("azorasql: parse dsn: pool=%q (want 1 to %d)", pool, azora.MaxPoolSize)
		}
		cfg.PoolSize = n
	}
	switch tx {
	case "", "none":
	case "batch":
		batchTx = true
	default:
		return cfg, false, fmt.Errorf("azorasql: parse dsn: tx=%q (want batch or none)", tx)
	}
	if timeout != "" {
		d, err := time.ParseDuration(timeout)
		if err != nil || d <= 0 {
			return cfg, false, fmt.Errorf("azorasql: parse dsn: timeout=%q (want a duration like 10s)", timeout)
		}
		cfg.RequestTimeout = d
	}
	cfg.ClientName = name
	return cfg, batchTx, nil
}

// Connector opens driver connections that share one azora.DB. Use it
// with sql.OpenDB.
type Connector struct {
	cfg   azora.Config
	batch bool
	owned bool // the connector opened db and closes it

	mu     sync.Mutex
	db     *azora.DB
	closed bool
}

// ConnectorOption tunes NewConnector and OpenDB.
type ConnectorOption func(*Connector)

// WithBatchTransactions enables buffered transactions (the tx=batch DSN
// parameter; see the package documentation for what that means).
func WithBatchTransactions() ConnectorOption { return func(c *Connector) { c.batch = true } }

// NewConnector returns a connector for cfg: the way to use what a DSN
// cannot say (a CredentialFunc, hooks, a logger). The azora.DB is opened
// on first use and closed by sql.DB.Close.
//
//	db := sql.OpenDB(azorasql.NewConnector(cfg))
func NewConnector(cfg azora.Config, opts ...ConnectorOption) *Connector {
	c := &Connector{cfg: cfg, owned: true}
	for _, o := range opts {
		o(c)
	}
	return c
}

// OpenDB returns a *sql.DB that runs on an azora.DB the application
// already has, sharing its connections. Closing the *sql.DB does not
// close db.
func OpenDB(db *azora.DB, opts ...ConnectorOption) *sql.DB {
	c := &Connector{db: db}
	for _, o := range opts {
		o(c)
	}
	return sql.OpenDB(c)
}

// Connect returns a driver connection, opening the shared azora.DB on
// first use.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	db, err := c.open(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{c: c, db: db}, nil
}

func (c *Connector) open(ctx context.Context) (*azora.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, azora.ErrClosed
	}
	if c.db == nil {
		db, err := azora.Open(ctx, c.cfg)
		if err != nil {
			return nil, err
		}
		c.db = db
	}
	return c.db, nil
}

// Driver returns the driver.
func (c *Connector) Driver() driver.Driver { return &Driver{} }

// Close closes the azora.DB the connector opened. sql.DB.Close calls it.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.db != nil && c.owned {
		return c.db.Close()
	}
	return nil
}

// DB returns the azora.DB behind the connector, opening it if needed:
// the way to reach subscriptions and storage from code that holds the
// connector.
func (c *Connector) DB(ctx context.Context) (*azora.DB, error) { return c.open(ctx) }

// Time scans a time stored as text (or as Unix seconds, or a Julian day
// number) into a time.Time, which database/sql does not do by itself for
// a driver without column types. Valid is false for NULL.
type Time struct {
	Time  time.Time
	Valid bool
}

// Scan implements sql.Scanner.
func (t *Time) Scan(src any) error {
	t.Time, t.Valid = time.Time{}, src != nil
	if src == nil {
		return nil
	}
	return azora.ScanValue(&t.Time, src)
}

// Value implements driver.Valuer.
func (t Time) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}
	return t.Time, nil
}
