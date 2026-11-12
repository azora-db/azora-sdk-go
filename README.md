# Azora DB for Go

`github.com/azora-db/azora-go` connects Go programs to an
[Azora DB](../azora-engine) database: SQL, writes that apply exactly once,
units (functions published into the database), live tables, change
notifications and file storage, over one connection
that is encrypted end to end and kept alive for you.

It needs Go 1.23 or later and has three dependencies (`flynn/noise`,
`golang.org/x/crypto`, `coder/websocket`). A `database/sql` driver is
included.

```go
db, err := azora.ConnectEnv(ctx) // AZORA_URL (public) and AZORA_TOKEN (private)
defer db.Close()

open, err := azora.All[Order](ctx, db, "SELECT * FROM orders WHERE status = ?", "open")
_, err = db.Exec(ctx, "UPDATE orders SET status = 'paid' WHERE id = ?", open[0].ID)

live, err := azora.Live[Order](ctx, db, azora.TableSpec("orders", azora.Eq("status", "open")))
live.OnUpdate(func(u azora.LiveUpdate[Order]) { render(live.Rows()) }) // every change, as it commits
```

- [Install](#install)
- [Connecting](#connecting)
- [Reading](#reading) · [Writing](#writing) · [Units](#units)
- [Live tables](#live-tables) · [Subscriptions](#subscriptions) · [Watching tables](#watching-tables)
- [File storage](#file-storage)
- [database/sql](#databasesql)
- [Errors, timeouts and cancellation](#errors-timeouts-and-cancellation)
- [Guarantees and limits](#guarantees-and-limits)
- [Performance](#performance)
- [Examples](#examples) · [Developing](#developing)

## Install

```sh
go get github.com/azora-db/azora-go
```

```go
import azora "github.com/azora-db/azora-go"
```

## Connecting

A client needs two values:

| | | |
|---|---|---|
| `AZORA_URL` | **public** | Where the database is: `https://db.example.com/shop?key=…`. It can sit in source code, a repository, a browser. |
| `AZORA_TOKEN` | **private** | Who the client is: an API key (`azk_…`) for a server, or a short-lived token (`azt1.…`) your backend mints for one user. |

On the server, `azora connect shop --key backend` prints both, ready for
an env file. Then:

```go
db, err := azora.ConnectEnv(ctx)                                  // from the environment
db, err := azora.Connect(ctx, url, azora.WithToken(token))        // or given explicitly
```

That is all there is to configure. From the URL the SDK works out the
realtime endpoint and where files are stored; the token decides what the
connection may do. The same URL serves every client: your backend with
its API key, each user's device with that user's token.

`Connect` returns once the first connection is up, or with the reason it
failed (unreachable server, wrong server key, refused token). After that
the `DB` reconnects by itself, with exponential backoff and jitter. Every
method is safe for concurrent use and requests from many goroutines share
the connection: keep one `DB` per process.

**What the URL holds.** The server's HTTP address, the database as the
last path segment, and the server's public key (`?key=`). The connection
is encrypted end to end against that key, so a proxy or CDN in front of
the server only sees ciphertext. Without `?key=`, the SDK reads the key
from `<address>/.well-known/azora.json`: over `https` the certificate
vouches for it; over plain `http` that is only accepted on loopback, and
`Connect` refuses anything else and asks for the key.

**The token.** For a token that expires, pass a function instead:
`azora.WithCredentialFunc(func(ctx context.Context) (string, error) { … })`
is called on every reconnect and when the server reports that the token
expired; the new one is sent on the open connection and subscriptions
continue. A user token can be limited to tables and pinned to filters
(`azora realtime token create shop --sub user:42 --table orders --filter
"orders: customer = 42"`).

**Raw TCP.** The URL above connects over WebSocket. Servers that want the
last tenth can name the realtime port instead; point reads are about 10%
faster. File storage then needs `WithStorageURL`.

| Connection string | Transport |
|---|---|
| `https://host/<database>[?key=…]`, `http://…` | WebSocket, storage at the same address |
| `azora://host[:7480]/<database>?key=…` | TCP |
| `wss://host[/rt/v1]?db=<database>&key=…`, `ws://…` | WebSocket, endpoint given explicitly |

`?fp=azp1-…` checks the server's key by fingerprint instead of pinning it.
`azora.Open(ctx, azora.Config{…})` takes every setting as a struct.

Other options: `WithPool(n)`, `WithRequestTimeout` (30 s),
`WithReconnectBackoff` (100 ms to 30 s), `WithClientName`, `WithLogger`,
`WithDialer`, `WithHTTPClient`, and `WithHooks` (`OnConnect`,
`OnDisconnect`, `OnResync`, `OnError`).

`db.Welcome()` describes the session (principal, scopes, security mode,
server key); `db.Stats()` and `db.Ping(ctx)` report on the connection.

## Reading

```go
orders, err := azora.All[Order](ctx, db, "SELECT * FROM orders WHERE customer = ?", 42) // []Order
order, err  := azora.One[Order](ctx, db, "SELECT * FROM orders WHERE id = ?", 7)        // azora.ErrNoRows when missing
count, err  := azora.Scalar[int64](ctx, db, "SELECT count(*) FROM orders")              // first cell

var list []Order
err = db.Select(ctx, &list, "SELECT * FROM orders")                                     // the same without generics
rows, err := db.Query(ctx, "SELECT id, total FROM orders")                              // *azora.Rows: Next, Scan, All, Maps
```

Rows decode into structs, `map[string]any` or a single value. Columns
match struct fields by their `db` tag, else by name, ignoring case and
underscores (`customer_id` fills `CustomerID`). Columns without a field
are skipped. `db:"-"` skips a field; `db:"meta,json"` decodes JSON text
into it. Pointers and `sql.Null*` types take NULL; `time.Time` reads the
text and integer forms SQLite stores.

Arguments bind to `?` placeholders:

| Go | SQLite |
|---|---|
| `nil`, nil pointers | NULL |
| integer types | INTEGER (64-bit) |
| `float32`, `float64` | REAL |
| `string`, `json.RawMessage` | TEXT |
| `[]byte` | BLOB |
| `bool` | 1 or 0 |
| `time.Time` | TEXT |
| `driver.Valuer` | what it returns |

A read returns at most 10,000 rows (`Rows.Truncated` tells when rows were
cut; page with `LIMIT` or a key). Reads are sent again on a new connection
if the connection drops.

## Writing

```go
res, err := db.Exec(ctx, "INSERT INTO orders(customer, status, total) VALUES (?, ?, ?)", 42, "open", 129.9)
// res.RowsAffected, res.LastInsertID

res, err = db.Batch(ctx, // one transaction: all or nothing
	azora.Stmt("UPDATE accounts SET balance = balance - ? WHERE id = ?", 2500, "alice"),
	azora.Stmt("UPDATE accounts SET balance = balance + ? WHERE id = ?", 2500, "bob"),
	azora.Stmt("INSERT INTO transfers(src, dst, amount) VALUES (?, ?, ?)", "alice", "bob", 2500),
)
```

**Every write applies exactly once.** `Exec` and `Batch` carry a random
mutation id and send the same id again after a reconnect; the server
stores the id with the write in the same transaction and answers a retry
with the first result (`Result.Replayed`). To keep that across restarts of
your own process, store an id (`azora.NewMutationID()`) with the queued
write and use `ExecMutation` / `BatchMutation` (see
[`examples/exactly-once`](examples/exactly-once/main.go)). The server
keeps ids for 24 hours (`realtime.mutation_window`).

`Batch` sends its statements together: there is no interactive
transaction, so a statement cannot use a value an earlier one returned
(except through SQL itself, such as `last_insert_rowid()` or a subquery).

Schema changes (`CREATE`, `ALTER`, `DROP`) need the `ddl` scope, run alone
in an `Exec`, and cannot carry a mutation id: if the connection drops
after one was sent, the call fails with `azora.ErrUnknownOutcome` and you
decide whether to check and run it again.

## Units

Units are functions written in TypeScript and published into the
database (see the engine's [docs/units.md](../azora-engine/docs/units.md)).
They run in the engine, next to the data, and any client calls them by
name:

```go
type CheckoutArgs struct{ CartID string `json:"cartId"` }
type CheckoutResult struct{ OrderID int64 `json:"orderId"` }

res, err := azora.Call[CheckoutResult](ctx, db, "checkout", CheckoutArgs{CartID: "c1"})
err = db.Call(ctx, "checkout", CheckoutArgs{CartID: "c1"}, &res)     // the same without generics
raw, err := db.CallRaw(ctx, "myOrders", nil)                         // raw.Value: the JSON result
```

Arguments are encoded with `encoding/json` (`nil`: none) and the result
decoded into `R` or `out` (a nil `out` discards it). Numbers travel as
JSON numbers, so integers beyond 2^53 lose precision. A blob is
`azora.Bytes`, which encodes as `{"$bytes": "<base64>"}`: the unit sees a
`Uint8Array`, and a blob it returns decodes back into `Bytes`.

**Exactly once.** Every call carries a random mutation id and is sent
again with the same id after a lost connection: a `mutation` unit is one
transaction and runs once per id, and a retry within 24 hours returns
its first result. To keep that across restarts of your own process,
store an id with the queued call:

```go
var replayed bool
res, err := azora.Call[CheckoutResult](ctx, db, "checkout", args,
	azora.MutationID(id), azora.Replayed(&replayed)) // replayed: answered from the first run
```

**Errors.** A unit that throws `UnitError(code, message, data)` returns a
`*azora.UnitError`; anything else it throws has code `failed`. Problems
around the unit are an `*azora.Error`: `not_found` (no such unit),
`permission_denied` (an internal unit), `invalid` (arguments that do not
match the unit's schema), `resource_exhausted` (a timeout or a limit).

```go
var ue *azora.UnitError
if errors.As(err, &ue) && ue.Code == azora.UnitCodeNotFound {
	fmt.Println(ue.Message, string(ue.Data)) // Data: the JSON the unit attached
}
```

**Who can call.** Any credential that can open the database, including
client tokens limited to some tables or rows, which cannot run SQL. A
unit runs with full access to its database and decides from `ctx.auth`
what the caller may do.

**Publishing and managing** needs an API key with the `ddl` scope. The
`azora-units` CLI does it from TypeScript sources; `db.Units()` does it
from Go with a bundle already built:

```go
units := db.Units()
d, err := units.Deploy(ctx, azora.DeployRequest{Bundle: bundle, Note: "v2"}) // d.Version, d.Units
list, err := units.List(ctx)                    // the active deploy's units
hist, err := units.Deploys(ctx, 20)             // the deploy history
_, err = units.Activate(ctx, hist.Deploys[1].Version) // roll back
runs, err := units.Runs(ctx, azora.RunsQuery{Unit: "checkout", Status: "error", Limit: 50}) // also Before, After (run ids)
err = units.Secrets().Set(ctx, "STRIPE_KEY", key) // ctx.env.STRIPE_KEY in units
```

Also `units.Call` (runs internal units too, as the caller),
`Triggers`, `SkipTrigger`, `Secrets().List` and `Secrets().Unset`.

**The run log.** Each `Run` has a `Status`: `azora.RunStatusOK`,
`RunStatusError` (the unit threw, timed out or hit a limit),
`RunStatusReplayed` (answered from an earlier call's stored result) or
`RunStatusRejected` (refused before the unit ran: arguments that do not
match, an internal unit called by a client). Its `Trigger` says what
started it: `RunTriggerCall` (a client), `RunTriggerSchedule`,
`RunTriggerChange` (onChange), `RunTriggerJob`, `RunTriggerAdmin` (the
admin API and dashboard), `RunTriggerCLI` (`units.Call` and
`azora-units call`) or `RunTriggerMCP`.

**Jobs.** A unit can run a mutation or action later with
`ctx.scheduler.after(ms, unit, args)` or `.at(time, unit, args)` (see
"Jobs" in [docs/units.md](../azora-engine/docs/units.md#jobs)). Jobs run
under `azora serve`, as principal `job:<id>`, with trigger `job` in the
run log. A job whose unit keeps throwing is retried with backoff up to
`units.job_attempts` times, then kept as failed:

```go
jobs, err := units.Jobs(ctx, azora.JobsQuery{State: azora.JobStateFailed, Limit: 50}) // also Unit
fmt.Println(jobs.Pending, jobs.Failed)              // counts of the whole database
for _, j := range jobs.Jobs {                       // j.ID, j.Unit, j.Args, j.RunAt, j.Attempts, j.Error ...
	err = units.RetryJob(ctx, j.ID)                 // a failed job runs again now, attempts reset
}
removed, err := units.CancelJob(ctx, id)            // pending or failed; false: no such job
```

`RetryJob` refuses a job that is not failed (`invalid`) and an unknown id
(`not_found`).

Deploys, activations, admin calls, trigger skips, job cancels and
retries, and secret changes are not resent after a lost connection
(`ErrUnknownOutcome`); reads are. Canceling again is harmless; before
retrying a retry, check `Jobs`: the job may already be running.

A server that supports units lists `"units"` in its
`/.well-known/azora.json`. When the SDK read that document (a URL
without `?key=`) and the feature is missing, calls fail with
`azora.ErrUnitsUnsupported` without sending anything.

## Live tables

A live table is a table (or the part of it a filter selects, or one row)
kept up to date in memory. Publish the table once (`azora realtime publish
shop orders`); then:

```go
open, err := azora.Live[Order](ctx, db, azora.TableSpec("orders",
	azora.And(azora.Eq("status", "open"), azora.Gt("total", 100)))) // or azora.MustParseFilter("status = 'open' AND total > 100")
defer open.Close()
err = open.Wait(ctx) // the first snapshot

open.Rows()          // []Order, in primary key order
open.Get(42)         // by primary key
stop := open.OnUpdate(func(u azora.LiveUpdate[Order]) { … })
for u, err := range open.Updates(ctx) { … }
```

- **Always consistent.** Rows change only at checkpoints, which end on
  transaction boundaries: a listener never sees half of a transaction, and
  one transaction is one update.
- **No gap between reading and listening.** A new listener is first
  called with the rows the table already holds (`Reset` is set), then with
  every change after them.
- **Survives disconnects.** After a reconnect the table resumes from its
  last checkpoint: nothing is lost or delivered twice. When the server can
  no longer resume (the change log was pruned, the table's columns
  changed, the database was restored), it takes a new snapshot and the
  update has `Reset` set.
- **Back pressure, not loss.** A slow listener slows the stream; the
  server stops sending until the client catches up.
- **Verifiable.** `open.Verify(ctx)` compares a checksum of the local rows
  with the server's; `LiveOptions.VerifyEvery` does it on a timer.

`azora.RowSpec("orders", 42)` follows one row; `db.Live` returns rows as
`azora.Record` maps when you have no struct.

## Subscriptions

`Subscribe` is the layer under `Live`: the stream of consistent updates
itself, for a store of your own.

```go
sub, err := db.Subscribe(ctx, azora.DatabaseSpec()) // or TableSpec, RowSpec, FilesSpec
replica := azora.NewReplica()
for {
	u, err := sub.Next(ctx)
	if err != nil { break }
	replica.Apply(u)                 // u.Reset: drop the copy first
	save(u.Checkpoint, u.Epoch)      // resume later with spec.ResumeFrom(checkpoint, epoch)
}
```

`Spec.On` limits the operations, `Spec.WithoutSnapshot` skips the initial
rows, `Spec.Manual` hands resynchronization to you (`ErrResync`, then
`sub.Resync()`).

## Watching tables

When you only need to know *that* something changed, to run a query
again:

```go
w, err := db.Watch(ctx, "orders", "order_items") // no tables: every table
for n, err := range w.Notices(ctx) {
	if err != nil { break }
	if n.Has("orders") { refresh() }
}
```

Notices carry table names, not rows, and need no publication. After a
reconnect one notice with `Everything` set stands for whatever was
missed.

## File storage

The engine stores files next to the database and serves them over HTTP,
at the address `AZORA_URL` already names.

```go
files := db.Storage()

obj, err := files.Upload(ctx, r, azora.UploadOptions{Path: "invoices/2026-0042.pdf"})
_, err = db.Exec(ctx, "INSERT INTO documents(title, file) VALUES (?, ?)", "Invoice", obj.ID) // FILE column

link, err := files.SignURL(ctx, obj.ID, azora.SignOptions{TTL: 5 * time.Minute}) // opens that file, no credentials
d, err := files.Download(ctx, obj.ID, azora.DownloadOptions{Offset: 0, Length: 1 << 20})

// Large files: chunks, resumed from what the server holds after a failure.
obj, err = files.UploadResumable(ctx, file, size, azora.UploadOptions{Name: "backup.bin"},
	azora.ResumableOptions{OnProgress: func(sent, total int64) { … }})
```

Also: `Stat`, `List` / `Objects` (an iterator over every page), `Delete`,
`SignUploadURL` with `UploadSigned` (a browser or another service uploads
without a credential), and upload sessions (`NewSession`, `ResumeSession`)
to continue an upload in another process.

## database/sql

```go
import _ "github.com/azora-db/azora-go/azorasql"

db, err := sql.Open("azora", os.Getenv("AZORA_URL")) // the token comes from AZORA_TOKEN
```

A DSN is the same URL; `&token=…`, `&tx=batch`, `&pool=N` and
`&timeout=10s` may be appended.

or, sharing one connection with the rest of the SDK,
`azorasql.OpenDB(adb, azorasql.WithBatchTransactions())`.

The protocol has no interactive transactions, so `Begin` fails unless
`tx=batch` is set (or `WithBatchTransactions`). With it, a transaction buffers its `Exec` statements
and sends them at `Commit` as one all-or-nothing batch: errors surface at
`Commit`, results of statements inside it are not available, and queries
inside it are refused. Code that reads inside a transaction to decide
what to write cannot use this mode. The package documentation lists the
other differences (positional parameters only, results held in memory).

## Errors, timeouts and cancellation

Every call takes a `context.Context`; a call without a deadline is bound
by `WithRequestTimeout`. A call whose context ends returns at once; the
protocol has no cancel message, so a statement the server already started
runs to its end (or to the server's own limits). A write abandoned that
way may have applied: send it again with the same id (`ExecMutation`) to
find out without applying it twice.

Errors the server reports are `*azora.Error` with a `Code`:

```go
_, err := db.Exec(ctx, "INSERT INTO orders(id) VALUES (1)")
switch {
case azora.IsInvalid(err):          // bad SQL, a constraint
case azora.IsPermissionDenied(err): // scope or token restriction
case azora.IsNotFound(err):
case azora.IsConflict(err):
case azora.IsRetryable(err):        // rate limit, database busy or reopening
case errors.Is(err, azora.ErrUnknownOutcome):
case errors.Is(err, azora.ErrClosed):
}
```

Rate limits and a briefly unavailable database are retried for you while
the context allows.

## Guarantees and limits

- Reads are retried on a new connection; writes are repeated under their
  mutation id and apply once; schema changes are never repeated.
- Live tables and subscriptions deliver every change in commit order,
  once, across reconnects, or tell you that they started over (`Reset`).
- A read holds at most 10,000 rows in memory; there is no streaming
  cursor.
- No interactive transactions: use `Batch`.
- Publish tables that have a primary key: without one, rows are
  identified by rowid, which `VACUUM` may renumber.
- A token limited to tables or filters cannot run SQL; it can subscribe
  to its tables.
- The SDK does not create databases, keys or tokens; that is the engine's
  CLI and admin API.

## Performance

Client and engine on one machine (8 cores), raw TCP (`azora://`), the
server's per-connection rate limit lifted (`go test -run '^$' -bench . -benchtime
2s`):

| | |
|---|---|
| Point read, one at a time | 109 µs (9,200/s) |
| Point reads from 64 goroutines, one connection | 45,000/s |
| The same over `WithPool(4)` | 65,000/s |
| 1,000 rows into structs | 1.4 ms |
| Insert, one at a time | 283 µs (3,500/s) |
| Inserts from 64 goroutines, one connection | 5,400/s |
| Batch of 10 inserts | 433 µs (23,000 rows/s) |
| A write, to its change arriving in a live table | 0.4 ms |

Through the public URL (WebSocket) a point read takes about 10% longer.

One connection carries concurrent requests, so a pool matters mostly when
the server's per-connection limit (`realtime.message_rate`, 1,000 requests
a second by default) is what holds a workload back.

## Examples

```sh
go run ./cmd/azora-dev          # terminal 1: a local engine with a seeded "shop" database
go run ./examples/quickstart    # terminal 2
```

| | |
|---|---|
| [`quickstart`](examples/quickstart/main.go) | Connect, read into structs, write, batch |
| [`live-orders`](examples/live-orders/main.go) | A filtered live table, changes as they commit, checksum verification |
| [`exactly-once`](examples/exactly-once/main.go) | An outbox replayed after a crash without applying a transfer twice |
| [`units`](examples/units/main.go) | Typed unit calls, a unit's error, a replayed checkout, a restricted token calling a unit ([`units.js`](examples/units/units.js)) |
| [`watch`](examples/watch/main.go) | Change notices driving a re-run of an aggregate query |
| [`tokens`](examples/tokens/main.go) | The same URL with a user's token, which sees only that user's rows |
| [`files`](examples/files/main.go) | Upload, bind to a row, signed link, resumable upload |
| [`sqldriver`](examples/sqldriver/main.go) | `database/sql` with buffered transactions |

`azora-dev` needs the engine binary: `$AZORA_BIN`, or
`../azora-engine/bin/azora` (`make build` in the engine repository). It
writes `AZORA_URL` and `AZORA_TOKEN` to `examples/.env`, which the
examples read; the tokens and units examples connect to the same URL
with a second, restricted token. When the engine supports units, it
publishes [`examples/units/units.js`](examples/units/units.js), a bundle
written by hand in the engine's bundle format (no build step).

## Developing

```sh
go test ./...                              # unit tests, a scripted fake server, and a real engine
go test -race ./...
go test -run '^$' -bench . -benchtime 2s   # benchmarks against the real engine
go test -run '^$' -fuzz FuzzParseURL .     # fuzz targets: FuzzParseURL, FuzzReplica, the codec and protocol decoders
```

The integration tests start the engine binary on a temporary data root
and are skipped when it is not found. They cover the three handshake
modes, connecting by public URL with a pinned and a discovered key, value
types, exactly-once writes across a dropped connection,
tokens and their refresh, subscriptions of every kind with resume and
resynchronization, live tables, file storage, units (skipped when the
engine does not list `"units"` in its discovery document), and a server
restart in the middle of all of it.
