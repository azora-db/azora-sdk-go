// Package azora is the Go client of Azora DB: SQL, writes that apply
// exactly once, units (functions published into the database), live
// tables, change notifications and file storage, over
// one connection that is encrypted end to end and kept alive for you.
//
//	db, err := azora.ConnectEnv(ctx) // AZORA_URL and AZORA_TOKEN
//	if err != nil { ... }
//	defer db.Close()
//
//	open, err := azora.All[Order](ctx, db, "SELECT * FROM orders WHERE status = ?", "open")
//	_, err = db.Exec(ctx, "UPDATE orders SET status = 'paid' WHERE id = ?", open[0].ID)
//
//	live, err := azora.Live[Order](ctx, db, azora.TableSpec("orders", azora.Eq("status", "open")))
//	live.OnUpdate(func(u azora.LiveUpdate[Order]) { render(live.Rows()) })
//
// # Connections
//
// A client needs two values. The database's public URL
// (https://db.example.com/shop?key=...) says where it is and holds
// nothing secret; a private token, an API key or a client token minted
// for one user, says who is connecting. `azora connect <database> --key
// <name>` on the server prints both; ConnectEnv reads them from AZORA_URL
// and AZORA_TOKEN, Connect takes them as arguments. The realtime endpoint
// and file storage are found from the URL.
//
// A DB is a client of one database. Connect returns once the first
// connection is up; after that the DB reconnects by itself with backoff,
// sends reads again, and repeats writes under the same mutation id so
// they apply once. Every method is safe for concurrent use, and requests
// from many goroutines share the connection: keep one DB per process.
// WithPool adds connections for workloads the server's per-connection
// limits would throttle.
//
// The connection is encrypted end to end against the server's public
// key: the one in the URL (?key=), or the one the server publishes, which
// is only trusted over HTTPS or on loopback; see ParseURL. A token that
// expires is supplied by a function (WithCredentialFunc).
//
// # Reading and writing
//
// Query returns Rows; Select, Get and the generic All, One and Scalar
// decode into structs, maps and scalars (see Rows.ScanStruct for how
// columns match fields). Exec runs one write, Batch several statements as
// one transaction. ExecMutation and BatchMutation take the mutation id
// from the caller, which keeps a write exactly-once across restarts of
// the calling process.
//
// # Units
//
// Units are functions published into the database (written in
// TypeScript, run by the engine next to the data) and called by name:
// Call[R] and DB.Call send the arguments as JSON and decode the result.
// A mutation unit is one transaction, run exactly once per mutation id;
// every call carries one and resends it after a lost connection. A unit
// that throws returns a *UnitError; Bytes carries binary values. Units
// can be called with any credential, including client tokens that cannot
// run SQL. DB.Units publishes bundles and reads the run log (ddl scope).
//
// # Realtime
//
// Live keeps a table, a filtered part of one, or a row current in memory
// and reports every change. Subscribe is the layer below it: the raw
// stream of consistent updates, resumable from a checkpoint, for a
// Replica or your own store. Watch only says which tables changed. Tables
// must be published on the server (azora realtime publish) for Live and
// Subscribe.
//
// # Files
//
// DB.Storage reaches the server's file storage over HTTP: uploads
// (streamed, or resumable in chunks), downloads, signed links.
//
// # Errors
//
// Errors the server reports are *Error values with a Code; IsNotFound,
// IsInvalid, IsPermissionDenied, IsConflict and IsRetryable classify
// them. ErrUnknownOutcome reports a statement that cannot be repeated
// safely (a schema change) whose answer was lost with the connection.
//
// The package azorasql is a database/sql driver built on this one.
package azora
