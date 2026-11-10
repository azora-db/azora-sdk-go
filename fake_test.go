package azora_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
)

// These tests run against the scripted in-process server (azptest): no
// engine binary is needed.

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeDB connects to the scripted server with fast reconnects.
func fakeDB(t testing.TB, srv *azptest.Server, opts ...azora.Option) *azora.DB {
	t.Helper()
	base := []azora.Option{azora.WithAPIKey("azk_test"), azora.WithReconnectBackoff(2*time.Millisecond, 20*time.Millisecond),
		azora.WithLogger(quiet)}
	db, err := azora.Connect(ctx(t), srv.URL("app"), append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// echo answers queries with the session's number, and writes with the
// number of statements.
func echo(ss *azptest.Session, m azp.Msg) {
	switch x := m.(type) {
	case *azp.Query:
		ss.Send(&azp.Result{ReqID: x.ReqID, Columns: []string{"session"}, Rows: [][]any{{int64(ss.N)}}})
	case *azp.Exec:
		ss.Send(&azp.Result{ReqID: x.ReqID, RowsAffected: 1})
	case *azp.Tx:
		ss.Send(&azp.Result{ReqID: x.ReqID, RowsAffected: int64(len(x.Statements))})
	case *azp.Auth:
		ss.Send(&azp.OK{ReqID: x.ReqID})
	}
}

func TestFakeSecurityModes(t *testing.T) {
	srv := azptest.New(t)
	srv.Handle(echo)
	key, fp := srv.ServerKey(), srv.Fingerprint()
	other, err := azp.GenerateKey(time.Now())
	require.NoError(t, err)
	for name, c := range map[string]struct {
		url  string
		mode string
	}{
		"IK tcp":        {"azora://" + srv.Addr() + "/app?key=" + key, "IK"},
		"IK+fp tcp":     {"azora://" + srv.Addr() + "/app?key=" + key + "&fp=" + fp, "IK"},
		"XX tcp":        {"azora://" + srv.Addr() + "/app?fp=" + fp, "XX"},
		"plaintext tcp": {"azora://" + srv.Addr() + "/app?plaintext=1", "plaintext"},
		"IK ws":         {srv.WebSocketURL() + "?db=app&key=" + key, "IK"},
		"XX ws":         {srv.WebSocketURL() + "?db=app&fp=" + fp, "XX"},
		"plaintext ws":  {srv.WebSocketURL() + "?db=app&plaintext=1", "plaintext"},
	} {
		t.Run(name, func(t *testing.T) {
			db, err := azora.Connect(ctx(t), c.url, azora.WithAPIKey("k"), azora.WithLogger(quiet))
			require.NoError(t, err)
			defer db.Close()
			w, ok := db.Welcome()
			require.True(t, ok)
			require.Equal(t, c.mode, w.Mode)
			if c.mode == "plaintext" {
				require.Empty(t, w.ServerKey)
			} else {
				require.Equal(t, key, w.ServerKey)
				require.Equal(t, fp, w.Fingerprint)
			}
			n, err := azora.Scalar[int](ctx(t), db, "SELECT 1")
			require.NoError(t, err)
			require.Positive(t, n)
			require.Equal(t, "app", db.Database())
		})
	}

	// A key the server does not hold, a fingerprint that does not match.
	_, err = azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app?key="+azp.EncodePublicKey(other.Public), azora.WithAPIKey("k"))
	require.ErrorIs(t, err, azora.ErrUnknownKey)
	_, err = azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app?fp="+azp.Fingerprint(other.Public), azora.WithAPIKey("k"))
	require.ErrorIs(t, err, azora.ErrHandshake)
	_, err = azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app?key="+key+"&fp="+azp.Fingerprint(other.Public), azora.WithAPIKey("k"))
	require.ErrorIs(t, err, azora.ErrHandshake)

	// Options complete a URL that does not say how to trust the server.
	db, err := azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app", azora.WithServerKey(key), azora.WithAPIKey("k"), azora.WithLogger(quiet))
	require.NoError(t, err)
	db.Close()
	db, err = azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app", azora.WithFingerprint(fp), azora.WithToken("azt1.x"), azora.WithLogger(quiet))
	require.NoError(t, err)
	db.Close()
	_, err = azora.Connect(ctx(t), "azora://"+srv.Addr()+"/app", azora.WithAPIKey("k"))
	require.ErrorContains(t, err, "ServerKey")
	_, err = azora.Connect(ctx(t), srv.URL("app"))
	require.ErrorContains(t, err, "credential")
	_, err = azora.Connect(ctx(t), srv.URL("app"), azora.WithAPIKey("k"), azora.WithPool(1000))
	require.ErrorContains(t, err, "PoolSize")
	_, err = azora.Open(ctx(t), azora.Config{Addr: srv.Addr(), ServerKey: key, Credential: "k"})
	require.ErrorContains(t, err, "Database")
	_, err = azora.Open(ctx(t), azora.Config{Database: "app", ServerKey: key, Credential: "k"})
	require.ErrorContains(t, err, "Addr")
	_, err = azora.Open(ctx(t), azora.Config{Database: "app", WebSocketURL: "http://x", ServerKey: key, Credential: "k"})
	require.ErrorContains(t, err, "WebSocketURL")
	_, err = azora.Open(ctx(t), azora.Config{Database: "app", Addr: srv.Addr(), ServerKey: key, Credential: "k", StorageURL: "ftp://x"})
	require.ErrorContains(t, err, "StorageURL")

	fpOf, err := azora.KeyFingerprint(key)
	require.NoError(t, err)
	require.Equal(t, fp, fpOf)
	_, err = azora.KeyFingerprint("nope")
	require.Error(t, err)
}

func TestFakeHelloSentAndClientName(t *testing.T) {
	srv := azptest.New(t)
	fakeDB(t, srv, azora.WithClientName("billing/2.1"))
	fakeDB(t, srv)
	hs := srv.Hellos()
	require.Len(t, hs, 2)
	require.Equal(t, azp.Hello{Version: 1, Database: "app", Credential: "azk_test", Client: "billing/2.1"}, hs[0])
	require.Equal(t, azora.DefaultClientName, hs[1].Client)
	require.Equal(t, "azora-go/"+azora.Version, hs[1].Client)
}

func TestFakeQueryRetriedAfterDrop(t *testing.T) {
	srv := azptest.New(t)
	var dropped atomic.Bool
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if _, ok := m.(*azp.Query); ok && dropped.CompareAndSwap(false, true) {
			ss.Drop() // the request arrived, the answer never leaves
			return
		}
		echo(ss, m)
	})
	db := fakeDB(t, srv)
	n, err := azora.Scalar[int](ctx(t), db, "SELECT 1")
	require.NoError(t, err)
	require.Equal(t, 2, n, "answered by the second session")
	st := db.Stats()
	require.Equal(t, uint64(1), st.Retries)
	require.Equal(t, uint64(2), st.Connects)
	require.Equal(t, uint64(1), st.Disconnects)
	require.True(t, st.Connected)
}

func TestFakeWriteKeepsMutationID(t *testing.T) {
	srv := azptest.New(t)
	var mu sync.Mutex
	var ids []string
	applied := map[string]bool{}
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		var id string
		var req uint32
		switch x := m.(type) {
		case *azp.Exec:
			id, req = x.MutationID, x.ReqID
		case *azp.Tx:
			id, req = x.MutationID, x.ReqID
		default:
			return
		}
		mu.Lock()
		ids = append(ids, id)
		replay := applied[id]
		applied[id] = true
		mu.Unlock()
		if !replay {
			ss.Drop() // applied, but the answer is lost
			return
		}
		ss.Send(&azp.Result{ReqID: req, RowsAffected: 1, LastInsertID: 9, Replayed: true})
	})
	db := fakeDB(t, srv)

	res, err := db.Exec(ctx(t), "INSERT INTO t VALUES (?)", 1)
	require.NoError(t, err)
	require.Equal(t, azora.Result{RowsAffected: 1, LastInsertID: 9, Replayed: true}, res)
	res, err = db.Batch(ctx(t), azora.Stmt("INSERT INTO t VALUES (?)", 1), azora.Stmt("DELETE FROM t"))
	require.NoError(t, err)
	require.True(t, res.Replayed)
	res, err = db.ExecMutation(ctx(t), "job-17", "UPDATE t SET a = 1")
	require.NoError(t, err)
	require.True(t, res.Replayed)
	res, err = db.NewBatch().Add("UPDATE t SET a = ?", 2).Add("UPDATE t SET b = ?", 3).MutationID("job-18").Run(ctx(t))
	require.NoError(t, err)
	require.True(t, res.Replayed)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, ids, 8)
	for i := 0; i < 8; i += 2 {
		require.Equal(t, ids[i], ids[i+1], "the retry carries the same mutation id")
		require.NotEmpty(t, ids[i])
	}
	require.NotEqual(t, ids[0], ids[2], "every write gets its own id")
	require.Equal(t, "job-17", ids[4])
	require.Equal(t, "job-18", ids[6])
}

func TestFakeDDLUnknownOutcome(t *testing.T) {
	srv := azptest.New(t)
	var got atomic.Value
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if x, ok := m.(*azp.Exec); ok {
			got.Store(x.MutationID)
			ss.Drop()
		}
	})
	db := fakeDB(t, srv)
	_, err := db.Exec(ctx(t), "  CREATE TABLE t (a)")
	require.ErrorIs(t, err, azora.ErrUnknownOutcome)
	require.Equal(t, "", got.Load(), "schema changes carry no mutation id")
	require.Zero(t, db.Stats().Retries)
}

func TestFakeRateLimitAndUnavailableRetry(t *testing.T) {
	srv := azptest.New(t)
	var n atomic.Int32
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		x, ok := m.(*azp.Query)
		if !ok {
			return
		}
		switch n.Add(1) {
		case 1:
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeResourceLimit, Message: "slow down", RetryAfterMS: 30})
		case 2:
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeUnavailable, Message: "reopening"})
		default:
			echo(ss, m)
		}
	})
	db := fakeDB(t, srv)
	start := time.Now()
	_, err := db.Query(ctx(t), "SELECT 1")
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond, "the retry hint was honoured")
	require.Equal(t, int32(3), n.Load())
	require.Equal(t, uint64(1), db.Stats().RateLimited)
	require.Equal(t, uint64(1), db.Stats().Connects, "no reconnect")
}

func TestFakeServerErrors(t *testing.T) {
	srv := azptest.New(t)
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if x, ok := m.(*azp.Query); ok {
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: x.SQL, Message: "nope"})
		}
	})
	db := fakeDB(t, srv)
	for code, check := range map[string]func(error) bool{
		azora.CodeInvalid:          azora.IsInvalid,
		azora.CodePermissionDenied: azora.IsPermissionDenied,
		azora.CodeNotFound:         azora.IsNotFound,
		azora.CodeUnauthenticated:  azora.IsUnauthenticated,
		azora.CodeInternal:         func(err error) bool { return azora.IsCode(err, azora.CodeInternal) },
	} {
		_, err := db.Query(ctx(t), code)
		require.True(t, check(err), "%s: %v", code, err)
		var e *azora.Error
		require.ErrorAs(t, err, &e)
		require.Equal(t, code, e.Code)
		require.Equal(t, "nope", e.Message)
		require.Zero(t, e.HTTPStatus)
		require.EqualError(t, err, "azora: "+code+": nope")
	}
	_, err := db.Query(ctx(t), azora.CodeNotFound)
	require.ErrorIs(t, err, azora.ErrNotFound)
	require.NotErrorIs(t, err, azora.ErrInvalid)
	_, err = db.Query(ctx(t), azora.CodePermissionDenied)
	require.ErrorIs(t, err, azora.ErrPermissionDenied)
	_, err = db.Query(ctx(t), azora.CodeInvalid)
	require.ErrorIs(t, err, azora.ErrInvalid)
	_, err = db.Query(ctx(t), azora.CodeUnauthenticated)
	require.ErrorIs(t, err, azora.ErrUnauthenticated)
	require.False(t, azora.IsRetryable(err))
	require.True(t, azora.IsRetryable(&azora.Error{Code: azora.CodeResourceLimit}))
	require.True(t, azora.IsRetryable(&azora.Error{Code: azora.CodeInvalid, RetryAfter: time.Second}))
	require.True(t, azora.IsRetryable(azora.ErrUnavailable))
	require.ErrorIs(t, &azora.Error{Code: azora.CodeResourceLimit}, azora.ErrResourceLimit)
	require.False(t, azora.IsRetryable(errors.New("x")))
	require.False(t, azora.IsConflict(err))
	require.False(t, azora.IsConflict(errors.New("x")))
	require.True(t, azora.IsConflict(&azora.Error{Code: azora.CodeAlreadyExists}))
}

func TestFakeGoAway(t *testing.T) {
	srv := azptest.New(t)
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if x, ok := m.(*azp.Query); ok && ss.N == 1 {
			// Announce the shutdown, then still answer the request in flight.
			ss.Send(&azp.GoAway{Reason: "draining", RetryAfterMS: 20})
			time.Sleep(20 * time.Millisecond)
			ss.Send(&azp.Result{ReqID: x.ReqID, Columns: []string{"session"}, Rows: [][]any{{int64(1)}}})
			return
		}
		echo(ss, m)
	})
	var disconnects atomic.Int32
	var reason atomic.Value
	db := fakeDB(t, srv, azora.WithHooks(azora.Hooks{OnDisconnect: func(err error) {
		reason.Store(err.Error())
		disconnects.Add(1)
	}}))
	n, err := azora.Scalar[int](ctx(t), db, "SELECT 1")
	require.NoError(t, err)
	require.Equal(t, 1, n, "the answer in flight still arrived")
	n, err = azora.Scalar[int](ctx(t), db, "SELECT 1")
	require.NoError(t, err)
	require.Equal(t, 2, n, "later requests wait for the next connection")
	require.Equal(t, int32(1), disconnects.Load())
	require.Contains(t, reason.Load(), "going away")
	require.Zero(t, db.Stats().Retries)
}

func TestFakeCredentialRefresh(t *testing.T) {
	srv := azptest.New(t)
	var mu sync.Mutex
	var auths []string
	valid := "tok-1"
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		mu.Lock()
		defer mu.Unlock()
		switch x := m.(type) {
		case *azp.Auth:
			auths = append(auths, x.Credential)
			if x.Credential != valid {
				ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeUnauthenticated, Message: "bad token"})
				return
			}
			ss.Send(&azp.OK{ReqID: x.ReqID})
		case *azp.Query:
			if x.SQL == "expired" && len(auths) == 0 {
				ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeUnauthenticated, Message: "token expired"})
				return
			}
			echo(ss, m)
		}
	})
	var calls atomic.Int32
	db := fakeDB(t, srv, azora.WithCredentialFunc(func(context.Context) (string, error) {
		return "tok-" + string(rune('0'+calls.Add(1))), nil
	}))
	require.Equal(t, "tok-1", srv.Hellos()[0].Credential, "Hello carries the function's credential")

	// A request refused as unauthenticated refreshes and is sent again.
	mu.Lock()
	valid = "tok-2"
	mu.Unlock()
	_, err := db.Query(ctx(t), "expired")
	require.NoError(t, err)
	require.Equal(t, uint64(1), db.Stats().CredentialRefreshes)

	// The server announcing the expiry (a session error) refreshes too.
	mu.Lock()
	valid = "tok-3"
	mu.Unlock()
	srv.Broadcast(&azp.Error{Code: azp.CodeUnauthenticated, Message: "credential expired"})
	eventually(t, 5*time.Second, func() bool { return db.Stats().CredentialRefreshes == 2 }, "second refresh")
	mu.Lock()
	require.Equal(t, []string{"tok-2", "tok-3"}, auths)
	mu.Unlock()
	require.Equal(t, uint64(1), db.Stats().Connects, "refreshing does not reconnect")

	// SetCredential sends Auth at once; a refused one is an error.
	mu.Lock()
	valid = "manual"
	mu.Unlock()
	require.NoError(t, db.SetCredential(ctx(t), "manual"))
	err = db.SetCredential(ctx(t), "wrong")
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
	require.Error(t, db.SetCredential(ctx(t), ""))
}

func TestFakeCredentialFuncErrors(t *testing.T) {
	srv := azptest.New(t)
	boom := errors.New("token service down")
	_, err := azora.Connect(ctx(t), srv.URL("app"), azora.WithCredentialFunc(func(context.Context) (string, error) { return "", boom }))
	require.ErrorIs(t, err, boom)
	_, err = azora.Connect(ctx(t), srv.URL("app"), azora.WithCredentialFunc(func(context.Context) (string, error) { return "", nil }))
	require.ErrorContains(t, err, "empty credential")
	require.Zero(t, srv.Accepted())
}

func TestFakeBadCredentialFailsFast(t *testing.T) {
	srv := azptest.New(t)
	srv.OnHello(func(h *azp.Hello) *azp.Error {
		if h.Credential != "good" {
			return &azp.Error{Code: azp.CodeUnauthenticated, Message: "invalid credential or database", Fatal: true}
		}
		return nil
	})
	srv.Handle(echo)
	_, err := azora.Connect(ctx(t), srv.URL("app"), azora.WithAPIKey("bad"))
	require.True(t, azora.IsUnauthenticated(err), "%v", err)

	// A credential that stops working while connected: requests fail at
	// once instead of waiting for a connection that cannot come.
	var errs atomic.Int32
	db, err := azora.Connect(ctx(t), srv.URL("app"), azora.WithAPIKey("good"), azora.WithLogger(quiet),
		azora.WithReconnectBackoff(time.Millisecond, 5*time.Millisecond), azora.WithHooks(azora.Hooks{OnError: func(error) { errs.Add(1) }}))
	require.NoError(t, err)
	defer db.Close()
	srv.OnHello(func(*azp.Hello) *azp.Error {
		return &azp.Error{Code: azp.CodeUnauthenticated, Message: "revoked", Fatal: true}
	})
	srv.DropAll()
	eventually(t, 5*time.Second, func() bool { return errs.Load() > 0 }, "a failed reconnect is reported")
	start := time.Now()
	_, err = db.Query(ctx(t), "SELECT 1")
	require.True(t, azora.IsUnauthenticated(err), "%v", err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.False(t, db.Stats().Connected)
	require.Positive(t, db.Stats().ConnectFailures)
	_, ok := db.Welcome()
	require.False(t, ok)
}

func TestFakeHeartbeat(t *testing.T) {
	srv := azptest.New(t)
	srv.SetHeartbeat(60 * time.Millisecond)
	srv.Handle(echo)
	var reason atomic.Value
	db := fakeDB(t, srv, azora.WithHooks(azora.Hooks{OnDisconnect: func(err error) { reason.Store(err.Error()) }}))
	w, _ := db.Welcome()
	require.Equal(t, 60*time.Millisecond, w.Heartbeat)

	// A quiet but live server is pinged, and its pongs keep the
	// connection up.
	time.Sleep(400 * time.Millisecond)
	require.Equal(t, uint64(1), db.Stats().Connects)
	require.True(t, db.Stats().Connected)

	// A server that stops answering is dropped after three intervals,
	// and the client reconnects when it answers again.
	srv.Silent.Store(true)
	eventually(t, 5*time.Second, func() bool { return db.Stats().Disconnects >= 1 }, "dead connection detected")
	require.Contains(t, reason.Load(), "heartbeat")
	srv.Silent.Store(false)
	_, err := db.Query(ctx(t), "SELECT 1")
	require.NoError(t, err)
}

func TestFakePing(t *testing.T) {
	srv := azptest.New(t)
	db := fakeDB(t, srv)
	d, err := db.Ping(ctx(t))
	require.NoError(t, err)
	require.Positive(t, d)

	// A rate-limited ping is answered with a session error naming no
	// request: Ping waits the hint and tries again.
	srv.NoPong.Store(true)
	var pings atomic.Int32
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if p, ok := m.(*azp.Ping); ok {
			if pings.Add(1) == 1 {
				ss.Send(&azp.Error{Code: azp.CodeResourceLimit, Message: "request rate limit", RetryAfterMS: 10})
				return
			}
			ss.Send(&azp.Pong{Nonce: p.Nonce})
		}
	})
	_, err = db.Ping(ctx(t))
	require.NoError(t, err)
	require.Equal(t, int32(2), pings.Load())
	require.Equal(t, uint64(1), db.Stats().RateLimited)

	// No pong at all: the context decides.
	srv.Handle(nil)
	c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = db.Ping(c)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestFakeLargeMessages(t *testing.T) {
	srv := azptest.New(t)
	big := make([]byte, 300_000) // several 64 KiB frames each way
	for i := range big {
		big[i] = byte(i * 7)
	}
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		if x, ok := m.(*azp.Query); ok {
			ss.Send(&azp.Result{ReqID: x.ReqID, Columns: []string{"v"}, Rows: [][]any{{x.Args[0]}}})
		}
	})
	for _, url := range []string{srv.URL("app"), srv.WebSocketURL() + "?db=app&key=" + srv.ServerKey(), "azora://" + srv.Addr() + "/app?plaintext=1"} {
		db, err := azora.Connect(ctx(t), url, azora.WithAPIKey("k"), azora.WithLogger(quiet))
		require.NoError(t, err)
		got, err := azora.Scalar[[]byte](ctx(t), db, "echo", big)
		require.NoError(t, err)
		require.Equal(t, big, got)
		db.Close()
	}
}

func TestFakeRequestTimeoutAndClose(t *testing.T) {
	srv := azptest.New(t)
	db := fakeDB(t, srv, azora.WithRequestTimeout(60*time.Millisecond))
	// The server never answers: RequestTimeout bounds a call whose
	// context has no deadline.
	start := time.Now()
	_, err := db.Query(context.Background(), "SELECT 1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second)
	// A caller's own deadline wins.
	c, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = db.Exec(c, "UPDATE t SET a = 1")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Close ends calls in progress with ErrClosed.
	db2 := fakeDB(t, srv, azora.WithRequestTimeout(-1))
	done := make(chan error, 1)
	go func() {
		_, err := db2.Query(context.Background(), "SELECT 1")
		done <- err
	}()
	eventually(t, 5*time.Second, func() bool { return db2.Stats().InFlight == 1 }, "the query is in flight")
	require.NoError(t, db2.Close())
	require.ErrorIs(t, <-done, azora.ErrClosed)
	_, err = db2.Query(ctx(t), "SELECT 1")
	require.ErrorIs(t, err, azora.ErrClosed)
	_, err = db2.Exec(ctx(t), "DELETE FROM t")
	require.ErrorIs(t, err, azora.ErrClosed)
	_, err = db2.Subscribe(ctx(t), azora.TableSpec("t"))
	require.ErrorIs(t, err, azora.ErrClosed)
	_, err = db2.Ping(ctx(t))
	require.ErrorIs(t, err, azora.ErrClosed)
	_, err = db2.Watch(ctx(t))
	require.ErrorIs(t, err, azora.ErrClosed)
	_, err = db2.Live(ctx(t), azora.TableSpec("t"))
	require.ErrorIs(t, err, azora.ErrClosed)
	require.NoError(t, db2.Close(), "closing twice is fine")
	db.Close()
	requireNoLeaks(t)
}

func TestFakeArgumentErrors(t *testing.T) {
	srv := azptest.New(t)
	srv.Handle(echo)
	db := fakeDB(t, srv)
	_, err := db.Query(ctx(t), "SELECT ?", struct{}{})
	require.ErrorContains(t, err, "argument 1")
	_, err = db.QueryN(ctx(t), -1, "SELECT 1")
	require.ErrorContains(t, err, "maxRows")
	_, err = db.Exec(ctx(t), "INSERT INTO t VALUES (?, ?)", 1, make(chan int))
	require.ErrorContains(t, err, "argument 2")
	_, err = db.Batch(ctx(t))
	require.ErrorContains(t, err, "no statements")
	_, err = db.Batch(ctx(t), azora.Stmt("a"), azora.Stmt("b", []int{1}))
	require.ErrorContains(t, err, "statement 2")
	_, err = db.ExecMutation(ctx(t), "", "DELETE FROM t")
	require.ErrorContains(t, err, "empty mutation id")
	_, err = db.BatchMutation(ctx(t), "", azora.Stmt("DELETE FROM t"))
	require.ErrorContains(t, err, "empty mutation id")
	_, err = db.ExecMutation(ctx(t), string(make([]byte, azora.MaxMutationIDLen+1)), "DELETE FROM t")
	require.ErrorContains(t, err, "at most 128")
	_, _, err = db.SignURL(ctx(t), "obj", -time.Second)
	require.ErrorContains(t, err, "ttl")
	b := db.NewBatch()
	require.Zero(t, b.Len())
	_, err = b.Run(ctx(t))
	require.ErrorContains(t, err, "no statements")
	res, err := b.Add("x").Add("y").Run(ctx(t))
	require.NoError(t, err)
	require.Equal(t, 2, b.Len())
	require.Equal(t, int64(2), res.RowsAffected)

	_, err = db.Subscribe(ctx(t), azora.Spec{Kind: 9})
	require.ErrorContains(t, err, "kind")
	_, err = db.Subscribe(ctx(t), azora.Spec{Kind: azora.KindTable, Ops: 99})
	require.ErrorContains(t, err, "ops")
	_, err = db.Subscribe(ctx(t), azora.RowSpec("t", struct{}{}))
	require.ErrorContains(t, err, "key")
	_, err = db.Subscribe(ctx(t), azora.TableSpec("t", azora.Eq("a", make(chan int))))
	require.Error(t, err)
}
