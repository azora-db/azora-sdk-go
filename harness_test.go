package azora_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/devengine"
)

// The integration tests run against the real engine binary (see
// devengine.FindBinary). One server is shared by the tests that do not
// restart it; it is started on first use and stopped by TestMain.

const testDB = "shop"

// tableSchema is the layout of every pre-published test table.
const tableSchema = "(id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, total REAL, note BLOB)"

// publishedTables are created and published before the shared server
// starts (publishing needs the command line, which cannot run while the
// server holds the data root). Each test uses its own.
var publishedTables = []string{"orders", "t_table", "t_row", "t_db", "t_filter", "t_resume", "t_live", "t_live2", "t_verify",
	"t_token", "t_ops", "t_conc", "t_schema", "t_slow", "t_bench", "t_sqlsub"}

// fixture is a running engine with its credentials.
type fixture struct {
	*devengine.Engine
	Key        string // read, write, ddl
	ReadKey    string // read
	WriteKey   string // write
	Token      string // read, unrestricted
	Restricted string // read, table t_token, filter customer_id = 1
	ShortToken string // read, expires a few seconds after the server starts
}

var shared struct {
	once sync.Once
	f    *fixture
	dir  string
	err  error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if shared.f != nil {
		shared.f.Stop()
	}
	if shared.dir != "" {
		os.RemoveAll(shared.dir)
	}
	os.Exit(code)
}

// setupEngine creates the standard database, tables and credentials on a
// stopped engine.
func setupEngine(e *devengine.Engine) (*fixture, error) {
	f := &fixture{Engine: e}
	if err := e.CreateDB(testDB); err != nil {
		return nil, err
	}
	for _, tb := range publishedTables {
		if err := e.Exec(testDB, "CREATE TABLE "+tb+" "+tableSchema); err != nil {
			return nil, err
		}
	}
	if err := e.Exec(testDB, "CREATE TABLE plain (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		return nil, err
	}
	if err := e.Publish(testDB, append(append([]string(nil), publishedTables...), "files")...); err != nil {
		return nil, err
	}
	var err error
	if f.Key, err = e.CreateKey(testDB, "app", "read", "write", "ddl"); err != nil {
		return nil, err
	}
	if f.ReadKey, err = e.CreateKey(testDB, "reader", "read"); err != nil {
		return nil, err
	}
	if f.WriteKey, err = e.CreateKey(testDB, "writer", "write"); err != nil {
		return nil, err
	}
	if f.Token, err = e.CreateToken(testDB, devengine.Token{Sub: "user:1", Scopes: []string{"read"}}); err != nil {
		return nil, err
	}
	if f.Restricted, err = e.CreateToken(testDB, devengine.Token{Sub: "user:1", Scopes: []string{"read"},
		Tables: []string{"t_token"}, Filters: []string{"t_token: customer_id = 1"}}); err != nil {
		return nil, err
	}
	return f, nil
}

// newEngine starts a dedicated engine for tests that restart the server
// or change its configuration. It is stopped when the test ends.
func newEngine(t testing.TB, cfg map[string]map[string]any) *fixture {
	t.Helper()
	bin, err := devengine.FindBinary()
	if err != nil {
		t.Skipf("integration test skipped: %v", err)
	}
	e, err := devengine.New(devengine.Options{Bin: bin, Dir: t.TempDir(), Config: cfg})
	require.NoError(t, err)
	f, err := setupEngine(e)
	require.NoError(t, err)
	require.NoError(t, e.Serve())
	t.Cleanup(func() { e.Stop() })
	return f
}

// engine returns the shared engine, starting it on first use.
func engine(t testing.TB) *fixture {
	t.Helper()
	bin, err := devengine.FindBinary()
	if err != nil {
		t.Skipf("integration test skipped: %v", err)
	}
	shared.once.Do(func() {
		dir, err := os.MkdirTemp("", "azora-go-test-")
		if err != nil {
			shared.err = err
			return
		}
		shared.dir = dir
		// The default request rate (1000/s per connection) would slow the
		// concurrency tests down; the rate limit has its own test.
		e, err := devengine.New(devengine.Options{Bin: bin, Dir: dir,
			Config: map[string]map[string]any{"realtime": {"message_rate": 1e6, "message_burst": 1000000}}})
		if err != nil {
			shared.err = err
			return
		}
		f, err := setupEngine(e)
		if err != nil {
			shared.err = err
			return
		}
		if f.ShortToken, err = e.CreateToken(testDB, devengine.Token{Sub: "user:short", Scopes: []string{"read"}, TTL: 4 * time.Second}); err != nil {
			shared.err = err
			return
		}
		if err := e.Serve(); err != nil {
			shared.err = err
			return
		}
		shared.f = f
	})
	require.NoError(t, shared.err, "start the shared engine")
	return shared.f
}

// connect opens a DB over TCP with the full-access key.
func (f *fixture) connect(t testing.TB, opts ...azora.Option) *azora.DB {
	t.Helper()
	return f.connectURL(t, f.URL(testDB), append([]azora.Option{azora.WithAPIKey(f.Key)}, opts...)...)
}

func (f *fixture) connectURL(t testing.TB, url string, opts ...azora.Option) *azora.DB {
	t.Helper()
	db, err := azora.Connect(ctx(t), url, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// ctx returns a context that ends with the test, or after 30 seconds.
func ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// mustExec runs a statement and fails the test on error.
func mustExec(t testing.TB, db *azora.DB, sql string, args ...any) azora.Result {
	t.Helper()
	res, err := db.Exec(ctx(t), sql, args...)
	require.NoError(t, err, sql)
	return res
}

// clearTable empties a test table when the test starts and ends.
func clearTable(t testing.TB, db *azora.DB, table string) {
	t.Helper()
	mustExec(t, db, "DELETE FROM "+table)
}

// next returns the subscription's next update or fails after d.
func next(t testing.TB, sub *azora.Subscription, d time.Duration) azora.Update {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	u, err := sub.Next(c)
	require.NoError(t, err)
	return u
}

// eventually polls cond until it holds or d passes.
func eventually(t testing.TB, d time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: %s", d, fmt.Sprintf(msg, args...))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// sdkGoroutines counts goroutines running the SDK's code.
func sdkGoroutines() (int, string) {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	var stacks []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "azora-go.(*") && !strings.Contains(g, "azora-go_test.") && !strings.Contains(g, "devengine") {
			n++
			stacks = append(stacks, g)
		}
	}
	return n, strings.Join(stacks, "\n\n")
}

// requireNoLeaks fails if SDK goroutines remain shortly after a Close.
func requireNoLeaks(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, stacks := sdkGoroutines()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d SDK goroutines still running:\n%s", n, stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stringsReader is strings.NewReader, for call sites that read better
// without the package name.
func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }

// httpGet fetches a URL without credentials and returns the body of a
// 200 answer.
func httpGet(t testing.TB, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	return string(body)
}
