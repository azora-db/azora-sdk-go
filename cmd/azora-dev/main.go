// Command azora-dev boots a local Azora engine with a seeded "shop"
// database for the examples, writes examples/.env and serves until
// Ctrl+C.
//
//	go run ./cmd/azora-dev
//
// It needs the engine binary: $AZORA_BIN, or ../azora-engine/bin/azora
// (`make build` in the engine repository).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/azora-db/azora-go/internal/devengine"
)

const db = "shop"

var schema = []string{
	`CREATE TABLE orders(id INTEGER PRIMARY KEY, customer INTEGER NOT NULL, status TEXT NOT NULL, total REAL NOT NULL, note TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')))`,
	`CREATE INDEX orders_status ON orders(status)`,
	`CREATE TABLE accounts(id TEXT PRIMARY KEY, owner TEXT NOT NULL, balance INTEGER NOT NULL CHECK (balance >= 0))`,
	`CREATE TABLE transfers(id INTEGER PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL, amount INTEGER NOT NULL, at TEXT NOT NULL DEFAULT (datetime('now')))`,
	`CREATE TABLE documents(id INTEGER PRIMARY KEY, title TEXT NOT NULL, file FILE)`,
	`INSERT INTO accounts(id, owner, balance) VALUES ('alice', 'Alice Mbeki', 50000), ('bob', 'Bob Okafor', 12000), ('carol', 'Carol Nyirenda', 8000)`,
	`INSERT INTO orders(customer, status, total, note) VALUES (42, 'open', 129.90, 'Standing desk frame'), (42, 'paid', 18.50, 'Cable tray'), (7, 'open', 349.00, 'Monitor arm, dual'), (7, 'shipped', 59.99, 'Desk mat'), (13, 'open', 24.00, 'Cable clips')`,
}

func main() {
	envFile := flag.String("env", filepath.Join("examples", ".env"), "where to write the connection settings")
	dir := flag.String("dir", "", "data directory (default: a temporary one, removed on exit)")
	flag.Parse()
	if err := run(*envFile, *dir); err != nil {
		fmt.Fprintln(os.Stderr, "azora-dev:", err)
		os.Exit(1)
	}
}

func run(envFile, dir string) error {
	if dir == "" {
		tmp, err := os.MkdirTemp("", "azora-go-dev-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		dir = tmp
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// The examples are not rate-limit demos: lift the per-connection limit.
	e, err := devengine.New(devengine.Options{Dir: dir, Config: map[string]map[string]any{"realtime": {"message_rate": 100000, "message_burst": 100000}}})
	if err != nil {
		return err
	}
	// Everything the command line does happens before the server takes
	// the data root.
	if err := e.CreateDB(db); err != nil {
		return err
	}
	if err := e.Exec(db, schema...); err != nil {
		return err
	}
	if err := e.Publish(db, "orders", "accounts", "transfers", "files"); err != nil {
		return err
	}
	key, err := e.CreateKey(db, "examples", "read", "write", "ddl")
	if err != nil {
		return err
	}
	// What a backend would mint for a signed-in customer: their own
	// orders, read-only.
	customer, err := e.CreateToken(db, devengine.Token{Sub: "user:42", Scopes: []string{"read"}, TTL: 12 * time.Hour,
		Tables: []string{"orders"}, Filters: []string{"orders: customer = 42"}})
	if err != nil {
		return err
	}
	if err := e.Serve(); err != nil {
		return fmt.Errorf("%w\n%s", err, e.Stderr())
	}
	defer e.Stop()

	// Two values are all a client needs: the public URL and a private
	// token. The second token and the raw TCP address are for the examples
	// that show those.
	env := strings.Join([]string{
		"AZORA_URL=" + e.ConnectURL(db),
		"AZORA_TOKEN=" + key,
		"AZORA_TOKEN_CUSTOMER42=" + customer,
		"AZORA_TCP_URL=" + e.URL(db),
	}, "\n") + "\n"
	if err := os.MkdirAll(filepath.Dir(envFile), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(envFile, []byte(env), 0o600); err != nil {
		return err
	}
	fmt.Printf(`Azora engine is serving the %q database.

  AZORA_URL  %s
  Raw TCP    %s
  Data root  %s

Wrote %s (AZORA_URL and a private AZORA_TOKEN). Run an example in
another terminal:

  go run ./examples/quickstart

Ctrl+C stops the engine.
`, db, e.ConnectURL(db), e.URL(db), dir, envFile)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	fmt.Println("\nstopping the engine")
	return nil
}
