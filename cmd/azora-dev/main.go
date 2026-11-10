// Command azora-dev boots a local Azora engine with a seeded "shop"
// database for the examples, publishes examples/units/units.js (when the
// engine supports units), writes examples/.env and serves until Ctrl+C.
//
//	go run ./cmd/azora-dev
//
// It needs the engine binary: $AZORA_BIN, or ../azora-engine/bin/azora
// (`make build` in the engine repository).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/devengine"
)

const db = "shop"

var schema = []string{
	`CREATE TABLE orders(id INTEGER PRIMARY KEY, customer INTEGER NOT NULL, status TEXT NOT NULL, total REAL NOT NULL, note TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')))`,
	`CREATE INDEX orders_status ON orders(status)`,
	`CREATE TABLE accounts(id TEXT PRIMARY KEY, owner TEXT NOT NULL, balance INTEGER NOT NULL CHECK (balance >= 0))`,
	`CREATE TABLE transfers(id INTEGER PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL, amount INTEGER NOT NULL, at TEXT NOT NULL DEFAULT (datetime('now')))`,
	`CREATE TABLE documents(id INTEGER PRIMARY KEY, title TEXT NOT NULL, file FILE)`,
	`CREATE TABLE carts(id TEXT PRIMARY KEY, owner INTEGER NOT NULL, total REAL NOT NULL, note TEXT)`,
	`INSERT INTO accounts(id, owner, balance) VALUES ('alice', 'Alice Mbeki', 50000), ('bob', 'Bob Okafor', 12000), ('carol', 'Carol Nyirenda', 8000)`,
	`INSERT INTO orders(customer, status, total, note) VALUES (42, 'open', 129.90, 'Standing desk frame'), (42, 'paid', 18.50, 'Cable tray'), (7, 'open', 349.00, 'Monitor arm, dual'), (7, 'shipped', 59.99, 'Desk mat'), (13, 'open', 24.00, 'Cable clips')`,
	`INSERT INTO carts(id, owner, total, note) VALUES ('c1', 42, 89.00, 'Monitor light bar'), ('c2', 7, 12.50, 'Velcro ties')`,
}

func main() {
	envFile := flag.String("env", filepath.Join("examples", ".env"), "where to write the connection settings")
	dir := flag.String("dir", "", "data directory (default: a temporary one, removed on exit)")
	units := flag.String("units", filepath.Join("examples", "units", "units.js"), "units bundle to publish")
	flag.Parse()
	if err := run(*envFile, *dir, *units); err != nil {
		fmt.Fprintln(os.Stderr, "azora-dev:", err)
		os.Exit(1)
	}
}

func run(envFile, dir, unitsFile string) error {
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

	// Units are published through the running server (the SDK), with the
	// key that has the ddl scope.
	unitsNote := publishUnits(e, key, unitsFile)

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
  Units      %s

Wrote %s (AZORA_URL and a private AZORA_TOKEN). Run an example in
another terminal:

  go run ./examples/quickstart

Ctrl+C stops the engine.
`, db, e.ConnectURL(db), e.URL(db), dir, unitsNote, envFile)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	fmt.Println("\nstopping the engine")
	return nil
}

// publishUnits publishes the units bundle and says how that went: the
// examples that do not use units work either way.
func publishUnits(e *devengine.Engine, key, file string) string {
	bundle, err := os.ReadFile(file)
	if err != nil {
		return "not published: " + err.Error()
	}
	if ok, err := e.Supports("units"); err != nil || !ok {
		return "not published: this engine does not support units"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d, err := e.DeployUnits(ctx, db, key, azora.DeployRequest{Bundle: string(bundle), Note: "azora-dev: " + file})
	if err != nil {
		return "not published: " + err.Error()
	}
	names := make([]string, len(d.Units))
	for i, u := range d.Units {
		names[i] = u.Name
	}
	return fmt.Sprintf("%s published as v%d (%s)", file, d.Version, strings.Join(names, ", "))
}
