// Quickstart: connect, read into structs, write, and run several
// statements atomically.
//
//	go run ./cmd/azora-dev          # terminal 1
//	go run ./examples/quickstart    # terminal 2
package main

import (
	"context"
	"fmt"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

// Order maps columns to fields by name; `db` tags override the name.
type Order struct {
	ID       int64
	Customer int64
	Status   string
	Total    float64
	Note     *string // NULL becomes nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two values: the database's public URL and a private token. With
	// both in the environment this is azora.ConnectEnv(ctx).
	db, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	defer db.Close()
	if w, ok := db.Welcome(); ok {
		fmt.Printf("connected as %s (%s, scopes: %v)\n", w.Principal, w.Mode, w.Scopes)
	}

	// Reads decode straight into structs.
	open, err := azora.All[Order](ctx, db, "SELECT * FROM orders WHERE status = ? ORDER BY total DESC", "open")
	demo.Check(err)
	fmt.Printf("%d open orders, largest: %s (%.2f)\n", len(open), *open[0].Note, open[0].Total)

	// One value.
	spent, err := azora.Scalar[float64](ctx, db, "SELECT coalesce(sum(total), 0) FROM orders WHERE customer = ?", 42)
	demo.Check(err)
	fmt.Printf("customer 42 has spent %.2f\n", spent)

	// A write applies exactly once, even if the connection drops while it
	// is in flight.
	res, err := db.Exec(ctx, "INSERT INTO orders(customer, status, total, note) VALUES (?, ?, ?, ?)", 42, "open", 75.25, "Footrest")
	demo.Check(err)
	fmt.Printf("created order %d\n", res.LastInsertID)

	// Batch runs statements in one transaction: all of them apply, or none.
	res, err = db.Batch(ctx,
		azora.Stmt("UPDATE orders SET status = 'paid' WHERE id = ?", res.LastInsertID),
		azora.Stmt("UPDATE orders SET note = note || ' (paid)' WHERE id = ?", res.LastInsertID),
	)
	demo.Check(err)
	fmt.Printf("batch changed %d rows\n", res.RowsAffected)

	// A failing statement rolls the whole batch back.
	_, err = db.Batch(ctx, azora.Stmt("DELETE FROM orders"), azora.Stmt("INSERT INTO no_such_table VALUES (1)"))
	fmt.Printf("batch refused: %v (invalid: %v)\n", err, azora.IsInvalid(err))
	n, err := azora.Scalar[int64](ctx, db, "SELECT count(*) FROM orders")
	demo.Check(err)
	fmt.Printf("orders are still there: %d\n", n)
}
