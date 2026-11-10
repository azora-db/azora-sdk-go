// A live view of the open orders: the table is kept current by the
// server's change stream, and every change prints as it commits. A second
// connection plays the part of other users writing.
//
//	go run ./examples/live-orders
package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

type Order struct {
	ID       int64
	Customer int64
	Status   string
	Total    float64
	Note     string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url, key := demo.Env("AZORA_URL"), azora.WithAPIKey(demo.Env("AZORA_API_KEY"))

	db, err := azora.Connect(ctx, url, key)
	demo.Check(err)
	defer db.Close()

	// The server evaluates the filter: only open orders travel, and an
	// order that stops being open leaves the view as a delete.
	open, err := azora.Live[Order](ctx, db, azora.TableSpec("orders", azora.Eq("status", "open")))
	demo.Check(err)
	defer open.Close()
	demo.Check(open.Wait(ctx)) // the first snapshot
	print(open)

	// Every consistent step (one database transaction) arrives as one
	// update. A new listener is first handed the rows the view already
	// holds (Reset), so nothing can slip between reading and listening;
	// they were just printed, so that call is skipped here.
	seen := make(chan struct{}, 16)
	stop := open.OnUpdate(func(u azora.LiveUpdate[Order]) {
		if u.Reset {
			return
		}
		for _, c := range u.Changes {
			switch c.Op {
			case azora.OpInsert:
				fmt.Printf("  + order %d entered the view: %s (%.2f)\n", c.New.ID, c.New.Note, c.New.Total)
			case azora.OpUpdate:
				fmt.Printf("  ~ order %d: total %.2f -> %.2f\n", c.New.ID, c.Old.Total, c.New.Total)
			case azora.OpDelete:
				fmt.Printf("  - order %d left the view\n", c.Old.ID)
			}
		}
		seen <- struct{}{}
	})
	defer stop()

	// Someone else writes.
	other, err := azora.Connect(ctx, url, key)
	demo.Check(err)
	defer other.Close()
	step := func(what string, write func() error) {
		fmt.Println(what)
		demo.Check(write())
		select {
		case <-seen:
		case <-ctx.Done():
			demo.Check(ctx.Err())
		}
	}
	var id int64
	step("another client creates an order:", func() error {
		res, err := other.Exec(ctx, "INSERT INTO orders(customer, status, total, note) VALUES (99, 'open', 42.00, 'Laptop stand')")
		id = res.LastInsertID
		return err
	})
	step("...changes its total:", func() error {
		_, err := other.Exec(ctx, "UPDATE orders SET total = 39.50 WHERE id = ?", id)
		return err
	})
	step("...and marks it paid, which takes it out of the filter:", func() error {
		_, err := other.Exec(ctx, "UPDATE orders SET status = 'paid' WHERE id = ?", id)
		return err
	})
	print(open)

	// The view can prove it matches the server.
	v, err := open.Verify(ctx)
	demo.Check(err)
	fmt.Printf("checksum verified against the server: match=%v over %d rows\n", v.Match, v.ServerRows)
}

func print(open *azora.LiveTable[Order]) {
	rows := open.Rows()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Total > rows[j].Total })
	fmt.Printf("%d open orders:\n", len(rows))
	for _, o := range rows {
		fmt.Printf("  #%d  customer %-3d %8.2f  %s\n", o.ID, o.Customer, o.Total, o.Note)
	}
}
