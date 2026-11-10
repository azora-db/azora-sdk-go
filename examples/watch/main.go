// Watching tables: the lightest way to stay current. The server says
// which tables a transaction wrote; the program runs its query again.
// Nothing needs to be published for row data, and any SQL works.
//
//	go run ./examples/watch
package main

import (
	"context"
	"fmt"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

type StatusTotal struct {
	Status string
	Orders int64
	Total  float64
}

func report(ctx context.Context, db *azora.DB) {
	rows, err := azora.All[StatusTotal](ctx, db, "SELECT status, count(*) AS orders, round(sum(total), 2) AS total FROM orders GROUP BY status ORDER BY status")
	demo.Check(err)
	for _, r := range rows {
		fmt.Printf("  %-8s %3d orders  %9.2f\n", r.Status, r.Orders, r.Total)
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	defer db.Close()

	w, err := db.Watch(ctx, "orders")
	demo.Check(err)
	defer w.Close()
	fmt.Println("totals by status:")
	report(ctx, db)

	// Writes from elsewhere in the program (or any other client).
	go func() {
		for i, q := range []string{
			"INSERT INTO orders(customer, status, total, note) VALUES (5, 'open', 12.00, 'Pen holder')",
			"UPDATE orders SET status = 'shipped' WHERE status = 'paid'",
			"UPDATE accounts SET balance = balance WHERE id = 'alice'", // another table: no notice
			"DELETE FROM orders WHERE note = 'Pen holder'",
		} {
			time.Sleep(300 * time.Millisecond)
			if _, err := db.Exec(ctx, q); err != nil {
				fmt.Println("write", i, "failed:", err)
			}
		}
	}()

	// Three writes touch orders, so three notices arrive.
	for i := 1; i <= 3; i++ {
		n, err := w.Next(ctx)
		demo.Check(err)
		fmt.Printf("notice %d: %v changed (everything: %v)\n", i, n.Tables, n.Everything)
		report(ctx, db)
	}
}
