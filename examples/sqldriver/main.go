// database/sql: the standard library's interface over an Azora
// connection, for code and libraries written against it. Transactions
// are buffered and sent at Commit as one all-or-nothing batch.
//
//	go run ./examples/sqldriver
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/azorasql"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// One azora.DB, shared: database/sql for queries, the SDK itself for
	// what database/sql has no notion of (live tables, storage).
	adb, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithAPIKey(demo.Env("AZORA_API_KEY")))
	demo.Check(err)
	defer adb.Close()
	db := azorasql.OpenDB(adb, azorasql.WithBatchTransactions())
	defer db.Close()
	// sql.Open("azora", url+"&api_key=...&tx=batch") does the same from a DSN.

	rows, err := db.QueryContext(ctx, "SELECT id, status, total, note FROM orders WHERE customer = ? ORDER BY id", 42)
	demo.Check(err)
	for rows.Next() {
		var (
			id     int64
			status string
			total  float64
			note   sql.NullString
		)
		demo.Check(rows.Scan(&id, &status, &total, &note))
		fmt.Printf("  #%d %-8s %8.2f  %s\n", id, status, total, note.String)
	}
	demo.Check(rows.Err())
	rows.Close()

	// Prepared statements live on the client; each Exec is one request.
	ins, err := db.PrepareContext(ctx, "INSERT INTO orders(customer, status, total, note) VALUES (?, 'open', ?, ?)")
	demo.Check(err)
	defer ins.Close()
	res, err := ins.ExecContext(ctx, 42, 9.90, "Screen wipes")
	demo.Check(err)
	id, _ := res.LastInsertId()
	fmt.Println("inserted order", id)

	// A transaction: its statements are sent together at Commit and apply
	// all or nothing, exactly once.
	tx, err := db.BeginTx(ctx, nil)
	demo.Check(err)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 100 WHERE id = 'alice'")
	demo.Check(err)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 100 WHERE id = 'bob'")
	demo.Check(err)
	demo.Check(tx.Commit())
	fmt.Println("transfer committed")

	// An overdraft violates the CHECK constraint: Commit reports it and
	// nothing changed.
	tx, err = db.BeginTx(ctx, nil)
	demo.Check(err)
	_, _ = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 10000000 WHERE id = 'carol'")
	_, _ = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 10000000 WHERE id = 'alice'")
	err = tx.Commit()
	var ae *azora.Error
	fmt.Printf("overdraft refused at commit: %v (server code %q)\n", err != nil, func() string {
		if errors.As(err, &ae) {
			return ae.Code
		}
		return ""
	}())

	var total int64
	demo.Check(db.QueryRowContext(ctx, "SELECT sum(balance) FROM accounts").Scan(&total))
	fmt.Println("money in the system is unchanged:", total)
}
