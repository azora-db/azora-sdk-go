// Exactly-once writes across crashes: a money transfer queued in a local
// outbox with its mutation id, replayed after a simulated crash. The
// balances move once, however many times the transfer is sent.
//
//	go run ./examples/exactly-once
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

// Queued is a transfer waiting in the outbox, with the id that makes
// sending it twice harmless.
type Queued struct {
	MutationID string `json:"mutation_id"`
	Src        string `json:"src"`
	Dst        string `json:"dst"`
	Amount     int64  `json:"amount"`
}

var outbox = filepath.Join(os.TempDir(), "azora-go-example-outbox.json")

// send moves the money in one transaction. The CHECK (balance >= 0) on
// accounts makes an overdraft fail the whole batch.
func send(ctx context.Context, db *azora.DB, t Queued) (azora.Result, error) {
	return db.BatchMutation(ctx, t.MutationID,
		azora.Stmt("UPDATE accounts SET balance = balance - ? WHERE id = ?", t.Amount, t.Src),
		azora.Stmt("UPDATE accounts SET balance = balance + ? WHERE id = ?", t.Amount, t.Dst),
		azora.Stmt("INSERT INTO transfers(src, dst, amount) VALUES (?, ?, ?)", t.Src, t.Dst, t.Amount),
	)
}

func balances(ctx context.Context, db *azora.DB) string {
	type account struct {
		ID      string
		Balance int64
	}
	rows, err := azora.All[account](ctx, db, "SELECT id, balance FROM accounts ORDER BY id")
	demo.Check(err)
	parts := make([]string, len(rows))
	for i, a := range rows {
		parts[i] = fmt.Sprintf("%s=%d", a.ID, a.Balance)
	}
	return strings.Join(parts, "  ")
}

func connect(ctx context.Context) *azora.DB {
	db, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	return db
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := connect(ctx)
	fmt.Println("before:  ", balances(ctx, db))

	// 1. Persist the intent with its id BEFORE sending it.
	queued := []Queued{{MutationID: azora.NewMutationID(), Src: "alice", Dst: "bob", Amount: 2500}}
	data, _ := json.Marshal(queued)
	demo.Check(os.WriteFile(outbox, data, 0o600))

	// 2. Send it, then "crash" before the outbox is cleared: the process
	//    no longer knows whether the transfer went through.
	_, err := send(ctx, db, queued[0])
	demo.Check(err)
	db.Close()
	fmt.Println("crashed after sending; the outbox still holds the transfer")

	// 3. On restart, replay the outbox with the stored ids.
	db = connect(ctx)
	defer db.Close()
	fmt.Println("replaying the outbox:")
	data, err = os.ReadFile(outbox)
	demo.Check(err)
	var pending []Queued
	demo.Check(json.Unmarshal(data, &pending))
	for _, t := range pending {
		res, err := send(ctx, db, t)
		demo.Check(err)
		how := "applied"
		if res.Replayed {
			how = "already applied, answered from the first run"
		}
		fmt.Printf("  %s -> %s %d: %s\n", t.Src, t.Dst, t.Amount, how)
	}
	demo.Check(os.Remove(outbox))
	fmt.Println("after:   ", balances(ctx, db))

	// An overdraft changes nothing: the batch is all or nothing.
	_, err = send(ctx, db, Queued{MutationID: azora.NewMutationID(), Src: "carol", Dst: "alice", Amount: 1_000_000})
	fmt.Println("overdraft refused:", err)
	fmt.Println("still:   ", balances(ctx, db))
}
