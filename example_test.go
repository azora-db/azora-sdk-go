package azora_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	azora "github.com/azora-db/azora-go"
)

type exampleOrder struct {
	ID       int64
	Customer int64
	Status   string
	Total    float64
}

func ExampleConnect() {
	ctx := context.Background()
	// The URL is public; the token is the secret.
	db, err := azora.Connect(ctx, "https://db.example.com/shop?key=<server key>",
		azora.WithToken(os.Getenv("AZORA_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	orders, err := azora.All[exampleOrder](ctx, db, "SELECT * FROM orders WHERE status = ?", "open")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(orders), "open orders")
}

func ExampleConnectEnv() {
	// AZORA_URL and AZORA_TOKEN, as printed by `azora connect shop --key backend`.
	db, err := azora.ConnectEnv(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func ExampleDB_Batch() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	// One transaction: both updates apply, or neither.
	res, err := db.Batch(ctx,
		azora.Stmt("UPDATE accounts SET balance = balance - ? WHERE id = ?", 2500, "alice"),
		azora.Stmt("UPDATE accounts SET balance = balance + ? WHERE id = ?", 2500, "bob"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.RowsAffected, "rows changed")
}

func ExampleDB_ExecMutation() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	// Store the id with the queued work before sending it. Sending the
	// write again with the same id, even from another process after a
	// crash, does not apply it twice.
	id := azora.NewMutationID()
	res, err := db.ExecMutation(ctx, id, "INSERT INTO payments(order_id, amount) VALUES (?, ?)", 42, 129.90)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("replayed:", res.Replayed)
}

func ExampleCall() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	type CheckoutArgs struct {
		CartID string `json:"cartId"`
	}
	type CheckoutResult struct {
		OrderID int64 `json:"orderId"`
	}
	// checkout is a mutation unit published into the database: it runs
	// there, in one transaction, exactly once even if the call is resent.
	res, err := azora.Call[CheckoutResult](ctx, db, "checkout", CheckoutArgs{CartID: "c1"})
	var ue *azora.UnitError
	switch {
	case errors.As(err, &ue) && ue.Code == azora.UnitCodeNotFound:
		fmt.Println("no such cart:", ue.Message)
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println("order", res.OrderID)
	}
}

func ExampleLive() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	open, err := azora.Live[exampleOrder](ctx, db, azora.TableSpec("orders", azora.Eq("status", "open")))
	if err != nil {
		log.Fatal(err)
	}
	defer open.Close()
	if err := open.Wait(ctx); err != nil { // the first snapshot
		log.Fatal(err)
	}
	for u, err := range open.Updates(ctx) {
		if err != nil {
			break
		}
		for _, c := range u.Changes {
			fmt.Println(c.Op, c.Key)
		}
		fmt.Println(open.Len(), "open orders now")
	}
}

func ExampleDB_Watch() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	w, err := db.Watch(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	for n, err := range w.Notices(ctx) {
		if err != nil {
			break
		}
		if n.Has("orders") {
			fmt.Println("orders changed: run the query again")
		}
	}
}

func ExampleStorage_Upload() {
	var db *azora.DB // from azora.Connect
	ctx := context.Background()

	f, err := os.Open("invoice.pdf")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	obj, err := db.Storage().Upload(ctx, f, azora.UploadOptions{Path: "invoices/2026-0042.pdf"})
	if err != nil {
		log.Fatal(err)
	}
	// A FILE column holds the object's id.
	_, err = db.Exec(ctx, "INSERT INTO documents(title, file) VALUES (?, ?)", "Invoice 2026-0042", obj.ID)
	if err != nil {
		log.Fatal(err)
	}
}
