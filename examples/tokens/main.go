// What an app on a user's device does: connect to the same public URL
// as everyone else, with a short-lived token minted by your backend.
// This token is pinned to customer 42's orders, so the server filters
// every subscription and refuses SQL. The URL says where; the token
// alone decides what.
//
//	go run ./examples/tokens
package main

import (
	"context"
	"fmt"
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

	// Your backend returns a fresh token for the signed-in user; here the
	// dev engine minted one (`azora realtime token create shop --sub
	// user:42 --table orders --filter "orders: customer = 42"`). The
	// function runs on every (re)connect and when the server says the
	// token expired, so the connection outlives any single token.
	fetchToken := func(context.Context) (string, error) { return demo.Env("AZORA_TOKEN_CUSTOMER42"), nil }

	app, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithCredentialFunc(fetchToken))
	demo.Check(err)
	defer app.Close()
	if w, ok := app.Welcome(); ok {
		fmt.Printf("connected as %s, scopes: %v\n", w.Principal, w.Scopes)
	}

	mine, err := azora.Live[Order](ctx, app, azora.TableSpec("orders"))
	demo.Check(err)
	defer mine.Close()
	demo.Check(mine.Wait(ctx))
	all42 := true
	for _, o := range mine.Rows() {
		all42 = all42 && o.Customer == 42
	}
	fmt.Printf("this user sees %d orders, all theirs: %v\n", mine.Len(), all42)

	got := make(chan Order, 4)
	stop := mine.OnUpdate(func(u azora.LiveUpdate[Order]) {
		if u.Reset {
			return // the rows the view already holds, counted above
		}
		for _, c := range u.Changes {
			if c.Op == azora.OpInsert {
				got <- c.New
			}
		}
	})
	defer stop()

	// The backend (same URL, its own token: an API key) writes orders for
	// two customers; only customer 42's reaches the app.
	backend, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	defer backend.Close()
	_, err = backend.Batch(ctx,
		azora.Stmt("INSERT INTO orders(customer, status, total, note) VALUES (7, 'open', 15, 'not yours')"),
		azora.Stmt("INSERT INTO orders(customer, status, total, note) VALUES (42, 'open', 15, 'yours')"),
	)
	demo.Check(err)
	select {
	case o := <-got:
		fmt.Printf("  received order #%d for customer %d: %q\n", o.ID, o.Customer, o.Note)
	case <-ctx.Done():
		demo.Check(ctx.Err())
	}
	select {
	case o := <-got:
		fmt.Printf("  UNEXPECTED: also received order #%d for customer %d\n", o.ID, o.Customer)
	case <-time.After(300 * time.Millisecond):
		fmt.Println("  nothing else arrived: the other customer's order stayed on the server")
	}

	// SQL is refused for a token limited to tables or filters.
	_, err = app.Query(ctx, "SELECT * FROM orders")
	fmt.Printf("SQL with this token: permission denied = %v\n", azora.IsPermissionDenied(err))
}
