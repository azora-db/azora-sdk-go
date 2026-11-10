// Units: functions published into the database (units.js, next to this
// file) and called by name like an API. checkout turns a cart into an
// order in one transaction, exactly once however often it is sent;
// myOrders lets a customer's restricted token read its orders even though
// that token cannot run SQL.
//
//	go run ./cmd/azora-dev    # publishes units.js when it seeds "shop"
//	go run ./examples/units
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

type CheckoutArgs struct {
	CartID string `json:"cartId"`
}

type CheckoutResult struct {
	OrderID    int64 `json:"orderId"`
	OpenOrders int   `json:"openOrders"`
}

type Order struct {
	ID       int64   `json:"id"`
	Customer int64   `json:"customer"`
	Status   string  `json:"status"`
	Total    float64 `json:"total"`
	Note     string  `json:"note"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backend, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	defer backend.Close()

	list, err := backend.Units().List(ctx) // needs the ddl scope, which this key has
	demo.Check(err)
	fmt.Printf("deploy v%d:", list.Version)
	for _, u := range list.Units {
		fmt.Printf("  %s (%s)", u.Name, u.Kind)
	}
	fmt.Println()

	// A cart for customer 42 (a fresh one on every run).
	cart := "cart-" + azora.NewMutationID()[2:10]
	_, err = backend.Exec(ctx, "INSERT INTO carts(id, owner, total, note) VALUES (?, 42, 75, 'Desk lamp')", cart)
	demo.Check(err)

	// A typed call. Keeping the mutation id lets the call be sent again
	// later (after a crash, say) without placing a second order.
	mid := azora.NewMutationID()
	res, err := azora.Call[CheckoutResult](ctx, backend, "checkout", CheckoutArgs{CartID: cart}, azora.MutationID(mid))
	demo.Check(err)
	fmt.Printf("checkout %s: order #%d, customer 42 has %d open orders\n", cart, res.OrderID, res.OpenOrders)

	// The same call again: the cart is gone, yet the answer is the first
	// one, read back from the database; the unit did not run again.
	var replayed bool
	again, err := azora.Call[CheckoutResult](ctx, backend, "checkout", CheckoutArgs{CartID: cart},
		azora.MutationID(mid), azora.Replayed(&replayed))
	demo.Check(err)
	fmt.Printf("sent again with the same mutation id: order #%d, replayed = %v\n", again.OrderID, replayed)

	// The error the unit threw, with its code and data.
	_, err = azora.Call[CheckoutResult](ctx, backend, "checkout", CheckoutArgs{CartID: "no-such-cart"})
	var ue *azora.UnitError
	if errors.As(err, &ue) {
		fmt.Printf("checkout of a missing cart: %s %q, data %s\n", ue.Code, ue.Message, ue.Data)
	} else {
		demo.Check(err)
	}

	// Errors around the unit are server errors: an internal helper cannot
	// be called by clients.
	err = backend.Call(ctx, "openOrders", map[string]int{"customer": 42}, nil)
	fmt.Printf("calling the internal openOrders: permission denied = %v\n", azora.IsPermissionDenied(err))

	// A customer's token, limited to its own orders: SQL is refused, the
	// unit is not, and it answers from ctx.auth.subject ("user:42").
	app, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN_CUSTOMER42")))
	demo.Check(err)
	defer app.Close()
	_, err = app.Query(ctx, "SELECT * FROM orders WHERE customer = 42")
	fmt.Printf("SQL with the customer's token: permission denied = %v\n", azora.IsPermissionDenied(err))
	mine, err := azora.Call[[]Order](ctx, app, "myOrders", nil)
	demo.Check(err)
	fmt.Printf("myOrders with the same token: %d orders\n", len(mine))
	for _, o := range mine[:min(3, len(mine))] {
		fmt.Printf("  #%d %-7s %7.2f  %s\n", o.ID, o.Status, o.Total, o.Note)
	}
}
