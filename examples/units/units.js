// The units of the "shop" database, written by hand in the bundle format
// the engine runs (docs/units.md, "Bundle format"): one plain script that
// sets globalThis.__azora_units. `npx azora-units publish` produces the
// same thing from TypeScript; this file needs no build step.
// `go run ./cmd/azora-dev` publishes it when it seeds the database.
(function () {
  "use strict";

  // What `throw new UnitError(code, message, data)` throws in TypeScript:
  // the caller receives code, message and data (azora.UnitError in Go).
  class UnitError extends Error {
    constructor(code, message, data) {
      super(message);
      this.name = "UnitError";
      this.code = code;
      this.data = data;
    }
  }

  // unit fills in the fields every definition carries.
  function unit(kind, def) {
    return Object.assign({ "~azora": 1, kind: kind, args: null, internal: false, description: "" }, def);
  }

  // customerOf returns the customer number of a client token's subject
  // ("user:42"), or 0 for other callers (API keys).
  function customerOf(auth) {
    const m = /^user:(\d+)$/.exec(auth.subject || "");
    return m ? Number(m[1]) : 0;
  }

  globalThis.__azora_units = {
    // The caller's own orders. A token limited to its rows cannot run
    // SQL, but it can call this: the unit decides from ctx.auth.
    myOrders: unit("query", {
      description: "The signed-in customer's orders, newest first.",
      handler(ctx) {
        const customer = customerOf(ctx.auth);
        if (!customer) throw new UnitError("permission_denied", "myOrders needs a customer's token");
        return ctx.db.all(
          "SELECT id, customer, status, total, note FROM orders WHERE customer = ? ORDER BY id DESC", customer);
      },
    }),

    // Turns a cart into an open order, in one transaction: the order is
    // inserted and the cart removed, or nothing happens.
    checkout: unit("mutation", {
      description: "Turn a cart into an open order.",
      args: {
        type: "object",
        properties: { cartId: { type: "string", minLength: 1 } },
        required: ["cartId"],
        additionalProperties: false,
      },
      handler(ctx, args) {
        const cart = ctx.db.one("SELECT id, owner, total, note FROM carts WHERE id = ?", args.cartId);
        if (!cart) throw new UnitError("not_found", "no such cart", { cartId: args.cartId });
        const customer = customerOf(ctx.auth);
        if (customer && customer !== cart.owner) throw new UnitError("permission_denied", "not your cart");
        const res = ctx.db.exec(
          "INSERT INTO orders(customer, status, total, note) VALUES (?, 'open', ?, ?)", cart.owner, cart.total, cart.note);
        ctx.db.exec("DELETE FROM carts WHERE id = ?", cart.id);
        ctx.log.info("checkout", cart.id, "order", res.lastInsertId);
        return { orderId: res.lastInsertId, openOrders: ctx.run("openOrders", { customer: cart.owner }) };
      },
    }),

    // A helper for other units: clients cannot call it.
    openOrders: unit("query", {
      internal: true,
      description: "How many open orders a customer has.",
      args: { type: "object", properties: { customer: { type: "integer" } }, required: ["customer"] },
      handler(ctx, args) {
        return ctx.db.value("SELECT count(*) FROM orders WHERE status = 'open' AND customer = ?", args.customer);
      },
    }),
  };
})();
