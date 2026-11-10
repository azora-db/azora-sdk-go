package azora_test

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

// Integration tests: units against the real engine. They are skipped
// while the engine binary does not list "units" in the features of its
// discovery document.

// testBundle is the units bundle of the integration tests.
const testBundle = `(function () {
  class UnitError extends Error {
    constructor(code, message, data) { super(message); this.name = "UnitError"; this.code = code; this.data = data; }
  }
  const unit = (kind, def) => Object.assign({ "~azora": 1, kind, args: null, internal: false, description: "" }, def);
  globalThis.__azora_units = {
    echo: unit("query", { handler: (ctx, args) => args === undefined ? null : args }),
    blob: unit("query", { handler: (ctx) => ({ b: ctx.db.value("SELECT x'0102ff'") }) }),
    blobLen: unit("query", { handler: (ctx, args) => args.data.length }),
    whoami: unit("query", { handler: (ctx) => ({ kind: ctx.auth.kind, subject: ctx.auth.subject }) }),
    checkout: unit("mutation", {
      args: { type: "object", properties: { cartId: { type: "string" } }, required: ["cartId"], additionalProperties: false },
      handler(ctx, { cartId }) {
        const cart = ctx.db.one("SELECT * FROM u_carts WHERE id = ?", cartId);
        if (!cart) throw new UnitError("not_found", "no such cart", { cartId });
        const { lastInsertId } = ctx.db.exec("INSERT INTO u_orders(customer, total) VALUES (?, ?)", cart.owner, cart.total);
        ctx.db.exec("DELETE FROM u_carts WHERE id = ?", cartId);
        return { orderId: lastInsertId, open: ctx.run("count", {}) };
      },
    }),
    writeThenThrow: unit("mutation", {
      handler(ctx) { ctx.db.exec("INSERT INTO u_orders(customer, total) VALUES (0, 0)"); throw new Error("x is undefined"); },
    }),
    count: unit("query", { internal: true, handler: (ctx) => ctx.db.value("SELECT count(*) FROM u_orders") }),
  };
})();
`

// unitsEngine returns the shared engine with the test bundle published,
// or skips the test when the engine has no units.
func unitsEngine(t *testing.T) *fixture {
	t.Helper()
	f := engine(t)
	ok, err := f.Supports("units")
	require.NoError(t, err)
	if !ok {
		t.Skip(`integration test skipped: the engine binary does not support units (no "units" in /.well-known/azora.json features)`)
	}
	return f
}

func TestEngineUnitsCall(t *testing.T) {
	f := unitsEngine(t)
	c := ctx(t)
	db := f.connect(t)
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS u_carts (id TEXT PRIMARY KEY, owner INTEGER NOT NULL, total REAL NOT NULL)")
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS u_orders (id INTEGER PRIMARY KEY, customer INTEGER NOT NULL, total REAL NOT NULL)")
	mustExec(t, db, "DELETE FROM u_carts")
	mustExec(t, db, "DELETE FROM u_orders")
	d, err := db.Units().Deploy(c, azora.DeployRequest{Bundle: testBundle, Note: "units test"})
	require.NoError(t, err)
	require.Positive(t, d.Version)
	names := map[string]azora.Unit{}
	for _, u := range d.Units {
		names[u.Name] = u
	}
	require.Equal(t, azora.UnitKindMutation, names["checkout"].Kind)
	require.True(t, names["count"].Internal)
	require.JSONEq(t, `["cartId"]`, string(mustField(t, names["checkout"].Args, "required")))

	// Values travel as JSON; blobs as {"$bytes": …}.
	type echoArgs struct {
		S string      `json:"s"`
		N float64     `json:"n"`
		B azora.Bytes `json:"b"`
		L []any       `json:"l"`
	}
	in := echoArgs{S: "héllo", N: 1.5, B: azora.Bytes{0, 1, 255}, L: []any{true, nil, "x"}}
	out, err := azora.Call[echoArgs](c, db, "echo", in)
	require.NoError(t, err)
	require.Equal(t, in, out)
	v, err := azora.Call[any](c, db, "echo", nil)
	require.NoError(t, err)
	require.Nil(t, v)
	blob, err := azora.Call[struct{ B azora.Bytes }](c, db, "blob", nil)
	require.NoError(t, err)
	require.Equal(t, azora.Bytes{1, 2, 255}, blob.B, "a blob read by the unit")
	n, err := azora.Call[int](c, db, "blobLen", map[string]any{"data": azora.Bytes("four")})
	require.NoError(t, err)
	require.Equal(t, 4, n, "a Bytes argument is a Uint8Array in the unit")

	// A mutation, exactly once per mutation id.
	mustExec(t, db, "INSERT INTO u_carts VALUES ('c1', 42, 10.5)")
	type checkout struct {
		OrderID int64 `json:"orderId"`
		Open    int   `json:"open"`
	}
	mid := azora.NewMutationID()
	var replayed bool
	res, err := azora.Call[checkout](c, db, "checkout", map[string]string{"cartId": "c1"}, azora.MutationID(mid), azora.Replayed(&replayed))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, 1, res.Open, "the internal unit ran inside the mutation's transaction")
	other := f.connect(t)
	raw, err := other.CallRaw(c, "checkout", map[string]string{"cartId": "c1"}, azora.MutationID(mid))
	require.NoError(t, err)
	require.True(t, raw.Replayed, "the cart is gone, the first answer is returned")
	var again checkout
	require.NoError(t, json.Unmarshal(raw.Value, &again))
	require.Equal(t, res, again)
	count := func() int {
		n, err := azora.Scalar[int](c, db, "SELECT count(*) FROM u_orders")
		require.NoError(t, err)
		return n
	}
	require.Equal(t, 1, count())

	// The error the unit threw.
	_, err = azora.Call[checkout](c, db, "checkout", map[string]string{"cartId": "c1"})
	var ue *azora.UnitError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, "checkout", ue.Unit)
	require.Equal(t, azora.UnitCodeNotFound, ue.Code)
	require.Equal(t, "no such cart", ue.Message)
	require.JSONEq(t, `{"cartId":"c1"}`, string(ue.Data))

	// Anything else thrown is "failed", and the mutation rolled back.
	err = db.Call(c, "writeThenThrow", nil, nil)
	require.ErrorAs(t, err, &ue)
	require.Equal(t, azora.UnitCodeFailed, ue.Code)
	require.Contains(t, ue.Message, "x is undefined")
	require.Equal(t, 1, count(), "the throw rolled the write back")

	// Failures around the unit are server errors.
	err = db.Call(c, "checkout", map[string]int{"cartId": 1}, nil)
	require.True(t, azora.IsInvalid(err), "arguments that do not match: %v", err)
	require.False(t, errors.As(err, &ue))
	err = db.Call(c, "nope", nil, nil)
	require.True(t, azora.IsNotFound(err), "no such unit: %v", err)
	err = db.Call(c, "count", nil, nil)
	require.True(t, azora.IsPermissionDenied(err), "an internal unit: %v", err)
	cv, err := db.Units().Call(c, "count", nil)
	require.NoError(t, err, "the admin call runs internal units")
	require.JSONEq(t, `1`, string(cv.Value))

	// A token limited to one table and its rows cannot run SQL, but it can
	// call units, which see who it is.
	app := f.connectURL(t, f.URL(testDB), azora.WithToken(f.Restricted))
	_, err = app.Query(c, "SELECT count(*) FROM u_orders")
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	who, err := azora.Call[map[string]string](c, app, "whoami", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"kind": "token", "subject": "user:1"}, who)
	_, err = app.Units().List(c)
	require.True(t, azora.IsPermissionDenied(err), "managing units needs ddl: %v", err)
	who, err = azora.Call[map[string]string](c, db, "whoami", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"kind": "key", "subject": ""}, who)

	// Connected by the public URL without a key (discovery), calls work.
	bare := f.connectURL(t, "http://"+f.HTTPAddr+"/"+testDB, azora.WithToken(f.ReadKey))
	_, err = azora.Call[int](c, bare, "blobLen", map[string]any{"data": azora.Bytes("x")})
	require.NoError(t, err)

	// The example bundle is valid.
	example, err := os.ReadFile("examples/units/units.js")
	require.NoError(t, err)
	dry, err := db.Units().Deploy(c, azora.DeployRequest{Bundle: string(example), DryRun: true})
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	require.Len(t, dry.Units, 3)
	l, err := db.Units().List(c)
	require.NoError(t, err)
	require.Equal(t, d.Version, l.Version, "a dry run publishes nothing")
}

func TestEngineUnitsAdmin(t *testing.T) {
	unitsEngine(t) // skips without units
	f := newEngine(t, nil)
	c := ctx(t)
	db := f.connect(t)
	units := db.Units()

	l, err := units.List(c)
	require.NoError(t, err)
	require.Zero(t, l.Version, "nothing published")
	err = db.Call(c, "v", nil, nil)
	require.True(t, azora.IsNotFound(err), "%v", err)

	bundle := func(v int) string {
		return `globalThis.__azora_units = {
  v: { "~azora": 1, kind: "query", args: null, internal: false, description: "version", handler: (ctx) => { ctx.log.info("v", ` + string(rune('0'+v)) + `); return { v: ` + string(rune('0'+v)) + `, secret: ctx.env.GREETING === undefined ? null : ctx.env.GREETING }; } },
  once: { "~azora": 1, kind: "mutation", args: null, internal: false, description: "", handler: (ctx) => ctx.uuid() },
  tick: { "~azora": 1, kind: "schedule", args: null, internal: true, description: "", mode: "mutation", cron: "", every: "1h", handler: () => {} },
};`
	}
	d1, err := units.Deploy(c, azora.DeployRequest{Bundle: bundle(1), Note: "one"})
	require.NoError(t, err)
	require.False(t, d1.Unchanged)
	same, err := units.Deploy(c, azora.DeployRequest{Bundle: bundle(1)})
	require.NoError(t, err)
	require.True(t, same.Unchanged, "the same bundle makes no new version")
	require.Equal(t, d1.Version, same.Version)
	d2, err := units.Deploy(c, azora.DeployRequest{Bundle: bundle(2), Note: "two"})
	require.NoError(t, err)
	require.Greater(t, d2.Version, d1.Version)

	type vres struct {
		V      int     `json:"v"`
		Secret *string `json:"secret"`
	}
	r, err := azora.Call[vres](c, db, "v", nil)
	require.NoError(t, err)
	require.Equal(t, 2, r.V)
	require.Nil(t, r.Secret)

	h, err := units.Deploys(c, 10)
	require.NoError(t, err)
	require.Equal(t, d2.Version, h.Active)
	require.Len(t, h.Deploys, 2)
	require.Equal(t, d2.Version, h.Deploys[0].Version, "newest first")
	require.True(t, h.Deploys[0].Active)
	require.Equal(t, "two", h.Deploys[0].Note)
	require.False(t, h.Deploys[0].At.IsZero())
	require.Equal(t, 3, h.Deploys[0].Units)

	act, err := units.Activate(c, d1.Version)
	require.NoError(t, err)
	require.Equal(t, d1.Version, act.Version)
	r, err = azora.Call[vres](c, db, "v", nil)
	require.NoError(t, err)
	require.Equal(t, 1, r.V, "rolled back")

	// Secrets: set, read by units, listed by name, removed.
	require.NoError(t, units.Secrets().Set(c, "GREETING", "hello"))
	r, err = azora.Call[vres](c, db, "v", nil)
	require.NoError(t, err)
	require.NotNil(t, r.Secret)
	require.Equal(t, "hello", *r.Secret)
	secrets, err := units.Secrets().List(c)
	require.NoError(t, err)
	require.Len(t, secrets, 1)
	require.Equal(t, "GREETING", secrets[0].Name)
	require.False(t, secrets[0].UpdatedAt.IsZero())
	removed, err := units.Secrets().Unset(c, "GREETING")
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = units.Secrets().Unset(c, "GREETING")
	require.NoError(t, err)
	require.False(t, removed)

	// The run log and the triggers.
	runs, err := units.Runs(c, azora.RunsQuery{Unit: "v", Limit: 2})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	require.Equal(t, "v", runs[0].Unit)
	require.Equal(t, "call", runs[0].Trigger)
	require.Equal(t, "ok", runs[0].Status)
	require.Equal(t, "key:app", runs[0].Principal)
	require.True(t, runs[0].At.After(runs[1].At) || runs[0].At.Equal(runs[1].At), "newest first")
	older, err := units.Runs(c, azora.RunsQuery{Unit: "v", Before: runs[1].ID})
	require.NoError(t, err)
	for _, o := range older {
		require.NotEqual(t, runs[0].ID, o.ID)
		require.NotEqual(t, runs[1].ID, o.ID)
	}
	// A replayed call is in the log as such; Status and After filter.
	mid := azora.NewMutationID()
	first, err := azora.Call[string](c, db, "once", nil, azora.MutationID(mid))
	require.NoError(t, err)
	var replayed bool
	again, err := azora.Call[string](c, db, "once", nil, azora.MutationID(mid), azora.Replayed(&replayed))
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, first, again, "the stored answer, not a new run")
	rep, err := units.Runs(c, azora.RunsQuery{Status: "replayed"})
	require.NoError(t, err)
	require.Len(t, rep, 1)
	require.Equal(t, "replayed", rep[0].Status)
	newer, err := units.Runs(c, azora.RunsQuery{After: runs[0].ID})
	require.NoError(t, err)
	require.Len(t, newer, 2, "the two calls since")
	for _, n := range newer {
		require.Greater(t, n.ID, runs[0].ID)
	}
	ac, err := units.Call(c, "v", nil)
	require.NoError(t, err)
	require.False(t, ac.Replayed)
	require.JSONEq(t, `{"v":1,"secret":null}`, string(ac.Value))

	trs, err := units.Triggers(c)
	require.NoError(t, err)
	require.Len(t, trs, 1)
	require.Equal(t, "tick", trs[0].Unit)
	require.Equal(t, azora.UnitKindSchedule, trs[0].Kind)
	require.False(t, trs[0].NextAt.IsZero())
	require.False(t, trs[0].Running)

	// Managing units needs ddl.
	ro := f.connectURL(t, f.URL(testDB), azora.WithToken(f.ReadKey))
	_, err = ro.Units().Deploy(c, azora.DeployRequest{Bundle: bundle(3)})
	require.True(t, azora.IsPermissionDenied(err), "%v", err)
	_, err = azora.Call[vres](c, ro, "v", nil)
	require.NoError(t, err, "calling needs no scope")

	// A bundle that does not run is refused and changes nothing.
	_, err = units.Deploy(c, azora.DeployRequest{Bundle: "this is not javascript"})
	require.True(t, azora.IsInvalid(err), "%v", err)
	l, err = units.List(c)
	require.NoError(t, err)
	require.Equal(t, d1.Version, l.Version)
}

func TestEngineUnitsOnChange(t *testing.T) {
	unitsEngine(t) // skips without units
	f := newEngine(t, nil)
	c := ctx(t)
	db := f.connect(t)
	mustExec(t, db, "CREATE TABLE ev (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE TABLE ev_bad (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE TABLE seen (n INTEGER PRIMARY KEY, src TEXT, op TEXT, k INTEGER, v TEXT)")
	// seen records every change of ev; bad records those of ev_bad and
	// fails its whole batch on a "poison" row.
	const bundle = `(function () {
  const unit = (def) => Object.assign({ "~azora": 1, kind: "onChange", args: null, internal: true, description: "", mode: "mutation", ops: [], batch: 100 }, def);
  const record = (ctx, src, changes) => {
    for (const ch of changes) {
      const row = ch.new || ch.old;
      ctx.db.exec("INSERT INTO seen(src, op, k, v) VALUES (?, ?, ?, ?)", src, ch.op, row.id, row.v);
    }
  };
  globalThis.__azora_units = {
    seenEv: unit({ table: "ev", handler: (ctx, { changes }) => record(ctx, "ev", changes) }),
    bad: unit({ table: "ev_bad", ops: ["insert"], handler(ctx, { changes }) {
      record(ctx, "ev_bad", changes);
      if (changes.some((ch) => ch.new.v === "poison")) throw new Error("poison row");
    } }),
  };
})();`
	_, err := db.Units().Deploy(c, azora.DeployRequest{Bundle: bundle})
	require.NoError(t, err)

	// Changes made right after the deploy are delivered, each once, in
	// commit order.
	mustExec(t, db, "INSERT INTO ev(id, v) VALUES (1, 'a')")
	_, err = db.Batch(c, azora.Stmt("INSERT INTO ev(id, v) VALUES (2, 'b')"), azora.Stmt("INSERT INTO ev(id, v) VALUES (3, 'c')"))
	require.NoError(t, err)
	mustExec(t, db, "UPDATE ev SET v = 'a2' WHERE id = 1")
	mustExec(t, db, "DELETE FROM ev WHERE id = 2")
	type change struct {
		Op string
		K  int64
		V  string
	}
	seen := func(src string) []change {
		rows, err := azora.All[change](c, db, "SELECT op, k, v FROM seen WHERE src = ? ORDER BY n", src)
		require.NoError(t, err)
		return rows
	}
	want := []change{{"insert", 1, "a"}, {"insert", 2, "b"}, {"insert", 3, "c"}, {"update", 1, "a2"}, {"delete", 2, "b"}}
	eventually(t, 15*time.Second, func() bool { return len(seen("ev")) >= len(want) }, "the onChange unit saw %d of %d changes", len(seen("ev")), len(want))
	time.Sleep(500 * time.Millisecond) // a second delivery would show up by now
	require.Equal(t, want, seen("ev"), "every change exactly once, in commit order")

	trigger := func(unit string) azora.Trigger {
		trs, err := db.Units().Triggers(c)
		require.NoError(t, err)
		for _, tr := range trs {
			if tr.Unit == unit {
				return tr
			}
		}
		t.Fatalf("no trigger for %s in %+v", unit, trs)
		return azora.Trigger{}
	}
	tr := trigger("seenEv")
	require.Equal(t, azora.UnitKindOnChange, tr.Kind)
	require.Positive(t, tr.Cursor)
	require.Zero(t, tr.Pending)
	require.Zero(t, tr.Failures)
	require.Empty(t, tr.Error)
	require.False(t, tr.LastAt.IsZero())
	runs, err := db.Units().Runs(c, azora.RunsQuery{Unit: "seenEv"})
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	for _, r := range runs {
		require.Equal(t, "change", r.Trigger)
		require.Equal(t, "ok", r.Status)
		require.Equal(t, "system", r.Principal)
	}

	// A failing batch rolls back and blocks the unit: failures, error and
	// pending show it, until SkipTrigger moves past it.
	mustExec(t, db, "INSERT INTO ev_bad(id, v) VALUES (1, 'poison')")
	eventually(t, 15*time.Second, func() bool { return trigger("bad").Failures > 0 }, "the failing batch is reported")
	tr = trigger("bad")
	require.Contains(t, tr.Error, "poison row")
	require.Equal(t, int64(1), tr.Pending, "only the unit's own table counts")
	require.Empty(t, seen("ev_bad"), "the failed run rolled back")
	failed, err := db.Units().Runs(c, azora.RunsQuery{Unit: "bad", Status: "error"})
	require.NoError(t, err)
	require.NotEmpty(t, failed)
	require.Contains(t, failed[0].Error, "poison row")

	skipped, err := db.Units().SkipTrigger(c, "bad")
	require.NoError(t, err)
	require.Equal(t, int64(1), skipped)
	mustExec(t, db, "INSERT INTO ev_bad(id, v) VALUES (2, 'fine')")
	eventually(t, 15*time.Second, func() bool { return len(seen("ev_bad")) > 0 }, "the unit runs again after the skip")
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, []change{{"insert", 2, "fine"}}, seen("ev_bad"), "the poison row was skipped, the next change delivered once")
	eventually(t, 5*time.Second, func() bool { tr = trigger("bad"); return tr.Failures == 0 && tr.Pending == 0 }, "the trigger recovered: %+v", tr)
	require.False(t, strings.Contains(tr.Error, "poison") && tr.Failures > 0)

	// last_at survives a restart. After it, last_at is when the cursor
	// last moved, which can be later than the last run.
	before := trigger("seenEv").LastAt
	require.NoError(t, f.Restart())
	db = f.connect(t)
	after := trigger("seenEv").LastAt
	require.False(t, after.IsZero())
	require.False(t, after.Before(before), "%v before %v", after, before)
}

// mustField returns a field of a JSON object.
func mustField(t *testing.T, obj json.RawMessage, name string) json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(obj, &m), "%s", obj)
	return m[name]
}

// TestEngineUnitsUnsupported runs against an engine without units:
// connected through discovery, the SDK refuses before sending anything.
func TestEngineUnitsUnsupported(t *testing.T) {
	f := engine(t)
	if ok, err := f.Supports("units"); err != nil || ok {
		t.Skipf("the engine supports units (or discovery failed: %v)", err)
	}
	bare := f.connectURL(t, "http://"+f.HTTPAddr+"/"+testDB, azora.WithToken(f.Key))
	err := bare.Call(ctx(t), "anything", nil, nil)
	require.ErrorIs(t, err, azora.ErrUnitsUnsupported)
	_, err = bare.Units().List(ctx(t))
	require.ErrorIs(t, err, azora.ErrUnitsUnsupported)
	_, err = bare.Query(ctx(t), "SELECT 1")
	require.NoError(t, err, "the connection itself is fine")
}
