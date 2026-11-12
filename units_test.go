package azora_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
)

// Units against the scripted server and without a server.

func TestBytesJSON(t *testing.T) {
	b, err := json.Marshal(azora.Bytes("hi\x00\xff"))
	require.NoError(t, err)
	require.JSONEq(t, `{"$bytes":"aGkA/w=="}`, string(b))
	b, err = json.Marshal(azora.Bytes(nil))
	require.NoError(t, err)
	require.Equal(t, "null", string(b))
	b, err = json.Marshal(azora.Bytes{})
	require.NoError(t, err)
	require.JSONEq(t, `{"$bytes":""}`, string(b))

	type file struct {
		Name string       `json:"name"`
		Data azora.Bytes  `json:"data"`
		Opt  *azora.Bytes `json:"opt"`
	}
	b, err = json.Marshal(file{Name: "a", Data: azora.Bytes{1, 2, 3}})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"a","data":{"$bytes":"AQID"},"opt":null}`, string(b))
	var back file
	require.NoError(t, json.Unmarshal(b, &back))
	require.Equal(t, azora.Bytes{1, 2, 3}, back.Data)
	require.Nil(t, back.Opt)

	for in, want := range map[string]azora.Bytes{
		`{"$bytes":"aGkA/w=="}`:     azora.Bytes("hi\x00\xff"),
		` { "$bytes" : "aGkA/w" } `: azora.Bytes("hi\x00\xff"), // unpadded
		`{"$bytes":""}`:             {},
		`"AQID"`:                    {1, 2, 3}, // encoding/json's []byte form
		`null`:                      nil,
	} {
		var got azora.Bytes
		require.NoError(t, json.Unmarshal([]byte(in), &got), in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{`{}`, `{"$bytes":1}`, `{"$bytes":"!!"}`, `[1]`, `12`} {
		var got azora.Bytes
		require.Error(t, json.Unmarshal([]byte(in), &got), in)
	}

	// In an untyped result a blob stays the object the server sent.
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{"b":{"$bytes":"AQID"}}`), &m))
	require.Equal(t, map[string]any{"$bytes": "AQID"}, m["b"])
}

func TestUnitErrorFormatting(t *testing.T) {
	var err error = &azora.UnitError{Unit: "checkout", Code: azora.UnitCodeNotFound, Message: "no such cart"}
	require.EqualError(t, err, "azora: unit checkout: not_found: no such cart")
	var ue *azora.UnitError
	require.True(t, errors.As(errors.Join(errors.New("ctx"), err), &ue))
	require.Equal(t, "checkout", ue.Unit)
	require.False(t, azora.IsNotFound(err), "a unit's not_found is not a server error")
}

// unitServer answers Call like a units engine: checkout fails without a
// cart, echo returns its arguments, nothing returns nothing; mutation ids
// are remembered, and the first run of each id loses its answer when
// dropFirst is set.
type unitServer struct {
	dropFirst atomic.Bool
	mu        sync.Mutex
	calls     []azp.Call
	runs      map[string]*azp.Returned
}

func (u *unitServer) handle(ss *azptest.Session, m azp.Msg) {
	c, ok := m.(*azp.Call)
	if !ok {
		return
	}
	u.mu.Lock()
	u.calls = append(u.calls, *c)
	if u.runs == nil {
		u.runs = map[string]*azp.Returned{}
	}
	prev := u.runs[c.MutationID]
	u.mu.Unlock()
	if prev != nil {
		r := *prev
		r.ReqID, r.Replayed = c.ReqID, true
		ss.Send(&r)
		return
	}
	var r *azp.Returned
	switch c.Unit {
	case "echo":
		r = &azp.Returned{ReqID: c.ReqID, OK: true, Value: c.Args}
	case "nothing":
		r = &azp.Returned{ReqID: c.ReqID, OK: true}
	case "checkout":
		var a struct {
			CartID string `json:"cartId"`
		}
		json.Unmarshal([]byte(c.Args), &a)
		if a.CartID != "c1" {
			r = &azp.Returned{ReqID: c.ReqID, ErrCode: "not_found", ErrMessage: "no such cart", ErrData: `{"cartId":"` + a.CartID + `"}`}
		} else {
			r = &azp.Returned{ReqID: c.ReqID, OK: true, Value: `{"orderId":7}`}
		}
	case "boom":
		r = &azp.Returned{ReqID: c.ReqID, ErrCode: "failed", ErrMessage: "x is undefined"}
	default:
		ss.Send(&azp.Error{ReqID: c.ReqID, Code: azp.CodeNotFound, Message: "no unit " + c.Unit})
		return
	}
	u.mu.Lock()
	u.runs[c.MutationID] = r
	u.mu.Unlock()
	if u.dropFirst.Load() {
		ss.Drop()
		return
	}
	ss.Send(r)
}

func (u *unitServer) sent() []azp.Call {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]azp.Call(nil), u.calls...)
}

func TestFakeCall(t *testing.T) {
	srv := azptest.New(t)
	us := &unitServer{}
	srv.Handle(us.handle)
	db := fakeDB(t, srv)
	c := ctx(t)

	type checkoutArgs struct {
		CartID string `json:"cartId"`
	}
	type checkoutResult struct {
		OrderID int64 `json:"orderId"`
	}
	res, err := azora.Call[checkoutResult](c, db, "checkout", checkoutArgs{CartID: "c1"})
	require.NoError(t, err)
	require.Equal(t, checkoutResult{OrderID: 7}, res)

	// The error the unit threw.
	_, err = azora.Call[checkoutResult](c, db, "checkout", checkoutArgs{CartID: "c9"})
	var ue *azora.UnitError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, &azora.UnitError{Unit: "checkout", Code: azora.UnitCodeNotFound, Message: "no such cart",
		Data: json.RawMessage(`{"cartId":"c9"}`)}, ue)
	err = db.Call(c, "boom", nil, nil)
	require.ErrorAs(t, err, &ue)
	require.Equal(t, azora.UnitCodeFailed, ue.Code)
	require.Nil(t, ue.Data)

	// A failure around the unit is a server error, not a UnitError.
	err = db.Call(c, "nope", nil, nil)
	require.True(t, azora.IsNotFound(err), "%v", err)
	require.False(t, errors.As(err, &ue))

	// Arguments: nil is none, anything else is JSON; results decode into
	// out, and a nil out ignores them.
	var anyOut any
	require.NoError(t, db.Call(c, "echo", map[string]any{"b": azora.Bytes{1}, "n": 1}, &anyOut))
	require.Equal(t, map[string]any{"b": map[string]any{"$bytes": "AQ=="}, "n": float64(1)}, anyOut)
	raw, err := db.CallRaw(c, "echo", json.RawMessage(`[1, 2]`))
	require.NoError(t, err)
	require.JSONEq(t, `[1,2]`, string(raw.Value))
	raw, err = db.CallRaw(c, "nothing", nil)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage("null"), raw.Value)
	require.NoError(t, db.Call(c, "echo", "x", nil))
	n, err := azora.Call[int](c, db, "nothing", nil)
	require.NoError(t, err)
	require.Zero(t, n)
	_, err = azora.Call[int](c, db, "echo", "text")
	require.ErrorContains(t, err, "decode the result")

	calls := us.sent()
	require.Equal(t, `{"cartId":"c1"}`, calls[0].Args)
	require.Equal(t, `{"b":{"$bytes":"AQ=="},"n":1}`, calls[4].Args)
	require.Equal(t, "", calls[6].Args, "nil arguments are none")
	seen := map[string]bool{}
	for _, c := range calls {
		require.True(t, strings.HasPrefix(c.MutationID, "m-"), c.MutationID)
		require.False(t, seen[c.MutationID], "every call has its own mutation id")
		seen[c.MutationID] = true
	}

	// Refused before anything is sent.
	before := len(us.sent())
	require.ErrorContains(t, db.Call(c, "", nil, nil), "empty unit name")
	require.ErrorContains(t, db.Call(c, "echo", func() {}, nil), "encode the arguments")
	require.ErrorContains(t, db.Call(c, "echo", nil, nil, azora.MutationID(strings.Repeat("x", azora.MaxMutationIDLen+1))), "mutation id")
	require.Len(t, us.sent(), before)
}

func TestFakeCallMutationID(t *testing.T) {
	srv := azptest.New(t)
	us := &unitServer{}
	srv.Handle(us.handle)
	db := fakeDB(t, srv)
	c := ctx(t)

	// The answer of the first run is lost: the call is sent again with the
	// same mutation id and answered from the first run.
	us.dropFirst.Store(true)
	var replayed bool
	raw, err := db.CallRaw(c, "checkout", map[string]string{"cartId": "c1"})
	us.dropFirst.Store(false)
	require.NoError(t, err)
	require.True(t, raw.Replayed)
	require.JSONEq(t, `{"orderId":7}`, string(raw.Value))
	calls := us.sent()
	require.Len(t, calls, 2)
	require.Equal(t, calls[0].MutationID, calls[1].MutationID, "the retry carries the same mutation id")
	require.Positive(t, db.Stats().Retries)

	// A chosen id, twice: the second call is a replay.
	require.NoError(t, db.Call(c, "checkout", map[string]string{"cartId": "c1"}, nil, azora.MutationID("order-17"), azora.Replayed(&replayed)))
	require.False(t, replayed)
	var out struct {
		OrderID int `json:"orderId"`
	}
	require.NoError(t, db.Call(c, "checkout", map[string]string{"cartId": "c1"}, &out, azora.MutationID("order-17"), azora.Replayed(&replayed)))
	require.True(t, replayed)
	require.Equal(t, 7, out.OrderID)
	calls = us.sent()
	require.Equal(t, "order-17", calls[2].MutationID)
	require.Equal(t, "order-17", calls[3].MutationID)

	// A replayed error is still the unit's error.
	_, err = db.CallRaw(c, "checkout", map[string]string{"cartId": "zz"}, azora.MutationID("bad-1"))
	var ue *azora.UnitError
	require.ErrorAs(t, err, &ue)
	replayed = true
	_, err = azora.Call[any](c, db, "checkout", map[string]string{"cartId": "zz"}, azora.MutationID("bad-1"), azora.Replayed(&replayed))
	require.ErrorAs(t, err, &ue)
	require.True(t, replayed)

	// Replayed is reset when the call fails before an answer.
	replayed = true
	require.Error(t, db.Call(c, "", nil, nil, azora.Replayed(&replayed)))
	require.False(t, replayed)
}

func TestFakeUnitsAdmin(t *testing.T) {
	srv := azptest.New(t)
	var mu sync.Mutex
	var got []azp.UnitAdmin
	var drop atomic.Bool
	srv.Handle(func(ss *azptest.Session, m azp.Msg) {
		x, ok := m.(*azp.UnitAdmin)
		if !ok {
			return
		}
		mu.Lock()
		got = append(got, *x)
		mu.Unlock()
		if drop.Swap(false) {
			ss.Drop()
			return
		}
		body := map[string]string{
			"deploy": `{"version":3,"sha256":"ab","unchanged":false,"dry_run":false,"units":[` +
				`{"name":"checkout","kind":"mutation","internal":false,"description":"","args":{"type":"object"}},` +
				`{"name":"nightly","kind":"schedule","internal":true,"description":"d","args":null,"mode":"mutation","cron":"0 3 * * *","every":""}]}`,
			"list":     `{"version":0,"units":[]}`,
			"deploys":  `{"active":3,"deploys":[{"version":3,"at":1762772400123,"by":"key:app","note":"n","sha256":"ab","size":120,"units":2,"active":true}]}`,
			"activate": `{"version":2,"units":[]}`,
			"call":     `{"ok":true,"value":{"a":1},"replayed":false}`,
			"runs": `{"runs":[{"id":9,"at":1762772400123,"unit":"checkout","kind":"mutation","trigger":"call","principal":"user:42",` +
				`"duration_ms":1.5,"status":"error","error":"no such cart","logs":"info checkout c1\n"}]}`,
			"triggers":      `{"triggers":[{"unit":"paid","kind":"onChange","spec":"orders","mode":"action","next_at":0,"last_at":1762772400000,"cursor":41,"pending":3,"failures":1,"error":"x","lagged":true,"running":true}]}`,
			"triggers.skip": `{"skipped":12}`,
			"secrets.list":  `{"secrets":[{"name":"STRIPE_KEY","updated_at":1762772400000}]}`,
			"secrets.set":   `{}`,
			"secrets.unset": `{"removed":true}`,
			"jobs": `{"jobs":[{"id":"job_a","unit":"remind","args":{"orderId":7},"run_at":1762772400123,"created_at":1762772400000,` +
				`"created_by":"key:app","source":"placeOrder","attempts":2,"state":"failed","error":"Error: smtp down"},` +
				`{"id":"job_b","unit":"tick","args":null,"run_at":1762772400123,"created_at":1762772400000,"created_by":"job:job_z","source":"tick","attempts":0,"state":"pending"}],` +
				`"pending":4,"failed":1}`,
			"jobs.cancel": `{"removed":true}`,
			"jobs.retry":  `{}`,
		}[x.Op]
		if x.Op == "jobs" && strings.Contains(x.Body, `"none"`) {
			body = `{"jobs":[],"pending":0,"failed":0}`
		}
		if x.Op == "jobs.retry" && strings.Contains(x.Body, `"job_p"`) {
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeInvalid, Message: "job job_p is pending, not failed"})
			return
		}
		if x.Op == "call" && strings.Contains(x.Body, `"boom"`) {
			body = `{"ok":false,"value":null,"error":{"code":"conflict","message":"taken","data":[1]}}`
		}
		if body == "" {
			ss.Send(&azp.Error{ReqID: x.ReqID, Code: azp.CodeInvalid, Message: "unknown op"})
			return
		}
		ss.Send(&azp.UnitAdminResult{ReqID: x.ReqID, Body: body})
	})
	db := fakeDB(t, srv)
	c := ctx(t)
	units := db.Units()

	d, err := units.Deploy(c, azora.DeployRequest{Bundle: "globalThis.__azora_units = {}", Note: "first"})
	require.NoError(t, err)
	require.Equal(t, int64(3), d.Version)
	require.Equal(t, []azora.Unit{
		{Name: "checkout", Kind: azora.UnitKindMutation, Args: json.RawMessage(`{"type":"object"}`)},
		{Name: "nightly", Kind: azora.UnitKindSchedule, Internal: true, Description: "d", Args: json.RawMessage(`null`), Mode: "mutation", Cron: "0 3 * * *"},
	}, d.Units)
	_, err = units.Deploy(c, azora.DeployRequest{})
	require.ErrorContains(t, err, "empty bundle")

	l, err := units.List(c)
	require.NoError(t, err)
	require.Equal(t, &azora.UnitList{Units: []azora.Unit{}}, l)

	h, err := units.Deploys(c, 5)
	require.NoError(t, err)
	at := time.UnixMilli(1762772400123)
	require.Equal(t, &azora.DeployHistory{Active: 3, Deploys: []azora.DeployInfo{
		{Version: 3, At: at, By: "key:app", Note: "n", SHA256: "ab", Size: 120, Units: 2, Active: true}}}, h)

	l, err = units.Activate(c, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), l.Version)

	v, err := units.Call(c, "helper", map[string]int{"x": 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1}`, string(v.Value))
	require.False(t, v.Replayed)
	_, err = units.Call(c, "boom", nil)
	var ue *azora.UnitError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, &azora.UnitError{Unit: "boom", Code: azora.UnitCodeConflict, Message: "taken", Data: json.RawMessage(`[1]`)}, ue)

	runs, err := units.Runs(c, azora.RunsQuery{Limit: 10, Unit: "checkout", Before: 100, Status: azora.RunStatusError})
	require.NoError(t, err)
	require.Equal(t, []azora.Run{{ID: 9, At: at, Unit: "checkout", Kind: "mutation", Trigger: "call", Principal: "user:42",
		Duration: 1500 * time.Microsecond, Status: "error", Error: "no such cart", Logs: "info checkout c1\n"}}, runs)

	trs, err := units.Triggers(c)
	require.NoError(t, err)
	require.Equal(t, []azora.Trigger{{Unit: "paid", Kind: "onChange", Spec: "orders", Mode: "action",
		LastAt: time.UnixMilli(1762772400000), Cursor: 41, Pending: 3, Failures: 1, Error: "x", Lagged: true, Running: true}}, trs)
	require.True(t, trs[0].NextAt.IsZero())

	n, err := units.SkipTrigger(c, "paid")
	require.NoError(t, err)
	require.Equal(t, int64(12), n)

	secrets := units.Secrets()
	list, err := secrets.List(c)
	require.NoError(t, err)
	require.Equal(t, []azora.Secret{{Name: "STRIPE_KEY", UpdatedAt: time.UnixMilli(1762772400000)}}, list)
	require.NoError(t, secrets.Set(c, "STRIPE_KEY", "sk_1"))
	removed, err := secrets.Unset(c, "STRIPE_KEY")
	require.NoError(t, err)
	require.True(t, removed)

	jobs, err := units.Jobs(c, azora.JobsQuery{State: azora.JobStateFailed, Unit: "remind", Limit: 5})
	require.NoError(t, err)
	created := time.UnixMilli(1762772400000)
	require.Equal(t, &azora.JobList{Pending: 4, Failed: 1, Jobs: []azora.Job{
		{ID: "job_a", Unit: "remind", Args: json.RawMessage(`{"orderId":7}`), RunAt: at, CreatedAt: created,
			CreatedBy: "key:app", Source: "placeOrder", Attempts: 2, State: azora.JobStateFailed, Error: "Error: smtp down"},
		{ID: "job_b", Unit: "tick", Args: json.RawMessage(`null`), RunAt: at, CreatedAt: created,
			CreatedBy: "job:job_z", Source: "tick", State: azora.JobStatePending},
	}}, jobs)
	none, err := units.Jobs(c, azora.JobsQuery{Unit: "none"})
	require.NoError(t, err)
	require.Equal(t, &azora.JobList{Jobs: []azora.Job{}}, none)
	removed, err = units.CancelJob(c, "job_a")
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, units.RetryJob(c, "job_a"))
	err = units.RetryJob(c, "job_p")
	require.True(t, azora.IsInvalid(err), "%v", err)
	mu.Lock()
	before := len(got)
	mu.Unlock()
	_, err = units.CancelJob(c, "")
	require.ErrorContains(t, err, "empty job id")
	require.ErrorContains(t, units.RetryJob(c, ""), "empty job id")
	mu.Lock()
	require.Len(t, got, before, "refused before anything is sent")
	mu.Unlock()

	mu.Lock()
	bodies := map[string]string{}
	for _, g := range got {
		bodies[g.Op] = g.Body
	}
	mu.Unlock()
	require.JSONEq(t, `{"bundle":"globalThis.__azora_units = {}","note":"first"}`, bodies["deploy"])
	require.JSONEq(t, `{}`, bodies["list"])
	require.JSONEq(t, `{"limit":5}`, bodies["deploys"])
	require.JSONEq(t, `{"version":2}`, bodies["activate"])
	require.JSONEq(t, `{"unit":"boom"}`, bodies["call"])
	require.JSONEq(t, `{"limit":10,"unit":"checkout","before":100,"status":"error"}`, bodies["runs"])
	require.JSONEq(t, `{}`, bodies["triggers"])
	require.JSONEq(t, `{"unit":"paid"}`, bodies["triggers.skip"])
	require.JSONEq(t, `{}`, bodies["secrets.list"])
	require.JSONEq(t, `{"name":"STRIPE_KEY","value":"sk_1"}`, bodies["secrets.set"])
	require.JSONEq(t, `{"name":"STRIPE_KEY"}`, bodies["secrets.unset"])
	require.JSONEq(t, `{"unit":"none"}`, bodies["jobs"])
	require.JSONEq(t, `{"id":"job_a"}`, bodies["jobs.cancel"])
	require.JSONEq(t, `{"id":"job_p"}`, bodies["jobs.retry"])
	for _, g := range got {
		if g.Op == "jobs" {
			require.JSONEq(t, `{"state":"failed","unit":"remind","limit":5}`, g.Body)
			break
		}
	}

	// Reads are sent again after a lost connection; changes are not.
	drop.Store(true)
	_, err = units.List(c)
	require.NoError(t, err)
	drop.Store(true)
	_, err = units.Deploy(c, azora.DeployRequest{Bundle: "x"})
	require.ErrorIs(t, err, azora.ErrUnknownOutcome)
	drop.Store(true)
	require.ErrorIs(t, secrets.Set(c, "A", "b"), azora.ErrUnknownOutcome)
	drop.Store(true)
	_, err = units.Jobs(c, azora.JobsQuery{})
	require.NoError(t, err, "listing jobs is a read")
	drop.Store(true)
	_, err = units.CancelJob(c, "job_a")
	require.ErrorIs(t, err, azora.ErrUnknownOutcome)
	drop.Store(true)
	require.ErrorIs(t, units.RetryJob(c, "job_a"), azora.ErrUnknownOutcome)
}

func TestUnitStructsWireForm(t *testing.T) {
	at := time.UnixMilli(1762772400123)
	for _, v := range []any{
		&azora.DeployInfo{Version: 1, At: at, By: "key:app", SHA256: "ab", Size: 3, Units: 1, Active: true},
		&azora.Run{ID: 2, At: at, Unit: "u", Duration: 2500 * time.Microsecond, Logs: "x"},
		&azora.Trigger{Unit: "u", NextAt: at, Cursor: 5},
		&azora.Secret{Name: "A", UpdatedAt: at},
		&azora.Job{ID: "job_a", Unit: "u", Args: json.RawMessage(`{"a":1}`), RunAt: at, CreatedAt: at, State: azora.JobStatePending},
	} {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		require.Contains(t, string(b), "1762772400123", "times travel as unix milliseconds")
		back := newOf(v)
		require.NoError(t, json.Unmarshal(b, back))
		require.Equal(t, v, back)
	}
	b, err := json.Marshal(azora.Run{Duration: 1500 * time.Microsecond})
	require.NoError(t, err)
	require.Contains(t, string(b), `"duration_ms":1.5`)
	require.Contains(t, string(b), `"at":0`)
}

// newOf returns a new zero value of the type v points to.
func newOf(v any) any {
	switch v.(type) {
	case *azora.DeployInfo:
		return &azora.DeployInfo{}
	case *azora.Run:
		return &azora.Run{}
	case *azora.Trigger:
		return &azora.Trigger{}
	case *azora.Secret:
		return &azora.Secret{}
	case *azora.Job:
		return &azora.Job{}
	}
	panic("unknown type")
}

// TestFakeUnitsFeature connects through a discovery document: without
// "units" in its features, calls fail before anything is sent.
func TestFakeUnitsFeature(t *testing.T) {
	srv := azptest.New(t)
	us := &unitServer{}
	srv.Handle(us.handle)
	wsURL, err := url.Parse(strings.Replace(srv.WebSocketURL(), "ws://", "http://", 1))
	require.NoError(t, err)
	wsURL.Path = ""
	proxy := httputil.NewSingleHostReverseProxy(wsURL)
	var features atomic.Value
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/azora.json" {
			w.Header().Set("Content-Type", "application/json")
			doc := map[string]any{"azora": 1, "server_key": srv.ServerKey(), "websocket": "/rt/v1"}
			if f := features.Load().([]string); f != nil {
				doc["features"] = f
			}
			json.NewEncoder(w).Encode(doc)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)

	for _, f := range [][]string{nil, {}, {"storage"}} {
		features.Store(f)
		db := fakeDB2(t, front.URL+"/app")
		_, err := db.Ping(ctx(t))
		require.NoError(t, err)
		err = db.Call(ctx(t), "echo", 1, nil)
		require.ErrorIs(t, err, azora.ErrUnitsUnsupported)
		_, err = db.Units().List(ctx(t))
		require.ErrorIs(t, err, azora.ErrUnitsUnsupported)
	}
	require.Empty(t, us.sent(), "nothing was sent")

	features.Store([]string{"storage", "units"})
	db := fakeDB2(t, front.URL+"/app")
	n, err := azora.Call[int](ctx(t), db, "echo", 5)
	require.NoError(t, err)
	require.Equal(t, 5, n)
}

// fakeDB2 connects to a URL with a short request timeout.
func fakeDB2(t testing.TB, u string) *azora.DB {
	t.Helper()
	db, err := azora.Connect(ctx(t), u, azora.WithToken("azk_test"), azora.WithLogger(quiet), azora.WithRequestTimeout(300*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}
