package azora

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// Kinds of unit (Unit.Kind, Run.Kind).
const (
	UnitKindQuery    = "query"
	UnitKindMutation = "mutation"
	UnitKindAction   = "action"
	UnitKindSchedule = "schedule"
	UnitKindOnChange = "onChange"
)

// Units manages a database's units: publishing (deploys), rollback, the
// run log, triggers and secrets. Every method needs a credential with the
// ddl (or admin) scope. Get one with DB.Units.
//
// Methods that change state (Deploy, Activate, Call, SkipTrigger and the
// secret setters) are not sent again after a lost connection: they
// return ErrUnknownOutcome. Reads are retried like queries.
type Units struct{ db *DB }

// Units returns the facade over the database's units.
func (db *DB) Units() *Units { return &Units{db: db} }

// Unit describes a published unit.
type Unit struct {
	Name string `json:"name"`
	// Kind is one of the UnitKind constants.
	Kind string `json:"kind"`
	// Internal units cannot be called by clients (schedule and onChange
	// units are always internal).
	Internal    bool   `json:"internal"`
	Description string `json:"description"`
	// Args is the JSON Schema of the arguments, or null when the unit
	// accepts anything.
	Args json.RawMessage `json:"args"`
	// Mode is "mutation" or "action" (schedule and onChange units).
	Mode string `json:"mode,omitempty"`
	// Cron or Every is a schedule unit's timing.
	Cron  string `json:"cron,omitempty"`
	Every string `json:"every,omitempty"`
	// Table and Ops are what an onChange unit runs on, at most Batch
	// changes per run.
	Table string   `json:"table,omitempty"`
	Ops   []string `json:"ops,omitempty"`
	Batch int      `json:"batch,omitempty"`
}

// DeployRequest is what Units.Deploy publishes.
type DeployRequest struct {
	// Bundle is the JavaScript bundle (see "Bundle format" in the
	// engine's docs/units.md): a script that sets
	// globalThis.__azora_units.
	Bundle string `json:"bundle"`
	// SourceMap (optional) maps stack traces in the run log back to the
	// source.
	SourceMap string `json:"source_map,omitempty"`
	// Note is shown in the deploy history.
	Note string `json:"note,omitempty"`
	// DryRun checks the bundle and lists its units without publishing.
	DryRun bool `json:"dry_run,omitempty"`
}

// Deploy is the outcome of Units.Deploy.
type Deploy struct {
	// Version is the deploy's version (the active one when Unchanged).
	Version int64  `json:"version"`
	SHA256  string `json:"sha256"`
	// Unchanged reports a bundle identical to the active deploy: no new
	// version was made.
	Unchanged bool   `json:"unchanged"`
	DryRun    bool   `json:"dry_run"`
	Units     []Unit `json:"units"`
}

// UnitList is the active deploy's units (Units.List, Units.Activate).
type UnitList struct {
	// Version is the active deploy (0: nothing published).
	Version int64  `json:"version"`
	Units   []Unit `json:"units"`
}

// DeployInfo is one entry of the deploy history.
type DeployInfo struct {
	Version int64     `json:"version"`
	At      time.Time `json:"at"`
	// By is the principal that published it.
	By     string `json:"by"`
	Note   string `json:"note"`
	SHA256 string `json:"sha256"`
	// Size is the bundle's size in bytes and Units the number of units
	// in it.
	Size   int64 `json:"size"`
	Units  int   `json:"units"`
	Active bool  `json:"active"`
}

// UnmarshalJSON decodes the wire form (times in unix milliseconds).
func (d *DeployInfo) UnmarshalJSON(b []byte) error {
	type plain DeployInfo
	w := struct {
		*plain
		At int64 `json:"at"`
	}{plain: (*plain)(d)}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	d.At = fromMillis(w.At)
	return nil
}

// MarshalJSON encodes the wire form.
func (d DeployInfo) MarshalJSON() ([]byte, error) {
	type plain DeployInfo
	return json.Marshal(struct {
		plain
		At int64 `json:"at"`
	}{plain(d), toMillis(d.At)})
}

// DeployHistory is the outcome of Units.Deploys.
type DeployHistory struct {
	// Active is the version calls run on (0: nothing published).
	Active  int64        `json:"active"`
	Deploys []DeployInfo `json:"deploys"`
}

// Run is one entry of the run log.
type Run struct {
	ID   int64     `json:"id"`
	At   time.Time `json:"at"`
	Unit string    `json:"unit"`
	Kind string    `json:"kind"`
	// Trigger is "call", "schedule", "change" or "admin".
	Trigger string `json:"trigger"`
	// Principal is who ran it (key:<name>, a token's subject,
	// admin:<id>, system).
	Principal string        `json:"principal"`
	Duration  time.Duration `json:"duration_ms"`
	// Status is "ok", "error" or "replayed".
	Status string `json:"status"`
	Error  string `json:"error"`
	// Logs is what the unit logged with ctx.log (the server keeps the
	// first few KiB).
	Logs string `json:"logs"`
}

// UnmarshalJSON decodes the wire form (times in unix milliseconds).
func (r *Run) UnmarshalJSON(b []byte) error {
	type plain Run
	w := struct {
		*plain
		At         int64   `json:"at"`
		DurationMS float64 `json:"duration_ms"`
	}{plain: (*plain)(r)}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	r.At = fromMillis(w.At)
	r.Duration = time.Duration(w.DurationMS * float64(time.Millisecond))
	return nil
}

// MarshalJSON encodes the wire form.
func (r Run) MarshalJSON() ([]byte, error) {
	type plain Run
	return json.Marshal(struct {
		plain
		At         int64   `json:"at"`
		DurationMS float64 `json:"duration_ms"`
	}{plain(r), toMillis(r.At), float64(r.Duration) / float64(time.Millisecond)})
}

// RunsQuery selects runs from the run log (Units.Runs).
type RunsQuery struct {
	// Limit is the most runs returned (0: the server's default).
	Limit int `json:"limit,omitempty"`
	// Unit keeps the runs of one unit.
	Unit string `json:"unit,omitempty"`
	// Before pages back: only runs with a smaller ID (pass the ID of the
	// last run of the previous page). After keeps the runs with a larger
	// ID (to follow the log).
	Before int64 `json:"before,omitempty"`
	After  int64 `json:"after,omitempty"`
	// Status keeps the runs with this status ("ok", "error",
	// "replayed").
	Status string `json:"status,omitempty"`
}

// Trigger is the state of a schedule or onChange unit.
type Trigger struct {
	Unit string `json:"unit"`
	Kind string `json:"kind"`
	// Spec is the cron expression, interval or table it runs on.
	Spec string `json:"spec"`
	Mode string `json:"mode"`
	// NextAt and LastAt are the next and last runs (zero: none).
	NextAt time.Time `json:"next_at"`
	LastAt time.Time `json:"last_at"`
	// Cursor is an onChange unit's position in the change log and
	// Pending the changes after it it has not processed yet.
	Cursor  int64 `json:"cursor"`
	Pending int64 `json:"pending"`
	// Failures counts consecutive failed runs; Error is the last one's.
	Failures int    `json:"failures"`
	Error    string `json:"error"`
	// Lagged reports an onChange unit that the change log dropped
	// changes for.
	Lagged bool `json:"lagged"`
	// Running reports a run in progress.
	Running bool `json:"running"`
}

// UnmarshalJSON decodes the wire form (times in unix milliseconds).
func (t *Trigger) UnmarshalJSON(b []byte) error {
	type plain Trigger
	w := struct {
		*plain
		NextAt int64 `json:"next_at"`
		LastAt int64 `json:"last_at"`
	}{plain: (*plain)(t)}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	t.NextAt, t.LastAt = fromMillis(w.NextAt), fromMillis(w.LastAt)
	return nil
}

// MarshalJSON encodes the wire form.
func (t Trigger) MarshalJSON() ([]byte, error) {
	type plain Trigger
	return json.Marshal(struct {
		plain
		NextAt int64 `json:"next_at"`
		LastAt int64 `json:"last_at"`
	}{plain(t), toMillis(t.NextAt), toMillis(t.LastAt)})
}

// Secret is a secret's name; values are never read back.
type Secret struct {
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// UnmarshalJSON decodes the wire form (times in unix milliseconds).
func (s *Secret) UnmarshalJSON(b []byte) error {
	type plain Secret
	w := struct {
		*plain
		UpdatedAt int64 `json:"updated_at"`
	}{plain: (*plain)(s)}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	s.UpdatedAt = fromMillis(w.UpdatedAt)
	return nil
}

// MarshalJSON encodes the wire form.
func (s Secret) MarshalJSON() ([]byte, error) {
	type plain Secret
	return json.Marshal(struct {
		plain
		UpdatedAt int64 `json:"updated_at"`
	}{plain(s), toMillis(s.UpdatedAt)})
}

// fromMillis converts unix milliseconds, 0 being the zero time.
func fromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// toMillis is fromMillis reversed.
func toMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// Deploy publishes a bundle: all its units at once, as a new version
// that calls run on from now on. Publishing the bundle that is already
// active makes no new version (Deploy.Unchanged).
func (u *Units) Deploy(ctx context.Context, req DeployRequest) (*Deploy, error) {
	if req.Bundle == "" {
		return nil, errors.New("azora: empty bundle")
	}
	var out Deploy
	if err := u.admin(ctx, once, "deploy", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// List returns the active deploy's units.
func (u *Units) List(ctx context.Context) (*UnitList, error) {
	var out UnitList
	if err := u.admin(ctx, retryable, "list", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Deploys returns the deploy history, newest first (limit 0: the
// server's default).
func (u *Units) Deploys(ctx context.Context, limit int) (*DeployHistory, error) {
	var out DeployHistory
	body := struct {
		Limit int `json:"limit,omitempty"`
	}{limit}
	if err := u.admin(ctx, retryable, "deploys", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Activate makes a kept deploy the active one again (a rollback).
func (u *Units) Activate(ctx context.Context, version int64) (*UnitList, error) {
	var out UnitList
	body := struct {
		Version int64 `json:"version"`
	}{version}
	if err := u.admin(ctx, once, "activate", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Call runs a unit as the caller, internal units included (for testing
// and operations), and returns its result as JSON. A unit that throws
// returns a *UnitError. Unlike DB.Call it carries no mutation id and is
// not sent again after a lost connection.
func (u *Units) Call(ctx context.Context, unit string, args any) (CallResult, error) {
	if unit == "" {
		return CallResult{}, errors.New("azora: empty unit name")
	}
	a, err := encodeArgs(args)
	if err != nil {
		return CallResult{}, fmt.Errorf("azora: unit %s: %w", unit, err)
	}
	body := struct {
		Unit string          `json:"unit"`
		Args json.RawMessage `json:"args,omitempty"`
	}{unit, json.RawMessage(a)}
	var out struct {
		OK       bool            `json:"ok"`
		Value    json.RawMessage `json:"value"`
		Replayed bool            `json:"replayed"`
		Error    *struct {
			Code    string          `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := u.admin(ctx, once, "call", body, &out); err != nil {
		return CallResult{}, err
	}
	if !out.OK {
		res := CallResult{Replayed: out.Replayed}
		if out.Error == nil {
			return res, unitError(unit, UnitCodeFailed, "the unit failed", "")
		}
		return res, unitError(unit, out.Error.Code, out.Error.Message, string(out.Error.Data))
	}
	if len(out.Value) == 0 {
		out.Value = json.RawMessage("null")
	}
	return CallResult{Value: out.Value, Replayed: out.Replayed}, nil
}

// Runs returns entries of the run log, newest first.
func (u *Units) Runs(ctx context.Context, q RunsQuery) ([]Run, error) {
	var out struct {
		Runs []Run `json:"runs"`
	}
	if err := u.admin(ctx, retryable, "runs", q, &out); err != nil {
		return nil, err
	}
	return out.Runs, nil
}

// Triggers returns the state of the schedule and onChange units.
func (u *Units) Triggers(ctx context.Context) ([]Trigger, error) {
	var out struct {
		Triggers []Trigger `json:"triggers"`
	}
	if err := u.admin(ctx, retryable, "triggers", struct{}{}, &out); err != nil {
		return nil, err
	}
	return out.Triggers, nil
}

// SkipTrigger skips a trigger's pending work: an onChange unit moves past
// the changes it has not processed (skipped counts them).
func (u *Units) SkipTrigger(ctx context.Context, unit string) (skipped int64, err error) {
	var out struct {
		Skipped int64 `json:"skipped"`
	}
	body := struct {
		Unit string `json:"unit"`
	}{unit}
	if err := u.admin(ctx, once, "triggers.skip", body, &out); err != nil {
		return 0, err
	}
	return out.Skipped, nil
}

// Secrets returns the facade over the units' secrets (ctx.env).
func (u *Units) Secrets() *Secrets { return &Secrets{u: u} }

// Secrets manages the named values units read as ctx.env.NAME. They are
// stored encrypted and never read back. Get one with Units.Secrets.
type Secrets struct{ u *Units }

// List returns the secrets' names.
func (s *Secrets) List(ctx context.Context) ([]Secret, error) {
	var out struct {
		Secrets []Secret `json:"secrets"`
	}
	if err := s.u.admin(ctx, retryable, "secrets.list", struct{}{}, &out); err != nil {
		return nil, err
	}
	return out.Secrets, nil
}

// Set creates or replaces a secret.
func (s *Secrets) Set(ctx context.Context, name, value string) error {
	body := struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{name, value}
	return s.u.admin(ctx, once, "secrets.set", body, nil)
}

// Unset removes a secret; removed is false when there was none.
func (s *Secrets) Unset(ctx context.Context, name string) (removed bool, err error) {
	var out struct {
		Removed bool `json:"removed"`
	}
	body := struct {
		Name string `json:"name"`
	}{name}
	if err := s.u.admin(ctx, once, "secrets.unset", body, &out); err != nil {
		return false, err
	}
	return out.Removed, nil
}

// admin sends one UnitAdmin operation and decodes its result into out
// (nil: ignored).
func (u *Units) admin(ctx context.Context, p policy, op string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("azora: units %s: %w", op, err)
	}
	if !u.db.supports("units") {
		return ErrUnitsUnsupported
	}
	m, err := u.db.call(ctx, callOpts{policy: p}, func(id uint32) azp.Msg {
		return &azp.UnitAdmin{ReqID: id, Op: op, Body: string(b)}
	})
	if err != nil {
		return err
	}
	r, ok := m.(*azp.UnitAdminResult)
	if !ok {
		return fmt.Errorf("azora: unexpected answer type %d", m.Type())
	}
	if out == nil || r.Body == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(r.Body), out); err != nil {
		return fmt.Errorf("azora: units %s: decode the result: %w", op, err)
	}
	return nil
}
