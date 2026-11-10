package azora

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/azora-db/azora-go/internal/azp"
)

// Codes a unit throws with UnitError (UnitError.Code). A unit that throws
// anything else reaches the caller with UnitCodeFailed.
const (
	UnitCodeInvalid          = "invalid"
	UnitCodeNotFound         = "not_found"
	UnitCodePermissionDenied = "permission_denied"
	UnitCodeConflict         = "conflict"
	UnitCodeFailed           = "failed"
)

// ErrUnitsUnsupported is returned by Call and the Units methods when the
// server's discovery document (/.well-known/azora.json) does not list
// "units" among its features: the server is too old for units, or has
// them turned off. It is only known when the DB read that document (a
// URL without ?key=); otherwise the server itself answers.
var ErrUnitsUnsupported = errors.New(`azora: the server does not support units (its /.well-known/azora.json does not list "units" in features): upgrade the engine`)

// UnitError is the error a unit threw: Call returns it when the unit
// ran and failed, as opposed to an *Error, which reports a failure around
// the unit (no such unit, arguments that do not match its schema, a
// timeout, a credential that may not call it). Match it with errors.As:
//
//	var ue *azora.UnitError
//	if errors.As(err, &ue) && ue.Code == azora.UnitCodeNotFound { … }
type UnitError struct {
	// Unit is the unit that was called.
	Unit string
	// Code is one of the UnitCode constants.
	Code    string
	Message string
	// Data is the JSON value the unit attached to the error (nil for
	// none).
	Data json.RawMessage
}

// Error formats the error as "azora: unit <name>: <code>: <message>".
func (e *UnitError) Error() string {
	return "azora: unit " + e.Unit + ": " + e.Code + ": " + e.Message
}

// Bytes is a byte slice that travels in a unit's arguments and results:
// it encodes to JSON as {"$bytes": "<base64>"}, the form units read as a
// Uint8Array and return blobs in. A nil Bytes encodes as null.
type Bytes []byte

// MarshalJSON encodes b as {"$bytes": "<base64>"}.
func (b Bytes) MarshalJSON() ([]byte, error) {
	if b == nil {
		return []byte("null"), nil
	}
	out := make([]byte, 0, base64.StdEncoding.EncodedLen(len(b))+13)
	out = append(out, `{"$bytes":"`...)
	out = base64.StdEncoding.AppendEncode(out, b)
	return append(out, `"}`...), nil
}

// UnmarshalJSON decodes {"$bytes": "<base64>"}, null (nil) or, as
// encoding/json writes a []byte, a base64 string.
func (b *Bytes) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if string(data) == "null" {
		*b = nil
		return nil
	}
	var s string
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
	} else {
		var w struct {
			B *string `json:"$bytes"`
		}
		if err := json.Unmarshal(data, &w); err != nil {
			return fmt.Errorf("azora: Bytes: %w", err)
		}
		if w.B == nil {
			return errors.New(`azora: Bytes: want {"$bytes": "<base64>"}`)
		}
		s = *w.B
	}
	v, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		if v, err = base64.RawStdEncoding.DecodeString(s); err != nil {
			return fmt.Errorf("azora: Bytes: %w", err)
		}
	}
	*b = v
	return nil
}

// CallOption tunes Call.
type CallOption func(*callConfig)

type callConfig struct {
	mutationID string
	replayed   *bool
}

// MutationID gives a call a mutation id of the caller's choosing (1 to
// MaxMutationIDLen bytes) instead of a random one. A mutation called
// again with the same id within the server's window (24h) returns the
// first result without running again; an application that stores the id
// with a queued call can resend it after a restart of its own.
func MutationID(id string) CallOption { return func(c *callConfig) { c.mutationID = id } }

// Replayed makes Call report in *replayed whether the result is the
// stored answer of an earlier call with the same mutation id (the unit
// did not run again).
func Replayed(replayed *bool) CallOption { return func(c *callConfig) { c.replayed = replayed } }

// CallResult is what CallRaw returns.
type CallResult struct {
	// Value is the JSON text of the unit's result ("null" when it
	// returned nothing).
	Value json.RawMessage
	// Replayed reports a mutation answered from its first run with the
	// same mutation id: it did not run again.
	Replayed bool
}

// CallRaw calls a published unit by name and returns its result as JSON.
// args is encoded with encoding/json (nil: no arguments); use Bytes for
// binary values.
//
// Every call carries a mutation id, random unless MutationID gives one,
// and is sent again with the same id after a lost connection, so a
// mutation runs exactly once even when an answer is lost.
//
// A unit that throws returns a *UnitError. Failures around the unit are
// an *Error: CodeNotFound (no such unit, or nothing published),
// CodePermissionDenied (an internal unit), CodeInvalid (arguments that do
// not match, a value too large), CodeResourceLimit (a timeout or a
// limit), CodeUnavailable (units disabled).
func (db *DB) CallRaw(ctx context.Context, unit string, args any, opts ...CallOption) (CallResult, error) {
	var c callConfig
	for _, o := range opts {
		o(&c)
	}
	if c.replayed != nil {
		*c.replayed = false
	}
	if unit == "" {
		return CallResult{}, errors.New("azora: empty unit name")
	}
	mid := c.mutationID
	if mid == "" {
		mid = NewMutationID()
	} else if err := checkMutationID(mid); err != nil {
		return CallResult{}, err
	}
	a, err := encodeArgs(args)
	if err != nil {
		return CallResult{}, fmt.Errorf("azora: unit %s: %w", unit, err)
	}
	if !db.supports("units") {
		return CallResult{}, ErrUnitsUnsupported
	}
	m, err := db.call(ctx, callOpts{}, func(id uint32) azp.Msg {
		return &azp.Call{ReqID: id, Unit: unit, Args: a, MutationID: mid}
	})
	if err != nil {
		return CallResult{}, err
	}
	r, ok := m.(*azp.Returned)
	if !ok {
		return CallResult{}, fmt.Errorf("azora: unexpected answer type %d", m.Type())
	}
	if c.replayed != nil {
		*c.replayed = r.Replayed
	}
	if !r.OK {
		return CallResult{Replayed: r.Replayed}, unitError(unit, r.ErrCode, r.ErrMessage, r.ErrData)
	}
	v := json.RawMessage(r.Value)
	if len(v) == 0 {
		v = json.RawMessage("null")
	}
	return CallResult{Value: v, Replayed: r.Replayed}, nil
}

// Call calls a published unit by name (see CallRaw) and decodes its
// result into out with encoding/json (a nil out discards it):
//
//	var res CheckoutResult
//	err := db.Call(ctx, "checkout", CheckoutArgs{CartID: "c1"}, &res)
//
// Numbers travel as JSON numbers: integers beyond 2^53 lose precision.
func (db *DB) Call(ctx context.Context, unit string, args any, out any, opts ...CallOption) error {
	res, err := db.CallRaw(ctx, unit, args, opts...)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(res.Value, out); err != nil {
		return fmt.Errorf("azora: unit %s: decode the result: %w", unit, err)
	}
	return nil
}

// Call calls a published unit by name and returns its result decoded
// into R (see DB.CallRaw):
//
//	res, err := azora.Call[CheckoutResult](ctx, db, "checkout", CheckoutArgs{CartID: "c1"})
func Call[R any](ctx context.Context, db *DB, unit string, args any, opts ...CallOption) (R, error) {
	var r R
	err := db.Call(ctx, unit, args, &r, opts...)
	return r, err
}

// encodeArgs returns the JSON text of a unit's arguments ("" for none).
func encodeArgs(args any) (string, error) {
	if args == nil {
		return "", nil
	}
	if raw, ok := args.(json.RawMessage); ok && raw == nil {
		return "", nil
	}
	b, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("encode the arguments: %w", err)
	}
	return string(b), nil
}

// unitError builds the error a unit threw.
func unitError(unit, code, message, data string) *UnitError {
	e := &UnitError{Unit: unit, Code: code, Message: message}
	if data != "" && data != "null" {
		e.Data = json.RawMessage(data)
	}
	return e
}
