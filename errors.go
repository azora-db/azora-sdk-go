package azora

import (
	"errors"
	"fmt"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// Error codes reported by the server over the realtime protocol
// (Error.Code). The storage HTTP API has a few more, listed below them.
const (
	CodeInvalid          = azp.CodeInvalid          // bad request, constraint, SQL error
	CodeUnauthenticated  = azp.CodeUnauthenticated  // bad or expired credential
	CodePermissionDenied = azp.CodePermissionDenied // scope or token restriction
	CodeNotFound         = azp.CodeNotFound         // table, object or subscription
	CodeResourceLimit    = azp.CodeResourceLimit    // rate or size limit, throttling
	CodeGone             = azp.CodeGone             // resume point invalid
	CodeUnavailable      = azp.CodeUnavailable      // database closed, reopening, shutting down
	CodeConflict         = azp.CodeConflict
	CodeInternal         = azp.CodeInternal // server error (details only in the server log)
	CodeCanceled         = azp.CodeCanceled // the request was canceled
)

// Codes used only by the storage HTTP API (see Storage).
const (
	CodeForbidden        = "forbidden"         // the API key lacks the scope
	CodeInvalidAPIKey    = "invalid_api_key"   // unknown, revoked or expired key
	CodeInvalidSignature = "invalid_signature" // a signed link that does not verify
	CodeExpired          = "expired"           // a signed link past its expiry
	CodeTooLarge         = "too_large"         // over max_object_size or a grant's max_size
	CodeQuotaExceeded    = "quota_exceeded"    // bucket or database storage quota
	CodeOffsetMismatch   = "offset_mismatch"   // an append that does not start at the session's offset
	CodeAlreadyExists    = "already_exists"    // the path is taken
	CodeInUse            = "in_use"            // the object is referenced by a row
	CodeTypeNotAllowed   = "type_not_allowed"  // content type refused
	CodeChecksumMismatch = "checksum_mismatch" // content does not match the declared SHA-256 or size
	CodeThrottled        = "throttled"         // database limits; retry after RetryAfter
	CodeStorageDisabled  = "storage_disabled"  // file storage is off for the database
)

// Error is an error reported by the server: by the realtime protocol
// (queries, writes, subscriptions) or by the storage HTTP API. Match it
// with errors.As, or use IsCode and the Is* helpers.
type Error struct {
	// Code is one of the Code constants.
	Code    string
	Message string
	// Fatal errors end the session (the client reconnects).
	Fatal bool
	// RetryAfter is the server's retry hint (rate limits).
	RetryAfter time.Duration
	// HTTPStatus is the response status of a storage HTTP error (0 for
	// errors of the realtime protocol).
	HTTPStatus int
	// Limit and Max name the storage limit a too_large or quota_exceeded
	// error hit ("max_object_size", "grant_max_size", "bucket_max_bytes",
	// ...) and its value.
	Limit string
	Max   int64
}

// Error formats the error as "azora: <code>: <message>".
func (e *Error) Error() string { return "azora: " + e.Code + ": " + e.Message }

// Is makes errors.Is(err, ErrNotFound) and the other code sentinels match
// server errors by code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t != nil && t.Message == "" && t.Code == e.Code
}

// Sentinel errors matching server errors by code with errors.Is, for
// example errors.Is(err, azora.ErrPermissionDenied).
var (
	ErrInvalid          error = &Error{Code: CodeInvalid}
	ErrUnauthenticated  error = &Error{Code: CodeUnauthenticated}
	ErrPermissionDenied error = &Error{Code: CodePermissionDenied}
	ErrNotFound         error = &Error{Code: CodeNotFound}
	ErrResourceLimit    error = &Error{Code: CodeResourceLimit}
	ErrUnavailable      error = &Error{Code: CodeUnavailable}
)

func fromWire(e *azp.Error) *Error {
	return &Error{Code: e.Code, Message: e.Message, Fatal: e.Fatal, RetryAfter: time.Duration(e.RetryAfterMS) * time.Millisecond}
}

// IsCode reports whether err is (or wraps) a server *Error with code.
func IsCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// IsPermissionDenied reports a request the credential may not make: a
// missing scope, a token restriction, or (storage) an API key without the
// scope.
func IsPermissionDenied(err error) bool {
	return IsCode(err, CodePermissionDenied) || IsCode(err, CodeForbidden)
}

// IsUnauthenticated reports a rejected or expired credential.
func IsUnauthenticated(err error) bool {
	return IsCode(err, CodeUnauthenticated) || IsCode(err, CodeInvalidAPIKey)
}

// IsNotFound reports a missing table, object, session or subscription.
func IsNotFound(err error) bool { return IsCode(err, CodeNotFound) }

// IsInvalid reports a malformed request (bad SQL, constraint violation, ...).
func IsInvalid(err error) bool { return IsCode(err, CodeInvalid) }

// IsConflict reports a storage conflict: a path that is taken, an object
// that rows still reference, a session offset that does not match.
func IsConflict(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.Code {
	case CodeConflict, CodeAlreadyExists, CodeInUse, CodeOffsetMismatch:
		return true
	}
	return false
}

// IsRetryable reports server errors worth retrying later without changing
// the request: rate limits and an unavailable or busy database. The
// client already retries these while the call's context allows; the
// helper is for callers that ran out of that time.
func IsRetryable(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.Code {
	case CodeResourceLimit, CodeUnavailable, CodeThrottled, "busy", "timeout":
		return true
	}
	return e.RetryAfter > 0
}

var (
	// ErrClosed is returned by every method once the DB (or the
	// Subscription, LiveTable, Watcher) is closed.
	ErrClosed = errors.New("azora: closed")
	// ErrNoRows is returned by Get, One and Scalar when the query returned
	// no row.
	ErrNoRows = errors.New("azora: no rows in result set")
	// ErrResync is returned by Subscription.Next for a subscription with
	// Spec.ManualResync whose resume point is gone: close it and subscribe
	// again without Since.
	ErrResync = errors.New("azora: resync required")
	// ErrRetry is returned by Subscription.Verify when the server's
	// checksum is for a checkpoint the application has not applied yet:
	// call Next (the next update stops exactly at that checkpoint), apply
	// it, and call Verify again.
	ErrRetry = errors.New("azora: verify again after applying the pending update")
	// ErrUnknownOutcome is returned when a non-idempotent request (a schema
	// change, which runs without a mutation id) was sent but the
	// connection dropped before its answer: it may or may not have run.
	ErrUnknownOutcome = errors.New("azora: connection lost before the answer; the statement may have run")
	// ErrNoStorageURL is returned by Storage methods when the HTTP address
	// of the server is unknown: pass WithStorageURL, or connect over
	// WebSocket (the address is then derived from the WebSocket URL).
	ErrNoStorageURL = errors.New("azora: storage URL unknown: use WithStorageURL")
	// ErrHandshake is returned when the encrypted handshake fails, for
	// example because the server's key does not match the pinned key or
	// fingerprint.
	ErrHandshake = azp.ErrHandshake
	// ErrUnknownKey is returned when the server does not hold the pinned
	// key (it names keys by id; a pinned key the server never had, or one
	// it removed, fails with it).
	ErrUnknownKey = azp.ErrUnknownKey
	// ErrPlaintextRefused is returned when plaintext was requested but the
	// server does not allow it.
	ErrPlaintextRefused = azp.ErrPlaintextRefused
)

// lostError marks a request whose connection ended before its answer.
type lostError struct {
	sent bool // the request was (at least partly) written
	err  error
}

func (e *lostError) Error() string { return fmt.Sprintf("connection lost: %v", e.err) }
func (e *lostError) Unwrap() error { return e.err }

// permanent reports Hello rejections that a reconnect will not fix by
// itself (requests waiting for a connection fail with them).
func permanent(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.Code {
	case CodeUnauthenticated, CodePermissionDenied, CodeInvalid, CodeNotFound:
		return true
	}
	return false
}
