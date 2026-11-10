package azora

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
	"github.com/stretchr/testify/require"
)

func TestIsDDL(t *testing.T) {
	for q, want := range map[string]bool{
		"CREATE TABLE t(a)":                     true,
		"  alter table t add b":                 true,
		"Drop Table t":                          true,
		"-- c\n/* d */ ;CREATE INDEX i ON t(a)": true,
		"INSERT INTO t VALUES (1)":              false,
		"created_at":                            false, // a longer word
		"DROPS":                                 false,
		"/* unterminated CREATE":                false,
		"-- only a comment":                     false,
		"":                                      false,
		"WITH x AS (SELECT 1) SELECT 1":         false,
		"crea":                                  false,
	} {
		require.Equal(t, want, isDDL(q), q)
	}
}

type valuer struct {
	v   driver.Value
	err error
}

func (v valuer) Value() (driver.Value, error) { return v.v, v.err }

type valuerOfValuer struct{}

func (valuerOfValuer) Value() (driver.Value, error) { return valuer{v: 1}, nil }

func TestNormalizeValuers(t *testing.T) {
	got, err := Normalize(valuer{v: int64(3)})
	require.NoError(t, err)
	require.Equal(t, int64(3), got)
	var nilValuer *valuer
	got, err = Normalize(nilValuer)
	require.NoError(t, err)
	require.Nil(t, got)
	_, err = Normalize(valuer{err: errors.New("bad")})
	require.EqualError(t, err, "bad")
	_, err = Normalize(valuerOfValuer{})
	require.Error(t, err)
	_, err = normalizeAll([]any{1, struct{}{}})
	require.ErrorContains(t, err, "argument 2")
	vs, err := normalizeAll(nil)
	require.NoError(t, err)
	require.Nil(t, vs)
}

func TestFilterWire(t *testing.T) {
	f := And(Eq("a", 1), Ne("b", int8(2)), Lt("c", 3.5), Le("d", "x"), Gt("e", uint32(4)), Ge("f", []byte{1}),
		In("g", 1, 2), NotIn("h", "x"), IsNull("i"), NotNull("j"), Prefix("k", "p/"), Or(Eq("l", true)), Not(Eq("m", nil)))
	w, err := f.wire(0)
	require.NoError(t, err)
	require.Len(t, w.Args, 13)
	ops := make([]string, len(w.Args))
	for i, a := range w.Args {
		ops[i] = a.Op
	}
	require.Equal(t, []string{"=", "!=", "<", "<=", ">", ">=", "in", "not_in", "is_null", "not_null", "prefix", "or", "not"}, ops)
	require.Equal(t, int64(1), w.Args[0].Val)
	require.Equal(t, int64(2), w.Args[1].Val)
	require.Equal(t, int64(4), w.Args[4].Val)
	require.Equal(t, []any{int64(1), int64(2)}, w.Args[6].Vals)
	require.Nil(t, w.Args[8].Vals, "no list for a leaf without values")
	// Encodable as is.
	_, err = azp.Encode(&azp.Subscribe{Where: w})
	require.NoError(t, err)

	nested := Eq("a", 1)
	for i := 0; i < 17; i++ {
		nested = Not(nested)
	}
	_, err = nested.wire(0)
	require.ErrorContains(t, err, "deeper than 16")
	bad := Or(Eq("a", 1), In("b", struct{}{}))
	_, err = bad.wire(0)
	require.Error(t, err)
	bad = Eq("a", make(chan int))
	_, err = bad.wire(0)
	require.Error(t, err)
	var none *Filter
	w, err = none.wire(0)
	require.NoError(t, err)
	require.Nil(t, w)
}

func TestNamesAndErrors(t *testing.T) {
	require.Equal(t, "insert", OpInsert.String())
	require.Equal(t, "update", OpUpdate.String())
	require.Equal(t, "delete", OpDelete.String())
	require.Equal(t, "Op(9)", Op(9).String())
	for k, n := range map[Kind]string{KindDatabase: "database", KindTable: "table", KindRow: "row", KindFiles: "files", KindNotify: "notify", 0: "Kind(0)"} {
		require.Equal(t, n, k.String())
	}
	id1, id2 := NewMutationID(), NewMutationID()
	require.NotEqual(t, id1, id2)
	require.True(t, strings.HasPrefix(id1, "m-"))
	require.Len(t, id1, 34)

	e := fromWire(&azp.Error{Code: CodePermissionDenied, Message: "no", Fatal: true, RetryAfterMS: 1500})
	require.Equal(t, "azora: permission_denied: no", e.Error())
	require.Equal(t, 1500*time.Millisecond, e.RetryAfter)
	wrapped := fmt.Errorf("op: %w", e)
	require.True(t, IsPermissionDenied(wrapped))
	require.False(t, IsNotFound(wrapped))
	require.True(t, IsUnauthenticated(&Error{Code: CodeUnauthenticated}))
	require.True(t, IsInvalid(&Error{Code: CodeInvalid}))
	require.True(t, IsNotFound(&Error{Code: CodeNotFound}))
	require.False(t, IsCode(errors.New("x"), CodeInvalid))

	for code, perm := range map[string]bool{CodeUnauthenticated: true, CodePermissionDenied: true, CodeInvalid: true,
		CodeNotFound: true, CodeUnavailable: false, CodeResourceLimit: false} {
		require.Equal(t, perm, permanent(fmt.Errorf("hello: %w", &Error{Code: code})), code)
	}
	require.False(t, permanent(errors.New("dial: refused")))
	require.False(t, permanent(nil))

	cause := errors.New("reset")
	le := &lostError{sent: true, err: cause}
	require.ErrorIs(t, le, cause)
	require.Equal(t, "connection lost: reset", le.Error())
}

func TestBackoff(t *testing.T) {
	c := &DB{cfg: Config{ReconnectMin: 10 * time.Millisecond, ReconnectMax: 80 * time.Millisecond}}
	for n := 0; n < 40; n++ {
		d := c.backoff(n)
		require.Greater(t, d, time.Duration(0))
		cap := 10 * time.Millisecond << min(n, 3)
		require.LessOrEqual(t, d, cap, "attempt %d", n)
	}
}
