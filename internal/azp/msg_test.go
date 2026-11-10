package azp

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func sampleMsgs() []Msg {
	vals := []any{nil, int64(-5), int64(math.MaxInt64), 2.5, math.Inf(-1), "héllo", []byte{}, []byte{0, 255}, true}
	f := &Filter{Op: "and", Args: []Filter{
		{Op: "=", Col: "a", Val: int64(1)},
		{Op: "in", Col: "b", Vals: []any{"x", []byte{1}}},
		{Op: "not", Args: []Filter{{Op: "is_null", Col: "c"}}},
	}}
	return []Msg{
		&Hello{Version: 1, Database: "orders", Credential: "azt1.x.y", Client: "azora-go/1"},
		&Welcome{SessionID: "s1", ServerTimeMS: 1700000000000, HeartbeatMS: 15000, MaxMessage: 16 << 20, Principal: "user:42", Scopes: []string{"read", "write"}, MaxSubscriptions: 100},
		&Error{ReqID: 7, Code: CodeGone, Message: "resync", Fatal: true, RetryAfterMS: 250},
		&Ping{Nonce: 99}, &Pong{Nonce: 99}, &OK{ReqID: 3},
		&Auth{ReqID: 2, Credential: "new"},
		&Subscribe{ReqID: 1, SubID: 5, Kind: KindTable, Table: "t", Key: []any{int64(1), "k"}, Where: f, Ops: 3, Since: 1 << 40, Epoch: -9, NoSnapshot: true, Bucket: "b", PathPrefix: "p/", Tables: []string{"a", "b"}},
		&Subscribe{ReqID: 2, SubID: 6, Kind: KindNotify},
		&Subscribed{ReqID: 1, SubID: 5},
		&Unsubscribe{ReqID: 4, SubID: 5},
		&Shape{Table: "t", TV: 3, Columns: []string{"id", "v"}, PK: []string{"id"}},
		&Batch{SubID: 5, Snapshot: true, Reset: true, Checkpoint: 42, Epoch: 7, Tables: []string{"x"}, Changes: []Change{
			{Seq: 41, Table: "t", TV: 3, Op: OpUpdate, Key: []any{int64(1)}, New: vals, Old: []any{}},
			{Seq: 42, Table: "t", TV: 3, Op: OpInsert, Key: []any{int64(2)}, New: []any{int64(2), "v"}},
		}},
		&Batch{SubID: 1, Checkpoint: 9},
		&SubEnd{SubID: 5, Code: CodeGone, Message: "schema changed"},
		&Ack{SubID: 5, Checkpoint: 42},
		&Query{ReqID: 8, SQL: "SELECT ?", Args: vals, MaxRows: 1000},
		&Exec{ReqID: 9, MutationID: "m-1", SQL: "INSERT INTO t VALUES (?)", Args: []any{int64(1)}},
		&Tx{ReqID: 10, MutationID: "m-2", Statements: []Statement{{SQL: "A", Args: []any{int64(1)}}, {SQL: "B"}}},
		&Result{ReqID: 8, Columns: []string{"a"}, Rows: [][]any{{int64(1)}, {}}, Truncated: true, RowsAffected: 3, LastInsertID: -1, Replayed: true},
		&Verify{ReqID: 11, SubID: 5},
		&Verified{ReqID: 11, SubID: 5, Epoch: 7, Checkpoint: 42, Rows: 10, Sum: math.MaxUint64},
		&Sign{ReqID: 12, ObjectID: "o1", TTLSeconds: 60},
		&Signed{ReqID: 12, URL: "https://x/o/db/o1?sig=1", ExpiresAtMS: 1},
		&GoAway{Reason: "shutdown", RetryAfterMS: 1000},
		&Call{ReqID: 13, Unit: "checkout", Args: `{"cartId":"c1"}`, MutationID: "m-3"},
		&Call{ReqID: 14, Unit: "ping"},
		&Returned{ReqID: 13, OK: true, Value: `{"orderId":7}`, Replayed: true},
		&Returned{ReqID: 14, ErrCode: "not_found", ErrMessage: "no such cart", ErrData: `{"cart":"c1"}`},
		&UnitAdmin{ReqID: 15, Op: "deploy", Body: `{"bundle":"x"}`},
		&UnitAdminResult{ReqID: 15, Body: `{"version":1}`},
	}
}

func TestMessagesRoundTrip(t *testing.T) {
	seen := map[MsgType]bool{}
	for _, m := range sampleMsgs() {
		b, err := Encode(m)
		require.NoError(t, err)
		require.Equal(t, byte(m.Type()), b[0])
		got, err := Decode(b)
		require.NoError(t, err, "%T", m)
		require.Equal(t, m, got, "%T", m)
		seen[m.Type()] = true
		// Every strict prefix fails to decode (no silent truncation).
		for n := 1; n < len(b); n++ {
			_, err := Decode(b[:n])
			require.ErrorIs(t, err, ErrMalformed, "%T truncated to %d", m, n)
		}
		// Trailing bytes are refused.
		_, err = Decode(append(append([]byte{}, b...), 0))
		require.ErrorIs(t, err, ErrMalformed)
	}
	for ty := TypeHello; ty <= TypeLast; ty++ {
		require.True(t, seen[ty], "message type %d has a round-trip case", ty)
	}
	_, err := Decode(nil)
	require.ErrorIs(t, err, ErrMalformed)
	_, err = Decode([]byte{200})
	require.ErrorIs(t, err, ErrMalformed)
	_, err = Encode(&Query{Args: []any{struct{}{}}})
	require.Error(t, err)
}

func TestDecodeRefusesHugeCounts(t *testing.T) {
	// A Result claiming 2^40 rows in a few bytes must fail without
	// allocating them.
	b := []byte{byte(TypeResult), 1, 0}
	b = append(b, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20)
	_, err := Decode(b)
	require.ErrorIs(t, err, ErrMalformed)
	// A deeply nested filter is refused.
	f := &Filter{Op: "=", Col: "a", Val: int64(1)}
	for i := 0; i < 40; i++ {
		f = &Filter{Op: "not", Args: []Filter{*f}}
	}
	enc, err := Encode(&Subscribe{Kind: KindTable, Table: "t", Where: f})
	require.NoError(t, err)
	_, err = Decode(enc)
	require.ErrorIs(t, err, ErrMalformed)
}

func TestRowHashGolden(t *testing.T) {
	// Fixed values, also computed by an independent Python implementation
	// of the documented byte layout: other SDKs check theirs against them.
	require.Equal(t, RowHash("T", []any{int64(1)}, []any{int64(1), "a", nil, 2.5, []byte{1}}),
		RowHash("t", []any{int64(1)}, []any{int64(1), "a", nil, 2.5, []byte{1}}), "table names are case-insensitive")
	require.NotEqual(t, RowHash("t", []any{int64(1)}, []any{"a"}), RowHash("t", []any{int64(1), "a"}, nil), "key/row boundary is hashed")
	require.Equal(t, uint64(0xa171adb8fb865d6d), RowHash("orders", []any{int64(7)}, []any{int64(7), "paid", 12.5}))
}

func FuzzDecode(f *testing.F) {
	for _, m := range sampleMsgs() {
		b, _ := Encode(m)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := Decode(b)
		if err != nil {
			return
		}
		// A decoded message re-encodes to something that decodes the same.
		b2, err := Encode(m)
		if err != nil {
			return // e.g. a decoded time value the encoder refuses
		}
		m2, err := Decode(b2)
		if err != nil {
			t.Fatalf("re-encoded %T does not decode: %v", m, err)
		}
		b3, _ := Encode(m2)
		if string(b2) != string(b3) {
			t.Fatalf("%T encoding is not stable", m)
		}
	})
}

// raw builds a message by hand: a type byte, then uvarints and raw bytes.
func raw(t MsgType, parts ...any) []byte {
	b := []byte{byte(t)}
	for _, p := range parts {
		switch x := p.(type) {
		case uint64:
			b = binary.AppendUvarint(b, x)
		case int:
			b = binary.AppendUvarint(b, uint64(x))
		case []byte:
			b = append(b, x...)
		case string:
			b = binary.AppendUvarint(b, uint64(len(x)))
			b = append(b, x...)
		}
	}
	return b
}

func TestDecodeRefusesMalformed(t *testing.T) {
	deep := &Filter{Op: "=", Col: "a", Val: int64(1)}
	for i := 0; i <= maxFilterDepth+1; i++ {
		deep = &Filter{Op: "not", Args: []Filter{*deep}}
	}
	wide := &Filter{Op: "and"}
	for i := 0; i < maxFilterNodes; i++ {
		wide.Args = append(wide.Args, Filter{Op: "=", Col: "a", Val: int64(i)})
	}
	encodeOK := func(m Msg) []byte {
		b, err := Encode(m)
		require.NoError(t, err)
		return b
	}
	ping := encodeOK(&Ping{Nonce: 1})
	why := map[string]string{
		"empty": "empty", "unknown type": "unknown type 0", "type 255": "unknown type 255", "trailing": "trailing bytes",
		"truncated": "corrupt", "hello version": "version 65536", "u32 overflow": "out of range", "bad flag": "bad flag 2",
		"subscribe kind": "kind 256", "subscribe ops": "ops 256", "batch op": "op 256", "filter deep": "filter too large",
		"filter wide": "filter too large", "filter missing": "missing filter argument", "string past end": "corrupt",
	}
	for name, b := range map[string][]byte{
		"empty":           nil,
		"unknown type":    {0},
		"type 255":        {255},
		"trailing":        append(append([]byte{}, ping...), 0),
		"truncated":       ping[:1],
		"hello version":   raw(TypeHello, uint64(1<<16), "db", "cred", "c"),
		"u32 overflow":    raw(TypeOK, uint64(1<<32)),
		"bad flag":        raw(TypeError, 1, "c", "m", []byte{2}, 0),
		"subscribe kind":  raw(TypeSubscribe, 1, 1, 256, "t", 0, []byte{0}, 0, 0, 0, []byte{0}, "", "", 0),
		"subscribe ops":   raw(TypeSubscribe, 1, 1, 2, "t", 0, []byte{0}, 256, 0, 0, []byte{0}, "", "", 0),
		"batch op":        raw(TypeBatch, 1, []byte{0, 0}, 0, 0, 0, 1, 1, "t", 0, 256, 0, 0, 0),
		"filter deep":     encodeOK(&Subscribe{Where: deep}),
		"filter wide":     encodeOK(&Subscribe{Where: wide}),
		"filter missing":  raw(TypeSubscribe, 1, 1, 2, "t", 0, []byte{1}, "and", "", []byte{0}, 0, 1, []byte{0}),
		"string past end": raw(TypeAuth, 1, uint64(100), []byte("short")),
	} {
		_, err := Decode(b)
		require.ErrorIs(t, err, ErrMalformed, name)
		require.Contains(t, err.Error(), why[name], name)
	}
}

func TestEncodeRefusesUnknownValues(t *testing.T) {
	_, err := Encode(&Query{SQL: "SELECT ?", Args: []any{struct{}{}}})
	require.Error(t, err)
	_, err = Encode(&Subscribe{Where: &Filter{Op: "=", Col: "a", Val: make(chan int)}})
	require.Error(t, err)
	e := &Error{Code: CodeGone, Message: "resync"}
	require.Equal(t, "gone: resync", e.Error())
	var err2 error = e
	require.EqualError(t, err2, "gone: resync")
}

func TestModeNames(t *testing.T) {
	require.Equal(t, "IK", ModeIK.String())
	require.Equal(t, "XX", ModeXX.String())
	require.Equal(t, "plaintext", ModePlaintext.String())
	require.Equal(t, "Mode(0x51)", Mode('Q').String())
}
