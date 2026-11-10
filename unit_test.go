package azora_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/azptest"
)

func testKey(t testing.TB) [32]byte {
	k, err := azp.GenerateKey(time.Now())
	require.NoError(t, err)
	return k.Public
}

func TestParseURL(t *testing.T) {
	pub := testKey(t)
	key := azp.EncodePublicKey(pub)
	fp := azp.Fingerprint(pub)

	cfg, err := azora.ParseURL("azora://db.example.com:7481/orders?key=" + key)
	require.NoError(t, err)
	require.Equal(t, "db.example.com:7481", cfg.Addr)
	require.Equal(t, "orders", cfg.Database)
	require.Equal(t, key, cfg.ServerKey)
	require.Empty(t, cfg.WebSocketURL)

	cfg, err = azora.ParseURL("azora://db.example.com/orders?fp=" + strings.ToUpper(fp))
	require.NoError(t, err)
	require.Equal(t, "db.example.com:7480", cfg.Addr, "default port")
	require.Equal(t, strings.ToUpper(fp), cfg.Fingerprint)

	cfg, err = azora.ParseURL("azora://[::1]:9000?db=app&plaintext=1")
	require.NoError(t, err)
	require.Equal(t, "[::1]:9000", cfg.Addr)
	require.Equal(t, "app", cfg.Database)
	require.True(t, cfg.AllowPlaintext)

	cfg, err = azora.ParseURL("azoras+ws://edge.example.com/rt/v1?db=orders&key=" + key)
	require.NoError(t, err)
	require.Equal(t, "wss://edge.example.com/rt/v1", cfg.WebSocketURL)
	require.Equal(t, "orders", cfg.Database)

	cfg, err = azora.ParseURL("azora+ws://127.0.0.1:8080?db=orders&key=" + key + "&fp=" + fp)
	require.NoError(t, err)
	require.Equal(t, "ws://127.0.0.1:8080/rt/v1", cfg.WebSocketURL, "default path")
	require.Equal(t, fp, cfg.Fingerprint)

	// Plain WebSocket URLs are accepted too.
	cfg, err = azora.ParseURL("wss://edge.example.com/custom/path?db=orders&key=" + key)
	require.NoError(t, err)
	require.Equal(t, "wss://edge.example.com/custom/path", cfg.WebSocketURL)
	cfg, err = azora.ParseURL("ws://127.0.0.1:8080?db=orders&plaintext=1")
	require.NoError(t, err)
	require.Equal(t, "ws://127.0.0.1:8080/rt/v1", cfg.WebSocketURL)
	require.True(t, cfg.AllowPlaintext)

	// Standard base64 with '+' (which a query decodes as a space) works.
	std := base64.StdEncoding.EncodeToString(pub[:])
	cfg, err = azora.ParseURL("azora://h/app?key=" + std)
	require.NoError(t, err)
	got, err := azora.DecodeServerKey(cfg.ServerKey)
	require.NoError(t, err)
	require.Equal(t, pub, got)

	for _, bad := range []string{
		"",
		"http://h/app?key=" + key,
		"azora:///app?key=" + key,
		"azora://h?key=" + key,                     // no database
		"azora://h/app",                            // no key, no fingerprint
		"azora://h/app?key=abc",                    // bad key
		"azora://h/app?fp=azp1-nope",               // bad fingerprint
		"azora://h/app?key=" + key + "&x=1",        // unknown parameter
		"azora://h/app?key=" + key + "&key=" + key, // repeated
		"azora://h/a/b?key=" + key,                 // not a database name
		"azora://h:99999/app?key=" + key,
		"azora://user:pw@h/app?key=" + key,
		"azora://h/app?db=other&key=" + key,
		"azora://h/app?plaintext=maybe",
		"azora+ws://h/rt/v1?key=" + key, // no db
		"wss://h/rt/v1?key=" + key,      // no db
		"https://h/rt/v1?db=x&key=" + key,
	} {
		_, err := azora.ParseURL(bad)
		require.Error(t, err, bad)
	}
}

func FuzzParseURL(f *testing.F) {
	key := azp.EncodePublicKey(testKey(f))
	for _, s := range []string{
		"azora://h:7480/app?key=" + key,
		"azoras+ws://h/rt/v1?db=x&fp=azp1-aaaa-bbbb-cccc-dddd-eeee-ffff",
		"azora+ws://h:1?db=x&plaintext=1",
		"azora://[::1]/a?plaintext=true",
		"azora://h/%zz",
		"wss://h/rt/v1?db=x&key=" + key,
		"ws://h?db=x&plaintext=1",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := azora.ParseURL(s)
		if err != nil {
			return
		}
		// A parsed config names a database, one destination, and a way to
		// authenticate the server.
		require.NotEmpty(t, cfg.Database)
		require.True(t, (cfg.Addr == "") != (cfg.WebSocketURL == ""))
		require.True(t, cfg.ServerKey != "" || cfg.Fingerprint != "" || cfg.AllowPlaintext)
		if cfg.ServerKey != "" {
			_, err := azora.DecodeServerKey(cfg.ServerKey)
			require.NoError(t, err)
		}
		if cfg.WebSocketURL != "" {
			require.True(t, strings.HasPrefix(cfg.WebSocketURL, "ws://") || strings.HasPrefix(cfg.WebSocketURL, "wss://"))
		}
	})
}

func TestProtocolConstants(t *testing.T) {
	// The values the wire protocol fixes (docs/protocol.md).
	require.Equal(t, "azora-rt/1", azptest.Prologue)
	require.Equal(t, "/rt/v1", azora.DefaultWebSocketPath)
	require.Equal(t, "7480", azora.DefaultPort)
	require.Equal(t, uint8(1), uint8(azora.KindDatabase))
	require.Equal(t, uint8(2), uint8(azora.KindTable))
	require.Equal(t, uint8(3), uint8(azora.KindRow))
	require.Equal(t, uint8(4), uint8(azora.KindFiles))
	require.Equal(t, uint8(5), uint8(azora.KindNotify))
	require.Equal(t, uint8(1), uint8(azora.OpInsert))
	require.Equal(t, uint8(2), uint8(azora.OpUpdate))
	require.Equal(t, uint8(3), uint8(azora.OpDelete))
	require.Equal(t, uint8(1), uint8(azora.OpsInsert))
	require.Equal(t, uint8(2), uint8(azora.OpsUpdate))
	require.Equal(t, uint8(4), uint8(azora.OpsDelete))
	// The checksum test vector of the protocol specification.
	require.Equal(t, uint64(0xa171adb8fb865d6d), azora.RowHash("orders", []any{int64(7)}, []any{int64(7), "paid", 12.5}))
}

type myInt int16
type myStr string
type myBytes []byte

func TestNormalize(t *testing.T) {
	now := time.Now()
	var nilPtr *int
	five := 5
	cases := []struct {
		in, want any
	}{
		{nil, nil}, {int(3), int64(3)}, {int8(-3), int64(-3)}, {uint16(9), int64(9)}, {uint64(1 << 40), int64(1 << 40)},
		{float32(1.5), 1.5}, {"x", "x"}, {[]byte{1}, []byte{1}}, {true, true}, {now, now}, {myInt(4), int64(4)},
		{myStr("s"), "s"}, {nilPtr, nil}, {&five, int64(5)}, {[]byte(nil), nil}, {[]byte{}, []byte{}},
		{json.RawMessage(`{"a":1}`), `{"a":1}`}, {myBytes{2}, []byte{2}}, {azora.JSON[[]int]{V: []int{1, 2}}, "[1,2]"},
	}
	for _, c := range cases {
		got, err := azora.Normalize(c.in)
		require.NoError(t, err, "%T", c.in)
		require.Equal(t, c.want, got, "%T", c.in)
	}
	_, err := azora.Normalize(uint64(1 << 63))
	require.Error(t, err)
	_, err = azora.Normalize(struct{}{})
	require.Error(t, err)
	_, err = azora.Normalize([]int{1})
	require.Error(t, err)
}

func TestReplicaApply(t *testing.T) {
	r := azora.NewReplica()
	cols := []string{"id", "v"}
	r.Apply(azora.Update{Reset: true, Snapshot: true, Changes: []azora.Change{
		{Table: "T", Op: azora.OpInsert, Key: []any{int64(1)}, New: []any{int64(1), "a"}, Columns: cols, PK: []string{"id"}},
		{Table: "t", Op: azora.OpInsert, Key: []any{int64(2)}, New: []any{int64(2), "b"}},
	}})
	require.Equal(t, 2, r.Len("t"))
	require.Equal(t, []string{"t"}, r.Tables())
	row, ok := r.Get("T", 1) // int and int64 keys are the same row
	require.True(t, ok)
	require.Equal(t, []any{int64(1), "a"}, row)
	c, pk := r.Columns("t")
	require.Equal(t, cols, c)
	require.Equal(t, []string{"id"}, pk)

	r.Apply(azora.Update{Changes: []azora.Change{
		{Seq: 5, Table: "t", Op: azora.OpUpdate, Key: []any{int64(1)}, New: []any{int64(1), "z"}, Old: []any{int64(1), "a"}},
		{Seq: 6, Table: "t", Op: azora.OpDelete, Key: []any{int64(2)}, Old: []any{int64(2), "b"}},
		{Seq: 7, Table: "u", Op: azora.OpInsert, Key: []any{"k"}, New: []any{"k", nil}},
	}})
	n, sum := r.Checksum()
	require.Equal(t, int64(2), n)
	want := azora.RowHash("T", []any{int64(1)}, []any{int64(1), "z"}) + azora.RowHash("u", []any{"k"}, []any{"k", nil})
	require.Equal(t, want, sum)
	require.Equal(t, want, azora.RowHash("t", []any{int64(1)}, []any{int64(1), "z"})+azora.RowHash("U", []any{"k"}, []any{"k", nil}),
		"table names are hashed in lowercase")
	seen := 0
	for k, v := range r.Rows("t") {
		require.Equal(t, []any{int64(1)}, k)
		require.Equal(t, "z", v[1])
		seen++
	}
	require.Equal(t, 1, seen)
	r.Apply(azora.Update{Reset: true})
	n, sum = r.Checksum()
	require.Zero(t, n)
	require.Zero(t, sum)
}

// FuzzReplica applies random changes to a replica and to a model map and
// checks rows and checksum agree.
func FuzzReplica(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{0, 0, 0, 255, 255, 3, 3, 3})
	f.Fuzz(func(t *testing.T, ops []byte) {
		r := azora.NewReplica()
		model := map[int64][]any{}
		for i := 0; i+1 < len(ops); i += 2 {
			id := int64(ops[i] % 16)
			table := "t"
			if ops[i]&0x80 != 0 {
				table = "T" // same table, other case
			}
			switch ops[i+1] % 4 {
			case 0, 1:
				row := []any{id, int64(ops[i+1]), nil}
				r.Apply(azora.Update{Changes: []azora.Change{{Table: table, Op: azora.OpInsert, Key: []any{id}, New: row}}})
				model[id] = row
			case 2:
				r.Apply(azora.Update{Changes: []azora.Change{{Table: table, Op: azora.OpDelete, Key: []any{id}}}})
				delete(model, id)
			case 3:
				if ops[i+1] > 250 {
					r.Apply(azora.Update{Reset: true})
					model = map[int64][]any{}
				}
			}
		}
		require.Equal(t, len(model), r.Len("t"))
		var sum uint64
		for id, row := range model {
			got, ok := r.Get("t", id)
			require.True(t, ok)
			require.Equal(t, row, got)
			sum += azora.RowHash("t", []any{id}, row)
		}
		n, s := r.Checksum()
		require.Equal(t, int64(len(model)), n)
		require.Equal(t, sum, s)
	})
}
