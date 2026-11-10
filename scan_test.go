package azora_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

type audit struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt *time.Time
}

type customer struct {
	ID        int64
	FullName  string  `db:"name"`
	Email     *string // NULL becomes nil
	Balance   float64
	VIP       bool
	Avatar    []byte
	Visits    uint32
	Prefs     map[string]any  `db:"prefs,json"`
	Raw       json.RawMessage `db:"raw"`
	Nick      sql.NullString
	Secret    string `db:"-"`
	hidden    int
	audit            // embedded: its fields are promoted
	ExtraInfo string `db:"extra_info"`
}

var customerCols = []string{"id", "name", "email", "balance", "vip", "avatar", "visits", "prefs", "raw", "nick", "secret",
	"hidden", "created_at", "updated_at", "extra_info", "unknown_column"}

func customerRow(id int64) []any {
	return []any{id, "Ann", nil, 12.5, int64(1), []byte{1, 2}, int64(7), `{"dark":true}`, `[1,2]`, "annie", "s3cret",
		int64(5), "2026-11-10 09:30:00.123456789+00:00", nil, "x", "ignored"}
}

func TestScanStruct(t *testing.T) {
	rows := azora.NewRows(customerCols, [][]any{customerRow(1), customerRow(2)})
	require.Equal(t, customerCols, rows.Columns())
	require.Equal(t, 2, rows.Len())
	require.False(t, rows.Truncated())

	var c customer
	require.ErrorContains(t, rows.ScanStruct(&c), "call Next first")
	require.True(t, rows.Next())
	require.NoError(t, rows.ScanStruct(&c))
	require.Equal(t, int64(1), c.ID)
	require.Equal(t, "Ann", c.FullName)
	require.Nil(t, c.Email)
	require.Equal(t, 12.5, c.Balance)
	require.True(t, c.VIP)
	require.Equal(t, []byte{1, 2}, c.Avatar)
	require.Equal(t, uint32(7), c.Visits)
	require.Equal(t, map[string]any{"dark": true}, c.Prefs)
	require.JSONEq(t, `[1,2]`, string(c.Raw))
	require.Equal(t, sql.NullString{String: "annie", Valid: true}, c.Nick)
	require.Empty(t, c.Secret, `db:"-" fields are skipped`)
	require.Zero(t, c.hidden, "unexported fields are skipped")
	require.Equal(t, time.Date(2026, 11, 10, 9, 30, 0, 123456789, time.UTC), c.CreatedAt.UTC())
	require.Nil(t, c.UpdatedAt)
	require.Equal(t, "x", c.ExtraInfo)

	// The cursor moves on, and ends.
	require.True(t, rows.Next())
	require.Equal(t, int64(2), rows.Row().Values()[0])
	require.False(t, rows.Next())
	require.False(t, rows.Next())
	require.ErrorContains(t, rows.Scan(), "call Next first")
	require.Nil(t, rows.Row().Values())
	rows.Reset()
	require.True(t, rows.Next())

	// Every row at once: values, pointers, maps.
	var all []customer
	require.NoError(t, rows.ScanAll(&all))
	require.Len(t, all, 2)
	require.Equal(t, int64(2), all[1].ID)
	var ptrs []*customer
	require.NoError(t, rows.ScanAll(&ptrs))
	require.Equal(t, "Ann", ptrs[1].FullName)
	var maps []map[string]any
	require.NoError(t, rows.ScanAll(&maps))
	require.Equal(t, "Ann", maps[0]["name"])
	require.Nil(t, maps[0]["email"])
	require.Equal(t, maps, rows.Maps())
	typed, err := azora.Collect[customer](rows)
	require.NoError(t, err)
	require.Equal(t, all, typed)

	// The iterator does not use the cursor.
	n := 0
	for row := range rows.All() {
		var c customer
		require.NoError(t, row.ScanStruct(&c))
		n++
		v, ok := row.Get("name")
		require.True(t, ok)
		require.Equal(t, "Ann", v)
		v, ok = row.Get("EXTRA_INFO")
		require.True(t, ok, "Get ignores case")
		require.Equal(t, "x", v)
		_, ok = row.Get("nope")
		require.False(t, ok)
		require.Equal(t, customerCols, row.Columns())
		require.Len(t, row.Map(), len(customerCols))
	}
	require.Equal(t, 2, n)
	require.Len(t, rows.Values(), 2)

	// An empty result gives an empty, non-nil slice.
	all = []customer{{ID: 9}}
	require.NoError(t, azora.NewRows(customerCols, nil).ScanAll(&all))
	require.NotNil(t, all)
	require.Empty(t, all)
}

func TestScanFieldMatching(t *testing.T) {
	type inner struct{ Deep string }
	type Base struct {
		ID   int
		Name string
	}
	type row struct {
		*Base             // an embedded pointer is allocated
		inner             // unexported embedded struct: its exported fields count
		Name       string // shadows Base.Name
		CustomerID int
		HTTPCode   int `db:"status"`
		OrderTotal float64
	}
	rows := azora.NewRows([]string{"ID", "name", "customer_id", "status", "ORDER_TOTAL", "deep"}, [][]any{{int64(1), "n", int64(2), int64(200), 3.5, "d"}})
	out, err := azora.Collect[row](rows)
	require.NoError(t, err)
	require.Equal(t, 1, out[0].ID)
	require.Equal(t, "n", out[0].Name)
	require.Empty(t, out[0].Base.Name, "the outer field wins")
	require.Equal(t, 2, out[0].CustomerID, "snake_case matches CamelCase")
	require.Equal(t, 200, out[0].HTTPCode)
	require.Equal(t, 3.5, out[0].OrderTotal)
	require.Equal(t, "d", out[0].Deep)

	// A column mapped to a field of an unsupported type is an error;
	// without such a column the field is left alone.
	type bad struct {
		ID int
		Ch chan int
	}
	_, err = azora.Collect[bad](azora.NewRows([]string{"id"}, [][]any{{int64(1)}}))
	require.NoError(t, err)
	_, err = azora.Collect[bad](azora.NewRows([]string{"id", "ch"}, [][]any{{int64(1), "x"}}))
	require.ErrorContains(t, err, `column "ch"`)
	require.ErrorContains(t, err, "cannot scan into chan int")
}

type upper string

func (u *upper) Scan(src any) error {
	s, ok := src.(string)
	if !ok {
		return errors.New("upper: not text")
	}
	*u = upper("<" + s + ">")
	return nil
}

func TestScanConversions(t *testing.T) {
	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	scan := func(src any, dest any) error {
		rows := azora.NewRows([]string{"c"}, [][]any{{src}})
		rows.Next()
		return rows.Scan(dest)
	}
	var (
		i   int
		i8  int8
		i64 int64
		u   uint
		u8  uint8
		f   float64
		f32 float32
		s   string
		b   []byte
		bl  bool
		tt  time.Time
		a   any
		ps  *string
		pi  *int64
		raw json.RawMessage
		ns  sql.NullInt64
		up  upper
		js  azora.JSON[[]int]
		my  myInt
	)
	ok := []struct {
		src  any
		dest any
		want any
	}{
		{int64(42), &i, 42}, {int64(-7), &i8, int8(-7)}, {int64(math.MaxInt64), &i64, int64(math.MaxInt64)},
		{int64(42), &u, uint(42)}, {int64(255), &u8, uint8(255)}, {int64(3), &f, 3.0}, {1.5, &f, 1.5}, {1.5, &f32, float32(1.5)},
		{4.0, &i, 4}, {"17", &i, 17}, {"2.5", &f, 2.5}, {[]byte("8"), &i, 8}, {"9", &u, uint(9)},
		{"text", &s, "text"}, {[]byte("bytes"), &s, "bytes"}, {int64(5), &s, "5"}, {2.5, &s, "2.5"},
		{[]byte{1, 2}, &b, []byte{1, 2}}, {"str", &b, []byte("str")},
		{int64(1), &bl, true}, {int64(0), &bl, false}, {"true", &bl, true}, {1.0, &bl, true},
		{"2026-01-02 03:04:05", &tt, tm}, {"2026-01-02T03:04:05Z", &tt, tm}, {"2026-01-02 03:04:05+00:00", &tt, tm},
		{"2026-01-02", &tt, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}, {tm.Unix(), &tt, tm}, {2440587.5, &tt, time.Unix(0, 0).UTC()},
		{float64(tm.Unix()) + 0.5, &tt, tm.Add(500 * time.Millisecond)}, {[]byte("2026-01-02T03:04"), &tt, time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)},
		{int64(3), &a, int64(3)}, {nil, &a, nil}, {"x", &a, "x"},
		{`{"a":1}`, &raw, json.RawMessage(`{"a":1}`)}, {[]byte(`[1]`), &raw, json.RawMessage(`[1]`)}, {int64(3), &raw, json.RawMessage(`3`)},
		{int64(9), &ns, sql.NullInt64{Int64: 9, Valid: true}}, {nil, &ns, sql.NullInt64{}},
		{"abc", &up, upper("<abc>")}, {"[1,2,3]", &js, azora.JSON[[]int]{V: []int{1, 2, 3}}}, {nil, &js, azora.JSON[[]int]{}},
		{int64(12), &my, myInt(12)},
		// NULL: the zero value, or nil for a pointer.
		{nil, &i, 0}, {nil, &s, ""}, {nil, &b, []byte(nil)}, {nil, &bl, false}, {nil, &tt, time.Time{}}, {nil, &f, 0.0},
		{nil, &raw, json.RawMessage(nil)}, {nil, &u, uint(0)},
	}
	for _, c := range ok {
		require.NoError(t, scan(c.src, c.dest), "%v into %T", c.src, c.dest)
		got := deref(c.dest)
		if wt, isTime := c.want.(time.Time); isTime {
			require.True(t, wt.Equal(got.(time.Time)), "%v into time: got %v", c.src, got)
			continue
		}
		require.Equal(t, c.want, got, "%v into %T", c.src, c.dest)
	}
	// Pointers: allocated for a value, nil for NULL.
	require.NoError(t, scan("x", &ps))
	require.Equal(t, "x", *ps)
	require.NoError(t, scan(nil, &ps))
	require.Nil(t, ps)
	require.NoError(t, scan(int64(5), &pi))
	require.Equal(t, int64(5), *pi)
	// A scanned blob is a copy: changing it does not change the result.
	rows := azora.NewRows([]string{"c"}, [][]any{{[]byte{1, 2}}})
	rows.Next()
	require.NoError(t, rows.Scan(&b))
	b[0] = 9
	require.Equal(t, []byte{1, 2}, rows.Values()[0][0])

	bad := []struct {
		src  any
		dest any
		msg  string
	}{
		{int64(300), &i8, "overflows int8"}, {int64(-1), &u, "overflows uint"}, {int64(256), &u8, "overflows uint8"},
		{1.5, &i, "cannot convert float 1.5 to int"}, {"abc", &i, `cannot convert text "abc" to int`},
		{"abc", &f, "cannot convert"}, {[]byte{1}, &bl, "cannot convert blob of 1 bytes to bool"}, {int64(2), &bl, "cannot convert integer 2 to bool"},
		{"not a time", &tt, "cannot convert"}, {int64(1), &b, "cannot convert integer 1 to []uint8"}, {int64(1), &up, "upper: not text"},
		{"{", &js, "unexpected end of JSON"}, {math.NaN(), &tt, "cannot convert"}, {-1.5, &u, "cannot convert"},
	}
	for _, c := range bad {
		err := scan(c.src, c.dest)
		require.ErrorContains(t, err, c.msg, "%v into %T", c.src, c.dest)
		require.ErrorContains(t, err, `column "c"`)
	}
	rows = azora.NewRows([]string{"a", "b"}, [][]any{{int64(1), int64(2)}})
	rows.Next()
	require.ErrorContains(t, rows.Scan(&i), "1 destinations for 2 columns")
	require.ErrorContains(t, rows.Scan(&i, nil), "destination is nil")
	require.ErrorContains(t, rows.Scan(&i, i), "not a non-nil pointer")
	require.ErrorContains(t, rows.Scan(&i, &struct{ A int }{}), "ScanStruct")
	require.ErrorContains(t, rows.ScanStruct(customer{}), "not a non-nil pointer")
	require.ErrorContains(t, rows.Row().ScanStruct(nil), "not a non-nil pointer")
	require.ErrorContains(t, rows.ScanAll(&i), "pointer to a slice")
	require.ErrorContains(t, rows.ScanAll([]int{}), "pointer to a slice")
	var ints []int
	require.ErrorContains(t, rows.ScanAll(&ints), "2 columns for a single int")
	require.NoError(t, rows.Row().Scan(&i, &i64))
	require.Equal(t, 1, i)
}

// deref returns what a Scan destination points to.
func deref(p any) any {
	switch d := p.(type) {
	case *int:
		return *d
	case *int8:
		return *d
	case *int64:
		return *d
	case *uint:
		return *d
	case *uint8:
		return *d
	case *float64:
		return *d
	case *float32:
		return *d
	case *string:
		return *d
	case *[]byte:
		return *d
	case *bool:
		return *d
	case *time.Time:
		return *d
	case *any:
		return *d
	case *json.RawMessage:
		return *d
	case *sql.NullInt64:
		return *d
	case *upper:
		return *d
	case *azora.JSON[[]int]:
		return *d
	case *myInt:
		return *d
	}
	panic("deref: unknown destination")
}

// stubQuerier answers every query with fixed rows.
type stubQuerier struct {
	rows *azora.Rows
	err  error
	sql  string
	args []any
}

func (s *stubQuerier) Query(_ context.Context, sql string, args ...any) (*azora.Rows, error) {
	s.sql, s.args = sql, args
	if s.err != nil {
		return nil, s.err
	}
	s.rows.Reset()
	return s.rows, nil
}

func TestGenericHelpers(t *testing.T) {
	q := &stubQuerier{rows: azora.NewRows([]string{"id", "name"}, [][]any{{int64(1), "a"}, {int64(2), "b"}})}
	type named struct {
		ID   int
		Name string
	}
	all, err := azora.All[named](context.Background(), q, "SELECT id, name FROM t WHERE x = ?", 5)
	require.NoError(t, err)
	require.Equal(t, []named{{1, "a"}, {2, "b"}}, all)
	require.Equal(t, "SELECT id, name FROM t WHERE x = ?", q.sql)
	require.Equal(t, []any{5}, q.args)
	recs, err := azora.All[azora.Record](context.Background(), q, "q")
	require.NoError(t, err)
	require.Equal(t, azora.Record{"id": int64(2), "name": "b"}, recs[1])
	ptrs, err := azora.All[*named](context.Background(), q, "q")
	require.NoError(t, err)
	require.Equal(t, "b", ptrs[1].Name)

	one, err := azora.One[named](context.Background(), q, "q")
	require.NoError(t, err)
	require.Equal(t, named{1, "a"}, one)
	id, err := azora.Scalar[int64](context.Background(), q, "q")
	require.NoError(t, err)
	require.Equal(t, int64(1), id, "Scalar takes the first column")
	_, err = azora.One[int64](context.Background(), q, "q")
	require.ErrorContains(t, err, "2 columns for a single int64")

	single := &stubQuerier{rows: azora.NewRows([]string{"n"}, [][]any{{int64(3)}, {int64(4)}, {nil}})}
	ns, err := azora.All[int](context.Background(), single, "q")
	require.NoError(t, err)
	require.Equal(t, []int{3, 4, 0}, ns)
	pns, err := azora.All[*int](context.Background(), single, "q")
	require.NoError(t, err)
	require.Nil(t, pns[2])
	require.Equal(t, 4, *pns[1])
	when, err := azora.All[sql.NullInt64](context.Background(), single, "q")
	require.NoError(t, err)
	require.False(t, when[2].Valid)

	empty := &stubQuerier{rows: azora.NewRows([]string{"n"}, nil)}
	_, err = azora.One[named](context.Background(), empty, "q")
	require.ErrorIs(t, err, azora.ErrNoRows)
	_, err = azora.Scalar[int](context.Background(), empty, "q")
	require.ErrorIs(t, err, azora.ErrNoRows)
	none, err := azora.All[named](context.Background(), empty, "q")
	require.NoError(t, err)
	require.Empty(t, none)

	boom := errors.New("boom")
	failing := &stubQuerier{err: boom}
	_, err = azora.All[named](context.Background(), failing, "q")
	require.ErrorIs(t, err, boom)
	_, err = azora.One[named](context.Background(), failing, "q")
	require.ErrorIs(t, err, boom)
	_, err = azora.Scalar[int](context.Background(), failing, "q")
	require.ErrorIs(t, err, boom)
	_, err = azora.All[chan int](context.Background(), q, "q")
	require.ErrorContains(t, err, "cannot scan into chan int")
	_, err = azora.Scalar[int](context.Background(), q, "q")
	require.NoError(t, err)
	_, err = azora.Scalar[int](context.Background(), &stubQuerier{rows: azora.NewRows([]string{"s"}, [][]any{{"x"}})}, "q")
	require.ErrorContains(t, err, `column "s"`)
}
