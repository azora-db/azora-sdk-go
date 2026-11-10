package azora_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

func TestParseFilter(t *testing.T) {
	cases := []struct {
		in   string
		want azora.Filter
	}{
		{"status = 'paid'", azora.Eq("status", "paid")},
		{"status == 'paid'", azora.Eq("status", "paid")},
		{"a != 1", azora.Ne("a", int64(1))},
		{"a <> 1", azora.Ne("a", int64(1))},
		{"a < 1.5", azora.Lt("a", 1.5)},
		{"a <= -2", azora.Le("a", int64(-2))},
		{"a > +3", azora.Gt("a", int64(3))},
		{"a >= 1e3", azora.Ge("a", 1000.0)},
		{"a = .5", azora.Eq("a", 0.5)},
		{"a = 9223372036854775807", azora.Eq("a", int64(math.MaxInt64))},
		{"a = 9223372036854775808", azora.Eq("a", 9223372036854775808.0)}, // too large for an integer: a float
		{"a = x'0aFF'", azora.Eq("a", []byte{0x0a, 0xff})},
		{"a = X''", azora.Eq("a", []byte{})},
		{"a = TRUE", azora.Eq("a", int64(1))},
		{"a = false", azora.Eq("a", int64(0))},
		{"name = 'O''Brien'", azora.Eq("name", "O'Brien")},
		{"name = ''", azora.Eq("name", "")},
		{`"my col" = 1`, azora.Eq("my col", int64(1))},
		{`"a""b" = 1`, azora.Eq(`a"b`, int64(1))},
		{"path ^= 'tmp/'", azora.Prefix("path", "tmp/")},
		{"a IN (1, 2, 3)", azora.In("a", int64(1), int64(2), int64(3))},
		{"a in ('x')", azora.In("a", "x")},
		{"a NOT IN (1, 'b', 2.5)", azora.NotIn("a", int64(1), "b", 2.5)},
		{"a IS NULL", azora.IsNull("a")},
		{"a is not null", azora.NotNull("a")},
		{"NOT a = 1", azora.Not(azora.Eq("a", int64(1)))},
		{"NOT NOT a = 1", azora.Not(azora.Not(azora.Eq("a", int64(1))))},
		{"(a = 1)", azora.Eq("a", int64(1))},
		{"a = 1 AND b = 2", azora.And(azora.Eq("a", int64(1)), azora.Eq("b", int64(2)))},
		{"a = 1 and b = 2 AND c = 3", azora.And(azora.Eq("a", int64(1)), azora.Eq("b", int64(2)), azora.Eq("c", int64(3)))},
		{"a = 1 OR b = 2", azora.Or(azora.Eq("a", int64(1)), azora.Eq("b", int64(2)))},
		// AND binds tighter than OR; NOT tighter than AND.
		{"a = 1 OR b = 2 AND c = 3", azora.Or(azora.Eq("a", int64(1)), azora.And(azora.Eq("b", int64(2)), azora.Eq("c", int64(3))))},
		{"(a = 1 OR b = 2) AND c = 3", azora.And(azora.Or(azora.Eq("a", int64(1)), azora.Eq("b", int64(2))), azora.Eq("c", int64(3)))},
		{"NOT a = 1 AND b = 2", azora.And(azora.Not(azora.Eq("a", int64(1))), azora.Eq("b", int64(2)))},
		{"NOT (path ^= 'tmp/')", azora.Not(azora.Prefix("path", "tmp/"))},
		{"status = 'paid' AND (total >= 100 OR vip IS NOT NULL) AND id IN (1, 2, 3)",
			azora.And(azora.Eq("status", "paid"), azora.Or(azora.Ge("total", int64(100)), azora.NotNull("vip")), azora.In("id", int64(1), int64(2), int64(3)))},
		{" \t\na\r=\n1 ", azora.Eq("a", int64(1))},
		{"a=1AND b=2", azora.And(azora.Eq("a", int64(1)), azora.Eq("b", int64(2)))},
		{"café = 'é'", azora.Eq("café", "é")},
		{"a = 'multi\nline'", azora.Eq("a", "multi\nline")},
	}
	for _, c := range cases {
		got, err := azora.ParseFilter(c.in)
		require.NoError(t, err, c.in)
		require.NotNil(t, got, c.in)
		require.Equal(t, c.want, *got, c.in)
		require.NoError(t, got.Validate(), c.in)

		// The text form round-trips.
		again, err := azora.ParseFilter(got.String())
		require.NoError(t, err, "%s -> %s", c.in, got.String())
		require.Equal(t, *got, *again, "%s -> %s", c.in, got.String())
	}

	for _, in := range []string{"", "   ", "\n"} {
		f, err := azora.ParseFilter(in)
		require.NoError(t, err)
		require.Nil(t, f, "an empty filter is no filter")
	}

	for in, msg := range map[string]string{
		"a":                    "expected an operator",
		"a =":                  "expected a value",
		"= 1":                  "expected a column name",
		"a = 1 AND":            "expected a column name",
		"a = 1 b = 2":          "unexpected",
		"(a = 1":               "expected )",
		"a = 1)":               "unexpected",
		"a = 'open":            "unterminated string",
		`"a = 1`:               "unterminated identifier",
		"a = x'0g'":            "bad blob",
		"a = x'0":              "unterminated blob",
		"a = 1.2.3":            "bad number",
		"a = NULL":             "expected a value",
		"a IS 1":               "expected NULL",
		"a IS NOT 1":           "expected NULL",
		"a IN 1":               "expected (",
		"a IN ()":              "expected a value",
		"a IN (1,":             "expected a value",
		"a IN (1 2)":           "expected )",
		"a NOT 1":              "expected IN",
		"a ! 1":                "unknown operator",
		"a => 1":               "unknown operator",
		"a ^ 'x'":              "unknown operator",
		"a = b":                "expected a value",
		"a = 1 ; DROP TABLE t": "unexpected",
		"a = $1":               "unexpected",
		"a LIKE 'x%'":          "expected an operator",
		"a BETWEEN 1 AND 2":    "expected an operator",
		"a = 1 AND (b = 2 OR)": "expected a column name",
		strings.Repeat("(", 18) + "a = 1" + strings.Repeat(")", 18): "nested too deeply",
	} {
		_, err := azora.ParseFilter(in)
		require.ErrorIs(t, err, azora.ErrFilterSyntax, in)
		require.ErrorContains(t, err, msg, in)
	}
	_, err := azora.ParseFilter("a IN (" + strings.Repeat("1,", 1000) + "1)")
	require.ErrorContains(t, err, "more than 1000 values")

	require.Equal(t, azora.Eq("a", int64(1)), *azora.MustParseFilter("a = 1"))
	require.Panics(t, func() { azora.MustParseFilter("a =") })
}

func TestFilterStringAndValidate(t *testing.T) {
	for want, f := range map[string]azora.Filter{
		"status = 'paid'":            azora.Eq("status", "paid"),
		"n != 3":                     azora.Ne("n", 3),
		"f < 2.0":                    azora.Lt("f", 2.0),
		"f <= 2.5":                   azora.Le("f", float32(2.5)),
		"b > x'01ff'":                azora.Gt("b", []byte{1, 255}),
		"ok >= TRUE":                 azora.Ge("ok", true),
		"id IN (1, 2)":               azora.In("id", 1, 2),
		"id NOT IN ('a')":            azora.NotIn("id", "a"),
		"v IS NULL":                  azora.IsNull("v"),
		"v IS NOT NULL":              azora.NotNull("v"),
		"p ^= 'a''b'":                azora.Prefix("p", "a'b"),
		"a = 1 AND b = 2":            azora.And(azora.Eq("a", 1), azora.Eq("b", 2)),
		"a = 1 OR (b = 2 AND c = 3)": azora.Or(azora.Eq("a", 1), azora.And(azora.Eq("b", 2), azora.Eq("c", 3))),
		"NOT (a = 1 OR b = 2)":       azora.Not(azora.Or(azora.Eq("a", 1), azora.Eq("b", 2))),
		`"my col" = 1`:               azora.Eq("my col", 1),
		`"null" = 2 AND "true" = 1`:  azora.And(azora.Eq("null", 2), azora.Eq("true", 1)),
		`"x" = 1`:                    azora.Eq("x", 1), // x'..' would start a blob
		`"1st" = 1`:                  azora.Eq("1st", 1),
		`"and" = 1`:                  azora.Eq("and", 1),
		"(a = 1)":                    azora.And(azora.Eq("a", 1)),
	} {
		require.Equal(t, want, f.String())
	}
	// What String writes, ParseFilter reads back to the same filter.
	for _, f := range []azora.Filter{
		azora.And(azora.Eq("a", int64(1)), azora.Or(azora.Lt("b", 2.5), azora.Not(azora.Prefix("c", "x'y")))),
		azora.In("weird name", "a", int64(2), 3.5, []byte{0}),
		azora.Or(azora.And(azora.IsNull("a"), azora.NotNull("b")), azora.NotIn("c", int64(1))),
	} {
		back, err := azora.ParseFilter(f.String())
		require.NoError(t, err, f.String())
		require.Equal(t, f, *back, f.String())
	}

	require.NoError(t, azora.And(azora.Eq("a", 1), azora.In("b", 1, 2)).Validate())
	for msg, f := range map[string]azora.Filter{
		"unknown filter operator":   {Op: "like", Col: "a", Val: "x"},
		"needs a column":            azora.Eq("", 1),
		"NULL never matches":        azora.Eq("a", nil),
		"prefix needs a text value": {Op: "prefix", Col: "a", Val: 1},
		"needs 1 to 1000 values":    azora.In("a"),
		"needs arguments":           azora.And(),
		"exactly one argument":      {Op: "not", Args: []azora.Filter{azora.Eq("a", 1), azora.Eq("b", 1)}},
		"is_null needs a column":    azora.IsNull(""),
	} {
		require.ErrorContains(t, f.Validate(), msg)
	}
	deep := azora.Eq("a", 1)
	for i := 0; i < 17; i++ {
		deep = azora.Not(deep)
	}
	require.ErrorContains(t, deep.Validate(), "deeper than 16")
	wide := make([]azora.Filter, 300)
	for i := range wide {
		wide[i] = azora.Eq("a", i)
	}
	require.ErrorContains(t, azora.Or(wide...).Validate(), "more than 256 nodes")

	// Spec.Filter combines with AND, flattening.
	s := azora.TableSpec("t", azora.Eq("a", 1), azora.Eq("b", 2)).Filter(azora.Eq("c", 3))
	require.Equal(t, "a = 1 AND b = 2 AND c = 3", s.Where.String())
	require.Nil(t, azora.TableSpec("t").Where)
}
