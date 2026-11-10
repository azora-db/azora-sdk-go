package azora

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/azora-db/azora-go/internal/azp"
)

// Limits of a filter, the same as the server's.
const (
	maxFilterDepth  = 16
	maxFilterNodes  = 256
	maxFilterValues = 1000
)

// Filter is a row predicate evaluated by the server on each change of a
// table subscription. Build one with the constructors (Eq, In, And, ...)
// or from text with ParseFilter. Semantics are SQLite's comparison rules
// without type conversion: NULL matches only IsNull, numbers compare
// numerically, text byte-wise, and across types NULL < numbers < text <
// blobs, so Eq("qty", "5") never matches the integer 5. Not negates in
// two-valued logic (see Not).
type Filter struct {
	// Op is one of = != < <= > >= in not_in is_null not_null prefix and or
	// not.
	Op   string
	Col  string
	Val  any
	Vals []any
	Args []Filter
}

// Eq matches rows whose column equals v.
func Eq(col string, v any) Filter { return Filter{Op: "=", Col: col, Val: v} }

// Ne matches rows whose column is not NULL and differs from v.
func Ne(col string, v any) Filter { return Filter{Op: "!=", Col: col, Val: v} }

// Lt matches rows whose column is less than v.
func Lt(col string, v any) Filter { return Filter{Op: "<", Col: col, Val: v} }

// Le matches rows whose column is less than or equal to v.
func Le(col string, v any) Filter { return Filter{Op: "<=", Col: col, Val: v} }

// Gt matches rows whose column is greater than v.
func Gt(col string, v any) Filter { return Filter{Op: ">", Col: col, Val: v} }

// Ge matches rows whose column is greater than or equal to v.
func Ge(col string, v any) Filter { return Filter{Op: ">=", Col: col, Val: v} }

// In matches rows whose column equals one of vs (1 to 1000 values).
func In(col string, vs ...any) Filter { return Filter{Op: "in", Col: col, Vals: vs} }

// NotIn matches rows whose column is not NULL and equals none of vs.
func NotIn(col string, vs ...any) Filter { return Filter{Op: "not_in", Col: col, Vals: vs} }

// IsNull matches rows whose column is NULL.
func IsNull(col string) Filter { return Filter{Op: "is_null", Col: col} }

// NotNull matches rows whose column is not NULL.
func NotNull(col string) Filter { return Filter{Op: "not_null", Col: col} }

// Prefix matches rows whose text column starts with p.
func Prefix(col, p string) Filter { return Filter{Op: "prefix", Col: col, Val: p} }

// And matches rows every filter matches.
func And(fs ...Filter) Filter { return Filter{Op: "and", Args: fs} }

// Or matches rows at least one filter matches.
func Or(fs ...Filter) Filter { return Filter{Op: "or", Args: fs} }

// Not matches rows f does not match. The logic is two-valued, unlike
// SQL's: a comparison with NULL is false, so Not(Eq("c", 1)) matches
// rows whose c is NULL (in SQL, NOT (c = 1) skips them). Ne and NotIn do
// not match NULL, like SQL.
func Not(f Filter) Filter { return Filter{Op: "not", Args: []Filter{f}} }

// wire converts f to its wire form, normalizing values.
func (f *Filter) wire(depth int) (*azp.Filter, error) {
	if f == nil {
		return nil, nil
	}
	if depth > maxFilterDepth {
		return nil, fmt.Errorf("azora: filter nested deeper than %d", maxFilterDepth)
	}
	out := &azp.Filter{Op: f.Op, Col: f.Col}
	var err error
	if out.Val, err = Normalize(f.Val); err != nil {
		return nil, err
	}
	if f.Vals != nil {
		if out.Vals, err = normalizeAll(f.Vals); err != nil {
			return nil, err
		}
	}
	for i := range f.Args {
		a, err := f.Args[i].wire(depth + 1)
		if err != nil {
			return nil, err
		}
		out.Args = append(out.Args, *a)
	}
	return out, nil
}

// String formats f in the text form ParseFilter reads, for logs and for
// `azora realtime token create --filter`.
func (f Filter) String() string {
	var b strings.Builder
	f.format(&b, false)
	return b.String()
}

func (f *Filter) format(b *strings.Builder, nested bool) {
	switch f.Op {
	case "and", "or":
		if len(f.Args) == 0 {
			b.WriteString("(" + strings.ToUpper(f.Op) + ")") // invalid: the server refuses it
			return
		}
		if nested || len(f.Args) == 1 {
			b.WriteByte('(')
		}
		for i := range f.Args {
			if i > 0 {
				b.WriteString(" " + strings.ToUpper(f.Op) + " ")
			}
			f.Args[i].format(b, true)
		}
		if nested || len(f.Args) == 1 {
			b.WriteByte(')')
		}
	case "not":
		b.WriteString("NOT (")
		for i := range f.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			f.Args[i].format(b, false)
		}
		b.WriteByte(')')
	case "is_null":
		b.WriteString(quoteIdent(f.Col) + " IS NULL")
	case "not_null":
		b.WriteString(quoteIdent(f.Col) + " IS NOT NULL")
	case "in", "not_in":
		b.WriteString(quoteIdent(f.Col))
		if f.Op == "not_in" {
			b.WriteString(" NOT")
		}
		b.WriteString(" IN (")
		for i, v := range f.Vals {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatFilterValue(v))
		}
		b.WriteByte(')')
	case "prefix":
		b.WriteString(quoteIdent(f.Col) + " ^= " + formatFilterValue(f.Val))
	default:
		b.WriteString(quoteIdent(f.Col) + " " + f.Op + " " + formatFilterValue(f.Val))
	}
}

// quoteIdent writes a column name, quoted unless it is a plain word that
// is not one of the grammar's keywords.
func quoteIdent(s string) string {
	plain := s != ""
	for i := 0; i < len(s) && plain; i++ {
		c := s[i]
		plain = c == '_' || c >= 0x80 || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0
	}
	if plain && (s[0] == 'x' || s[0] == 'X') {
		plain = false // x'..' starts a blob
	}
	switch strings.ToLower(s) {
	case "and", "or", "not", "in", "is", "null", "true", "false":
		plain = false
	}
	if plain {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// formatFilterValue writes a value as a filter literal.
func formatFilterValue(v any) string {
	n, err := Normalize(v)
	if err != nil {
		return fmt.Sprintf("<%T>", v)
	}
	switch x := n.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		s := strconv.FormatFloat(x, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		return s
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "x'" + hex.EncodeToString(x) + "'"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	}
	return fmt.Sprintf("'%v'", n)
}

// Validate checks f the way the server does before it accepts a
// subscription: known operators, the arguments each needs, and the size
// limits (16 levels, 256 nodes, 1000 values in a list). Column names are
// checked by the server against the table.
func (f Filter) Validate() error {
	nodes := 0
	return f.validate(0, &nodes)
}

func (f *Filter) validate(depth int, nodes *int) error {
	*nodes++
	if depth > maxFilterDepth {
		return fmt.Errorf("azora: filter nested deeper than %d", maxFilterDepth)
	}
	if *nodes > maxFilterNodes {
		return fmt.Errorf("azora: filter has more than %d nodes", maxFilterNodes)
	}
	switch f.Op {
	case "=", "!=", "<", "<=", ">", ">=":
		if f.Col == "" {
			return fmt.Errorf("azora: filter %s needs a column", f.Op)
		}
		if f.Val == nil {
			return fmt.Errorf("azora: filter %s %s NULL never matches: use IsNull or NotNull", f.Col, f.Op)
		}
	case "prefix":
		if f.Col == "" {
			return errors.New("azora: filter prefix needs a column")
		}
		if _, ok := f.Val.(string); !ok {
			return errors.New("azora: filter prefix needs a text value")
		}
	case "in", "not_in":
		if f.Col == "" {
			return fmt.Errorf("azora: filter %s needs a column", f.Op)
		}
		if len(f.Vals) == 0 || len(f.Vals) > maxFilterValues {
			return fmt.Errorf("azora: filter %s needs 1 to %d values", f.Op, maxFilterValues)
		}
	case "is_null", "not_null":
		if f.Col == "" {
			return fmt.Errorf("azora: filter %s needs a column", f.Op)
		}
	case "and", "or":
		if len(f.Args) == 0 {
			return fmt.Errorf("azora: filter %s needs arguments", f.Op)
		}
	case "not":
		if len(f.Args) != 1 {
			return errors.New("azora: filter not needs exactly one argument")
		}
	default:
		return fmt.Errorf("azora: unknown filter operator %q", f.Op)
	}
	for i := range f.Args {
		if err := f.Args[i].validate(depth+1, nodes); err != nil {
			return err
		}
	}
	return nil
}
