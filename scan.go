package azora

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// typePlan says how rows are decoded into values of one Go type. Plans
// are built once per type and cached.
type typePlan struct {
	typ reflect.Type
	// Exactly one of the following describes the type.
	scalar setter      // a single column: int, string, time.Time, sql.Scanner, ...
	isMap  bool        // map[string]any (or a named type of it): every column
	fields []fieldPlan // a struct: columns matched to fields by name
	exact  map[string]int
	folded map[string]int
	elem   *typePlan // a pointer to a struct or map: a new value per row
	err    error     // the type cannot be scanned into
}

// fieldPlan is one struct field a column can be stored in.
type fieldPlan struct {
	index []int // path from the struct, through embedded structs
	set   setter
	err   error // the field's type cannot be scanned into (reported when a column maps to it)
}

var plans sync.Map // reflect.Type -> *typePlan

// planFor returns the cached plan of t.
func planFor(t reflect.Type) *typePlan {
	if p, ok := plans.Load(t); ok {
		return p.(*typePlan)
	}
	p, _ := plans.LoadOrStore(t, buildPlan(t))
	return p.(*typePlan)
}

func buildPlan(t reflect.Type) *typePlan {
	p := &typePlan{typ: t}
	switch {
	case t.Kind() == reflect.Pointer && isRowType(t.Elem()):
		p.elem = planFor(t.Elem())
		p.err = p.elem.err
	case t.Kind() == reflect.Map:
		if t.Key().Kind() != reflect.String || t.Elem().Kind() != reflect.Interface || t.Elem().NumMethod() != 0 {
			p.err = fmt.Errorf("azora: cannot scan into %s (only map[string]any)", t)
			return p
		}
		p.isMap = true
	case isRowType(t):
		p.exact, p.folded = map[string]int{}, map[string]int{}
		p.addFields(t, nil, map[reflect.Type]bool{t: true})
	default:
		p.scalar, p.err = setterFor(t, false)
	}
	return p
}

// isRowType reports types a whole row is decoded into: structs (other
// than time.Time and sql.Scanner implementations) and maps.
func isRowType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Map:
		return true
	case reflect.Struct:
		return t != timeType && !reflect.PointerTo(t).Implements(scannerType)
	}
	return false
}

// addFields registers the exported fields of struct type t, reached
// through index. Fields of embedded structs are promoted like Go's own:
// an outer field hides an inner one of the same name.
func (p *typePlan) addFields(t reflect.Type, index []int, seen map[reflect.Type]bool) {
	var embedded []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, tagged := f.Tag.Lookup("db")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if f.Anonymous && !tagged && ft.Kind() == reflect.Struct && isRowType(ft) {
			if f.IsExported() || f.Type.Kind() != reflect.Pointer { // an unexported pointer cannot be allocated
				embedded = append(embedded, f)
			}
			continue
		}
		if !f.IsExported() {
			continue
		}
		fp := fieldPlan{index: append(append([]int(nil), index...), i)}
		fp.set, fp.err = setterFor(f.Type, hasOption(opts, "json"))
		p.fields = append(p.fields, fp)
		n := len(p.fields) - 1
		if name == "" {
			name = f.Name
		}
		if _, dup := p.exact[name]; !dup {
			p.exact[name] = n
		}
		if k := foldName(name); k != "" {
			if _, dup := p.folded[k]; !dup {
				p.folded[k] = n
			}
		}
	}
	for _, f := range embedded {
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if seen[ft] {
			continue // a recursive embedding
		}
		seen[ft] = true
		p.addFields(ft, append(append([]int(nil), index...), f.Index[0]), seen)
		delete(seen, ft)
	}
}

func hasOption(opts, want string) bool {
	for opts != "" {
		var o string
		o, opts, _ = strings.Cut(opts, ",")
		if o == want {
			return true
		}
	}
	return false
}

// foldName lowercases a name and drops underscores, so the column
// customer_id matches the fields CustomerID, CustomerId and Customer_ID.
func foldName(s string) string {
	plain := true
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == '_' || c >= 'A' && c <= 'Z' || c >= 0x80 {
			plain = false
			break
		}
	}
	if plain {
		return s
	}
	return strings.ReplaceAll(strings.ToLower(s), "_", "")
}

// binding maps the columns of one result to the fields of a struct plan:
// for column i, fields[i] is the index in plan.fields, or -1 when no
// field takes it.
type binding struct {
	plan   *typePlan
	cols   []string
	fields []int
	inner  *binding // of plan.elem
}

// bind matches cols to the plan's struct fields: by db tag or exact field
// name first, then ignoring case and underscores. Columns without a field
// are skipped.
func (p *typePlan) bind(cols []string) (*binding, error) {
	b := &binding{plan: p, cols: cols}
	if p.elem != nil {
		inner, err := p.elem.bind(cols)
		if err != nil {
			return nil, err
		}
		b.inner = inner
		return b, nil
	}
	if p.exact == nil {
		return b, nil // a scalar or a map
	}
	b.fields = make([]int, len(cols))
	for i, c := range cols {
		n, ok := p.exact[c]
		if !ok {
			n, ok = p.folded[foldName(c)]
		}
		if !ok {
			b.fields[i] = -1
			continue
		}
		if err := p.fields[n].err; err != nil {
			return nil, fmt.Errorf("column %q: %w", c, err)
		}
		b.fields[i] = n
	}
	return b, nil
}

// decode stores row into dst, a settable value of the plan's type.
func (b *binding) decode(dst reflect.Value, row []any) error {
	p := b.plan
	switch {
	case p.err != nil:
		return p.err
	case b.inner != nil:
		v := reflect.New(p.typ.Elem())
		if err := b.inner.decode(v.Elem(), row); err != nil {
			return err
		}
		dst.Set(v)
		return nil
	case p.scalar != nil:
		if len(row) != 1 {
			return fmt.Errorf("azora: %d columns for a single %s (select one column, or scan into a struct)", len(row), p.typ)
		}
		if err := p.scalar(dst, row[0]); err != nil {
			return b.colErr(0, err)
		}
		return nil
	case p.isMap:
		m := reflect.MakeMapWithSize(p.typ, len(row))
		for i, v := range row {
			if i >= len(b.cols) {
				break
			}
			ev := reflect.New(p.typ.Elem()).Elem()
			if v != nil {
				ev.Set(reflect.ValueOf(v))
			}
			m.SetMapIndex(reflect.ValueOf(b.cols[i]).Convert(p.typ.Key()), ev)
		}
		dst.Set(m)
		return nil
	}
	for i, n := range b.fields {
		if n < 0 || i >= len(row) {
			continue
		}
		f := &p.fields[n]
		fv := dst
		if len(f.index) == 1 {
			fv = fv.Field(f.index[0])
		} else {
			fv = fieldByIndex(dst, f.index)
		}
		if err := f.set(fv, row[i]); err != nil {
			return b.colErr(i, err)
		}
	}
	return nil
}

func (b *binding) colErr(i int, err error) error {
	if i < len(b.cols) {
		return fmt.Errorf("column %q: %w", b.cols[i], err)
	}
	return err
}

// fieldByIndex is reflect.Value.FieldByIndex that allocates embedded
// struct pointers on the way.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// ScanValue stores one database value (nil, int64, float64, string or
// []byte) into dest, a pointer, converting as Rows.Scan does. It is for
// code that receives raw values elsewhere: a Change's row, a Record, a
// database/sql Scanner.
func ScanValue(dest any, src any) error { return scanValue(dest, src) }

// scanValue stores one database value into a Scan destination. Common
// pointer types take a fast path without reflection.
func scanValue(dest any, src any) error {
	switch d := dest.(type) {
	case nil:
		return fmt.Errorf("azora: Scan destination is nil")
	case *any:
		*d = src
		return nil
	case *int64:
		if v, ok := src.(int64); ok {
			*d = v
			return nil
		}
	case *int:
		if v, ok := src.(int64); ok && int64(int(v)) == v {
			*d = int(v)
			return nil
		}
	case *string:
		if v, ok := src.(string); ok {
			*d = v
			return nil
		}
	case *float64:
		if v, ok := src.(float64); ok {
			*d = v
			return nil
		}
	case *[]byte:
		if v, ok := src.([]byte); ok {
			*d = append(make([]byte, 0, len(v)), v...)
			return nil
		}
	case *time.Time:
		if src == nil {
			*d = time.Time{}
			return nil
		}
		t, ok := parseTime(src)
		if !ok {
			return fmt.Errorf("azora: cannot convert %s to time.Time", describe(src))
		}
		*d = t
		return nil
	case sql.Scanner:
		return d.Scan(src)
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("azora: Scan destination %T is not a non-nil pointer", dest)
	}
	p := planFor(rv.Type().Elem())
	if p.scalar == nil {
		if p.err != nil {
			return p.err
		}
		return fmt.Errorf("azora: cannot scan one column into %s (use ScanStruct)", p.typ)
	}
	return p.scalar(rv.Elem(), src)
}
