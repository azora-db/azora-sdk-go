package azora

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// Rows is the result of a query, held in memory (the server returns at
// most MaxQueryRows rows per query). Read it in whichever way fits:
//
//	for rows.Next() { err := rows.Scan(&id, &name) }      // cursor
//	for row := range rows.All() { ... row.Values() ... }  // iterator
//	err := rows.ScanAll(&users)                           // structs
//	rows.Values()                                         // raw [][]any
//
// Values are nil (NULL), int64, float64, string or []byte.
//
// The cursor (Next, Scan, ScanStruct, Row) is for one goroutine; the
// other methods only read and may be called concurrently.
type Rows struct {
	cols      []string
	rows      [][]any
	truncated bool
	cur       int
	bound     *binding // of the last ScanStruct destination type
}

// NewRows builds a Rows from column names and row values: useful to test
// code that consumes query results, or to scan data that did not come
// from a query.
func NewRows(columns []string, values [][]any) *Rows {
	return &Rows{cols: columns, rows: values, cur: -1}
}

// Columns returns the column names. Do not modify the slice.
func (r *Rows) Columns() []string { return r.cols }

// Len returns the number of rows.
func (r *Rows) Len() int { return len(r.rows) }

// Truncated reports that more rows existed than the query's row limit
// (MaxQueryRows, or the limit given to QueryN) allowed.
func (r *Rows) Truncated() bool { return r.truncated }

// Values returns every row as raw values, in column order. The slices
// belong to the Rows: do not modify them.
func (r *Rows) Values() [][]any { return r.rows }

// Next advances the cursor to the next row, reporting whether there is
// one. The cursor starts before the first row.
func (r *Rows) Next() bool {
	if r.cur+1 >= len(r.rows) {
		r.cur = len(r.rows)
		return false
	}
	r.cur++
	return true
}

// Reset moves the cursor back before the first row.
func (r *Rows) Reset() { r.cur = -1 }

// current returns the cursor's row.
func (r *Rows) current() ([]any, error) {
	if r.cur < 0 || r.cur >= len(r.rows) {
		return nil, errors.New("azora: Scan called without a row: call Next first and check its result")
	}
	return r.rows[r.cur], nil
}

// Row returns the cursor's row (the zero Row before Next or after the
// last row).
func (r *Rows) Row() Row {
	row, err := r.current()
	if err != nil {
		return Row{}
	}
	return Row{cols: r.cols, vals: row}
}

// Scan copies the columns of the cursor's row into dest, one pointer per
// column, converting like database/sql does:
//
//   - integers into any integer or float type (with a range check),
//     strings and bool (0 and 1);
//   - floats into float types, and integer types when the value is whole;
//   - text into string, []byte, numbers (parsed), bool, and time.Time;
//   - blobs into []byte and string;
//   - a time.Time from text ("2006-01-02 15:04:05", RFC 3339, a date),
//     an integer (Unix seconds) or a float (Julian day);
//   - json.RawMessage from text; sql.Scanner implementations (sql.Null*,
//     azora.JSON[T], your own types); *any for the raw value.
//
// NULL sets a pointer destination (**T) to nil and any other destination
// to its zero value: scan into *T or sql.Null* to tell NULL from zero.
func (r *Rows) Scan(dest ...any) error {
	row, err := r.current()
	if err != nil {
		return err
	}
	return scanRow(r.cols, row, dest)
}

func scanRow(cols []string, row []any, dest []any) error {
	if len(dest) != len(row) {
		return fmt.Errorf("azora: Scan got %d destinations for %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		if err := scanValue(d, row[i]); err != nil {
			if i < len(cols) {
				return fmt.Errorf("column %q: %w", cols[i], err)
			}
			return err
		}
	}
	return nil
}

// ScanStruct decodes the cursor's row into dest, a pointer to a struct
// (or to a map[string]any). Columns are matched to fields by their db
// tag, else by field name, ignoring case and underscores (customer_id
// fills CustomerID); columns without a field are skipped and fields
// without a column keep their value. Embedded structs are flattened.
// Tag options: `db:"-"` skips a field, `db:"name,json"` decodes JSON
// text into the field. Values convert as in Scan.
func (r *Rows) ScanStruct(dest any) error {
	row, err := r.current()
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("azora: ScanStruct destination %T is not a non-nil pointer", dest)
	}
	b := r.bound
	if b == nil || b.plan.typ != rv.Type().Elem() {
		if b, err = planFor(rv.Type().Elem()).bind(r.cols); err != nil {
			return err
		}
		r.bound = b
	}
	return b.decode(rv.Elem(), row)
}

// All iterates over the rows without moving the cursor:
//
//	for row := range rows.All() {
//		var o Order
//		if err := row.ScanStruct(&o); err != nil { ... }
//	}
func (r *Rows) All() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		for _, vals := range r.rows {
			if !yield(Row{cols: r.cols, vals: vals}) {
				return
			}
		}
	}
}

// ScanAll decodes every row into dest, a pointer to a slice of structs,
// struct pointers, maps or single-column values ([]int64, []string, ...),
// replacing the slice's content. See ScanStruct for how columns map to
// fields. An empty result leaves dest an empty, non-nil slice.
func (r *Rows) ScanAll(dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("azora: ScanAll destination %T is not a pointer to a slice", dest)
	}
	sv := rv.Elem()
	b, err := planFor(sv.Type().Elem()).bind(r.cols)
	if err != nil {
		return err
	}
	out := reflect.MakeSlice(sv.Type(), len(r.rows), len(r.rows))
	for i, row := range r.rows {
		if err := b.decode(out.Index(i), row); err != nil {
			return fmt.Errorf("row %d: %w", i+1, err)
		}
	}
	sv.Set(out)
	return nil
}

// Maps returns every row as a map from column name to value.
func (r *Rows) Maps() []map[string]any {
	out := make([]map[string]any, len(r.rows))
	for i, row := range r.rows {
		out[i] = Row{cols: r.cols, vals: row}.Map()
	}
	return out
}

// Row is one row of a result.
type Row struct {
	cols []string
	vals []any
}

// Columns returns the column names.
func (r Row) Columns() []string { return r.cols }

// Values returns the row's raw values, in column order.
func (r Row) Values() []any { return r.vals }

// Get returns the value of the named column (exact match first, then
// ignoring case); ok is false when the row has no such column.
func (r Row) Get(column string) (v any, ok bool) {
	for i, c := range r.cols {
		if c == column && i < len(r.vals) {
			return r.vals[i], true
		}
	}
	f := foldName(column)
	for i, c := range r.cols {
		if foldName(c) == f && i < len(r.vals) {
			return r.vals[i], true
		}
	}
	return nil, false
}

// Scan copies the row's columns into dest (see Rows.Scan).
func (r Row) Scan(dest ...any) error { return scanRow(r.cols, r.vals, dest) }

// ScanStruct decodes the row into dest (see Rows.ScanStruct).
func (r Row) ScanStruct(dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("azora: ScanStruct destination %T is not a non-nil pointer", dest)
	}
	b, err := planFor(rv.Type().Elem()).bind(r.cols)
	if err != nil {
		return err
	}
	return b.decode(rv.Elem(), r.vals)
}

// Map returns the row as a map from column name to value.
func (r Row) Map() map[string]any {
	m := make(map[string]any, len(r.cols))
	for i, c := range r.cols {
		if i < len(r.vals) {
			m[c] = r.vals[i]
		}
	}
	return m
}

// Querier is what the generic helpers (All, One, Scalar) and Select/Get
// need: *DB implements it, and so can a test double.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (*Rows, error)
}

// limitedQuerier is implemented by queriers that can cap the rows the
// server sends (*DB).
type limitedQuerier interface {
	QueryN(ctx context.Context, maxRows int, sql string, args ...any) (*Rows, error)
}

// Select runs a query and decodes every row into dest, a pointer to a
// slice (see Rows.ScanAll):
//
//	var orders []Order
//	err := db.Select(ctx, &orders, "SELECT * FROM orders WHERE status = ?", "paid")
//
// A result cut at MaxQueryRows is an error here, because the slice would
// silently miss rows: page the query, or use Query and check
// Rows.Truncated.
func (db *DB) Select(ctx context.Context, dest any, sql string, args ...any) error {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	if rows.Truncated() {
		return fmt.Errorf("azora: the query returned more than %d rows: page it with LIMIT", rows.Len())
	}
	return rows.ScanAll(dest)
}

// Get runs a query and decodes its first row into dest: a pointer to a
// struct, a map[string]any, or a single-column value (*int64, *string,
// *time.Time, ...). It returns ErrNoRows when the query returned none.
//
//	var o Order
//	err := db.Get(ctx, &o, "SELECT * FROM orders WHERE id = ?", 7)
//	var n int
//	err = db.Get(ctx, &n, "SELECT COUNT(*) FROM orders")
func (db *DB) Get(ctx context.Context, dest any, sql string, args ...any) error {
	rows, err := db.QueryN(ctx, 1, sql, args...)
	if err != nil {
		return err
	}
	if !rows.Next() {
		return ErrNoRows
	}
	return rows.ScanStruct(dest)
}

// All runs a query and returns its rows decoded as T: a struct, a
// map[string]any or a single-column type.
//
//	orders, err := azora.All[Order](ctx, db, "SELECT * FROM orders WHERE total > ?", 100)
//	ids, err := azora.All[int64](ctx, db, "SELECT id FROM orders")
//
// Like DB.Select it fails for a result cut at MaxQueryRows.
func All[T any](ctx context.Context, q Querier, sql string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if rows.Truncated() {
		return nil, fmt.Errorf("azora: the query returned more than %d rows: page it with LIMIT", rows.Len())
	}
	return Collect[T](rows)
}

// Collect decodes every row of rows as T (see All).
func Collect[T any](rows *Rows) ([]T, error) {
	b, err := planFor(reflect.TypeFor[T]()).bind(rows.cols)
	if err != nil {
		return nil, err
	}
	out := make([]T, len(rows.rows))
	for i, row := range rows.rows {
		if err := b.decode(reflect.ValueOf(&out[i]).Elem(), row); err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
	}
	return out, nil
}

// One runs a query and returns its first row decoded as T, or ErrNoRows.
//
//	o, err := azora.One[Order](ctx, db, "SELECT * FROM orders WHERE id = ?", 7)
func One[T any](ctx context.Context, q Querier, sql string, args ...any) (T, error) {
	var zero T
	var rows *Rows
	var err error
	if lq, ok := q.(limitedQuerier); ok {
		rows, err = lq.QueryN(ctx, 1, sql, args...)
	} else {
		rows, err = q.Query(ctx, sql, args...)
	}
	if err != nil {
		return zero, err
	}
	if len(rows.rows) == 0 {
		return zero, ErrNoRows
	}
	b, err := planFor(reflect.TypeFor[T]()).bind(rows.cols)
	if err != nil {
		return zero, err
	}
	var out T
	if err := b.decode(reflect.ValueOf(&out).Elem(), rows.rows[0]); err != nil {
		return zero, err
	}
	return out, nil
}

// Scalar runs a query and returns the first column of its first row as
// T, or ErrNoRows. Unlike One it accepts results with several columns.
//
//	n, err := azora.Scalar[int](ctx, db, "SELECT COUNT(*) FROM orders")
func Scalar[T any](ctx context.Context, q Querier, sql string, args ...any) (T, error) {
	var zero T
	var rows *Rows
	var err error
	if lq, ok := q.(limitedQuerier); ok {
		rows, err = lq.QueryN(ctx, 1, sql, args...)
	} else {
		rows, err = q.Query(ctx, sql, args...)
	}
	if err != nil {
		return zero, err
	}
	if len(rows.rows) == 0 || len(rows.rows[0]) == 0 {
		return zero, ErrNoRows
	}
	var out T
	if err := scanValue(&out, rows.rows[0][0]); err != nil {
		if len(rows.cols) > 0 {
			return zero, fmt.Errorf("column %q: %w", rows.cols[0], err)
		}
		return zero, err
	}
	return out, nil
}
