package azora

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"
)

// Normalize converts a Go value to one the protocol carries: nil, int64,
// float64, string, []byte, bool or time.Time. Other integer and float
// types, named types, pointers and driver.Valuer are converted; nil
// pointers and nil byte slices become NULL; json.RawMessage becomes text;
// anything else is an error. Query, Exec, Batch and filter values are
// normalized with it.
//
// The server stores booleans as 1 and 0 and a time.Time as text the way
// the engine's SQLite driver does ("2006-01-02 15:04:05.999999999-07:00"),
// which Rows.Scan reads back into a time.Time.
func Normalize(v any) (any, error) {
	switch x := v.(type) {
	case nil, int64, float64, string, bool, time.Time:
		return v, nil
	case []byte:
		if x == nil {
			return nil, nil
		}
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case json.RawMessage:
		if x == nil {
			return nil, nil
		}
		return string(x), nil
	case driver.Valuer:
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, nil
		}
		dv, err := x.Value()
		if err != nil {
			return nil, err
		}
		if _, ok := dv.(driver.Valuer); ok {
			return nil, fmt.Errorf("azora: %T.Value returned a Valuer", v)
		}
		return Normalize(dv)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil, nil
		}
		return Normalize(rv.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("azora: %d overflows int64", u)
		}
		return int64(u), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.String:
		return rv.String(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.IsNil() {
				return nil, nil
			}
			return rv.Bytes(), nil
		}
	}
	return nil, fmt.Errorf("azora: unsupported value type %T", v)
}

func normalizeAll(vs []any) ([]any, error) {
	if vs == nil {
		return nil, nil
	}
	out := make([]any, len(vs))
	for i, v := range vs {
		n, err := Normalize(v)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}
		out[i] = n
	}
	return out, nil
}
