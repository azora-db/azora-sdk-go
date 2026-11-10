package azora

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// A setter stores one database value (nil, int64, float64, string,
// []byte; also bool and time.Time, which only test servers send) into a
// settable reflect.Value of the type it was made for.
type setter func(dst reflect.Value, src any) error

var (
	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType    = reflect.TypeOf(time.Time{})
	rawJSONType = reflect.TypeOf(json.RawMessage(nil))
	bytesType   = reflect.TypeOf([]byte(nil))
)

// setterFor returns the setter of type t. asJSON decodes JSON text into
// types that have no direct conversion (structs, maps, slices) and is
// selected by the `json` option of a db tag.
func setterFor(t reflect.Type, asJSON bool) (setter, error) {
	switch {
	case reflect.PointerTo(t).Implements(scannerType):
		return setScanner, nil
	case t == timeType:
		return setTime, nil
	case t == rawJSONType:
		return setRawJSON, nil
	case asJSON:
		return setJSON, nil
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem, err := setterFor(t.Elem(), false)
		if err != nil {
			return nil, err
		}
		return func(dst reflect.Value, src any) error {
			if src == nil {
				dst.SetZero()
				return nil
			}
			if dst.IsNil() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			return elem(dst.Elem(), src)
		}, nil
	case reflect.Interface:
		if t.NumMethod() == 0 {
			return setAny, nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return setInt, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return setUint, nil
	case reflect.Float32, reflect.Float64:
		return setFloat, nil
	case reflect.String:
		return setString, nil
	case reflect.Bool:
		return setBool, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return setBytes, nil
		}
	}
	return nil, fmt.Errorf("azora: cannot scan into %s (use a db:\"name,json\" tag to decode JSON text)", t)
}

func convErr(src any, dst reflect.Value) error {
	return fmt.Errorf("azora: cannot convert %s to %s", describe(src), dst.Type())
}

// describe names a database value for error messages without printing
// large content.
func describe(src any) string {
	switch x := src.(type) {
	case nil:
		return "NULL"
	case int64:
		return "integer " + strconv.FormatInt(x, 10)
	case float64:
		return "float " + strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		if len(x) > 40 {
			return fmt.Sprintf("text %q...", x[:40])
		}
		return fmt.Sprintf("text %q", x)
	case []byte:
		return fmt.Sprintf("blob of %d bytes", len(x))
	}
	return fmt.Sprintf("%T", src)
}

func setAny(dst reflect.Value, src any) error {
	if src == nil {
		dst.SetZero()
		return nil
	}
	dst.Set(reflect.ValueOf(src))
	return nil
}

func setScanner(dst reflect.Value, src any) error {
	return dst.Addr().Interface().(sql.Scanner).Scan(src)
}

func setInt(dst reflect.Value, src any) error {
	var n int64
	switch x := src.(type) {
	case nil:
	case int64:
		n = x
	case float64:
		if x != math.Trunc(x) || x < -(1<<63) || x >= 1<<63 {
			return convErr(src, dst)
		}
		n = int64(x)
	case string:
		v, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return convErr(src, dst)
		}
		n = v
	case []byte:
		v, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return convErr(src, dst)
		}
		n = v
	case bool:
		if x {
			n = 1
		}
	default:
		return convErr(src, dst)
	}
	if dst.OverflowInt(n) {
		return fmt.Errorf("azora: integer %d overflows %s", n, dst.Type())
	}
	dst.SetInt(n)
	return nil
}

func setUint(dst reflect.Value, src any) error {
	var n uint64
	switch x := src.(type) {
	case nil:
	case int64:
		if x < 0 {
			return fmt.Errorf("azora: integer %d overflows %s", x, dst.Type())
		}
		n = uint64(x)
	case float64:
		if x != math.Trunc(x) || x < 0 || x >= 1<<64 {
			return convErr(src, dst)
		}
		n = uint64(x)
	case string:
		v, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			return convErr(src, dst)
		}
		n = v
	case []byte:
		v, err := strconv.ParseUint(string(x), 10, 64)
		if err != nil {
			return convErr(src, dst)
		}
		n = v
	case bool:
		if x {
			n = 1
		}
	default:
		return convErr(src, dst)
	}
	if dst.OverflowUint(n) {
		return fmt.Errorf("azora: integer %d overflows %s", n, dst.Type())
	}
	dst.SetUint(n)
	return nil
}

func setFloat(dst reflect.Value, src any) error {
	var f float64
	switch x := src.(type) {
	case nil:
	case float64:
		f = x
	case int64:
		f = float64(x)
	case string:
		v, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return convErr(src, dst)
		}
		f = v
	case []byte:
		v, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return convErr(src, dst)
		}
		f = v
	default:
		return convErr(src, dst)
	}
	dst.SetFloat(f)
	return nil
}

func setString(dst reflect.Value, src any) error {
	switch x := src.(type) {
	case nil:
		dst.SetString("")
	case string:
		dst.SetString(x)
	case []byte:
		dst.SetString(string(x))
	case int64:
		dst.SetString(strconv.FormatInt(x, 10))
	case float64:
		dst.SetString(strconv.FormatFloat(x, 'g', -1, 64))
	case bool:
		dst.SetString(strconv.FormatBool(x))
	case time.Time:
		dst.SetString(x.Format(time.RFC3339Nano))
	default:
		return convErr(src, dst)
	}
	return nil
}

func setBool(dst reflect.Value, src any) error {
	switch x := src.(type) {
	case nil:
		dst.SetBool(false)
	case bool:
		dst.SetBool(x)
	case int64:
		if x != 0 && x != 1 {
			return convErr(src, dst)
		}
		dst.SetBool(x == 1)
	case float64:
		if x != 0 && x != 1 {
			return convErr(src, dst)
		}
		dst.SetBool(x == 1)
	case string:
		v, err := strconv.ParseBool(x)
		if err != nil {
			return convErr(src, dst)
		}
		dst.SetBool(v)
	default:
		return convErr(src, dst)
	}
	return nil
}

func setBytes(dst reflect.Value, src any) error {
	switch x := src.(type) {
	case nil:
		dst.SetZero()
	case []byte:
		dst.SetBytes(append(make([]byte, 0, len(x)), x...))
	case string:
		dst.SetBytes([]byte(x))
	default:
		return convErr(src, dst)
	}
	return nil
}

func setRawJSON(dst reflect.Value, src any) error {
	switch x := src.(type) {
	case nil:
		dst.SetZero()
	case string:
		dst.SetBytes([]byte(x))
	case []byte:
		dst.SetBytes(append(make([]byte, 0, len(x)), x...))
	case int64, float64, bool:
		b, _ := json.Marshal(x)
		dst.SetBytes(b)
	default:
		return convErr(src, dst)
	}
	return nil
}

// setJSON decodes JSON text into dst; NULL gives the zero value.
func setJSON(dst reflect.Value, src any) error {
	var data []byte
	switch x := src.(type) {
	case nil:
		dst.SetZero()
		return nil
	case string:
		data = []byte(x)
	case []byte:
		data = x
	default:
		return convErr(src, dst)
	}
	dst.SetZero()
	if err := json.Unmarshal(data, dst.Addr().Interface()); err != nil {
		return fmt.Errorf("azora: decode JSON into %s: %w", dst.Type(), err)
	}
	return nil
}

// timeLayouts are the text forms of a time, tried in order: the engine's
// SQLite driver format first, then RFC 3339 and the forms SQLite's date
// functions write.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"2006-01-02",
}

// parseTime reads a time stored as text (see timeLayouts; without a zone
// it is UTC), as an integer (Unix seconds) or as a float (a Julian day
// number below 5373484.5, like SQLite's 'auto' modifier, else Unix
// seconds).
func parseTime(src any) (time.Time, bool) {
	switch x := src.(type) {
	case time.Time:
		return x, true
	case int64:
		return time.Unix(x, 0).UTC(), true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return time.Time{}, false
		}
		if x >= 0 && x < 5373484.5 {
			x = (x - 2440587.5) * 86400 // Julian day to Unix seconds
		}
		sec, frac := math.Modf(x)
		return time.Unix(int64(sec), int64(math.Round(frac*1e9))).UTC(), true
	case []byte:
		return parseTime(string(x))
	case string:
		for _, l := range timeLayouts {
			if t, err := time.Parse(l, x); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func setTime(dst reflect.Value, src any) error {
	if src == nil {
		dst.SetZero()
		return nil
	}
	t, ok := parseTime(src)
	if !ok {
		return convErr(src, dst)
	}
	dst.Set(reflect.ValueOf(t))
	return nil
}

// JSON wraps a value stored as JSON text in a column: it scans by
// decoding the text and binds as an argument by encoding V.
//
//	var prefs azora.JSON[map[string]bool]
//	err := db.Get(ctx, &prefs, "SELECT prefs FROM users WHERE id = ?", id)
//	_, err = db.Exec(ctx, "UPDATE users SET prefs = ? WHERE id = ?", azora.JSON[Prefs]{V: p}, id)
//
// NULL scans to the zero V. In a struct, a field tagged `db:"prefs,json"`
// does the same without the wrapper.
type JSON[T any] struct{ V T }

// Scan implements sql.Scanner.
func (j *JSON[T]) Scan(src any) error {
	var zero T
	j.V = zero
	switch x := src.(type) {
	case nil:
		return nil
	case string:
		return json.Unmarshal([]byte(x), &j.V)
	case []byte:
		return json.Unmarshal(x, &j.V)
	}
	return fmt.Errorf("azora: cannot decode %s as JSON", describe(src))
}

// Value implements driver.Valuer: the JSON encoding of V, as text.
func (j JSON[T]) Value() (driver.Value, error) {
	b, err := json.Marshal(j.V)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}
