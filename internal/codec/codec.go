// Package codec encodes SQLite driver values compactly: nil, int64,
// float64, string, []byte, bool and time.Time. The cache bucket stores
// query results with it and the change journal stores statement arguments.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// ErrUnsupported is returned for values that cannot be encoded.
var ErrUnsupported = errors.New("codec: unsupported value type")

// ErrCorrupt is returned for malformed input.
var ErrCorrupt = errors.New("codec: corrupt data")

const (
	TNil byte = iota
	TInt
	TFloat
	TString
	TBytes
	TFalse
	TTrue
	TTime
)

// AppendValue appends the encoding of v to b.
func AppendValue(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, TNil), nil
	case int64:
		return binary.AppendVarint(append(b, TInt), x), nil
	case int:
		return binary.AppendVarint(append(b, TInt), int64(x)), nil
	case int32:
		return binary.AppendVarint(append(b, TInt), int64(x)), nil
	case float64:
		return binary.LittleEndian.AppendUint64(append(b, TFloat), math.Float64bits(x)), nil
	case string:
		b = binary.AppendUvarint(append(b, TString), uint64(len(x)))
		return append(b, x...), nil
	case []byte:
		b = binary.AppendUvarint(append(b, TBytes), uint64(len(x)))
		return append(b, x...), nil
	case bool:
		if x {
			return append(b, TTrue), nil
		}
		return append(b, TFalse), nil
	case time.Time:
		tb, err := x.MarshalBinary()
		if err != nil {
			return nil, err
		}
		b = binary.AppendUvarint(append(b, TTime), uint64(len(tb)))
		return append(b, tb...), nil
	}
	return nil, fmt.Errorf("%w: %T", ErrUnsupported, v)
}

// AppendString appends a length-prefixed string.
func AppendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// Decoder reads values written by AppendValue and the Append helpers. The
// first error sticks; check Err after decoding.
type Decoder struct {
	b   []byte
	err error
}

// NewDecoder wraps b.
func NewDecoder(b []byte) *Decoder { return &Decoder{b: b} }

// Err returns the first decoding error.
func (d *Decoder) Err() error { return d.err }

// Remaining returns how many bytes are left.
func (d *Decoder) Remaining() int { return len(d.b) }

func (d *Decoder) fail() {
	if d.err == nil {
		d.err = ErrCorrupt
	}
}

// Uvarint reads an unsigned varint.
func (d *Decoder) Uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// Varint reads a signed varint.
func (d *Decoder) Varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// Bytes returns the next n bytes (a view, not a copy).
func (d *Decoder) Bytes(n uint64) []byte {
	if uint64(len(d.b)) < n {
		d.fail()
		return nil
	}
	out := d.b[:n:n]
	d.b = d.b[n:]
	return out
}

// String reads a length-prefixed string.
func (d *Decoder) String() string { return string(d.Bytes(d.Uvarint())) }

// Value reads one encoded value.
func (d *Decoder) Value() any {
	if len(d.b) == 0 {
		d.fail()
		return nil
	}
	t := d.b[0]
	d.b = d.b[1:]
	switch t {
	case TNil:
		return nil
	case TInt:
		return d.Varint()
	case TFloat:
		b := d.Bytes(8)
		if b == nil {
			return nil
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b))
	case TString:
		return d.String()
	case TBytes:
		src := d.Bytes(d.Uvarint())
		out := make([]byte, len(src)) // never nil: an empty blob is not NULL
		copy(out, src)
		return out
	case TFalse:
		return false
	case TTrue:
		return true
	case TTime:
		var tm time.Time
		if err := tm.UnmarshalBinary(d.Bytes(d.Uvarint())); err != nil {
			d.fail()
		}
		return tm
	}
	d.fail()
	return nil
}
