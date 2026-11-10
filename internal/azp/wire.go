package azp

import (
	"encoding/binary"
	"fmt"

	"github.com/azora-db/azora-go/internal/codec"
)

// encoder appends a message's fields; the first value it cannot encode
// is kept in err.
type encoder struct {
	b   []byte
	err error
}

func (e *encoder) u(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) i(v int64)  { e.b = binary.AppendVarint(e.b, v) }
func (e *encoder) s(v string) { e.b = codec.AppendString(e.b, v) }
func (e *encoder) flag(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}

func (e *encoder) val(v any) {
	if e.err != nil {
		return
	}
	e.b, e.err = codec.AppendValue(e.b, v)
}

// vals writes a list of values; nil and empty differ (a nil row is
// "absent", for example the old row of an insert).
func (e *encoder) vals(vs []any) {
	if vs == nil {
		e.u(0)
		return
	}
	e.u(uint64(len(vs)) + 1)
	for _, v := range vs {
		e.val(v)
	}
}

func (e *encoder) strs(ss []string) {
	e.u(uint64(len(ss)))
	for _, s := range ss {
		e.s(s)
	}
}

// decoder reads a message's fields; the first malformation is kept in
// err (the codec.Decoder keeps its own).
type decoder struct {
	d   *codec.Decoder
	err error
}

func (d *decoder) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
}

func (d *decoder) u() uint64 { return d.d.Uvarint() }
func (d *decoder) i() int64  { return d.d.Varint() }
func (d *decoder) s() string { return d.d.String() }

func (d *decoder) u32() uint32 {
	v := d.d.Uvarint()
	if v > 1<<32-1 {
		d.fail("value %d out of range", v)
	}
	return uint32(v)
}

func (d *decoder) flag() bool {
	b := d.d.Bytes(1)
	if len(b) != 1 {
		return false
	}
	if b[0] > 1 {
		d.fail("bad flag %d", b[0])
	}
	return b[0] == 1
}

// count reads a list length, refusing lengths the rest of the message
// cannot hold (every element takes at least one byte).
func (d *decoder) count() int {
	n := d.d.Uvarint()
	if n > uint64(d.d.Remaining()) {
		d.fail("list of %d elements in %d bytes", n, d.d.Remaining())
		return 0
	}
	return int(n)
}

func (d *decoder) vals() []any {
	n := d.d.Uvarint()
	if n == 0 {
		return nil
	}
	n--
	if n > uint64(d.d.Remaining()) {
		d.fail("list of %d values in %d bytes", n, d.d.Remaining())
		return nil
	}
	out := make([]any, n)
	for i := range out {
		out[i] = d.d.Value()
		if d.d.Err() != nil {
			return nil
		}
	}
	return out
}

func (d *decoder) strs() []string {
	n := d.count()
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = d.s()
	}
	return out
}
