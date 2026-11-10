package codec

import (
	"testing"
	"time"
)

func FuzzDecoder(f *testing.F) {
	b, _ := AppendValue(nil, int64(-5))
	b, _ = AppendValue(b, "héllo")
	b, _ = AppendValue(b, []byte{0, 1})
	b, _ = AppendValue(b, 2.5)
	b, _ = AppendValue(b, true)
	b, _ = AppendValue(b, nil)
	b, _ = AppendValue(b, time.Unix(1, 2))
	f.Add(b)
	f.Add([]byte{TString, 0xff, 0xff, 0xff})
	f.Add([]byte{TTime, 3, 1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder(data)
		for i := 0; i < 64 && d.Err() == nil && d.Remaining() > 0; i++ {
			v := d.Value()
			if d.Err() != nil {
				break
			}
			// Every decoded value must re-encode.
			if _, err := AppendValue(nil, v); err != nil {
				t.Fatalf("decoded value %#v cannot be encoded: %v", v, err)
			}
		}
	})
}
