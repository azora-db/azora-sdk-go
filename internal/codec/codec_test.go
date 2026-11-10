package codec

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 123456789, time.UTC)
	values := []any{
		nil, int64(0), int64(-1), int64(math.MaxInt64), int64(math.MinInt64),
		float64(0), math.Pi, math.Inf(-1), "", "héllo, wörld", []byte{}, []byte{0, 1, 2, 255},
		false, true, now, now.In(time.FixedZone("x", -5*3600)),
	}
	var b []byte
	for _, v := range values {
		var err error
		b, err = AppendValue(b, v)
		require.NoError(t, err)
	}
	b = AppendString(b, "trailer")

	d := NewDecoder(b)
	for i, want := range values {
		got := d.Value()
		require.NoError(t, d.Err(), "value %d", i)
		switch w := want.(type) {
		case time.Time:
			require.True(t, w.Equal(got.(time.Time)), "value %d", i)
		case []byte:
			require.NotNil(t, got, "an empty blob must not decode as NULL")
			require.Equal(t, w, got, "value %d", i)
		default:
			require.Equal(t, want, got, "value %d", i)
		}
	}
	require.Equal(t, "trailer", d.String())
	require.NoError(t, d.Err())
	require.Equal(t, 0, d.Remaining())

	// Reading past the end is an error, and the first error sticks.
	require.Nil(t, d.Value())
	require.ErrorIs(t, d.Err(), ErrCorrupt)
}

func TestIntWidthsAndUnsupported(t *testing.T) {
	b, err := AppendValue(nil, int(7))
	require.NoError(t, err)
	b, err = AppendValue(b, int32(-7))
	require.NoError(t, err)
	d := NewDecoder(b)
	require.Equal(t, int64(7), d.Value())
	require.Equal(t, int64(-7), d.Value())
	require.NoError(t, d.Err())

	_, err = AppendValue(nil, struct{}{})
	require.ErrorIs(t, err, ErrUnsupported)
	_, err = AppendValue(nil, uint64(1))
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestDecoderRejectsMalformedInput(t *testing.T) {
	good, err := AppendValue(nil, "some text")
	require.NoError(t, err)
	cases := map[string][]byte{
		"unknown tag":         {0x7f},
		"truncated string":    good[:len(good)-3],
		"truncated float":     {TFloat, 1, 2, 3},
		"truncated varint":    {TInt, 0x80},
		"bad time":            {TTime, 2, 0xff, 0xff},
		"length past the end": {TBytes, 0x40, 1, 2},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			d := NewDecoder(in)
			d.Value()
			require.ErrorIs(t, d.Err(), ErrCorrupt)
		})
	}
	// A decoder never panics on random input.
	for seed := 0; seed < 2000; seed++ {
		in := make([]byte, seed%17)
		for i := range in {
			in[i] = byte(seed*31 + i*7)
		}
		d := NewDecoder(in)
		for d.Err() == nil && d.Remaining() > 0 {
			d.Value()
		}
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	row := []any{int64(42), "order-12345", 19.99, []byte("blob"), nil, true}
	var buf []byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		for _, v := range row {
			buf, _ = AppendValue(buf, v)
		}
		d := NewDecoder(buf)
		for range row {
			d.Value()
		}
	}
}
