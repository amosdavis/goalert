package deltatable

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type normInner struct {
	Note   string     `parquet:"note"`
	Data   []byte     `parquet:"data"`
	Tags   []string   `parquet:"tags,list"`
	At     *time.Time `parquet:"at,optional,timestamp(microsecond:utc)"`
	Parent *string    `parquet:"parent,optional"`
	hidden int
}

type normKind string

type normRow struct {
	ID       string        `parquet:"id"`
	Kind     normKind      `parquet:"kind"`
	Bool     bool          `parquet:"bool"`
	I8       int8          `parquet:"i8"`
	I16      int16         `parquet:"i16"`
	I32      int32         `parquet:"i32"`
	I64      int64         `parquet:"i64"`
	Int      int           `parquet:"int"`
	F64      float64       `parquet:"f64"`
	Data     []byte        `parquet:"data"`
	OptData  []byte        `parquet:"opt_data,optional"`
	At       time.Time     `parquet:"at,timestamp(microsecond:utc)"`
	OptAt    *time.Time    `parquet:"opt_at,optional,timestamp(microsecond:utc)"`
	BareAt   *time.Time    `parquet:"bare_at,timestamp(microsecond:utc)"`
	OptStr   *string       `parquet:"opt_str,optional"`
	BareStr  *string       `parquet:"bare_str"`
	OptI64   *int64        `parquet:"opt_i64,optional"`
	OptBool  *bool         `parquet:"opt_bool,optional"`
	OptF64   *float64      `parquet:"opt_f64,optional"`
	ZeroStr  string        `parquet:"zero_str,optional"`
	ZeroInt  int64         `parquet:"zero_int,optional"`
	ZeroBool bool          `parquet:"zero_bool,optional"`
	Strs     []string      `parquet:"strs,list"`
	Kinds    []normKind    `parquet:"kinds,list"`
	OptStrs  []string      `parquet:"opt_strs,optional,list"`
	Ints     []int64       `parquet:"ints,list"`
	Blobs    [][]byte      `parquet:"blobs,list"`
	Inners   []normInner   `parquet:"inners,list"`
	Inner    normInner     `parquet:"inner"`
	OptInner *normInner    `parquet:"opt_inner,optional"`
	Dur      time.Duration `parquet:"dur"`
	Skipped  string        `parquet:"-"`
	Untagged int32
	hidden   int
}

// randomValue fills v with a random value, with plenty of zeros, nils and
// empty slices.
func randomValue(r *rand.Rand, v reflect.Value) {
	if v.Type() == timeType {
		zone := time.FixedZone("zone", r.IntN(24)*3600-12*3600)
		v.Set(reflect.ValueOf(time.Unix(r.Int64N(4e9), r.Int64N(1e9)).In(zone)))
		return
	}
	if v.Type() != reflect.TypeFor[normRow]() && v.Kind() != reflect.Struct && r.IntN(4) == 0 {
		return // zero
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(r.IntN(2) == 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(r.Uint64()) >> (64 - v.Type().Bits()))
	case reflect.Float64:
		v.SetFloat([]float64{0, math.Copysign(0, -1), r.Float64(), math.Inf(1), math.MaxFloat64, math.SmallestNonzeroFloat64}[r.IntN(6)])
	case reflect.String:
		v.SetString([]string{"", "a", "with \x00 zero", "\xff not utf8", "ünïcode", "a longer string, to cross a size"}[r.IntN(6)])
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		randomValue(r, v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), r.IntN(4), 8))
		for i := range v.Len() {
			randomValue(r, v.Index(i))
		}
	case reflect.Uint8:
		v.SetUint(uint64(r.IntN(256)))
	case reflect.Struct:
		for i := range v.NumField() {
			if f := v.Field(i); f.CanSet() {
				randomValue(r, f)
			}
		}
	}
}

// TestNormalizeMatchesRoundTrip checks that the fast way gives exactly what
// writing a row to a data file and reading it back gives (F25).
func TestNormalizeMatchesRoundTrip(t *testing.T) {
	rt, err := NewRowType[normRow]("rows")
	require.NoError(t, err)
	require.NotNil(t, rt.norm, "the fast way must cover the types GoAlert uses")

	r := rand.New(rand.NewPCG(1, 2))
	for i := range 3000 {
		var in normRow
		randomValue(r, reflect.ValueOf(&in).Elem())
		in.hidden, in.Inner.hidden = 1, 2
		if i == 0 {
			in = normRow{At: time.UnixMicro(0)} // everything else zero
		}
		want, err := rt.roundTrip(in)
		require.NoError(t, err)
		got, err := rt.Normalize(in)
		require.NoError(t, err)
		require.Equal(t, math.Signbit(want.F64), math.Signbit(got.F64))
		require.Equal(t, want, got, "value %d: %#v", i, in)

		again, err := rt.Normalize(got)
		require.NoError(t, err)
		require.Equal(t, got, again, "normalizing twice changes nothing")
	}

	nan := math.Float64frombits(0x7ff8000000000123)
	in := normRow{F64: nan, At: time.UnixMicro(1)}
	want, err := rt.roundTrip(in)
	require.NoError(t, err)
	got, err := rt.Normalize(in)
	require.NoError(t, err)
	assert.Equal(t, math.Float64bits(want.F64), math.Float64bits(got.F64), "NaNs keep their bits")
}

// TestNormalizeCopies checks that a normalized value shares nothing with the
// caller's, which may go on changing it.
func TestNormalizeCopies(t *testing.T) {
	rt, err := NewRowType[normRow]("rows")
	require.NoError(t, err)
	str, at := "a", time.UnixMicro(5)
	in := normRow{
		At: at, OptAt: &at, OptStr: &str, Data: []byte{1}, OptData: []byte{1}, Strs: []string{"a"}, Blobs: [][]byte{{1}},
		Inners: []normInner{{Data: []byte{1}}}, OptInner: &normInner{Tags: []string{"a"}},
	}
	out, err := rt.Normalize(in)
	require.NoError(t, err)
	want, err := rt.roundTrip(in)
	require.NoError(t, err)

	*in.OptAt, *in.OptStr, in.Data[0], in.OptData[0], in.Strs[0], in.Blobs[0][0] = time.UnixMicro(9), "b", 2, 2, "b", 2
	in.Inners[0].Data[0], in.OptInner.Tags[0] = 2, "b"
	assert.Equal(t, want, out)
}

// TestNormalizeFallsBack checks that a shape the fast way doesn't cover still
// gives what a data file would.
func TestNormalizeFallsBack(t *testing.T) {
	type inner struct {
		Note string `parquet:"note"`
		Data []byte `parquet:"data"`
	}
	type odd struct {
		ID    string  `parquet:"id"`
		Inner inner   `parquet:"inner,optional"` // a zero struct reads back as missing
		F     float64 `parquet:"f,optional"`
		Data  *[]byte `parquet:"data,optional"`
	}
	rt, err := NewRowType[odd]("rows")
	require.NoError(t, err)
	require.Nil(t, rt.norm)
	for _, in := range []odd{{}, {Inner: inner{Note: "n"}, F: 1, Data: &[]byte{}}} {
		want, err := rt.roundTrip(in)
		require.NoError(t, err)
		got, err := rt.Normalize(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

// TestRejectsPointerLists covers lists of pointers, whose elements parquet-go
// can read back as nil although they were not.
func TestRejectsPointerLists(t *testing.T) {
	type ptrs struct {
		L []*string `parquet:"l,list"`
	}
	_, err := NewRowType[ptrs]("rows")
	assert.ErrorContains(t, err, "list elements")

	type structs struct {
		L []*normInner `parquet:"l,list"`
	}
	_, err = NewRowType[structs]("rows")
	assert.ErrorContains(t, err, "list elements")
}
