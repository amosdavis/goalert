// Package deltatable maps GoAlert's tables onto one Delta Lake table: data
// files are Parquet, partitioned by GoAlert table and hash bucket, and each
// GoAlert table is one struct column (docs/filestore-plan.md).
//
// Only column types with an unambiguous Delta mapping are allowed, so other
// Delta readers (Spark, Databricks, delta-rs) read exactly what GoAlert wrote
// (docs/filestore-failure-modes.md, F35).
package deltatable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"regexp"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// tableName is what a GoAlert table may be called: it becomes a Delta column
// name and a partition directory (tbl=<name>), so it stays plain. A leading
// underscore marks the store's own tables, such as its sequences.
var tableName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// rootName names the root of every data file's Parquet schema.
const rootName = "goalert"

// parquet-go v0.32.0 writes a timestamp as microseconds but reads it back
// through int64 nanoseconds, so only times from 1677-09-21 to 2262-04-11
// read back as written. Go's zero time does not. Outside that range,
// GoAlert would read garbage where other Delta readers see the right time
// (F25, F35), so such times are refused when written.
const (
	minMicros = math.MinInt64 / 1000
	maxMicros = math.MaxInt64 / 1000
)

var timeType = reflect.TypeFor[time.Time]()

// hasTimes reports whether values of t can hold a time.Time.
func hasTimes(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return hasTimes(t.Elem())
	case reflect.Struct:
		if t == timeType {
			return true
		}
		for i := range t.NumField() {
			if f := t.Field(i); f.IsExported() && hasTimes(f.Type) {
				return true
			}
		}
	}
	return false
}

// checkTimes refuses a value holding a time that would not read back as
// written.
func checkTimes(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return checkTimes(v.Elem())
	case reflect.Struct:
		if v.Type() == timeType {
			t := v.Interface().(time.Time)
			if us := t.UnixMicro(); us < minMicros || us > maxMicros {
				return fmt.Errorf("time %s is outside the range a data file holds (%s to %s); use a pointer for a time that can be unset",
					t.Format(time.RFC3339), time.UnixMicro(minMicros).UTC().Format(time.DateOnly), time.UnixMicro(maxMicros).UTC().Format(time.DateOnly))
			}
			return nil
		}
		for i := range v.NumField() {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			if err := checkTimes(v.Field(i)); err != nil {
				return fmt.Errorf("%s: %w", v.Type().Field(i).Name, err)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if err := checkTimes(v.Index(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// RowType describes the rows of one GoAlert table in Delta data files.
type RowType[V any] struct {
	name    string
	wrapper reflect.Type // struct { Row *V `parquet:"<name>,optional"` }
	schema  *parquet.Schema
	field   json.RawMessage // this table's column in the Delta schema
	times   bool            // whether rows can hold times, which are checked
	norm    normalizer      // nil if only the round trip is known to be right
}

// NewRowType describes table name, whose rows are V. V's fields use parquet
// struct tags; times must be tagged timestamp(microsecond:utc).
func NewRowType[V any](name string) (*RowType[V], error) {
	if !tableName.MatchString(name) {
		return nil, fmt.Errorf("deltatable: invalid table name %q", name)
	}
	vType := reflect.TypeFor[V]()
	if vType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("deltatable: %s rows must be structs, not %s", name, vType)
	}
	wrapper := reflect.StructOf([]reflect.StructField{{
		Name: "Row",
		Type: reflect.PointerTo(vType),
		Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional"`, name)),
	}})
	// The wrapper type has no name, so the file's root gets one here.
	schema := parquet.NewSchema(rootName, parquet.SchemaOf(reflect.New(wrapper).Interface()))
	fields := schema.Fields()
	if len(fields) != 1 {
		return nil, fmt.Errorf("deltatable: %s: unexpected schema %s", name, schema)
	}
	field, err := deltaField(fields[0])
	if err != nil {
		return nil, fmt.Errorf("deltatable: %s: %w", name, err)
	}
	raw, err := json.Marshal(field)
	if err != nil {
		return nil, err
	}
	return &RowType[V]{name: name, wrapper: wrapper, schema: schema, field: raw, times: hasTimes(vType), norm: structNormalizer(vType)}, nil
}

// Name returns the table's name.
func (rt *RowType[V]) Name() string { return rt.name }

// DeltaField returns this table's column in the Delta schema, as JSON.
func (rt *RowType[V]) DeltaField() json.RawMessage { return rt.field }

func (rt *RowType[V]) wrap(v *V) reflect.Value {
	w := reflect.New(rt.wrapper)
	w.Elem().Field(0).Set(reflect.ValueOf(v))
	return w
}

// Encode writes rows as one Snappy-compressed Parquet file.
func (rt *RowType[V]) Encode(rows []V) ([]byte, error) {
	return rt.encode(rows)
}

func (rt *RowType[V]) encode(rows []V, opts ...parquet.WriterOption) ([]byte, error) {
	var buf bytes.Buffer
	opts = append([]parquet.WriterOption{rt.schema, parquet.Compression(&parquet.Snappy)}, opts...)
	w := parquet.NewWriter(&buf, opts...)
	for i := range rows {
		if rt.times {
			if err := checkTimes(reflect.ValueOf(&rows[i]).Elem()); err != nil {
				return nil, fmt.Errorf("deltatable: %s: row %d: %w", rt.name, i, err)
			}
		}
		if err := w.Write(rt.wrap(&rows[i]).Interface()); err != nil {
			return nil, fmt.Errorf("deltatable: %s: write row %d: %w", rt.name, i, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("deltatable: %s: %w", rt.name, err)
	}
	return buf.Bytes(), nil
}

// Decode reads the rows of a Parquet file written by Encode.
func (rt *RowType[V]) Decode(data []byte) ([]V, error) {
	r := parquet.NewReader(bytes.NewReader(data), rt.schema)
	defer func() { _ = r.Close() }()
	rows := make([]V, 0, r.NumRows())
	for {
		w := reflect.New(rt.wrapper)
		err := r.Read(w.Interface())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("deltatable: %s: read row %d: %w", rt.name, len(rows), err)
		}
		p, _ := w.Elem().Field(0).Interface().(*V)
		if p == nil {
			return nil, fmt.Errorf("deltatable: %s: row %d has no %s value", rt.name, len(rows), rt.name)
		}
		rows = append(rows, *p)
	}
	return rows, nil
}

// Normalize returns v as a Parquet file would give it back, for example
// with times at microsecond precision in UTC. Every pod builds its state
// from decoded files, so a writer stores the normalized value too (F25).
func (rt *RowType[V]) Normalize(v V) (V, error) {
	if rt.norm == nil {
		return rt.roundTrip(v)
	}
	src := reflect.ValueOf(&v).Elem()
	if rt.times {
		if err := checkTimes(src); err != nil {
			return v, fmt.Errorf("deltatable: %s: %w", rt.name, err)
		}
	}
	var out V
	rt.norm(reflect.ValueOf(&out).Elem(), src)
	return out, nil
}

// roundTrip writes v to a Parquet file and reads it back.
func (rt *RowType[V]) roundTrip(v V) (V, error) {
	// One row needs small buffers; the defaults are sized for whole files.
	data, err := rt.encode([]V{v}, parquet.PageBufferSize(4<<10), parquet.WriteBufferSize(4<<10))
	if err != nil {
		return v, err
	}
	rows, err := rt.Decode(data)
	if err != nil {
		return v, err
	}
	return rows[0], nil
}

// Delta schema types (PROTOCOL.md, "Schema Serialization Format").
type deltaStruct struct {
	Type   string        `json:"type"` // "struct"
	Fields []deltaFieldT `json:"fields"`
}

type deltaArray struct {
	Type         string `json:"type"` // "array"
	ElementType  any    `json:"elementType"`
	ContainsNull bool   `json:"containsNull"`
}

type deltaFieldT struct {
	Name     string            `json:"name"`
	Type     any               `json:"type"`
	Nullable bool              `json:"nullable"`
	Metadata map[string]string `json:"metadata"`
}

func deltaField(f parquet.Field) (deltaFieldT, error) {
	t, err := deltaType(f)
	if err != nil {
		return deltaFieldT{}, fmt.Errorf("column %s: %w", f.Name(), err)
	}
	return deltaFieldT{Name: f.Name(), Type: t, Nullable: f.Optional(), Metadata: map[string]string{}}, nil
}

// deltaType maps a Parquet node to its Delta type, refusing anything without
// an exact, reader-version-1 equivalent.
func deltaType(n parquet.Node) (any, error) {
	if !n.Leaf() {
		if lt := n.Type().LogicalType(); lt != nil {
			if _, ok := lt.Value.(*format.ListType); ok {
				elem := n.Fields()[0].Fields()[0] // list.element
				if elem.Optional() {
					// parquet-go reads some elements back as nil.
					return nil, fmt.Errorf("list elements can't be optional (pointers)")
				}
				et, err := deltaType(elem)
				if err != nil {
					return nil, err
				}
				return deltaArray{Type: "array", ElementType: et, ContainsNull: false}, nil
			}
			return nil, fmt.Errorf("unsupported group type %s", lt)
		}
		var s deltaStruct
		s.Type = "struct"
		for _, f := range n.Fields() {
			df, err := deltaField(f)
			if err != nil {
				return nil, err
			}
			s.Fields = append(s.Fields, df)
		}
		return s, nil
	}
	if n.Repeated() {
		return nil, fmt.Errorf("repeated columns need the list tag")
	}
	t := n.Type()
	var logical any
	if lt := t.LogicalType(); lt != nil {
		logical = lt.Value
	}
	switch t.Kind() {
	case parquet.Boolean:
		if logical == nil {
			return "boolean", nil
		}
	case parquet.Int32:
		if logical == nil {
			return "integer", nil
		}
		if it, ok := logical.(*format.IntType); ok && it.IsSigned {
			switch it.BitWidth {
			case 8:
				return "byte", nil
			case 16:
				return "short", nil
			case 32:
				return "integer", nil
			}
		}
	case parquet.Int64:
		if logical == nil {
			return "long", nil
		}
		if it, ok := logical.(*format.IntType); ok && it.IsSigned && it.BitWidth == 64 {
			return "long", nil
		}
		if ts, ok := logical.(*format.TimestampType); ok {
			_, micros := ts.Unit.Value.(*format.MicroSeconds)
			if ts.IsAdjustedToUTC && micros {
				return "timestamp", nil
			}
			return nil, fmt.Errorf("timestamps must be tagged timestamp(microsecond:utc)")
		}
	case parquet.Double:
		if logical == nil {
			return "double", nil
		}
	case parquet.ByteArray:
		if logical == nil {
			return "binary", nil
		}
		if _, ok := logical.(*format.StringType); ok {
			return "string", nil
		}
	}
	return nil, fmt.Errorf("unsupported type %s %v", t.Kind(), logical)
}
