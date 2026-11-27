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

// Delta timestamps hold signed 64-bit Unix microseconds. time.Time supports
// larger years, whose UnixMicro result can overflow (F25, F35).
const (
	minMicros = math.MinInt64
	maxMicros = math.MaxInt64
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

// checkTimes refuses a value holding a time that cannot round-trip through the
// signed 64-bit Unix microsecond representation. Truncation to microseconds is
// expected; wrapping around into a different instant is not.
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
			us := t.UnixMicro()
			if !time.UnixMicro(us).Equal(t.Truncate(time.Microsecond)) {
				return fmt.Errorf("time %s is outside the range of Unix microseconds (%s to %s)",
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

// Rows describes the rows of one GoAlert table in Delta data files. A row is a
// pointer to a value of its Go type.
type Rows struct {
	name    string
	typ     reflect.Type // of what rows are pointers to
	wrapper reflect.Type // struct { Row *typ `parquet:"<name>,optional"` }
	schema  *parquet.Schema
	field   json.RawMessage // this table's column in the Delta schema
	times   bool            // whether rows can hold times, which are checked
	norm    normalizer      // nil if only the round trip is known to be right
}

// NewRows describes table name, whose rows are pointers to a struct of type
// typ. Its fields use parquet struct tags; times must be tagged
// timestamp(microsecond:utc).
func NewRows(name string, typ reflect.Type) (*Rows, error) {
	if !tableName.MatchString(name) {
		return nil, fmt.Errorf("deltatable: invalid table name %q", name)
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("deltatable: %s rows must be structs, not %s", name, typ)
	}
	wrapper := reflect.StructOf([]reflect.StructField{{
		Name: "Row",
		Type: reflect.PointerTo(typ),
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
	return &Rows{
		name: name, typ: typ, wrapper: wrapper, schema: schema, field: raw,
		times: hasTimes(typ), norm: structNormalizer(typ),
	}, nil
}

// Name returns the table's name.
func (r *Rows) Name() string { return r.name }

// DeltaField returns this table's column in the Delta schema, as JSON.
func (r *Rows) DeltaField() json.RawMessage { return r.field }

func (r *Rows) wrap(row any) reflect.Value {
	w := reflect.New(r.wrapper)
	w.Elem().Field(0).Set(reflect.ValueOf(row))
	return w
}

// Encode writes rows as one Snappy-compressed Parquet file.
func (r *Rows) Encode(rows []any) ([]byte, error) {
	return r.encode(rows)
}

func (r *Rows) encode(rows []any, opts ...parquet.WriterOption) ([]byte, error) {
	var buf bytes.Buffer
	opts = append([]parquet.WriterOption{r.schema, parquet.Compression(&parquet.Snappy)}, opts...)
	w := parquet.NewWriter(&buf, opts...)
	for i, row := range rows {
		if r.times {
			if err := checkTimes(reflect.ValueOf(row).Elem()); err != nil {
				return nil, fmt.Errorf("deltatable: %s: row %d: %w", r.name, i, err)
			}
		}
		if err := w.Write(r.wrap(row).Interface()); err != nil {
			return nil, fmt.Errorf("deltatable: %s: write row %d: %w", r.name, i, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("deltatable: %s: %w", r.name, err)
	}
	return buf.Bytes(), nil
}

// Decode reads the rows of a Parquet file written by Encode.
func (r *Rows) Decode(data []byte) ([]any, error) {
	pr := parquet.NewReader(bytes.NewReader(data), r.schema)
	defer func() { _ = pr.Close() }()
	rows := make([]any, 0, pr.NumRows())
	for {
		w := reflect.New(r.wrapper)
		err := pr.Read(w.Interface())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("deltatable: %s: read row %d: %w", r.name, len(rows), err)
		}
		row := w.Elem().Field(0)
		if row.IsNil() {
			return nil, fmt.Errorf("deltatable: %s: row %d has no %s value", r.name, len(rows), r.name)
		}
		rows = append(rows, row.Interface())
	}
	return rows, nil
}

// Normalize returns a copy of row as a Parquet file would give it back, for
// example with times at microsecond precision in UTC. Every pod builds its
// state from decoded files, so a writer stores the normalized value too (F25).
func (r *Rows) Normalize(row any) (any, error) {
	if r.norm == nil {
		return r.roundTrip(row)
	}
	src := reflect.ValueOf(row).Elem()
	if r.times {
		if err := checkTimes(src); err != nil {
			return nil, fmt.Errorf("deltatable: %s: %w", r.name, err)
		}
	}
	out := reflect.New(r.typ)
	r.norm(out.Elem(), src)
	return out.Interface(), nil
}

// roundTrip writes a row to a Parquet file and reads it back.
func (r *Rows) roundTrip(row any) (any, error) {
	// One row needs small buffers; the defaults are sized for whole files.
	data, err := r.encode([]any{row}, parquet.PageBufferSize(4<<10), parquet.WriteBufferSize(4<<10))
	if err != nil {
		return nil, err
	}
	rows, err := r.Decode(data)
	if err != nil {
		return nil, err
	}
	return rows[0], nil
}

// RowType is Rows of a type that is known, with rows as values.
type RowType[V any] struct{ *Rows }

// NewRowType describes table name, whose rows are V, see NewRows.
func NewRowType[V any](name string) (*RowType[V], error) {
	r, err := NewRows(name, reflect.TypeFor[V]())
	if err != nil {
		return nil, err
	}
	return &RowType[V]{r}, nil
}

// Encode writes rows as one Snappy-compressed Parquet file.
func (rt *RowType[V]) Encode(rows []V) ([]byte, error) {
	ptrs := make([]any, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	return rt.Rows.Encode(ptrs)
}

// Decode reads the rows of a Parquet file written by Encode.
func (rt *RowType[V]) Decode(data []byte) ([]V, error) {
	ptrs, err := rt.Rows.Decode(data)
	if err != nil {
		return nil, err
	}
	rows := make([]V, len(ptrs))
	for i, p := range ptrs {
		rows[i] = *p.(*V)
	}
	return rows, nil
}

// Normalize returns v as a Parquet file would give it back, see Rows.
func (rt *RowType[V]) Normalize(v V) (V, error) { return asValue[V](rt.Rows.Normalize(&v)) }

func (rt *RowType[V]) roundTrip(v V) (V, error) { return asValue[V](rt.Rows.roundTrip(&v)) }

func asValue[V any](row any, err error) (V, error) {
	if err != nil {
		var zero V
		return zero, err
	}
	return *row.(*V), nil
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
