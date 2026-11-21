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
	"reflect"
	"regexp"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// tableName is what a GoAlert table may be called: it becomes a Delta column
// name and a partition directory, so it stays plain.
var tableName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// rootName names the root of every data file's Parquet schema.
const rootName = "goalert"

// RowType describes the rows of one GoAlert table in Delta data files.
type RowType[V any] struct {
	name    string
	wrapper reflect.Type // struct { Row *V `parquet:"<name>,optional"` }
	schema  *parquet.Schema
	field   json.RawMessage // this table's column in the Delta schema
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
	return &RowType[V]{name: name, wrapper: wrapper, schema: schema, field: raw}, nil
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
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, rt.schema, parquet.Compression(&parquet.Snappy))
	for i := range rows {
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
	data, err := rt.Encode([]V{v})
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
				et, err := deltaType(elem)
				if err != nil {
					return nil, err
				}
				return deltaArray{Type: "array", ElementType: et, ContainsNull: elem.Optional()}, nil
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
