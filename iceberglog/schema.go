package iceberglog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/target/goalert/tablelog"
)

// deltaSchema is the Spark SQL JSON representation stored in Delta metadata.
// Types are RawMessages because primitive types are strings while compound
// types are objects.
type deltaSchema struct {
	Type   string       `json:"type"`
	Fields []deltaField `json:"fields"`
}

type deltaField struct {
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
	Nullable bool            `json:"nullable"`
}

func icebergSchema(meta tablelog.MetaData) (*iceberg.Schema, error) {
	var root deltaSchema
	if err := json.Unmarshal([]byte(meta.SchemaString), &root); err != nil {
		return nil, fmt.Errorf("%w: invalid Delta schema JSON: %v", tablelog.ErrUnsupported, err)
	}
	if root.Type != "struct" {
		return nil, fmt.Errorf("%w: Delta root type %q is not struct", tablelog.ErrUnsupported, root.Type)
	}
	partitioned := make(map[string]bool, len(meta.PartitionColumns))
	for _, name := range meta.PartitionColumns {
		partitioned[name] = true
	}

	nextID := 1
	var fields []iceberg.NestedField
	for _, field := range root.Fields {
		// Delta's physical files omit these columns. tbl and bucket remain
		// logical Add partition values, not an Iceberg partition spec.
		if partitioned[field.Name] {
			continue
		}
		converted, err := convertDeltaField(field, &nextID)
		if err != nil {
			return nil, err
		}
		fields = append(fields, converted)
	}
	return iceberg.NewSchema(0, fields...), nil
}

func convertDeltaField(field deltaField, nextID *int) (iceberg.NestedField, error) {
	typ, err := convertDeltaType(field.Type, nextID)
	if err != nil {
		return iceberg.NestedField{}, fmt.Errorf("%w: column %s: %v", tablelog.ErrUnsupported, field.Name, err)
	}
	id := *nextID
	*nextID++
	return iceberg.NestedField{ID: id, Name: field.Name, Type: typ, Required: !field.Nullable}, nil
}

func convertDeltaType(data json.RawMessage, nextID *int) (iceberg.Type, error) {
	var primitive string
	if err := json.Unmarshal(data, &primitive); err == nil {
		switch primitive {
		case "boolean":
			return iceberg.PrimitiveTypes.Bool, nil
		case "byte", "short", "integer":
			return iceberg.PrimitiveTypes.Int32, nil
		case "long":
			return iceberg.PrimitiveTypes.Int64, nil
		case "float":
			return iceberg.PrimitiveTypes.Float32, nil
		case "double":
			return iceberg.PrimitiveTypes.Float64, nil
		case "string":
			return iceberg.PrimitiveTypes.String, nil
		case "binary":
			return iceberg.PrimitiveTypes.Binary, nil
		case "date":
			return iceberg.PrimitiveTypes.Date, nil
		case "timestamp":
			return iceberg.PrimitiveTypes.TimestampTz, nil
		case "timestamp_ntz":
			return iceberg.PrimitiveTypes.Timestamp, nil
		}
		if strings.HasPrefix(primitive, "decimal(") && strings.HasSuffix(primitive, ")") {
			parts := strings.Split(primitive[len("decimal("):len(primitive)-1], ",")
			if len(parts) == 2 {
				precision, perr := strconv.Atoi(strings.TrimSpace(parts[0]))
				scale, serr := strconv.Atoi(strings.TrimSpace(parts[1]))
				if perr == nil && serr == nil {
					return iceberg.DecimalTypeOf(precision, scale), nil
				}
			}
		}
		return nil, fmt.Errorf("unsupported Delta type %q", primitive)
	}

	var compound struct {
		Type              string          `json:"type"`
		Fields            []deltaField    `json:"fields"`
		ElementType       json.RawMessage `json:"elementType"`
		ContainsNull      bool            `json:"containsNull"`
		KeyType           json.RawMessage `json:"keyType"`
		ValueType         json.RawMessage `json:"valueType"`
		ValueContainsNull bool            `json:"valueContainsNull"`
	}
	if err := json.Unmarshal(data, &compound); err != nil {
		return nil, fmt.Errorf("invalid Delta type: %v", err)
	}
	switch compound.Type {
	case "struct":
		fields := make([]iceberg.NestedField, 0, len(compound.Fields))
		for _, field := range compound.Fields {
			converted, err := convertDeltaField(field, nextID)
			if err != nil {
				return nil, err
			}
			fields = append(fields, converted)
		}
		return &iceberg.StructType{FieldList: fields}, nil
	case "array":
		element, err := convertDeltaType(compound.ElementType, nextID)
		if err != nil {
			return nil, err
		}
		id := *nextID
		*nextID++
		return &iceberg.ListType{ElementID: id, Element: element, ElementRequired: !compound.ContainsNull}, nil
	case "map":
		key, err := convertDeltaType(compound.KeyType, nextID)
		if err != nil {
			return nil, err
		}
		keyID := *nextID
		*nextID++
		value, err := convertDeltaType(compound.ValueType, nextID)
		if err != nil {
			return nil, err
		}
		valueID := *nextID
		*nextID++
		return &iceberg.MapType{KeyID: keyID, KeyType: key, ValueID: valueID, ValueType: value, ValueRequired: !compound.ValueContainsNull}, nil
	default:
		return nil, fmt.Errorf("unsupported compound Delta type %q", compound.Type)
	}
}
