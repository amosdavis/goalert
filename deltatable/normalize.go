package deltatable

import (
	"reflect"
	"strings"
	"time"
)

// Writing a row to Parquet and reading it back costs about half a
// millisecond, which every Put would pay. A normalizer does the same to a
// value by reflection: it copies src, which it leaves alone, into dst, which
// is the zero value.
//
// It must give exactly what the round trip gives (F25), so anything it is not
// known to do right has no normalizer, and the round trip is used. The tests
// compare the two.
type normalizer func(dst, src reflect.Value)

// structNormalizer returns the normalizer of a struct type, or nil.
func structNormalizer(t reflect.Type) normalizer {
	type field struct {
		index int
		norm  normalizer
	}
	var fields []field
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous {
			return nil
		}
		name, opts, _ := strings.Cut(f.Tag.Get("parquet"), ",")
		if !f.IsExported() || name == "-" && opts == "" {
			continue // not stored, so it reads back as zero
		}
		norm := fieldNormalizer(f.Type, name, opts)
		if norm == nil {
			return nil
		}
		fields = append(fields, field{i, norm})
	}
	return func(dst, src reflect.Value) {
		for _, f := range fields {
			f.norm(dst.Field(f.index), src.Field(f.index))
		}
	}
}

func fieldNormalizer(t reflect.Type, name, opts string) normalizer {
	if name == "-" {
		return nil
	}
	var optional, list, timestamp bool
	for opt := range strings.SplitSeq(opts, ",") {
		switch opt {
		case "":
		case "optional":
			optional = true
		case "list":
			list = true
		case "timestamp(microsecond:utc)":
			timestamp = true
		default:
			return nil
		}
	}
	switch {
	case t == timeType:
		if timestamp && !optional && !list {
			return normalizeTime
		}
	case timestamp:
		if t.Kind() == reflect.Pointer && t.Elem() == timeType && !list {
			return pointerNormalizer(t, normalizeTime)
		}
	case list:
		if t.Kind() == reflect.Slice {
			if elem := valueNormalizer(t.Elem()); elem != nil {
				return sliceNormalizer(t, optional, elem)
			}
		}
	case t.Kind() == reflect.Pointer:
		if t.Elem() == timeType || t.Elem().Kind() == reflect.Slice {
			return nil
		}
		if elem := valueNormalizer(t.Elem()); elem != nil {
			return pointerNormalizer(t, elem)
		}
	case isBytes(t):
		return bytesNormalizer(optional)
	case !optional:
		return valueNormalizer(t)
	case t.Kind() != reflect.Struct && t.Kind() != reflect.Float64:
		// An optional zero is stored as missing, and reads back as zero.
		return valueNormalizer(t)
	}
	return nil
}

// valueNormalizer is for required values without tags of their own.
func valueNormalizer(t reflect.Type) normalizer {
	if isBytes(t) {
		return bytesNormalizer(false)
	}
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float64, reflect.String:
		return func(dst, src reflect.Value) { dst.Set(src) }
	case reflect.Struct:
		if t != timeType {
			return structNormalizer(t)
		}
	}
	return nil
}

func isBytes(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8
}

// normalizeTime keeps microseconds, in UTC. checkTimes has refused times that
// do not fit Delta's signed 64-bit microsecond representation.
func normalizeTime(dst, src reflect.Value) {
	t := src.Interface().(time.Time)
	dst.Set(reflect.ValueOf(time.UnixMicro(t.UnixMicro()).UTC()))
}

func pointerNormalizer(t reflect.Type, elem normalizer) normalizer {
	return func(dst, src reflect.Value) {
		if src.IsNil() {
			return
		}
		p := reflect.New(t.Elem())
		elem(p.Elem(), src.Elem())
		dst.Set(p)
	}
}

// bytesNormalizer copies bytes. A nil slice is missing if optional, and
// empty otherwise.
func bytesNormalizer(optional bool) normalizer {
	return func(dst, src reflect.Value) {
		if optional && src.IsNil() {
			return
		}
		b := make([]byte, src.Len())
		copy(b, src.Bytes())
		dst.SetBytes(b)
	}
}

func sliceNormalizer(t reflect.Type, optional bool, elem normalizer) normalizer {
	return func(dst, src reflect.Value) {
		if optional && src.IsNil() {
			return
		}
		s := reflect.MakeSlice(t, src.Len(), src.Len())
		for i := range src.Len() {
			elem(s.Index(i), src.Index(i))
		}
		dst.Set(s)
	}
}
