package fixture

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// The values of expressions: nil, bool, int64, literal, time.Time, localTime,
// clockTime and interval.
type (
	// literal is a string, which takes the type that it is used as.
	literal string
	// localTime is a timestamp without time zone, as if it were in UTC.
	localTime time.Time
	// clockTime is a time of day.
	clockTime time.Duration
	interval  struct {
		months, days int
		d            time.Duration
	}
)

type expr interface {
	eval(x *execution) (any, error)
}

type (
	constant struct{ v any }
	now      struct{}
	sum      struct {
		a, b  expr
		minus bool
	}
	cast struct {
		e   expr
		typ string
	}
	atZone struct {
		e    expr
		zone string
	}
)

func (e constant) eval(*execution) (any, error) { return e.v, nil }
func (now) eval(x *execution) (any, error)      { return x.tx.Now(), nil }

func (e sum) eval(x *execution) (any, error) {
	a, err := e.a.eval(x)
	if err != nil {
		return nil, err
	}
	b, err := e.b.eval(x)
	if err != nil {
		return nil, err
	}
	iv, ok := b.(interval)
	if !ok {
		return nil, fmt.Errorf("can't add or subtract a %T", b)
	}
	if e.minus {
		iv = interval{-iv.months, -iv.days, -iv.d}
	}
	switch a := a.(type) {
	case time.Time:
		return a.In(x.zone).AddDate(0, iv.months, iv.days).Add(iv.d), nil
	case localTime:
		return localTime(time.Time(a).AddDate(0, iv.months, iv.days).Add(iv.d)), nil
	case interval:
		return interval{a.months + iv.months, a.days + iv.days, a.d + iv.d}, nil
	}
	return nil, fmt.Errorf("can't add an interval to a %T", a)
}

func (e atZone) eval(x *execution) (any, error) {
	v, err := e.e.eval(x)
	if err != nil {
		return nil, err
	}
	zone, err := time.LoadLocation(e.zone)
	if err != nil {
		return nil, err
	}
	switch v := v.(type) {
	case time.Time:
		return toLocal(v, zone), nil
	case localTime:
		return fromLocal(v, zone), nil
	}
	return nil, fmt.Errorf("a %T has no time zone", v)
}

func toLocal(t time.Time, zone *time.Location) localTime {
	t = t.In(zone)
	return localTime(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC))
}

func fromLocal(l localTime, zone *time.Location) time.Time {
	t := time.Time(l)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), zone)
}

func (e cast) eval(x *execution) (any, error) {
	v, err := e.e.eval(x)
	if err != nil || v == nil {
		return nil, err
	}
	s, isLiteral := v.(literal)
	switch e.typ {
	case "interval":
		if iv, ok := v.(interval); ok {
			return iv, nil
		}
		if isLiteral {
			return parseInterval(string(s))
		}
	case "timestamptz":
		return x.time(v)
	case "timestamp":
		t, err := x.time(v)
		return toLocal(t, x.zone), err
	case "time":
		return x.clock(v)
	case "text", "uuid", "json", "jsonb":
		if isLiteral {
			return v, nil
		}
	}
	return nil, fmt.Errorf("can't cast a %T to %s", v, e.typ)
}

// time is v as a point in time.
func (x *execution) time(v any) (time.Time, error) {
	switch v := v.(type) {
	case time.Time:
		return v, nil
	case localTime:
		return fromLocal(v, x.zone), nil
	case literal:
		return parseTime(string(v), x.zone)
	}
	return time.Time{}, fmt.Errorf("a %T is not a time", v)
}

// clock is v as a time of day.
func (x *execution) clock(v any) (clockTime, error) {
	switch v := v.(type) {
	case clockTime:
		return v, nil
	case literal:
		return parseClock(string(v))
	case localTime:
		t := time.Time(v)
		return clockTime(t.Sub(t.Truncate(24 * time.Hour))), nil
	case time.Time:
		return x.clock(toLocal(v, x.zone))
	}
	return 0, fmt.Errorf("a %T is not a time of day", v)
}

func parseTime(s string, zone *time.Location) (time.Time, error) {
	// A name of a time zone can come last.
	if i := strings.LastIndexByte(s, ' '); i > 0 && strings.ContainsAny(s[i+1:], "/ABCDEFGHIJKLMNOPQRSTUVWXYZ") && !strings.Contains(s[i+1:], ":") {
		loc, err := time.LoadLocation(s[i+1:])
		if err != nil {
			return time.Time{}, err
		}
		s, zone = s[:i], loc
	}
	for _, layout := range []string{
		time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999-07",
		"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04", "2006-01-02",
	} {
		if t, err := time.ParseInLocation(layout, s, zone); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("can't read the time %q", s)
}

func parseClock(s string) (clockTime, error) {
	for _, layout := range []string{"15:04", "15:04:05.999999999"} {
		if t, err := time.Parse(layout, s); err == nil {
			return clockTime(t.Sub(t.Truncate(24 * time.Hour))), nil
		}
	}
	return 0, fmt.Errorf("can't read the time of day %q", s)
}

func (c clockTime) String() string {
	return time.Time{}.Add(time.Duration(c)).Format("15:04:05.999999")
}

// parseInterval reads an interval such as "1 hour 50 minutes" or "01:30:00".
func parseInterval(s string) (iv interval, err error) {
	fields := strings.Fields(s)
	for len(fields) > 0 {
		if strings.Contains(fields[0], ":") {
			c, err := parseClock(fields[0])
			if err != nil {
				return iv, err
			}
			iv.d += time.Duration(c)
			fields = fields[1:]
			continue
		}
		if len(fields) < 2 {
			return iv, fmt.Errorf("can't read the interval %q", s)
		}
		n, err := strconv.Atoi(fields[0])
		if err != nil {
			return iv, fmt.Errorf("can't read the interval %q", s)
		}
		switch strings.TrimSuffix(strings.ToLower(fields[1]), "s") {
		case "second", "sec":
			iv.d += time.Duration(n) * time.Second
		case "minute", "min":
			iv.d += time.Duration(n) * time.Minute
		case "hour":
			iv.d += time.Duration(n) * time.Hour
		case "day":
			iv.days += n
		case "week":
			iv.days += 7 * n
		case "month":
			iv.months += n
		case "year":
			iv.months += 12 * n
		default:
			return iv, fmt.Errorf("can't read the interval %q", s)
		}
		fields = fields[2:]
	}
	return iv, nil
}

// micros is an interval as the interval columns of the Delta table have it.
// Like Postgres, a month is 30 days when it is not added to a date.
func (iv interval) micros() int64 {
	d := time.Duration(iv.months*30+iv.days)*24*time.Hour + iv.d
	return int64(d / time.Microsecond)
}

// assign sets a field of a row to v.
func (x *execution) assign(f reflect.Value, v any) error {
	if v == nil {
		if f.Kind() != reflect.Pointer && f.Kind() != reflect.Slice {
			return fmt.Errorf("it can't be null")
		}
		f.SetZero()
		return nil
	}
	if f.Kind() == reflect.Pointer {
		p := reflect.New(f.Type().Elem())
		if err := x.assign(p.Elem(), v); err != nil {
			return err
		}
		f.Set(p)
		return nil
	}
	switch f.Addr().Interface().(type) {
	case *time.Time:
		t, err := x.time(v)
		if err != nil {
			return err
		}
		f.Set(reflect.ValueOf(t.UTC()))
		return nil
	case *[]byte:
		if s, ok := v.(literal); ok {
			f.SetBytes([]byte(s))
			return nil
		}
	}
	switch v := v.(type) {
	case literal:
		if f.Kind() == reflect.String {
			f.SetString(string(v))
			return nil
		}
		if f.CanInt() {
			// SQL coerces a quoted number into an integer column. If it is not
			// a number, the column may be an interval instead.
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil && !f.OverflowInt(i) {
				f.SetInt(i)
				return nil
			}
			iv, err := parseInterval(string(v))
			if err != nil {
				return err
			}
			f.SetInt(iv.micros())
			return nil
		}
	case clockTime:
		if f.Kind() == reflect.String {
			f.SetString(v.String())
			return nil
		}
	case int64:
		if f.CanInt() && !f.OverflowInt(v) {
			f.SetInt(v)
			return nil
		}
	case interval:
		if f.Kind() == reflect.Int64 {
			f.SetInt(v.micros())
			return nil
		}
	case bool:
		if f.Kind() == reflect.Bool {
			f.SetBool(v)
			return nil
		}
	}
	return fmt.Errorf("a %T can't be in a column of type %s", v, f.Type())
}
