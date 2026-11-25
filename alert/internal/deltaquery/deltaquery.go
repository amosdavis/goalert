// Package deltaquery has what the alert packages need from filestore indexes
// beyond looking up a key: reading rows by time range, and keeping the first
// rows of an order.
package deltaquery

import (
	"slices"
	"time"

	"github.com/target/goalert/filestore"
)

const (
	yearKey = "2006"
	dayKey  = "2006-01-02"
)

// The times a row can hold (F25), by day.
var (
	minDay = time.Date(1677, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDay = time.Date(2262, 12, 31, 0, 0, 0, 0, time.UTC)
)

// DayKeys returns the keys of a row with time t in an index that Days reads:
// its year and its day, in UTC. An index finds rows by exact key only, so a
// range is read day by day, and the year keys make years without rows cheap.
func DayKeys(t time.Time) []string {
	t = t.UTC()
	return []string{t.Format(yearKey), t.Format(dayKey)}
}

// Days calls fn with the rows of each day that has any, from the day of from
// to the day of to, or in reverse with desc, until fn returns false. A zero
// time is no bound. The rows of a day are in primary key order, and those of
// the first and last day can be outside the times given.
func Days[V any](ix *filestore.Index[V], r filestore.Reader, from, to time.Time, desc bool, fn func(rows []V) bool) {
	first, last := minDay, maxDay
	if !from.IsZero() && from.After(minDay) {
		first = day(from)
	}
	if !to.IsZero() && to.Before(maxDay) {
		last = day(to)
	}
	if last.Before(first) {
		return
	}
	years := make([]int, 0, last.Year()-first.Year()+1)
	for y := first.Year(); y <= last.Year(); y++ {
		years = append(years, y)
	}
	step := 1
	if desc {
		slices.Reverse(years)
		step = -1
	}
	for _, y := range years {
		start := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		if !has(ix, r, start.Format(yearKey)) {
			continue
		}
		end := time.Date(y, 12, 31, 0, 0, 0, 0, time.UTC)
		if start.Before(first) {
			start = first
		}
		if end.After(last) {
			end = last
		}
		d := start
		if desc {
			d = end
		}
		for ; !d.Before(start) && !d.After(end); d = d.AddDate(0, 0, step) {
			var rows []V
			ix.Ascend(r, d.Format(dayKey), func(v V) bool {
				rows = append(rows, v)
				return true
			})
			if len(rows) > 0 && !fn(rows) {
				return
			}
		}
	}
}

func day(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func has[V any](ix *filestore.Index[V], r filestore.Reader, key string) bool {
	found := false
	ix.Ascend(r, key, func(V) bool {
		found = true
		return false
	})
	return found
}

// Top keeps the first rows of an order, like ORDER BY with a LIMIT.
type Top[V any] struct {
	limit int
	cmp   func(a, b V) int
	rows  []V
}

// NewTop returns a Top that keeps the first limit rows in the order of cmp.
func NewTop[V any](limit int, cmp func(a, b V) int) *Top[V] {
	return &Top[V]{limit: limit, cmp: cmp}
}

// Add adds a row.
func (t *Top[V]) Add(v V) {
	t.rows = append(t.rows, v)
	if len(t.rows) >= 2*t.limit+64 {
		t.trim()
	}
}

// Rows returns the rows kept, in order.
func (t *Top[V]) Rows() []V {
	t.trim()
	return t.rows
}

func (t *Top[V]) trim() {
	slices.SortFunc(t.rows, t.cmp)
	if len(t.rows) > t.limit {
		clear(t.rows[t.limit:])
		t.rows = t.rows[:t.limit]
	}
}
