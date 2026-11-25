package schedule

import (
	"context"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn/deltatxn"
)

// search does in memory what searchTemplate does on Postgres. Names are
// compared byte by byte after lowercasing; they are ASCII (validate.IDName),
// so that is also Postgres's order with the C collation. opts are
// normalized.
func (d *deltaSchedules) search(ctx context.Context, opts *SearchOptions) ([]Schedule, error) {
	favUser := canonicalID(opts.FavoritesUserID)
	omit := make(map[string]bool, len(opts.Omit))
	for _, id := range opts.Omit {
		omit[canonicalID(id)] = true
	}
	match := newSearchMatch(opts.Search)
	after := strings.ToLower(opts.After.Name)

	type hit struct {
		row  scheduleRow
		name string // lowercase
		fav  bool
	}
	var result []Schedule
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		var hits []hit
		check := func(row scheduleRow) {
			h := hit{row: row, name: strings.ToLower(row.Name)}
			if omit[row.ID] || !match.schedule(row) {
				return
			}
			h.fav = d.isFavorite(s.Reader, favUser, row.ID)
			if opts.FavoritesOnly && !h.fav {
				return
			}
			if after != "" {
				past := h.name > after
				if opts.FavoritesFirst && opts.After.IsFavorite {
					past = past || !h.fav
				} else if opts.FavoritesFirst {
					past = past && !h.fav
				}
				if !past {
					return
				}
			}
			hits = append(hits, h)
		}
		d.eachOf(s.Reader, opts.Only, check)

		slices.SortFunc(hits, func(a, b hit) int {
			if opts.FavoritesFirst && a.fav != b.fav {
				if a.fav {
					return -1
				}
				return 1
			}
			return strings.Compare(a.name, b.name) // unique (schedules_name)
		})
		if len(hits) > opts.Limit {
			hits = hits[:opts.Limit]
		}
		for _, h := range hits {
			sched, err := h.row.schedule(h.fav)
			if err != nil {
				return err
			}
			result = append(result, sched)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// eachOf calls fn for the schedules with the given ids, or for every
// schedule if there are none.
func (d *deltaSchedules) eachOf(r filestore.Reader, ids []string, fn func(scheduleRow)) {
	if len(ids) == 0 {
		d.Schedules.Ascend(r, func(row scheduleRow) bool { fn(row); return true })
		return
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = canonicalID(id)
		if seen[id] {
			continue
		}
		seen[id] = true
		if row, ok := d.Schedules.Get(r, id); ok {
			fn(row)
		}
	}
}

// canonicalID is id as rows have it, like a cast to uuid. An id that isn't
// one is returned as it is, and matches nothing.
func canonicalID(id string) string {
	u, err := uuid.Parse(id)
	if err != nil {
		return id
	}
	return u.String()
}

// searchMatch matches a search as the Postgres query does: the words of the
// search are prefixes of words of the name, in order, or the name or
// description contains the search (`ilike`).
type searchMatch struct {
	search string // lowercase; empty matches everything
	terms  []string
}

func newSearchMatch(search string) searchMatch {
	search = strings.ToLower(search)
	return searchMatch{search: search, terms: strings.FieldsFunc(search, func(r rune) bool { return !isWord(r) })}
}

func isWord(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

func (m searchMatch) schedule(row scheduleRow) bool {
	if m.search == "" {
		return true
	}
	name := strings.ToLower(row.Name)
	return m.orderedPrefixes(name) ||
		strings.Contains(strings.ToLower(row.Description), m.search) ||
		strings.Contains(name, m.search)
}

var searchSpaces = strings.NewReplacer("_", " ", "-", " ")

// orderedPrefixes matches the regular expression `\mterm1.*\mterm2...`,
// where \m is the start of a word, with "_" and "-" separating words.
// Without any terms that is `\m`: any word at all.
func (m searchMatch) orderedPrefixes(name string) bool {
	name = searchSpaces.Replace(name)
	terms := m.terms
	if len(terms) == 0 {
		terms = []string{""}
	}
	prev := ' '
	for i := 0; i < len(name); {
		r, n := utf8.DecodeRuneInString(name[i:])
		if isWord(r) && !isWord(prev) && strings.HasPrefix(name[i:], terms[0]) {
			// The earliest match of a term leaves the most for the others.
			if terms = terms[1:]; len(terms) == 0 {
				return true
			}
		}
		prev = r
		i += n
	}
	return false
}
