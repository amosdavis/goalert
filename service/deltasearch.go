package service

import (
	"context"
	"slices"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn/deltatxn"
)

// wordRep is how the search template prepares a name for a match of words.
var wordRep = strings.NewReplacer("_", " ", "-", " ")

// words returns the words of s in lower case, like splitSearchTerms in package
// search.
func words(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

// hasOrderedPrefixes is the search templates' orderedPrefixSearch: name has
// words that start with the terms, in the same order. Without terms, its
// regular expression is `\m`, which matches any word.
func hasOrderedPrefixes(name string, terms []string) bool {
	nameWords := words(wordRep.Replace(name))
	if len(terms) == 0 {
		return len(nameWords) > 0
	}
	for _, w := range nameWords {
		if strings.HasPrefix(w, terms[0]) {
			terms = terms[1:]
			if len(terms) == 0 {
				return true
			}
		}
	}
	return false
}

// candidates calls fn for the services that a search has to look at: the
// fewest that an index or an option gives, or else all of them.
func (d *deltaServices) candidates(r filestore.Reader, lists [][]string, fn func(serviceRow)) {
	if len(lists) == 0 {
		d.Services.Ascend(r, func(row serviceRow) bool {
			fn(row)
			return true
		})
		return
	}
	shortest := slices.MinFunc(lists, func(a, b []string) int { return len(a) - len(b) })
	seen := make(map[string]bool, len(shortest))
	for _, id := range shortest {
		if seen[id] {
			continue
		}
		seen[id] = true
		if row, ok := d.Services.Get(r, id); ok {
			fn(row)
		}
	}
}

func idSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func canonicalIDs(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = uuid.MustParse(id).String()
	}
	return out
}

// search is the search template: each option is a condition that a service
// must meet, and those that list services also choose where to look.
func (d *deltaServices) search(ctx context.Context, data *renderData) ([]Service, error) {
	type hit struct {
		svc   Service
		lower string // lower(name)
	}
	var hits []hit
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		hits = nil
		var lists [][]string                   // the services must be in all of these
		var in []map[string]bool               // the same, as sets
		omit := idSet(canonicalIDs(data.Omit)) // and in none of this
		require := func(ids []string) {
			lists = append(lists, ids)
			in = append(in, idSet(ids))
		}
		if len(data.Only) > 0 {
			require(canonicalIDs(data.Only))
		}

		keyID := data.IntegrationKey()
		if keyID != "" {
			if d.IntegrationKeyService == nil {
				return deltatxn.ErrNotPorted
			}
			id, err := uuid.Parse(keyID)
			if err != nil {
				return err // the Postgres query fails too
			}
			var ids []string
			if svcID, ok := d.IntegrationKeyService(s.Reader, id.String()); ok {
				ids = []string{svcID}
			}
			require(ids)
		}

		labelKey := data.LabelKey()
		if labelKey != "" {
			if d.ServicesWithLabel == nil {
				return deltatxn.ErrNotPorted
			}
			value := data.LabelValue()
			if value == "*" {
				value = ""
			}
			ids := d.ServicesWithLabel(s.Reader, labelKey, value)
			if data.LabelNegate() {
				for _, id := range ids {
					omit[id] = true
				}
			} else {
				require(ids)
			}
		}

		var favs map[string]bool
		if data.FavoritesUserID != "" {
			favs = d.favorites(s.Reader, parseUserID(data.FavoritesUserID))
		}
		if data.FavoritesOnly {
			ids := make([]string, 0, len(favs))
			for id := range favs {
				ids = append(ids, id)
			}
			require(ids)
		}

		byText := data.Search != "" && labelKey == "" && keyID == ""
		terms, text := words(data.Search), strings.ToLower(data.Search)
		after := strings.ToLower(data.After.Name)

		d.candidates(s.Reader, lists, func(row serviceRow) {
			if omit[row.ID] || slices.ContainsFunc(in, func(set map[string]bool) bool { return !set[row.ID] }) {
				return
			}
			lower, fav := strings.ToLower(row.Name), favs[row.ID]
			if byText && !hasOrderedPrefixes(row.Name, terms) &&
				!strings.Contains(strings.ToLower(row.Description), text) && !strings.Contains(lower, text) {
				return
			}
			if data.After.Name != "" {
				var ok bool
				switch {
				case !data.FavoritesFirst:
					ok = lower > after
				case data.After.IsFavorite:
					ok = !fav || lower > after
				default:
					ok = !fav && lower > after
				}
				if !ok {
					return
				}
			}
			svc := row.service()
			svc.isUserFavorite = fav
			hits = append(hits, hit{svc: svc, lower: lower})
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(hits, func(a, b hit) int {
		if data.FavoritesFirst && a.svc.isUserFavorite != b.svc.isUserFavorite {
			if a.svc.isUserFavorite {
				return -1
			}
			return 1
		}
		return strings.Compare(a.lower, b.lower)
	})
	if len(hits) > data.Limit {
		hits = hits[:data.Limit]
	}
	var result []Service // nil without hits, as the Postgres store returns
	for _, h := range hits {
		result = append(result, h.svc)
	}
	return result, nil
}
