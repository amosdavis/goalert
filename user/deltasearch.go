package user

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"unicode"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// searchHit is a user that matches a search.
type searchHit struct {
	row   userRow
	lower string // the name
	fav   bool
}

// search matches users like searchTemplate. It orders names by their bytes,
// not by the database's collation.
func (d *deltaUsers) search(ctx context.Context, opts *renderData) ([]User, error) {
	if len(opts.DestArgs) > 0 && d.destUsers == nil {
		return nil, deltatxn.ErrNotPorted
	}
	if opts.DestType != "" && len(opts.DestArgs) == 0 {
		// The Postgres query fails then too: it filters on a table it
		// doesn't join.
		return nil, validation.NewGenericError("DestArgs must be set when DestType is set")
	}

	favUser := strings.ToLower(opts.FavoritesUserID)
	omit, only := lowerSet(opts.Omit), lowerSet(opts.Only)
	email := strings.ToLower(opts.Email())
	matchName := nameMatcher(opts.SearchString())
	after := strings.ToLower(opts.After.Name)

	// match is the WHERE clause, and the join with user_favorites.
	match := func(r filestore.Reader, row userRow) (searchHit, bool) {
		h := searchHit{row: row, lower: strings.ToLower(row.Name)}
		if omit[row.ID] || (len(only) > 0 && !only[row.ID]) {
			return h, false
		}
		if email != "" && strings.ToLower(row.Email) != email {
			return h, false
		}
		if opts.Search != "" && !matchName(row.Name) {
			return h, false
		}
		h.fav = favUser != "" && d.isFavorite(r, favUser, row.ID)
		if opts.FavoritesOnly && !h.fav {
			return h, false
		}
		if after == "" {
			return h, true
		}
		switch {
		case !opts.FavoritesFirst:
			return h, h.lower > after
		case opts.After.IsFavorite:
			return h, !h.fav || h.lower > after
		default:
			return h, !h.fav && h.lower > after
		}
	}

	var users []User
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var hits []searchHit
		d.searchCandidates(s.Reader, opts, func(row userRow) {
			if h, ok := match(s.Reader, row); ok {
				hits = append(hits, h)
			}
		})
		slices.SortFunc(hits, func(a, b searchHit) int {
			if opts.FavoritesFirst && a.fav != b.fav {
				if a.fav {
					return -1
				}
				return 1
			}
			return cmp.Or(cmp.Compare(a.lower, b.lower), cmp.Compare(a.row.ID, b.row.ID))
		})
		users = nil
		for _, h := range hits[:min(len(hits), opts.Limit)] {
			// Like the Postgres query's, the results have no avatar URL.
			users = append(users, User{
				ID: h.row.ID, Name: h.row.Name, Email: h.row.Email,
				Role: permission.Role(h.row.Role), isUserFavorite: h.fav,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

// searchCandidates calls fn, once each, for the users that can match: those
// the most selective option leaves, without reading every user if possible
// (F15). With DestArgs, it applies them.
func (d *deltaUsers) searchCandidates(r filestore.Reader, opts *renderData, fn func(userRow)) {
	each := func(row userRow) bool { fn(row); return true }
	var ids []string
	switch {
	case len(opts.DestArgs) > 0:
		ids = d.destUsers(r, opts.DestType, opts.DestArgs)
	case len(opts.Only) > 0:
		ids = opts.Only
	case opts.Email() != "":
		d.byEmail.Ascend(r, strings.ToLower(opts.Email()), each)
		return
	default:
		d.Users.Ascend(r, each)
		return
	}
	for id := range lowerSet(ids) {
		if row, ok := d.Users.Get(r, id); ok {
			fn(row)
		}
	}
}

func lowerSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[strings.ToLower(id)] = true
	}
	return set
}

// nameMatcher returns the name condition of searchTemplate: the name contains
// the search text, or words that start with the words of the search text, in
// the same order (search.Helpers' contains and orderedPrefixSearch).
func nameMatcher(text string) func(name string) bool {
	text = strings.ToLower(text)
	terms := make([][]rune, 0)
	for _, w := range strings.FieldsFunc(text, func(r rune) bool { return !isWordRune(r) }) {
		terms = append(terms, []rune(w))
	}
	return func(name string) bool {
		name = strings.ToLower(name)
		return strings.Contains(name, text) || hasOrderedPrefixes(name, terms)
	}
}

// isWordRune reports whether r is part of a word. An underscore, which is in
// a Postgres regular expression, is not: the query replaces it first.
func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

// hasOrderedPrefixes reports whether words that start with the terms are in
// name, in that order. Without terms, any word is enough, as for the regular
// expression that stands for none.
func hasOrderedPrefixes(name string, terms [][]rune) bool {
	if len(terms) == 0 {
		return strings.ContainsFunc(name, isWordRune)
	}
	n := []rune(name)
	for i := range n {
		if !isWordRune(n[i]) || (i > 0 && isWordRune(n[i-1])) {
			continue // not the start of a word
		}
		if t := terms[0]; len(n)-i >= len(t) && slices.Equal(n[i:i+len(t)], t) {
			if terms = terms[1:]; len(terms) == 0 {
				return true
			}
		}
	}
	return false
}
