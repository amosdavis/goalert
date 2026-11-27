package alert

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/target/goalert/alert/internal/deltaquery"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/search"
	"github.com/target/goalert/txn/deltatxn"
)

// stopWords are the words that Postgres's english text search ignores.
var stopWords = func() map[string]bool {
	m := make(map[string]bool)
	for _, w := range strings.Fields(`
		i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers
		herself it its itself they them their theirs themselves what which who whom this that these those am is are
		was were be been being have has had having do does did doing a an the and but if or because as until while of
		at by for with about against between into through during before after above below to from up down in out on
		off over under again further then once here there when where why how all any both each few more most other
		some such no nor not only own same so than too very s t can will just don should now`) {
		m[w] = true
	}
	return m
}()

// searchWords returns the words of s that a search compares, each once. It
// stands for to_tsvector('english', ...): without case, stop words and
// endings, so that "failed" finds "failing". It is simpler: words are split
// at everything but letters and digits, and only plurals, -ed and -ing are
// taken off, so "connect" doesn't find "connection" (F24). Like there, a word
// that is too long is left out, so that it can't get in the way of storing an
// alert (F1).
func searchWords(s string) []string {
	var words []string
	for _, w := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if stopWords[w] || len(w) > maxWordLen {
			continue
		}
		if w = stem(w); !slices.Contains(words, w) {
			words = append(words, w)
		}
	}
	return words
}

// maxWordLen is enough for any word of a search.
const maxWordLen = 4 * search.MaxQueryLen

func isVowel(c byte) bool { return strings.IndexByte("aeiou", c) >= 0 }

func hasVowel(s string) bool { return strings.ContainsAny(s, "aeiouy") }

// stem takes an ending off an English word, by the first rules of Porter's
// algorithm. What is left need not be a word.
func stem(w string) string {
	if len(w) <= 2 || strings.ContainsFunc(w, func(r rune) bool { return r < 'a' || r > 'z' }) {
		return w
	}
	switch {
	case strings.HasSuffix(w, "sses"), strings.HasSuffix(w, "ies"):
		w = w[:len(w)-2]
	case strings.HasSuffix(w, "ss"), strings.HasSuffix(w, "us"):
	case strings.HasSuffix(w, "s") && hasVowel(w[:len(w)-2]):
		w = w[:len(w)-1]
	}
	for _, end := range []string{"ed", "ing"} {
		base, ok := strings.CutSuffix(w, end)
		if !ok || len(base) < 2 || !hasVowel(base) || (end == "ed" && strings.HasSuffix(base, "e")) { // not exceed
			continue
		}
		w = base
		// stopped -> stop, but calling -> call
		if n := len(w); n > 2 && w[n-1] == w[n-2] && !strings.Contains("aeioulsz", w[n-1:]) {
			w = w[:n-1]
		}
		break
	}
	if n := len(w); n > 2 && w[n-1] == 'y' && !isVowel(w[n-2]) {
		w = w[:n-1] + "i"
	}
	if n := len(w); n > 2 && w[n-1] == 'e' {
		w = w[:n-1]
	}
	return w
}

func hasAll(words, want []string) bool {
	for _, w := range want {
		if !slices.Contains(words, w) {
			return false
		}
	}
	return true
}

// alertQuery is a search in one view of the tables.
type alertQuery struct {
	*deltaAlerts
	r    filestore.Reader
	opts *renderData

	statuses []Status // that can match, in their order
	omit     map[int64]bool

	// For opts.Search: an alert matches by ID, by its summary having all the
	// words (if there are any), or by its service.
	searchID     int64
	words        []string
	nameServices map[string]bool

	// For opts.ServiceFilter: an alert matches by its service, or by the user
	// having been notified.
	services map[string]bool
	notified map[int64]bool
}

func (d *deltaAlerts) search(ctx context.Context, opts *renderData) ([]Alert, error) {
	if opts.Search != "" && d.ServiceNames == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var alerts []Alert
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows := d.newQuery(s.Reader, opts).run()
		alerts = make([]Alert, 0, len(rows)) // never nil, as the Postgres store returns
		for _, row := range rows {
			a, err := row.alert()
			if err != nil {
				return err
			}
			alerts = append(alerts, a)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return alerts, nil
}

func (d *deltaAlerts) newQuery(r filestore.Reader, opts *renderData) *alertQuery {
	q := &alertQuery{deltaAlerts: d, r: r, opts: opts, omit: make(map[int64]bool)}
	for _, st := range statuses {
		if len(opts.Status) == 0 || slices.Contains(opts.Status, st) {
			q.statuses = append(q.statuses, st)
		}
	}
	for _, id := range opts.Omit {
		q.omit[int64(id)] = true
	}
	if opts.Search != "" {
		if id, err := strconv.ParseInt(opts.Search, 10, 64); err == nil {
			q.searchID = id
		}
		q.nameServices = make(map[string]bool)
		if q.words = searchWords(opts.Search); len(q.words) > 0 {
			d.ServiceNames(r, func(id, name string) bool {
				if hasAll(searchWords(name), q.words) {
					q.nameServices[id] = true
				}
				return true
			})
		}
	}
	if opts.ServiceFilter.Valid {
		q.services = make(map[string]bool)
		for _, id := range opts.ServiceFilter.IDs {
			q.services[canonicalID(id)] = true
		}
		if opts.NotifiedUserID != "" {
			q.notified = make(map[int64]bool)
			d.logs.AscendNotified(r, strings.ToLower(opts.NotifiedUserID), func(alertID int64) bool {
				q.notified[alertID] = true
				return true
			})
		}
	}
	return q
}

// compare is the ORDER BY.
func (q *alertQuery) compare(a, b DeltaAlert) int {
	switch q.opts.Sort {
	case SortModeDateID:
		return cmp.Or(b.CreatedAt.Compare(a.CreatedAt), cmp.Compare(b.ID, a.ID))
	case SortModeDateIDReverse:
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	}
	return cmp.Or(cmp.Compare(statusOrder[Status(a.Status)], statusOrder[Status(b.Status)]), cmp.Compare(b.ID, a.ID))
}

// cursor is the row that the After option stands for: matching rows come after
// it in the order.
func (q *alertQuery) cursor() (DeltaAlert, bool) {
	after := q.opts.After
	if after.ID == 0 {
		return DeltaAlert{}, false
	}
	if after.Status == "" {
		after.Status = StatusTriggered // preserve the old cursor default
	}
	return DeltaAlert{ID: int64(after.ID), Status: string(after.Status), CreatedAt: after.Created}, true
}

// match reports whether an alert meets the search criteria. Date-ordered
// cursors compare both creation time and ID (the old SQL query compared a
// nonexistent column, so it failed on the second page).
func (q *alertQuery) match(a DeltaAlert) bool {
	if q.omit[a.ID] || !slices.Contains(q.statuses, Status(a.Status)) {
		return false
	}
	if q.opts.Search != "" && a.ID != q.searchID && !q.nameServices[a.serviceID()] &&
		(len(q.words) == 0 || !hasAll(searchWords(a.Summary), q.words)) {
		return false
	}
	if q.opts.ServiceFilter.Valid && !q.services[a.serviceID()] && !q.notified[a.ID] {
		return false
	}
	if !q.opts.Before.IsZero() && !a.CreatedAt.Before(q.opts.Before) {
		return false
	}
	if a.CreatedAt.Before(q.opts.NotBefore) {
		return false
	}
	if cur, ok := q.cursor(); ok && q.compare(cur, a) >= 0 {
		return false
	}
	return true
}

// run returns the rows of the result. It looks at the rows that the filters
// narrow the search to through an index. Without one that does, it reads in
// the order of the result and stops when it has enough (F15).
func (q *alertQuery) run() []DeltaAlert {
	top := deltaquery.NewTop(q.opts.Limit, q.compare)
	add := func(a DeltaAlert) bool {
		if q.match(a) {
			top.Add(a)
		}
		return true
	}
	addID := func(id int64) {
		if a, ok := q.Alerts.Get(q.r, filestore.IntKey(id)); ok {
			add(a)
		}
	}
	addServices := func(services map[string]bool, fn func(DeltaAlert) bool) {
		for id := range services {
			for _, st := range q.statuses {
				q.byServiceStatus.Ascend(q.r, serviceStatusKey(id, st), fn)
			}
		}
	}

	switch {
	case q.opts.ServiceFilter.Valid:
		addServices(q.services, add)
		for id := range q.notified {
			if a, ok := q.Alerts.Get(q.r, filestore.IntKey(id)); ok && !q.services[a.serviceID()] {
				add(a)
			}
		}
	case q.opts.Search != "":
		seen := make(map[int64]bool)
		addOnce := func(a DeltaAlert) bool {
			if !seen[a.ID] {
				seen[a.ID] = true
				add(a)
			}
			return true
		}
		if len(q.words) > 0 {
			// The longest word is likely the rarest.
			word := slices.MaxFunc(q.words, func(a, b string) int { return cmp.Compare(len(a), len(b)) })
			q.byWord.Ascend(q.r, word, addOnce)
		}
		addServices(q.nameServices, addOnce)
		if !seen[q.searchID] {
			addID(q.searchID)
		}
	case !slices.Contains(q.statuses, StatusClosed):
		for _, st := range q.statuses {
			q.byOpenStatus.Ascend(q.r, string(st), add)
		}
	case q.opts.Sort == SortModeDateID, q.opts.Sort == SortModeDateIDReverse:
		return q.scanCreated()
	case !q.opts.NotBefore.IsZero() || !q.opts.Before.IsZero():
		q.AscendCreated(q.r, q.opts.NotBefore, q.opts.Before, add)
	default:
		return q.scanStatus()
	}
	return top.Rows()
}

// scanCreated reads in the order of creation.
func (q *alertQuery) scanCreated() []DeltaAlert {
	from, to := q.opts.NotBefore, q.opts.Before
	desc := q.opts.Sort == SortModeDateID
	if cur, ok := q.cursor(); ok {
		if desc && (to.IsZero() || cur.CreatedAt.Before(to)) {
			to = cur.CreatedAt
		}
		if !desc && cur.CreatedAt.After(from) {
			from = cur.CreatedAt
		}
	}
	var rows []DeltaAlert
	deltaquery.Days(q.byCreated, q.r, from, to, desc, func(day []DeltaAlert) bool {
		day = slices.DeleteFunc(day, func(a DeltaAlert) bool { return !q.match(a) })
		slices.SortFunc(day, q.compare)
		rows = append(rows, day...)
		return len(rows) < q.opts.Limit
	})
	return rows[:min(len(rows), q.opts.Limit)]
}

// scanStatus reads in the order of status and then ID, newest first. The open
// alerts are few, and the closed ones are found in the table itself.
func (q *alertQuery) scanStatus() []DeltaAlert {
	top := deltaquery.NewTop(q.opts.Limit, q.compare)
	n := 0
	add := func(a DeltaAlert) bool {
		if q.match(a) {
			top.Add(a)
			n++
		}
		return true
	}
	for _, st := range q.statuses {
		if n >= q.opts.Limit {
			break
		}
		if st != StatusClosed {
			q.byOpenStatus.Ascend(q.r, string(st), add)
			continue
		}
		q.Alerts.Descend(q.r, func(a DeltaAlert) bool {
			if Status(a.Status) == StatusClosed {
				add(a)
			}
			return n < q.opts.Limit
		})
	}
	return top.Rows()
}
