package notification

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// sortHistory puts the newest status first.
func sortHistory(hist []gadb.MessageStatusHistory) {
	slices.SortFunc(hist, func(a, b gadb.MessageStatusHistory) int {
		return cmp.Or(b.Timestamp.Compare(a.Timestamp), cmp.Compare(b.ID, a.ID))
	})
}

// compareMessages is the order of Search: pending messages first, then the
// latest sent, or the latest status if not sent, then the newest.
func compareMessages(a, b MessageRow) int {
	return cmp.Or(
		compareBool(a.LastStatus == statusPending, b.LastStatus == statusPending),
		compareTimes(a.listedAt(), b.listedAt()),
		b.CreatedAt.Compare(a.CreatedAt),
		cmp.Compare(a.ID, b.ID),
	)
}

// listedAt is coalesce(sent_at, last_status_at).
func (m MessageRow) listedAt() *time.Time {
	if m.SentAt != nil {
		return m.SentAt
	}
	return m.LastStatusAt
}

// compareBool puts true first.
func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return -1
	}
	return 1
}

// compareTimes puts the latest first, and null before that.
func compareTimes(a, b *time.Time) int {
	if a == nil || b == nil {
		return compareBool(a == nil, b == nil)
	}
	return b.Compare(*a)
}

// topMessages keeps the first max messages, in the order of Search.
type topMessages struct {
	max  int
	rows []MessageRow
}

func (t *topMessages) add(m MessageRow) {
	i, _ := slices.BinarySearchFunc(t.rows, m, compareMessages)
	if i >= t.max {
		return
	}
	t.rows = slices.Insert(t.rows, i, m)
	if len(t.rows) > t.max {
		t.rows = t.rows[:t.max]
	}
}

// messageFilter is which messages a search finds. It excludes bundled ones.
type messageFilter struct {
	omit          map[string]bool
	after, before time.Time // zero for no bound
	text          *textFilter
	look          *DeltaLookups
	matched       map[textKey]bool // by matchText, in one reader
}

func (d *deltaStore) newMessageFilter(opts *renderData) (*messageFilter, error) {
	f := &messageFilter{look: &d.look, omit: make(map[string]bool, len(opts.Omit))}
	for _, id := range opts.Omit {
		id, err := canonicalID(id)
		if err != nil {
			return nil, err
		}
		f.omit[id] = true
	}
	// created_at has microseconds.
	if !opts.CreatedAfter.IsZero() {
		f.after = opts.CreatedAfter.Truncate(time.Microsecond)
	}
	if !opts.CreatedBefore.IsZero() {
		f.before = opts.CreatedBefore.Truncate(time.Microsecond)
	}
	if opts.Search != "" {
		f.text = newTextFilter(opts.Search)
	}
	return f, nil
}

func (f *messageFilter) match(r filestore.Reader, m MessageRow) bool {
	return m.LastStatus != statusBundled &&
		!f.omit[m.ID] &&
		(f.after.IsZero() || !m.CreatedAt.Before(f.after)) &&
		(f.before.IsZero() || m.CreatedAt.Before(f.before)) &&
		(f.text == nil || f.matchText(r, m))
}

// textKey is a row of another table that the text of a search can match.
type textKey struct {
	table byte
	id    string
}

// matchText looks at each user, service, contact method and channel once, as
// many messages share them.
func (f *messageFilter) matchText(r filestore.Reader, m MessageRow) bool {
	match := func(table byte, id *string, fn func(id string) bool) bool {
		if id == nil {
			return false
		}
		key := textKey{table, *id}
		ok, known := f.matched[key]
		if !known {
			ok = fn(*id)
			f.matched[key] = ok
		}
		return ok
	}
	return match('u', m.UserID, func(id string) bool {
		name, ok := f.look.UserName(r, id)
		return ok && f.text.matchName(name)
	}) || match('s', m.ServiceID, func(id string) bool {
		name, ok := f.look.ServiceName(r, id)
		return ok && f.text.matchName(name)
	}) || match('m', m.ContactMethodID, func(id string) bool {
		typ, value, ok := f.look.MethodTypeValue(r, id)
		return ok && (f.text.matchPattern(value) || strings.ToLower(typ) == f.text.lower)
	}) || match('c', m.ChannelID, func(id string) bool {
		name, typ, ok := f.look.ChannelNameType(r, id)
		return ok && (f.text.matchPattern(name) || strings.ToLower(typ) == f.text.lower)
	})
}

// ascendMatches calls fn for each message the filter matches, reading only
// the days of its time range if it has a start. No message is created after
// now, so a range without an end reads up to tomorrow.
func (d *deltaStore) ascendMatches(s deltatxn.Scope, f *messageFilter, fn func(MessageRow)) {
	f.matched = make(map[textKey]bool)
	visit := func(m MessageRow) bool {
		if f.match(s.Reader, m) {
			fn(m)
		}
		return true
	}
	if f.after.IsZero() {
		d.Messages.Ascend(s.Reader, visit)
		return
	}
	end := f.before
	if end.IsZero() {
		end = s.Now.Add(24 * time.Hour)
	}
	d.byCreated.ascend(s.Reader, f.after, end, visit)
}

// textFilter is a search text. Names match by the prefixes of their words,
// in order, or by containing the text; pattern columns match like ILIKE.
type textFilter struct {
	lower   string
	terms   []string
	pattern []rune
}

func newTextFilter(search string) *textFilter {
	lower := strings.ToLower(search)
	return &textFilter{lower: lower, terms: words(lower), pattern: []rune("%" + lower + "%")}
}

// words splits s into runs of letters and digits.
func words(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

func (f *textFilter) matchName(name string) bool {
	name = strings.ToLower(name)
	return hasWordPrefixes(words(name), f.terms) || strings.Contains(name, f.lower)
}

// hasWordPrefixes reports whether words include one that starts with each of
// the prefixes, in order. No prefixes match anything with a word.
func hasWordPrefixes(words, prefixes []string) bool {
	if len(words) == 0 {
		return false
	}
	for _, w := range words {
		if len(prefixes) == 0 {
			break
		}
		if strings.HasPrefix(w, prefixes[0]) {
			prefixes = prefixes[1:]
		}
	}
	return len(prefixes) == 0
}

func (f *textFilter) matchPattern(s string) bool {
	return like([]rune(strings.ToLower(s)), f.pattern)
}

// like is LIKE: in pattern, % matches any text, _ any character, and a
// character after a backslash only itself.
func like(s, pattern []rune) bool {
	si, pi := 0, 0
	retryP, retryS := -1, 0 // where to go on after the last %, one character later
	for si < len(s) {
		if pi < len(pattern) && pattern[pi] == '%' {
			pi++
			retryP, retryS = pi, si
			continue
		}
		if pi < len(pattern) {
			want, n := pattern[pi], 1
			literal := want == '\\' && pi+1 < len(pattern)
			if literal {
				want, n = pattern[pi+1], 2
			}
			if !literal && want == '_' || want == s[si] {
				si++
				pi += n
				continue
			}
		}
		if retryP < 0 {
			return false
		}
		retryS++
		si, pi = retryS, retryP
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

// search returns the messages that match, in the order of compareMessages.
// With a cursor, they are those after its message in that order, or, if it
// was deleted, those created before it.
func (d *deltaStore) search(ctx context.Context, opts *renderData) ([]MessageLog, error) {
	f, err := d.newMessageFilter(opts)
	if err != nil {
		return nil, err
	}
	var cursorID string
	if opts.After.ID != "" {
		id, err := validate.ParseUUID("After", opts.After.ID)
		if err != nil {
			return nil, err
		}
		cursorID = id.String()
	}
	cursorAt := opts.After.CreatedAt.Truncate(time.Microsecond)

	var result []MessageLog
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		cursor, listed := d.Messages.Get(s.Reader, cursorID)
		past := func(m MessageRow) bool {
			switch {
			case cursorID == "":
				return true
			case listed:
				return compareMessages(m, cursor) > 0
			}
			return m.CreatedAt.Before(cursorAt) || m.CreatedAt.Equal(cursorAt) && m.ID > cursorID
		}
		top := topMessages{max: opts.Limit}
		d.ascendMatches(s, f, func(m MessageRow) {
			if past(m) {
				top.add(m)
			}
		})
		for _, m := range top.rows {
			l, err := d.messageLog(s.Reader, m)
			if err != nil {
				return err
			}
			result = append(result, l)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *deltaStore) messageLog(r filestore.Reader, m MessageRow) (MessageLog, error) {
	l := MessageLog{
		ID:            m.ID,
		CreatedAt:     m.CreatedAt,
		MessageType:   gadb.EnumOutgoingMessagesType(m.MessageType),
		StatusDetails: m.StatusDetails,
		RetryCount:    int(m.RetryCount),
	}
	var err error
	if l.LastStatus, err = messageStateFromStatus(gadb.EnumOutgoingMessagesStatus(m.LastStatus), false); err != nil {
		return l, err
	}
	if m.LastStatusAt != nil {
		l.LastStatusAt = *m.LastStatusAt
	}
	if m.SentAt != nil {
		sentAt := *m.SentAt
		l.SentAt = &sentAt
	}
	if m.SrcValue != nil {
		l.SrcValue = *m.SrcValue
	}
	if m.AlertID != nil {
		l.AlertID = int(*m.AlertID)
	}
	if m.ProviderMsgID != nil {
		id, err := gadb.ParseProviderMessageID(*m.ProviderMsgID)
		if err != nil {
			return l, err
		}
		l.ProviderMsgID = &id
	}
	if m.UserID != nil {
		l.UserID = *m.UserID
		l.UserName, _ = d.look.UserName(r, l.UserID)
	}
	if m.ServiceID != nil {
		l.ServiceID = *m.ServiceID
		l.ServiceName, _ = d.look.ServiceName(r, l.ServiceID)
	}
	if l.ContactMethodID, err = optionalUUID(m.ContactMethodID); err != nil {
		return l, fmt.Errorf("outgoing_messages %s: contact_method_id: %w", m.ID, err)
	}
	if l.ChannelID, err = optionalUUID(m.ChannelID); err != nil {
		return l, fmt.Errorf("outgoing_messages %s: channel_id: %w", m.ID, err)
	}
	return l, nil
}

func optionalUUID(id *string) (uuid.UUID, error) {
	if id == nil {
		return uuid.Nil, nil
	}
	return uuid.Parse(*id)
}

// timeSeries counts the messages that match, by when they were created, in
// intervals that start at the origin. A cursor makes no difference to it.
func (d *deltaStore) timeSeries(ctx context.Context, opts *renderData) ([]TimeSeriesBucket, error) {
	interval := opts.TimeSeriesInterval.Microseconds()
	if interval < time.Second.Microseconds() {
		return nil, validation.NewFieldError("TimeSeriesInterval", "must be at least 1 second")
	}
	f, err := d.newMessageFilter(opts)
	if err != nil {
		return nil, err
	}
	// A zero origin is too far from any message for a time.Duration.
	origin := opts.TimeSeriesOrigin.Unix() * time.Second.Microseconds()
	index := func(t time.Time) int64 {
		i := (t.UnixMicro() - origin) / interval
		if t.UnixMicro() < origin+i*interval {
			i-- // before the origin, Go rounds up
		}
		return i
	}
	var counts map[int64]int
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		counts = make(map[int64]int)
		d.ascendMatches(s, f, func(m MessageRow) { counts[index(m.CreatedAt)]++ })
		return nil
	})
	if err != nil {
		return nil, err
	}

	var buckets []TimeSeriesBucket
	for t := opts.CreatedAfter; t.Before(opts.CreatedBefore); t = t.Add(opts.TimeSeriesInterval) {
		buckets = append(buckets, TimeSeriesBucket{Start: t, End: t.Add(opts.TimeSeriesInterval), Count: counts[index(t)]})
	}
	return buckets, nil
}
