package notification

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/validation"
)

var sysCtx = permission.SystemContext(context.Background(), "test")

func logIDs(logs []MessageLog) []string {
	var ids []string
	for _, l := range logs {
		ids = append(ids, l.ID)
	}
	return ids
}

func (p *deltaPod) search(t *testing.T, opts SearchOptions) []string {
	t.Helper()
	logs, err := p.Search(sysCtx, &opts)
	require.NoError(t, err)
	return logIDs(logs)
}

// The same rows and order as TestGraphQLAdminMessageLogs.
func TestDeltaSearchOrder(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	at := func(s string) time.Time {
		v, err := time.Parse(time.DateTime, "2022-01-01 "+s)
		require.NoError(t, err)
		return v
	}
	msg := func(created string, sent string, status string) string {
		m := MessageRow{MessageType: typeTest, UserID: &x.user, ContactMethodID: &x.cm, CreatedAt: at(created), LastStatus: status}
		if sent != "" {
			m.SentAt = ptr(at(sent))
		}
		return p.insert(t, m).ID
	}
	om1 := msg("00:01:00", "00:01:01", "delivered")
	om2 := msg("00:02:00", "00:01:02", "delivered")
	om3 := msg("00:03:00", "", "failed")
	om4 := msg("00:05:00", "", "pending")
	om5 := msg("00:04:00", "00:01:04", "delivered")
	msg("00:06:00", "", "bundled")

	assert.Equal(t, []string{om4, om3, om5, om2, om1}, p.search(t, SearchOptions{}))
	assert.Equal(t, []string{om4, om3}, p.search(t, SearchOptions{Limit: 2}))
	assert.Equal(t, []string{om3, om2}, p.search(t, SearchOptions{Omit: []string{strings.ToUpper(om4), om5, om1}}))

	// A page goes on after the last message of the one before.
	var all []string
	opts := SearchOptions{Limit: 2}
	for {
		logs, err := p.Search(sysCtx, &opts)
		require.NoError(t, err)
		if len(logs) == 0 {
			break
		}
		all = append(all, logIDs(logs)...)
		last := logs[len(logs)-1]
		opts.After = SearchCursor{ID: last.ID, CreatedAt: last.CreatedAt}
	}
	assert.Equal(t, []string{om4, om3, om5, om2, om1}, all)

	// Also if it was deleted: then those created before it.
	after := SearchCursor{ID: om5, CreatedAt: at("00:04:00")}
	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.tables.Messages.Delete(tx, om5)
		return err
	})
	assert.Equal(t, []string{om3, om2, om1}, p.search(t, SearchOptions{After: after}))
	assert.Equal(t, []string{om2}, p.search(t, SearchOptions{After: after, Omit: []string{om3, om1}}), "other filters still apply")

	_, err := p.Search(sysCtx, &SearchOptions{After: SearchCursor{ID: "nope"}})
	assert.True(t, validation.IsClientError(err))
	_, err = p.Search(asUser(x.user), nil)
	assert.True(t, permission.IsPermissionError(err))
	logs, err := p.Search(sysCtx, nil)
	require.NoError(t, err)
	assert.Len(t, logs, 4)
	none := newDeltaEnv(t).pod(t, "a")
	logs, err = none.Search(sysCtx, nil)
	require.NoError(t, err)
	assert.Nil(t, logs)
}

func TestDeltaSearchRow(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	sent := deltaStart.Add(time.Minute)
	m := x.alertMessage()
	m.LastStatus, m.SentAt, m.StatusDetails, m.SrcValue, m.RetryCount = "delivered", &sent, "done", ptr("src"), 2
	m.ProviderMsgID = ptr("Twilio-Voice:CA1")
	m = p.insert(t, m)
	ch := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel})

	logs, err := e.pod(t, "b").Search(sysCtx, &SearchOptions{})
	require.NoError(t, err)
	require.Len(t, logs, 2)
	assert.Equal(t, MessageLog{
		ID: ch.ID, CreatedAt: deltaStart, LastStatusAt: deltaStart, MessageType: MessageTypeTest,
		LastStatus: StatePending, ChannelID: uuid.MustParse(x.channel),
	}, logs[0])
	assert.Equal(t, MessageLog{
		ID: m.ID, CreatedAt: deltaStart, LastStatusAt: deltaStart, MessageType: MessageTypeAlert,
		LastStatus: StateDelivered, StatusDetails: "done", SrcValue: "src", AlertID: 1,
		ProviderMsgID: &ProviderMessageID{ProviderName: "builtin-twilio-voice", ExternalID: "CA1"},
		UserID:        x.user, UserName: "Alice", ContactMethodID: uuid.MustParse(x.cm),
		ServiceID: x.service, ServiceName: "Service", SentAt: &sent, RetryCount: 2,
	}, logs[1])

	*logs[1].SentAt = time.Time{}
	assert.Equal(t, sent, *e.pod(t, "c").message(t, m.ID).SentAt)
	assert.Equal(t, sent, *m.SentAt, "a result is the caller's to change")
}

func TestDeltaSearchText(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	user := p.addUser(t, "Joe_the-Plumber O'Brien")
	cm, ch, svc := uuid.NewString(), uuid.NewString(), uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error {
		if err := e.methods.Put(tx, deltaMethod{ID: cm, UserID: user, Type: "SMS", Value: "+17635550100"}); err != nil {
			return err
		}
		if err := e.channels.Put(tx, deltaChannel{ID: ch, Name: "#ops_room 100%", Type: "SLACK"}); err != nil {
			return err
		}
		return e.svcs.Put(tx, deltaNamed{ID: svc, Name: "Payments API (EU)"})
	})
	toUser := p.insert(t, MessageRow{MessageType: typeTest, UserID: &user, ContactMethodID: &cm}).ID
	toChannel := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &ch}).ID
	forService := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: ptr(p.addChannel(t)), ServiceID: &svc}).ID

	for search, want := range map[string][]string{
		"":              {toUser, toChannel, forService},
		"joe":           {toUser},
		"plumb":         {toUser},
		"joe pl":        {toUser}, // prefixes of words, in order
		"pl joe":        nil,
		"umber":         {toUser}, // contains
		"joe joe":       nil,
		"o'brien":       {toUser},
		"JOE_THE-PL":    {toUser},
		"payments eu":   {forService},
		"eu payments":   nil,
		"(EU)":          {forService},
		"635550":        {toUser}, // in the contact method's value
		"7_35550":       {toUser}, // ILIKE: _ is any character
		"5%00":          {toUser}, // ILIKE: % is any text
		"+1763%9":       nil,      //
		"sms":           {toUser}, // the contact method's type
		"sm":            nil,      // the type only as a whole
		"slack":         {toChannel, forService},
		"OPS":           {toChannel, forService},
		"ops_room":      {toChannel},
		"room 100\\%":   {toChannel}, // an escaped %
		"room 100":      {toChannel},
		"#o":            {toUser, toChannel, forService}, // the word o as well
		"no such thing": nil,
	} {
		assert.ElementsMatch(t, want, p.search(t, SearchOptions{Search: search}), "search %q", search)
	}
	// A text without letters or digits has no words to look for, so any name
	// with a word matches.
	assert.Subset(t, p.search(t, SearchOptions{Search: "!"}), []string{toUser, forService})

	_, err := p.Search(sysCtx, &SearchOptions{Search: strings.Repeat("x", 256)})
	assert.True(t, validation.IsClientError(err))
	_, err = p.Search(sysCtx, &SearchOptions{Limit: 102})
	assert.True(t, validation.IsClientError(err))
}

func TestLike(t *testing.T) {
	for _, c := range []struct {
		s, pattern string
		want       bool
	}{
		{"", "", true}, {"", "%", true}, {"", "_", false}, {"a", "", false},
		{"abc", "abc", true}, {"abc", "ab", false}, {"abc", "a%", true}, {"abc", "%c", true}, {"abc", "%b%", true},
		{"abc", "a_c", true}, {"abc", "a__", true}, {"abc", "a___", false}, {"abc", "%%%", true},
		{"aXbXc", "%X%X%", true}, {"aXbc", "%X%X%", false}, {"aab", "%ab", true}, {"mississippi", "%iss%ip%", true},
		{"a%b", `a\%b`, true}, {"axb", `a\%b`, false}, {"a_b", `a\_b`, true}, {"axb", `a\_b`, false},
		{`a\b`, `a\\b`, true}, {"ab", `a\b`, true}, {`a\`, `a\`, true},
		{"čšž", "_š_", true},
	} {
		assert.Equal(t, c.want, like([]rune(c.s), []rune(c.pattern)), "%q like %q", c.s, c.pattern)
	}
}

func TestDeltaSearchTimes(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	e.clock.Set(deltaStart.Add(1000 * time.Hour))
	ages := []time.Duration{-72 * time.Hour, -24 * time.Hour, -time.Microsecond, 0, time.Microsecond, time.Hour, 12 * time.Hour, 24 * time.Hour, 999 * time.Hour, 1000 * time.Hour}
	ids := make(map[time.Duration]string)
	for _, d := range ages {
		ids[d] = p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel, CreatedAt: deltaStart.Add(d)}).ID
	}
	const unbounded = 30 * 365 * 24 * time.Hour
	for _, r := range []struct{ after, before time.Duration }{
		{0, unbounded}, {-unbounded, 0}, {-unbounded, unbounded}, {0, time.Hour}, {-24 * time.Hour, 24 * time.Hour},
		{time.Microsecond, 12*time.Hour + time.Microsecond}, {-72 * time.Hour, unbounded}, {time.Hour, 0}, {1000 * time.Hour, unbounded},
		{-3000 * 24 * time.Hour, unbounded},
	} {
		opts := SearchOptions{Limit: 100}
		if r.after != -unbounded {
			opts.CreatedAfter = deltaStart.Add(r.after + 999) // below the precision of created_at
		}
		if r.before != unbounded {
			opts.CreatedBefore = deltaStart.Add(r.before + 999)
		}
		var want []string
		for d, id := range ids {
			if d >= r.after && d < r.before {
				want = append(want, id)
			}
		}
		assert.ElementsMatch(t, want, p.search(t, opts), "%s to %s", r.after, r.before)
	}
}

// The filtered, sorted and limited result is the same as sorting everything.
func TestDeltaSearchTop(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	var rows []MessageRow
	p.write(t, func(tx *filestore.Tx) error {
		rows = nil
		for i := range 200 {
			created := deltaStart.Add(-time.Duration(i*i%37) * time.Minute)
			m := MessageRow{MessageType: typeTest, ChannelID: &x.channel, CreatedAt: created, LastStatusAt: ptr(created.Add(time.Duration(i%5) * time.Second))}
			switch i % 3 {
			case 0:
				m.LastStatus, m.SentAt = "sent", ptr(created.Add(time.Duration(i%7)*time.Second))
			case 1:
				m.LastStatus = statusFailed
			}
			m, err := e.tables.InsertMessage(tx, m)
			if err != nil {
				return err
			}
			rows = append(rows, m)
		}
		return nil
	})
	slices.SortFunc(rows, compareMessages)
	var want []string
	for _, m := range rows {
		want = append(want, m.ID)
	}
	assert.Equal(t, want[:100], p.search(t, SearchOptions{Limit: 100}))
	assert.Equal(t, want[:50], p.search(t, SearchOptions{}), "50 by default")
	assert.Equal(t, want[100:150], p.search(t, SearchOptions{After: SearchCursor{ID: want[99]}}))
	for i := 1; i < len(rows); i++ {
		pending := func(m MessageRow) bool { return m.LastStatus == statusPending }
		assert.False(t, pending(rows[i]) && !pending(rows[i-1]), "pending messages come first")
	}
	// A message without any time, which can't happen, sorts like null.
	assert.Negative(t, compareMessages(MessageRow{LastStatus: "sent"}, MessageRow{LastStatus: "sent", SentAt: &deltaStart}))
	assert.Positive(t, compareMessages(MessageRow{LastStatus: "sent", SentAt: &deltaStart}, MessageRow{LastStatus: "sent"}))
}

func TestDeltaTimeSeries(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	for _, d := range []time.Duration{-time.Second, 0, time.Minute, 10*time.Minute - time.Microsecond, 10 * time.Minute, 35 * time.Minute, time.Hour} {
		p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel, CreatedAt: deltaStart.Add(d)})
	}
	p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel, CreatedAt: deltaStart.Add(time.Minute), LastStatus: statusBundled})
	opts := TimeSeriesOpts{
		SearchOptions:    SearchOptions{CreatedAfter: deltaStart, CreatedBefore: deltaStart.Add(time.Hour)},
		TimeSeriesOrigin: deltaStart, TimeSeriesInterval: 10 * time.Minute,
	}
	counts := func(opts TimeSeriesOpts) []int {
		t.Helper()
		buckets, err := p.TimeSeries(sysCtx, opts)
		require.NoError(t, err)
		var counts []int
		for i, b := range buckets {
			assert.Equal(t, opts.CreatedAfter.Add(time.Duration(i)*opts.TimeSeriesInterval), b.Start)
			assert.Equal(t, b.Start.Add(opts.TimeSeriesInterval), b.End)
			counts = append(counts, b.Count)
		}
		return counts
	}
	assert.Equal(t, []int{3, 1, 0, 1, 0, 0}, counts(opts))

	withCursor := opts
	withCursor.After = SearchCursor{ID: uuid.NewString(), CreatedAt: deltaStart}
	assert.Equal(t, []int{3, 1, 0, 1, 0, 0}, counts(withCursor), "a page of the search is not the series")

	for name, origin := range map[string]time.Time{"none": {}, "before": deltaStart.Add(-time.Hour), "after": deltaStart.Add(30 * time.Minute)} {
		o := opts
		o.TimeSeriesOrigin = origin
		assert.Equal(t, []int{3, 1, 0, 1, 0, 0}, counts(o), "origin %s", name)
	}

	o := opts
	o.CreatedAfter = deltaStart.Add(-30 * time.Minute)
	o.TimeSeriesOrigin = deltaStart
	assert.Equal(t, []int{0, 0, 1, 3, 1, 0, 1, 0, 0}, counts(o), "the intervals on both sides of the origin are apart")

	o = opts
	o.Search = "no such thing"
	assert.Equal(t, make([]int, 6), counts(o))

	for name, change := range map[string]func(o *TimeSeriesOpts){
		"an interval of nothing": func(o *TimeSeriesOpts) { o.TimeSeriesInterval = 0 },
		"less than a second":     func(o *TimeSeriesOpts) { o.TimeSeriesInterval = time.Second - 1 },
		"no start":               func(o *TimeSeriesOpts) { o.CreatedAfter = time.Time{} },
		"no end":                 func(o *TimeSeriesOpts) { o.CreatedBefore = time.Time{} },
	} {
		o := opts
		change(&o)
		_, err := p.TimeSeries(sysCtx, o)
		assert.True(t, validation.IsClientError(err), name)
	}
	_, err := p.TimeSeries(asUser(x.user), opts)
	assert.True(t, permission.IsPermissionError(err))
	backwards := opts
	backwards.CreatedAfter, backwards.CreatedBefore = opts.CreatedBefore, opts.CreatedAfter
	assert.Empty(t, counts(backwards))
}
