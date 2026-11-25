package alertlog

import (
	"context"
	"database/sql"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// alertRow stands in for an alert.
type alertRow struct {
	ID        int64  `parquet:"id"`
	ServiceID string `parquet:"service_id"`
	Status    string `parquet:"status"`
}

// provider is a destination type whose TypeInfo would call out, unless its
// context is already canceled.
type provider struct {
	id, name string
	calls    atomic.Int32 // with a context that lets it call out
	canceled atomic.Int32 // with a canceled one
}

func (p *provider) ID() string { return p.id }
func (p *provider) TypeInfo(ctx context.Context) (*nfydest.TypeInfo, error) {
	if ctx.Err() != nil {
		p.canceled.Add(1)
	} else {
		p.calls.Add(1)
	}
	return &nfydest.TypeInfo{Type: p.id, Name: p.name}, nil
}
func (p *provider) ValidateField(context.Context, string, string) error { return nil }
func (p *provider) DisplayInfo(context.Context, map[string]string) (*nfydest.DisplayInfo, error) {
	return &nfydest.DisplayInfo{}, nil
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	alerts *filestore.Table[alertRow]
	clock  *filestore.FakeClock

	// Stand-ins for the other packages' rows, by id.
	users, keys, monitors, channels map[string]string // names
	dests                           map[string]gadb.DestV1
	keyTypes                        map[string]integrationkey.Type
	intervals                       map[string]time.Duration
	policies                        map[string][]string

	sms, chat *provider
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &deltaEnv{
		store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock,
		users: map[string]string{}, keys: map[string]string{}, monitors: map[string]string{}, channels: map[string]string{},
		dests: map[string]gadb.DestV1{}, keyTypes: map[string]integrationkey.Type{},
		intervals: map[string]time.Duration{}, policies: map[string][]string{},
		sms: &provider{id: "sms", name: "Text Message (SMS)"}, chat: &provider{id: "chat", name: "Chat Channel"},
	}
	e.alerts = filestore.NewTable(e.schema, "alerts", func(r alertRow) string { return filestore.IntKey(r.ID) })
	e.tables = DefineDeltaTables(e.schema)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

func (e *deltaEnv) lookups() DeltaLookups {
	name := func(m map[string]string) func(filestore.Reader, string) (string, bool) {
		return func(_ filestore.Reader, id string) (string, bool) {
			s, ok := m[id]
			return s, ok
		}
	}
	dest := func(_ filestore.Reader, id string) (gadb.DestV1, error) {
		d, ok := e.dests[id]
		if !ok {
			return gadb.DestV1{}, sql.ErrNoRows
		}
		return d, nil
	}
	return DeltaLookups{
		UserName: name(e.users), IntegrationKeyName: name(e.keys), HeartbeatMonitorName: name(e.monitors), ChannelName: name(e.channels),
		ChannelDest: dest, ContactMethodDest: dest, MessageDest: dest,
		IntegrationKeyType: func(_ filestore.Reader, id string) (integrationkey.Type, bool) {
			typ, ok := e.keyTypes[id]
			return typ, ok
		},
		HeartbeatInterval: func(_ filestore.Reader, id string) (time.Duration, bool) {
			d, ok := e.intervals[id]
			return d, ok
		},
		PolicyServices: func(_ filestore.Reader, id string) []string { return e.policies[id] },
		AlertIDs: func(r filestore.Reader, serviceID string, statuses ...string) []int64 {
			var ids []int64
			e.alerts.Ascend(r, func(a alertRow) bool {
				if a.ServiceID == serviceID && (len(statuses) == 0 || slices.Contains(statuses, a.Status)) {
					ids = append(ids, a.ID)
				}
				return true
			})
			return ids
		},
	}
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) *Store {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	reg := nfydest.NewRegistry()
	reg.RegisterProvider(context.Background(), e.sms)
	reg.RegisterProvider(context.Background(), e.chat)
	return NewDeltaStore(deltatxn.DB{DB: fdb}, e.tables, reg, e.lookups())
}

func (s *Store) write(t *testing.T, fn func(s deltatxn.Scope) error) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), s.delta.db, nil, fn))
}

func (e *deltaEnv) addAlerts(t *testing.T, s *Store, rows ...alertRow) {
	t.Helper()
	s.write(t, func(s deltatxn.Scope) error {
		for _, r := range rows {
			if err := e.alerts.Put(s.Tx, r); err != nil {
				return err
			}
		}
		return nil
	})
}

// all returns the rows as a pod opened now sees them (F29).
func (e *deltaEnv) all(t *testing.T) []DeltaLog {
	t.Helper()
	var rows []DeltaLog
	s := e.pod(t, "reader")
	require.NoError(t, deltatxn.Read(context.Background(), s.delta.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		e.tables.Logs.Ascend(s.Reader, func(l DeltaLog) bool {
			rows = append(rows, l)
			return true
		})
		return nil
	}))
	return rows
}

func source(ctx context.Context, typ permission.SourceType, id string) context.Context {
	return permission.SourceContext(ctx, &permission.SourceInfo{Type: typ, ID: id})
}

func TestDeltaLogSubjects(t *testing.T) {
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	userID, keyID, hbID, ncID, cmID, msgID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	e.users[userID], e.keys[keyID], e.monitors[hbID], e.channels[ncID] = "Alice", "Grafana prod", "Cron", "#ops"
	e.dests[cmID], e.dests[msgID], e.dests[ncID] = gadb.NewDestV1("sms"), gadb.NewDestV1("sms"), gadb.NewDestV1("chat")
	e.keyTypes[keyID] = integrationkey.TypeGrafana
	e.intervals[hbID] = 90 * time.Second

	sys := permission.SystemContext(context.Background(), "test")
	user := permission.UserContext(context.Background(), userID, permission.RoleUser)

	check := func(name string, ctx context.Context, typ Type, meta any, want *Subject, text string) {
		t.Helper()
		require.NoError(t, s.LogTx(ctx, nil, 1, typ, meta), name)
		got, err := s.FindLatestByType(ctx, 1, typ)
		require.NoError(t, err, name)
		assert.Equal(t, want, got.Subject(), name)
		assert.Equal(t, text, got.String(ctx), name)
		assert.Equal(t, 1, got.AlertID(), name)
		assert.Equal(t, start, got.Timestamp(), name)
	}
	check("no source", sys, TypeCreated, &CreatedMetaData{}, nil, "Created")
	check("web", source(user, permission.SourceTypeAuthProvider, "basic"), TypeAcknowledged, nil,
		&Subject{Type: SubjectTypeUser, ID: userID, Name: "Alice", Classifier: "Web"}, "Acknowledged by Alice (Web)")
	require.NoError(t, s.LogTx(source(user, permission.SourceTypeAuthProvider, "basic"), nil, 1, _TypeAcknowledgeAll, nil))
	require.NoError(t, s.LogTx(source(user, permission.SourceTypeAuthProvider, "basic"), nil, 1, _TypeCloseAll, nil))
	check("contact method", source(user, permission.SourceTypeContactMethod, cmID), TypeNotificationSent, NotificationMetaData{MessageID: msgID},
		&Subject{Type: SubjectTypeUser, ID: userID, Name: "Alice", Classifier: "Text Message (SMS)"}, "Notification sent to Alice (Text Message (SMS))")
	check("no rule", source(user, permission.SourceTypeContactMethod, ""), TypeNoNotificationSent, nil,
		&Subject{Type: SubjectTypeUser, ID: userID, Name: "Alice", Classifier: "no immediate rule"}, "No notification sent to Alice (no immediate rule)")
	check("reply", source(user, permission.SourceTypeNotificationCallback, msgID), TypeEscalationRequest, nil,
		&Subject{Type: SubjectTypeUser, ID: userID, Name: "Alice", Classifier: "Text Message (SMS)"}, "Escalation requested by Alice (Text Message (SMS))")
	check("channel", source(sys, permission.SourceTypeNotificationChannel, ncID), TypeNotificationSent, NotificationMetaData{MessageID: msgID},
		&Subject{Type: SubjectTypeChannel, ID: ncID, Name: "#ops", Classifier: "Chat Channel"}, "Notification sent to #ops (Chat Channel)")
	check("heartbeat", source(sys, permission.SourceTypeHeartbeat, hbID), TypeCreated, nil,
		&Subject{Type: SubjectTypeHeartbeatMonitor, ID: hbID, Name: "Cron", Classifier: "expired after 2 minutes"}, "Created via Cron heartbeat monitor (expired after 2 minutes)")
	check("heartbeat again", source(sys, permission.SourceTypeHeartbeat, hbID), TypeClosed, nil,
		&Subject{Type: SubjectTypeHeartbeatMonitor, ID: hbID, Name: "Cron", Classifier: "healthy"}, "Closed via Cron heartbeat monitor (healthy)")
	check("integration key", source(sys, permission.SourceTypeIntegrationKey, keyID), TypeCreated, nil,
		&Subject{Type: SubjectTypeIntegrationKey, ID: keyID, Name: "Grafana prod", Classifier: "Grafana"}, "Created via Grafana prod integration (Grafana)")
	e.intervals[hbID] = 89 * time.Second
	check("one minute", source(sys, permission.SourceTypeHeartbeat, hbID), TypeCreated, nil,
		&Subject{Type: SubjectTypeHeartbeatMonitor, ID: hbID, Name: "Cron", Classifier: "expired after 1 minute"}, "Created via Cron heartbeat monitor (expired after 1 minute)")

	// A subject that is deleted later keeps its id, without a name.
	delete(e.keys, keyID)
	got, err := s.FindOne(sys, 11)
	require.NoError(t, err)
	assert.Equal(t, 11, got.ID())
	assert.Equal(t, &Subject{Type: SubjectTypeIntegrationKey, ID: keyID, Classifier: "Grafana"}, got.Subject())
	assert.Equal(t, "Created via [unknown] integration (Grafana)", got.String(sys))

	m, ok := func() (*NotificationMetaData, bool) {
		got, err := s.FindOne(sys, 8)
		require.NoError(t, err)
		m, ok := got.Meta(sys).(*NotificationMetaData)
		return m, ok
	}()
	require.True(t, ok)
	assert.Equal(t, msgID, m.MessageID)

	rows := e.all(t)
	require.Len(t, rows, 12)
	assert.Nil(t, rows[0].SubType)
	assert.Equal(t, `{"EPNoSteps":false}`, string(rows[0].Meta))
	assert.Equal(t, "acknowledged", rows[2].Event)
	assert.Equal(t, "Web, Ack-All", rows[2].SubClassifier)
	assert.Equal(t, "closed", rows[3].Event)
	assert.Equal(t, "Web, Close-All", rows[3].SubClassifier)
	assert.Nil(t, rows[2].Meta)

	_, err = s.FindOne(sys, 99)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = s.FindLatestByType(sys, 99, TypeCreated)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = s.FindOne(context.Background(), 1)
	assert.True(t, permission.IsPermissionError(err))
	assert.True(t, permission.IsPermissionError(s.LogTx(context.Background(), nil, 1, TypeCreated, nil)))
}

// TestDeltaLogMissingSubject: an entry whose subject can't be described is an
// error for LogTx and is left out by MustLogTx, and the caller's transaction
// goes on, as on Postgres.
func TestDeltaLogMissingSubject(t *testing.T) {
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	sys := permission.SystemContext(context.Background(), "test")
	e.dests["9e0b1f6e-0000-4000-8000-000000000001"] = gadb.NewDestV1("unregistered")
	for name, ctx := range map[string]context.Context{
		"integration key":     source(sys, permission.SourceTypeIntegrationKey, uuid.NewString()),
		"not an id":           source(sys, permission.SourceTypeIntegrationKey, "x"),
		"contact method":      source(sys, permission.SourceTypeContactMethod, uuid.NewString()),
		"message":             source(sys, permission.SourceTypeNotificationCallback, uuid.NewString()),
		"not a message id":    source(sys, permission.SourceTypeNotificationCallback, "x"),
		"channel":             source(sys, permission.SourceTypeNotificationChannel, uuid.NewString()),
		"heartbeat":           source(sys, permission.SourceTypeHeartbeat, uuid.NewString()),
		"unknown destination": source(sys, permission.SourceTypeNotificationChannel, "9e0b1f6e-0000-4000-8000-000000000001"),
	} {
		require.Error(t, s.LogTx(ctx, nil, 1, TypeCreated, nil), name)
		require.NoError(t, s.delta.db.Run(ctx, nil, func(tx txn.Tx) error {
			s.MustLogTx(ctx, tx, 1, TypeCreated, nil)
			s.MustLogTx(sys, tx, 2, TypeCreated, nil)
			return nil
		}), name)
		s.MustLog(ctx, 1, TypeCreated, nil)
	}
	rows := e.all(t)
	require.Len(t, rows, 8)
	for _, l := range rows {
		assert.EqualValues(t, 2, *l.AlertID)
	}

	err := s.LogTx(sys, nil, 1, "bogus", nil)
	require.ErrorContains(t, err, "bogus", "enum_alert_log_event")
	err = s.LogTx(sys, nil, 1, TypeCreated, func() {})
	require.Error(t, err, "metadata that is not JSON")
}

// TestDeltaLogNoIO: naming a destination type can call its provider, so it is
// done before a transaction, and in one only with a context that prevents it.
func TestDeltaLogNoIO(t *testing.T) {
	e := newDeltaEnv(t)
	ncID, cmID := uuid.NewString(), uuid.NewString()
	e.dests[ncID], e.dests[cmID] = gadb.NewDestV1("chat"), gadb.NewDestV1("sms")
	ctx := source(permission.SystemContext(context.Background(), "test"), permission.SourceTypeNotificationChannel, ncID)

	a := e.pod(t, "a")
	require.NoError(t, a.LogTx(ctx, nil, 1, TypeNotificationSent, nil))
	require.NoError(t, a.LogTx(ctx, nil, 1, TypeNotificationSent, nil))
	assert.EqualValues(t, 1, e.chat.calls.Load(), "the name is looked up once, before the transaction")
	assert.Zero(t, e.chat.canceled.Load())

	b := e.pod(t, "b")
	b.Prepare(ctx)
	assert.EqualValues(t, 2, e.chat.calls.Load())
	require.NoError(t, b.delta.db.Run(ctx, nil, func(tx txn.Tx) error { return b.LogTx(ctx, tx, 1, TypeNotificationSent, nil) }))
	assert.Zero(t, e.chat.canceled.Load(), "Prepare did it")

	c := e.pod(t, "c")
	require.NoError(t, c.delta.db.Run(ctx, nil, func(tx txn.Tx) error { return c.LogTx(ctx, tx, 1, TypeNotificationSent, nil) }))
	assert.EqualValues(t, 2, e.chat.calls.Load(), "no call from a transaction")
	assert.EqualValues(t, 1, e.chat.canceled.Load())

	rows := e.all(t)
	require.Len(t, rows, 4)
	for _, l := range rows {
		assert.Equal(t, "Chat Channel", l.SubClassifier)
	}

	// Contexts that need no name.
	for _, src := range []permission.SourceType{permission.SourceTypeAuthProvider, permission.SourceTypeHeartbeat} {
		c.Prepare(source(permission.SystemContext(context.Background(), "test"), src, cmID))
	}
	c.Prepare(source(ctx, permission.SourceTypeContactMethod, "not an id"))
	c.Prepare(context.Background())
	assert.Zero(t, e.sms.calls.Load())
	c.Prepare(source(ctx, permission.SourceTypeContactMethod, cmID))
	c.Prepare(source(ctx, permission.SourceTypeNotificationCallback, cmID))
	assert.EqualValues(t, 1, e.sms.calls.Load())
	(&Store{}).Prepare(ctx) // nothing to do on Postgres
}

func TestDeltaLogMany(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	svc1, svc2, svc3, epID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	e.policies[epID] = []string{svc2, svc1}
	e.addAlerts(t, s,
		alertRow{ID: 1, ServiceID: svc1, Status: "triggered"}, alertRow{ID: 2, ServiceID: svc1, Status: "active"},
		alertRow{ID: 3, ServiceID: svc1, Status: "closed"}, alertRow{ID: 4, ServiceID: svc2, Status: "triggered"},
		alertRow{ID: 5, ServiceID: svc3, Status: "triggered"},
	)

	last := 0
	added := func() (ids []int64, event string) {
		t.Helper()
		rows := e.all(t)
		for _, l := range rows[last:] {
			ids = append(ids, *l.AlertID)
			event = l.Event
		}
		last = len(rows)
		return ids, event
	}

	require.NoError(t, s.LogManyTx(sys, nil, []int{4, 1, 77}, TypeEscalated, &EscalationMetaData{NewStepIndex: 1}))
	ids, event := added()
	assert.Equal(t, []int64{4, 1, 77}, ids, "in the order given, whether or not the alert exists, as on Postgres")
	assert.Equal(t, "escalated", event)
	require.NoError(t, s.LogManyTx(sys, nil, nil, TypeEscalated, nil))
	ids, _ = added()
	assert.Empty(t, ids)

	require.NoError(t, s.LogEPTx(sys, nil, epID, TypePolicyUpdated, nil))
	ids, event = added()
	assert.Equal(t, []int64{1, 2, 4}, ids, "the open alerts of the policy's services")
	assert.Equal(t, "policy_updated", event)
	rows := e.all(t)
	assert.Equal(t, "null", string(rows[len(rows)-1].Meta), "a nil *EscalationMetaData is JSON null, as on Postgres")
	require.Error(t, s.LogEPTx(sys, nil, "not a uuid", TypePolicyUpdated, nil))

	require.NoError(t, s.LogServiceTx(sys, nil, svc1, TypeClosed, nil))
	ids, event = added()
	assert.Equal(t, []int64{1, 2}, ids, "every open alert of the service")
	assert.Equal(t, "closed", event)
	require.NoError(t, s.LogServiceTx(sys, nil, svc1, TypeAcknowledged, nil))
	ids, event = added()
	assert.Equal(t, []int64{1}, ids, "every unacknowledged alert of the service")
	assert.Equal(t, "acknowledged", event)
	require.NoError(t, s.LogServiceTx(sys, nil, svc1, TypeNotificationSent, nil))
	ids, _ = added()
	assert.Equal(t, []int64{1}, ids)
	require.NoError(t, s.LogServiceTx(sys, nil, svc1, TypeEscalated, nil))
	ids, _ = added()
	assert.Empty(t, ids, "no other event, as on Postgres")
	require.Error(t, s.LogServiceTx(sys, nil, "not a uuid", TypeClosed, nil))

	// In the caller's transaction, entries are gone when it fails.
	err := s.delta.db.Run(sys, nil, func(tx txn.Tx) error {
		require.NoError(t, s.LogTx(sys, tx, 1, TypeClosed, nil))
		s.MustLogTx(sys, tx, 1, TypeDuplicateSupressed, nil)
		return sql.ErrTxDone
	})
	require.ErrorIs(t, err, sql.ErrTxDone)
	ids, _ = added()
	assert.Empty(t, ids)

	// A read-only transaction refuses entries.
	err = s.delta.db.Run(sys, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error { return s.LogTx(sys, tx, 1, TypeClosed, nil) })
	require.ErrorIs(t, err, deltatxn.ErrReadOnly)
}

func TestDeltaLogDuplicates(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	count := func() int { return len(e.all(t)) }

	s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
	assert.Equal(t, 1, count())
	s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
	assert.Equal(t, 1, count(), "only one in 5 seconds")
	s.MustLog(sys, 2, TypeDuplicateSupressed, nil)
	assert.Equal(t, 2, count(), "each alert on its own")
	s.MustLog(sys, 1, TypeCreated, nil)
	s.MustLog(sys, 1, TypeCreated, nil)
	assert.Equal(t, 4, count(), "other entries are not suppressed")

	e.clock.Advance(5*time.Second - time.Microsecond)
	s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
	assert.Equal(t, 4, count())
	e.clock.Advance(time.Microsecond)
	s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
	assert.Equal(t, 5, count())
	// Wherever the 5 seconds start (the index is in fixed steps of them).
	for range 12 {
		e.clock.Advance(4900 * time.Millisecond)
		n := count()
		s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
		assert.Equal(t, n, count())
		e.clock.Advance(101 * time.Millisecond)
		s.MustLog(sys, 1, TypeDuplicateSupressed, nil)
		assert.Equal(t, n+1, count())
	}

	s.MustLog(context.Background(), 9, TypeDuplicateSupressed, nil)
	for _, l := range e.all(t) {
		assert.NotEqualValues(t, 9, *l.AlertID, "not without permission")
	}
}

// TestDeltaLogDuplicateRace: pods that suppress a duplicate of the same alert
// at once log it once (F4).
func TestDeltaLogDuplicateRace(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	pods := []*Store{e.pod(t, "a"), e.pod(t, "b")}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pods[i%2].MustLog(sys, 1, TypeDuplicateSupressed, nil)
			pods[i%2].MustLog(sys, 1, TypeNotificationSent, nil)
		}()
	}
	wg.Wait()
	n := map[string]int{}
	ids := map[int64]bool{}
	for _, l := range e.all(t) {
		n[l.Event]++
		ids[l.ID] = true
	}
	assert.Equal(t, map[string]int{"duplicate_suppressed": 1, "notification_sent": 8}, n)
	assert.Len(t, ids, 9, "each entry has its own id")
}

func TestDeltaLogSearch(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	svc1, svc2 := uuid.New(), uuid.New()
	e.addAlerts(t, s, alertRow{ID: 1, ServiceID: svc1.String()}, alertRow{ID: 2, ServiceID: svc2.String()}, alertRow{ID: 3, ServiceID: svc1.String()})
	// Entries 1 to 12, for alerts 1, 2, 3 and 4 (which is deleted) in turn, one
	// a minute.
	for i := range 12 {
		require.NoError(t, s.LogTx(sys, nil, i%4+1, TypeCreated, nil))
		e.clock.Advance(time.Minute)
	}
	search := func(opts *SearchOptions) []int {
		t.Helper()
		entries, err := s.Search(sys, opts)
		require.NoError(t, err)
		var ids []int
		for _, e := range entries {
			ids = append(ids, e.ID())
		}
		return ids
	}
	at := func(min int) *time.Time {
		ts := start.Add(time.Duration(min) * time.Minute)
		return &ts
	}
	assert.Equal(t, []int{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, search(nil), "newest first")
	assert.Equal(t, []int{12, 11}, search(&SearchOptions{Limit: 2}))
	assert.Equal(t, []int{10, 9}, search(&SearchOptions{Limit: 2, After: SearchCursor{ID: 11}}))
	assert.Equal(t, []int{12, 11}, search(&SearchOptions{Since: at(10)}), "at or after the time")
	assert.Equal(t, []int{9, 5, 1}, search(&SearchOptions{FilterAlertIDs: []int{1}}))
	assert.Equal(t, []int{12, 9, 8, 5, 4, 1}, search(&SearchOptions{FilterAlertIDs: []int{1, 4, 1}}), "of deleted alerts too")
	assert.Equal(t, []int{12, 9}, search(&SearchOptions{FilterAlertIDs: []int{1, 4}, Limit: 2}))
	assert.Equal(t, []int{8, 5}, search(&SearchOptions{FilterAlertIDs: []int{1, 4}, Limit: 2, After: SearchCursor{ID: 9}}))
	assert.Equal(t, []int{11, 9, 7, 5, 3, 1}, search(&SearchOptions{FilterServiceID: &svc1}))
	assert.Equal(t, []int{11, 9}, search(&SearchOptions{FilterServiceID: &svc1, Since: at(8)}))
	assert.Equal(t, []int{11, 7, 3}, search(&SearchOptions{FilterServiceID: &svc1, FilterAlertIDs: []int{3, 2}}))
	assert.Empty(t, search(&SearchOptions{FilterServiceID: &svc1, FilterAlertIDs: []int{2}}))

	entries, err := s.Search(sys, &SearchOptions{FilterAlertIDs: []int{99}})
	require.NoError(t, err)
	assert.Nil(t, entries, "nil without rows, as on Postgres")
	_, err = s.Search(sys, &SearchOptions{Limit: 151})
	require.Error(t, err)
	_, err = s.Search(context.Background(), nil)
	assert.True(t, permission.IsPermissionError(err))

	// More than the limit keeps in memory at once.
	require.NoError(t, s.LogManyTx(sys, nil, slices.Repeat([]int{1}, 400), TypeNotificationSent, nil))
	got := search(&SearchOptions{FilterAlertIDs: []int{1}, Limit: 3})
	assert.Equal(t, []int{412, 411, 410}, got)
	latest, err := s.FindLatestByType(sys, 1, TypeNotificationSent)
	require.NoError(t, err)
	assert.Equal(t, 412, latest.ID())
}

func TestDeltaLogTables(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	userID := uuid.NewString()
	day := func(d int) *time.Time {
		at := time.Date(2026, 11, d, 8, 0, 0, 0, time.UTC)
		return &at
	}
	id := func(n int64) *int64 { return &n }
	s.write(t, func(sc deltatxn.Scope) error {
		for _, l := range []DeltaLog{
			{AlertID: id(1), Event: "closed", Timestamp: day(3)},
			{AlertID: id(2), Event: "closed", Timestamp: day(1)},
			{AlertID: id(3), Event: "created", Timestamp: day(1), SubUserID: &userID},
			{AlertID: id(4), Event: "notification_sent", SubUserID: &userID},
			{AlertID: id(5), Event: "no_notification_sent", SubUserID: &userID},
			{AlertID: id(6), Event: "notification_sent"},
			{AlertID: id(4), Event: "escalated", Timestamp: day(2)},
			{Event: "reopened", Timestamp: day(2)},
			{ID: 100, AlertID: id(4), Event: "closed", Timestamp: day(2)},
		} {
			if err := e.tables.Insert(sc.Tx, l); err != nil {
				return err
			}
		}
		return nil
	})
	b := e.pod(t, "b")
	require.NoError(t, deltatxn.Read(ctx, b.delta.db, nil, func(sc deltatxn.Scope) error {
		var closed, notified, of4 []int64
		e.tables.AscendClosed(sc.Reader, *day(2), time.Time{}, func(l DeltaLog) bool {
			closed = append(closed, *l.AlertID)
			return true
		})
		assert.Equal(t, []int64{4, 1}, closed)
		e.tables.AscendNotified(sc.Reader, userID, func(alertID int64) bool {
			notified = append(notified, alertID)
			return true
		})
		assert.Equal(t, []int64{4, 5}, notified)
		notified = nil
		e.tables.AscendNotified(sc.Reader, userID, func(alertID int64) bool {
			notified = append(notified, alertID)
			return false
		})
		assert.Equal(t, []int64{4}, notified)
		e.tables.AscendAlert(sc.Reader, 4, func(l DeltaLog) bool {
			of4 = append(of4, l.ID)
			return len(of4) < 2
		})
		assert.Equal(t, []int64{100, 7}, of4)
		return nil
	}))

	err := deltatxn.Write(ctx, s.delta.db, nil, func(sc deltatxn.Scope) error {
		return e.tables.Insert(sc.Tx, DeltaLog{ID: 100, Event: "closed"})
	})
	var uErr *filestore.UniqueError
	require.ErrorAs(t, err, &uErr)
	assert.Equal(t, "alert_logs_pkey", uErr.Index)

	// The engine deletes the entries of deleted alerts.
	s.write(t, func(sc deltatxn.Scope) error {
		_, err := e.tables.Logs.Delete(sc.Tx, filestore.IntKey(100))
		return err
	})
	assert.Len(t, e.all(t), 8)
}

func TestDeltaLogNotPorted(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	s.delta.DeltaLookups = DeltaLookups{}
	id := uuid.NewString()
	svcID := uuid.New()
	require.ErrorIs(t, s.LogEPTx(sys, nil, id, TypePolicyUpdated, nil), deltatxn.ErrNotPorted)
	require.ErrorIs(t, s.LogServiceTx(sys, nil, id, TypeClosed, nil), deltatxn.ErrNotPorted)
	_, err := s.Search(sys, &SearchOptions{FilterServiceID: &svcID})
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)
	for i, src := range []permission.SourceType{
		permission.SourceTypeNotificationChannel, permission.SourceTypeContactMethod, permission.SourceTypeNotificationCallback,
		permission.SourceTypeHeartbeat, permission.SourceTypeIntegrationKey,
	} {
		require.ErrorIs(t, s.LogTx(source(sys, src, id), nil, 1, TypeCreated, nil), deltatxn.ErrNotPorted, strconv.Itoa(i))
	}
	require.NoError(t, s.LogTx(source(permission.UserContext(sys, id, permission.RoleUser), permission.SourceTypeAuthProvider, ""), nil, 1, TypeCreated, nil))
	_, err = s.FindOne(sys, 1)
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)
}
