package alert

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// Stand-ins for other packages' tables.
type (
	svcRow struct {
		ID       string `parquet:"id"`
		Name     string `parquet:"name"`
		PolicyID string `parquet:"policy_id"`
		Maint    bool   `parquet:"maint"`
	}
	idRow struct {
		ID string `parquet:"id"`
	}
	// stateRow stands in for escalation_policy_state.
	stateRow struct {
		AlertID        int64      `parquet:"alert_id"`
		Force          bool       `parquet:"force"`
		MultiAck       bool       `parquet:"multi_ack"`
		LastEscalation *time.Time `parquet:"last_escalation,optional,timestamp(microsecond:utc)"`
	}
	// jobRow stands in for the engine's jobs.
	jobRow struct {
		ID      int64 `parquet:"id"`
		AlertID int64 `parquet:"alert_id"`
	}
)

type deltaEnv struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	logs     *alertlog.DeltaTables
	metrics  *alertmetrics.DeltaTables
	services *filestore.Table[svcRow]
	channels *filestore.Table[idRow]
	methods  *filestore.Table[idRow]
	states   *filestore.Table[stateRow]
	jobs     *filestore.Table[jobRow]
	clock    *filestore.FakeClock

	steps   map[string]bool // policies with steps
	limit   atomic.Int64
	pending []PendingMessage
	names   map[string]string
	dests   map[string]gadb.DestV1
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	s := filestore.NewSchema()
	e := &deltaEnv{
		store: objstore.NewMem(), schema: s, clock: clock,
		steps: map[string]bool{}, names: map[string]string{}, dests: map[string]gadb.DestV1{},
	}
	e.limit.Store(-1)
	e.services = filestore.NewTable(s, "services", func(r svcRow) string { return r.ID })
	e.channels = filestore.NewTable(s, "notification_channels", func(r idRow) string { return r.ID })
	e.methods = filestore.NewTable(s, "user_contact_methods", func(r idRow) string { return r.ID })
	e.states = filestore.NewTable(s, "escalation_policy_state", func(r stateRow) string { return filestore.IntKey(r.AlertID) })
	e.jobs = filestore.NewTable(s, "jobs", func(r jobRow) string { return filestore.IntKey(r.ID) })
	e.tables = DefineDeltaTables(s)
	e.logs = alertlog.DefineDeltaTables(s)
	e.metrics = alertmetrics.DefineDeltaTables(s)
	DeclareDeltaForeignKeys(e.tables, e.services, e.channels, e.methods)
	alertmetrics.DeclareDeltaForeignKeys(e.metrics, e.tables.Alerts)
	filestore.References(e.states, "escalation_policy_state_alert_id_fkey", e.tables.Alerts,
		func(r stateRow) (string, bool) { return filestore.IntKey(r.AlertID), true }, filestore.Cascade, nil)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, s, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

type pod struct {
	*Store
	env  *deltaEnv
	fdb  *filestore.DeltaDB
	db   txn.DB
	logs *alertlog.Store
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	p := &pod{env: e, fdb: fdb, db: deltatxn.DB{DB: fdb}}
	nameOf := func(_ filestore.Reader, id string) (string, bool) {
		n, ok := e.names[id]
		return n, ok
	}
	destOf := func(_ filestore.Reader, id string) (gadb.DestV1, error) {
		d, ok := e.dests[id]
		if !ok {
			return gadb.DestV1{}, sql.ErrNoRows
		}
		return d, nil
	}
	p.logs = alertlog.NewDeltaStore(p.db, e.logs, nfydest.NewRegistry(), alertlog.DeltaLookups{
		UserName: nameOf, IntegrationKeyName: nameOf, HeartbeatMonitorName: nameOf, ChannelName: nameOf,
		ChannelDest: destOf, ContactMethodDest: destOf, MessageDest: destOf,
		AlertIDs: e.tables.AlertIDs,
	})
	p.Store = NewDeltaStore(p.db, e.tables, p.logs, e.logs, e.metrics, DeltaLookups{
		Service: func(r filestore.Reader, id string) (DeltaService, bool) {
			s, ok := e.services.Get(r, id)
			return DeltaService{Name: s.Name, EscalationPolicyID: s.PolicyID, InMaintenance: s.Maint}, ok
		},
		ServiceNames: func(r filestore.Reader, fn func(id, name string) bool) {
			e.services.Ascend(r, func(s svcRow) bool { return fn(s.ID, s.Name) })
		},
		MaxUnacked:     func(filestore.Reader) int { return int(e.limit.Load()) },
		PolicyHasSteps: func(_ filestore.Reader, id string) bool { return e.steps[id] },
		InsertEPState: func(tx *filestore.Tx, alertID int64, serviceID string) error {
			s, ok := e.services.Get(tx, serviceID)
			if !ok || !e.steps[s.PolicyID] {
				return nil
			}
			return e.states.Put(tx, stateRow{AlertID: alertID})
		},
		ClearEPState: func(tx *filestore.Tx, alertID int64) error {
			_, err := e.states.Delete(tx, filestore.IntKey(alertID))
			return err
		},
		RequestEscalation: func(tx *filestore.Tx, alertID int64, asOf time.Time) (bool, error) {
			st, ok := e.states.Get(tx, filestore.IntKey(alertID))
			if !ok || (st.LastEscalation != nil && st.LastEscalation.After(asOf)) {
				return false, nil
			}
			if st.Force {
				return true, nil
			}
			st.Force = true
			if err := e.states.Put(tx, st); err != nil {
				return false, err
			}
			return true, p.DeltaSetStatus(tx, alertID, StatusTriggered) // trg_30
		},
		EPState: func(r filestore.Reader, alertID int64) (State, bool) {
			st, ok := e.states.Get(r, filestore.IntKey(alertID))
			res := State{StepNumber: 2, RepeatCount: 1}
			if st.LastEscalation != nil {
				res.LastEscalation = *st.LastEscalation
			}
			return res, ok
		},
		MultiAck: func(r filestore.Reader, alertID int64) bool {
			st, _ := e.states.Get(r, filestore.IntKey(alertID))
			return st.MultiAck
		},
		StatusChanged: func(tx *filestore.Tx, alertID int64) error {
			id, err := tx.NextID("jobs_id_seq")
			if err != nil {
				return err
			}
			return e.jobs.Put(tx, jobRow{ID: id, AlertID: alertID})
		},
		PendingMessages: func(_ filestore.Reader, _ int64, _ string, before time.Time) []PendingMessage {
			if !before.Equal(e.clock.Now().Add(-15 * time.Second)) {
				return nil
			}
			return e.pending
		},
		UserName: nameOf, ChannelName: nameOf, ChannelDest: destOf, ContactMethodDest: destOf,
	})
	return p
}

func (p *pod) write(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), p.db, nil, func(s deltatxn.Scope) error { return fn(s.Tx) }))
}

// read runs fn on a pod opened now, so it sees every commit (F29).
func (e *deltaEnv) read(t *testing.T, fn func(r filestore.Reader)) {
	t.Helper()
	fn(e.pod(t, "reader").fdb.View())
}

// addService adds a service, whose policy has steps, and returns its ID.
func (p *pod) addService(t *testing.T, name string) string {
	t.Helper()
	s := svcRow{ID: uuid.NewString(), Name: name, PolicyID: uuid.NewString()}
	p.env.steps[s.PolicyID] = true
	p.write(t, func(tx *filestore.Tx) error { return p.env.services.Put(tx, s) })
	return s.ID
}

func (p *pod) insert(t *testing.T, a DeltaAlert) int {
	t.Helper()
	p.write(t, func(tx *filestore.Tx) (err error) {
		a, err = p.DeltaInsert(tx, a)
		return err
	})
	return int(a.ID)
}

// setState changes an alert's escalation state.
func (p *pod) setState(t *testing.T, alertID int, fn func(st *stateRow)) {
	t.Helper()
	p.write(t, func(tx *filestore.Tx) error {
		st, ok := p.env.states.Get(tx, filestore.IntKey(int64(alertID)))
		require.True(t, ok)
		fn(&st)
		return p.env.states.Put(tx, st)
	})
}

// row returns an alert and whether it has escalation state, as a pod opened now
// sees them.
func (e *deltaEnv) row(t *testing.T, id int) (a DeltaAlert, hasState bool) {
	t.Helper()
	e.read(t, func(r filestore.Reader) {
		var ok bool
		a, ok = e.tables.Alerts.Get(r, filestore.IntKey(int64(id)))
		require.True(t, ok)
		_, hasState = e.states.Get(r, filestore.IntKey(int64(id)))
	})
	return a, hasState
}

// events returns the types of an alert's log entries, oldest first.
func (e *deltaEnv) events(t *testing.T, alertID int) []string {
	t.Helper()
	var rows []alertlog.DeltaLog
	e.read(t, func(r filestore.Reader) {
		e.logs.AscendAlert(r, int64(alertID), func(l alertlog.DeltaLog) bool {
			rows = append(rows, l)
			return true
		})
	})
	slices.SortFunc(rows, func(a, b alertlog.DeltaLog) int { return cmp.Compare(a.ID, b.ID) })
	var events []string
	for _, l := range rows {
		events = append(events, l.Event)
	}
	return events
}

// jobsFor returns how many jobs there are for an alert.
func (e *deltaEnv) jobsFor(t *testing.T, alertID int) int {
	t.Helper()
	n := 0
	e.read(t, func(r filestore.Reader) {
		e.jobs.Ascend(r, func(j jobRow) bool {
			if j.AlertID == int64(alertID) {
				n++
			}
			return true
		})
	})
	return n
}

func (e *deltaEnv) count(t *testing.T) (n int) {
	t.Helper()
	e.read(t, func(r filestore.Reader) { n = e.tables.Alerts.Len(r) })
	return n
}

var (
	sysCtx  = permission.SystemContext(context.Background(), "test")
	userCtx = permission.UserContext(context.Background(), "11111111-1111-4111-8111-111111111111", permission.RoleUser)
)

func constraint(t *testing.T, err error) string {
	t.Helper()
	var ce interface{ ConstraintName() string }
	require.ErrorAs(t, err, &ce)
	return ce.ConstraintName()
}

func text(s string) *string { return &s }

func TestDeltaCreateOrUpdate(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	in := &Alert{Summary: "Disk full", Details: "on db1", ServiceID: strings.ToUpper(svcID), Source: SourceGrafana, Dedup: NewUserDedup("disk")}
	before := p.fdb.View().Index()

	n, isNew, err := p.CreateOrUpdate(sysCtx, in)
	require.NoError(t, err)
	assert.True(t, isNew)
	want := &Alert{ID: 1, Status: StatusTriggered, Summary: "Disk full", Details: "on db1", Source: SourceGrafana, ServiceID: in.ServiceID, CreatedAt: start, Dedup: in.Dedup}
	assert.Equal(t, want, n)
	assert.Equal(t, &Alert{Summary: "Disk full", Details: "on db1", ServiceID: strings.ToUpper(svcID), Source: SourceGrafana, Dedup: NewUserDedup("disk")}, in, "the caller's alert is not changed")

	row, hasState := e.row(t, 1)
	assert.Equal(t, DeltaAlert{
		ID: 1, Summary: "Disk full", Details: "on db1", ServiceID: &svcID, Source: "grafana", Status: "triggered",
		CreatedAt: start, DedupKey: text("user:1:disk"), LastEscalation: &start,
	}, row)
	assert.True(t, hasState, "trg_10_insert_ep_state_on_alert_insert (F2)")
	assert.Equal(t, []string{"created"}, e.events(t, 1))
	assert.Equal(t, before+1, p.fdb.View().Index(), "the alert, its escalation state and its log entry are one commit (F2)")
	assert.Zero(t, e.jobsFor(t, 1))

	// The same event again, from another pod.
	b := e.pod(t, "b")
	e.clock.Advance(time.Minute)
	again := &Alert{Summary: "Disk full again", ServiceID: svcID, Source: SourceGeneric, Dedup: NewUserDedup("disk")}
	for range 2 {
		n, isNew, err = b.CreateOrUpdate(sysCtx, again)
		require.NoError(t, err)
		assert.False(t, isNew)
		assert.Equal(t, &Alert{ID: 1, Status: StatusTriggered, Summary: "Disk full", Details: "on db1", Source: SourceGrafana, ServiceID: svcID, CreatedAt: start, Dedup: again.Dedup}, n, "the existing alert")
	}
	assert.Equal(t, []string{"created", "duplicate_suppressed"}, e.events(t, 1), "one entry about duplicates in 5 seconds")

	// Acknowledge it.
	ack := &Alert{Summary: "x", ServiceID: svcID, Status: StatusActive, Dedup: NewUserDedup("disk")}
	for range 2 {
		n, isNew, err = b.CreateOrUpdate(sysCtx, ack)
		require.NoError(t, err)
		assert.False(t, isNew)
		assert.Equal(t, 1, n.ID)
		assert.Equal(t, StatusActive, n.Status)
		assert.Equal(t, "Disk full", n.Summary)
	}
	assert.Equal(t, []string{"created", "duplicate_suppressed", "acknowledged"}, e.events(t, 1), "acknowledged once")
	assert.Equal(t, 1, e.jobsFor(t, 1), "trg_track_alert_status_update, only for a change")
	_, hasState = e.row(t, 1)
	assert.True(t, hasState)

	n, _, err = b.CreateOrUpdate(sysCtx, again)
	require.NoError(t, err)
	assert.Equal(t, StatusActive, n.Status, "a duplicate of an acknowledged alert leaves it so")

	// Close it.
	cls := &Alert{Summary: "x", ServiceID: svcID, Status: StatusClosed, Dedup: NewUserDedup("disk")}
	n, isNew, err = b.CreateOrUpdate(sysCtx, cls)
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, 1, n.ID)
	assert.Equal(t, StatusClosed, n.Status)
	row, hasState = e.row(t, 1)
	assert.Equal(t, "closed", row.Status)
	assert.Nil(t, row.DedupKey, "trg_clear_dedup_on_close")
	assert.False(t, hasState, "trg_10_clear_ep_state_on_alert_close")
	assert.Equal(t, 2, e.jobsFor(t, 1))
	for _, a := range []*Alert{cls, ack} {
		n, isNew, err = b.CreateOrUpdate(sysCtx, a)
		require.NoError(t, err)
		assert.Nil(t, n, "already closed")
		assert.False(t, isNew)
	}
	assert.Equal(t, []string{"created", "duplicate_suppressed", "acknowledged", "closed"}, e.events(t, 1)[:4])

	// A new one after that, without a dedup key of its own.
	n, isNew, err = b.CreateOrUpdate(sysCtx, &Alert{Summary: "a  b\nc", ServiceID: svcID})
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, 2, n.ID)
	assert.Nil(t, n.Dedup)
	row, _ = e.row(t, 2)
	assert.Equal(t, "a b c", row.Summary)
	assert.Equal(t, "manual", row.Source)
	assert.True(t, strings.HasPrefix(*row.DedupKey, "auto:1:"))
	assert.Len(t, *row.DedupKey, 7+128)

	// Invalid, or not allowed.
	_, _, err = b.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: "x"})
	assert.True(t, validation.IsClientError(err))
	_, _, err = b.CreateOrUpdate(permission.ServiceContext(context.Background(), uuid.NewString()), in)
	assert.True(t, permission.IsPermissionError(err))
	_, _, err = b.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: uuid.NewString()})
	assert.Equal(t, "alerts_services_id_fkey", constraint(t, err))
	var fe validation.FieldError
	assert.ErrorAs(t, errutil.MapDBError(err), &fe, "the same message as on Postgres")
	assert.Equal(t, 2, e.count(t))

	// For a service, its own alerts.
	_, isNew, err = b.CreateOrUpdate(permission.ServiceContext(context.Background(), svcID), in)
	require.NoError(t, err)
	assert.True(t, isNew)
}

// TestDeltaCreateNoSteps: the log entry of an alert that nobody gets says so.
func TestDeltaCreateNoSteps(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	clear(e.steps)
	n, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID})
	require.NoError(t, err)
	_, hasState := e.row(t, n.ID)
	assert.False(t, hasState)
	entry, err := p.logs.FindLatestByType(sysCtx, n.ID, alertlog.TypeCreated)
	require.NoError(t, err)
	assert.Equal(t, &alertlog.CreatedMetaData{EPNoSteps: true}, entry.Meta(sysCtx))
}

func TestDeltaCreateTx(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	var n *Alert
	in := &Alert{Summary: "x", ServiceID: svcID, Status: StatusActive, Dedup: NewUserDedup("k")}
	require.NoError(t, p.db.Run(userCtx, nil, func(tx txn.Tx) (err error) {
		if n, err = p.CreateTx(userCtx, tx, in); err != nil {
			return err
		}
		return p.SetMetadataTx(userCtx, tx, n.ID, map[string]string{"a": "b"})
	}))
	assert.Equal(t, &Alert{ID: 1, Status: StatusActive, Summary: "x", Source: SourceManual, ServiceID: svcID, CreatedAt: start, Dedup: in.Dedup}, n)
	row, hasState := e.row(t, 1)
	assert.Equal(t, "active", row.Status)
	assert.True(t, hasState, "for any alert that is not closed")
	assert.Equal(t, []string{"created"}, e.events(t, 1))
	md, err := p.Metadata(userCtx, nil, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, md)

	_, err = p.CreateTx(userCtx, nil, in)
	assert.Equal(t, "idx_no_alert_duplicates", constraint(t, err))
	var fe validation.FieldError
	assert.ErrorAs(t, errutil.MapDBError(err), &fe, "the same message as on Postgres")
	_, err = p.CreateTx(userCtx, nil, &Alert{Summary: "x", ServiceID: svcID, Status: StatusClosed})
	assert.True(t, validation.IsClientError(err))
	_, err = p.CreateTx(userCtx, nil, &Alert{ServiceID: "x"})
	assert.True(t, validation.IsClientError(err))
	_, err = p.CreateTx(permission.ServiceContext(context.Background(), uuid.NewString()), nil, &Alert{Summary: "y", ServiceID: svcID})
	assert.True(t, permission.IsPermissionError(err))
	_, err = p.CreateTx(permission.ServiceContext(context.Background(), svcID), nil, &Alert{Summary: "y", ServiceID: svcID})
	require.NoError(t, err)
	assert.Equal(t, 2, e.count(t))

	// With the caller's transaction, nothing is left when that fails.
	err = p.db.Run(userCtx, nil, func(tx txn.Tx) error {
		_, err := p.CreateTx(userCtx, tx, &Alert{Summary: "z", ServiceID: svcID})
		require.NoError(t, err)
		return sql.ErrTxDone
	})
	require.ErrorIs(t, err, sql.ErrTxDone)
	assert.Equal(t, 2, e.count(t))
	assert.Empty(t, e.events(t, 3))
}

func TestDeltaAlertLimit(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID, other := p.addService(t, "Web"), p.addService(t, "Other")
	e.limit.Store(2)
	create := func(svcID, summary string) error {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: summary, ServiceID: svcID})
		return err
	}
	require.NoError(t, create(svcID, "1"))
	require.NoError(t, create(svcID, "2"))
	err := create(svcID, "3")
	assert.Equal(t, "unacked_alerts_per_service_limit", constraint(t, err))
	mapped := errutil.MapDBError(err)
	require.True(t, errutil.IsLimitError(mapped))
	var le errutil.LimitError
	require.ErrorAs(t, mapped, &le)
	assert.Equal(t, 2, le.Max())
	require.NoError(t, create(svcID, "1"), "a duplicate is not a new alert")
	require.NoError(t, create(other, "3"), "each service has its own limit")
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusActive))
	require.NoError(t, create(svcID, "3"), "only unacknowledged alerts count")
	e.limit.Store(1)
	_, err = p.CreateTx(userCtx, nil, &Alert{Summary: "4", ServiceID: svcID, Status: StatusActive})
	assert.Equal(t, "unacked_alerts_per_service_limit", constraint(t, err), "checked for any new alert, as on Postgres")
	e.limit.Store(-1)
	require.NoError(t, create(svcID, "4"))
	e.read(t, func(r filestore.Reader) {
		assert.Equal(t, 3, e.tables.UnackedCount(r, svcID))
		n, ok := e.tables.Status(r, 1)
		assert.True(t, ok)
		assert.Equal(t, StatusActive, n)
		_, ok = e.tables.Status(r, 99)
		assert.False(t, ok)
	})
}

func TestDeltaUpdateStatus(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID})
	require.NoError(t, err)

	src := permission.SourceContext(userCtx, &permission.SourceInfo{Type: permission.SourceTypeAuthProvider})
	e.names[permission.UserID(userCtx)] = "Alice"
	require.NoError(t, p.UpdateStatus(src, 1, StatusActive))
	row, hasState := e.row(t, 1)
	assert.Equal(t, "active", row.Status)
	assert.NotNil(t, row.DedupKey)
	assert.True(t, hasState)
	assert.Equal(t, 1, e.jobsFor(t, 1))

	err = p.UpdateStatus(userCtx, 1, StatusActive)
	assert.True(t, IsAlreadyAcknowledged(err))
	assert.False(t, IsAlreadyClosed(err))
	assert.Equal(t, 1, AlertID(err))
	var le LogEntryFetcher
	require.ErrorAs(t, err, &le)
	entry, err := le.LogEntry(sysCtx)
	require.NoError(t, err)
	assert.Equal(t, "Acknowledged by Alice (Web)", entry.String(sysCtx))

	// With multi-ack, everyone who acknowledges it is logged.
	p.setState(t, 1, func(st *stateRow) { st.MultiAck = true })
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusActive))
	assert.Equal(t, []string{"created", "acknowledged", "acknowledged"}, e.events(t, 1))
	assert.Equal(t, 1, e.jobsFor(t, 1), "the status didn't change")

	// Triggered again, its state stays as it is, so that it escalates (F2).
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusTriggered))
	row, hasState = e.row(t, 1)
	assert.Equal(t, "triggered", row.Status)
	assert.True(t, hasState)
	assert.Len(t, e.events(t, 1), 3, "which is not logged")
	assert.Equal(t, 2, e.jobsFor(t, 1))
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusTriggered))
	assert.Equal(t, 2, e.jobsFor(t, 1))

	require.NoError(t, p.UpdateStatus(permission.ServiceContext(context.Background(), svcID), 1, StatusClosed))
	row, hasState = e.row(t, 1)
	assert.Equal(t, "closed", row.Status)
	assert.Nil(t, row.DedupKey)
	assert.False(t, hasState)
	for _, st := range statuses {
		err = p.UpdateStatus(userCtx, 1, st)
		assert.True(t, IsAlreadyClosed(err), "trg_prevent_reopen never gets to be asked")
	}
	require.ErrorAs(t, err, &le)
	entry, err = le.LogEntry(sysCtx)
	require.NoError(t, err)
	assert.Equal(t, alertlog.TypeClosed, entry.Type())
	assert.Equal(t, []string{"created", "acknowledged", "acknowledged", "closed"}, e.events(t, 1))

	require.ErrorIs(t, p.UpdateStatus(userCtx, 99, StatusClosed), sql.ErrNoRows)
	require.ErrorIs(t, p.UpdateStatus(permission.ServiceContext(context.Background(), svcID), 99, StatusClosed), sql.ErrNoRows)
	require.ErrorIs(t, p.UpdateStatusTx(userCtx, nil, 99, StatusClosed), sql.ErrNoRows)
	assert.True(t, validation.IsClientError(p.UpdateStatus(userCtx, 1, "bogus")))
	n, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "y", ServiceID: svcID})
	require.NoError(t, err)
	assert.True(t, permission.IsPermissionError(p.UpdateStatus(permission.ServiceContext(context.Background(), uuid.NewString()), n.ID, StatusClosed)))
	assert.True(t, permission.IsPermissionError(p.UpdateStatus(context.Background(), n.ID, StatusClosed)))
	row, _ = e.row(t, n.ID)
	assert.Equal(t, "triggered", row.Status)

	// An alert that another pod just created, and this one hasn't seen.
	b := e.pod(t, "b")
	n, _, err = p.CreateOrUpdate(sysCtx, &Alert{Summary: "z", ServiceID: svcID})
	require.NoError(t, err)
	require.NoError(t, b.UpdateStatus(permission.ServiceContext(context.Background(), svcID), n.ID, StatusClosed))
}

func TestDeltaUpdateMany(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID, other := p.addService(t, "Web"), p.addService(t, "Other")
	for i := range 6 {
		svc := svcID
		if i == 5 {
			svc = other
		}
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svc})
		require.NoError(t, err)
	}
	require.NoError(t, p.UpdateStatus(userCtx, 2, StatusActive))
	require.NoError(t, p.UpdateStatus(userCtx, 3, StatusClosed))

	ids, err := p.UpdateManyAlertStatus(userCtx, StatusActive, []int{4, 3, 2, 1, 4, 99}, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 4}, ids, "only those that change")
	assert.Equal(t, []string{"created", "acknowledged"}, e.events(t, 1))
	assert.Equal(t, []string{"created", "acknowledged"}, e.events(t, 2))
	assert.Equal(t, []string{"created", "closed"}, e.events(t, 3))

	ids, err = p.UpdateManyAlertStatus(userCtx, StatusActive, []int{1, 3}, nil)
	require.NoError(t, err)
	assert.Nil(t, ids)

	ids, err = p.UpdateManyAlertStatus(sysCtx, StatusClosed, []int{1, 3}, alertlog.AutoClose{AlertAutoCloseDays: 3})
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ids)
	entry, err := p.logs.FindLatestByType(sysCtx, 1, alertlog.TypeClosed)
	require.NoError(t, err)
	assert.Equal(t, &alertlog.AutoClose{AlertAutoCloseDays: 3}, entry.Meta(sysCtx))
	row, hasState := e.row(t, 1)
	assert.Nil(t, row.DedupKey)
	assert.False(t, hasState)

	ids, err = p.UpdateManyAlertStatus(userCtx, StatusClosed, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, ids)
	_, err = p.UpdateManyAlertStatus(userCtx, StatusTriggered, []int{1}, nil)
	assert.True(t, validation.IsClientError(err))
	_, err = p.UpdateManyAlertStatus(userCtx, StatusClosed, make([]int, 501), nil)
	assert.True(t, validation.IsClientError(err))

	// On a multi-ack step, everyone who acknowledges it is logged (F4).
	p.setState(t, 2, func(st *stateRow) { st.MultiAck = true })
	p.setState(t, 4, func(st *stateRow) { st.MultiAck = true })
	jobs := e.jobsFor(t, 2)
	ids, err = p.UpdateManyAlertStatus(userCtx, StatusActive, []int{2, 1, 3}, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{2}, ids)
	assert.Equal(t, []string{"created", "acknowledged", "acknowledged"}, e.events(t, 2))
	assert.Equal(t, jobs, e.jobsFor(t, 2))
	ids, err = p.UpdateManyAlertStatus(userCtx, StatusClosed, []int{3}, nil)
	require.NoError(t, err)
	assert.Nil(t, ids)

	// By service.
	_, _, err = p.CreateOrUpdate(sysCtx, &Alert{Summary: "new", ServiceID: svcID}) // 7
	require.NoError(t, err)
	before := p.fdb.View().Index()
	require.NoError(t, p.UpdateStatusByService(userCtx, strings.ToUpper(svcID), StatusActive))
	assert.Equal(t, before+1, p.fdb.View().Index(), "one commit")
	for id, want := range map[int]string{1: "closed", 2: "active", 3: "closed", 4: "active", 5: "active", 6: "triggered", 7: "active"} {
		row, _ := e.row(t, id)
		assert.Equal(t, want, row.Status, id)
	}
	assert.Equal(t, []string{"created", "acknowledged", "acknowledged"}, e.events(t, 4), "a multi-ack step")
	p.setState(t, 2, func(st *stateRow) { st.MultiAck = false })
	require.NoError(t, p.UpdateStatusByService(userCtx, svcID, StatusActive))
	assert.Len(t, e.events(t, 2), 4, "acknowledged before")
	assert.Len(t, e.events(t, 4), 4)
	assert.Equal(t, []string{"created", "acknowledged"}, e.events(t, 7))
	require.NoError(t, p.UpdateStatusByService(userCtx, svcID, StatusClosed))
	assert.Equal(t, []string{"created", "acknowledged", "acknowledged", "acknowledged", "closed"}, e.events(t, 2))
	assert.Equal(t, []string{"created", "closed"}, e.events(t, 3))
	before = p.fdb.View().Index()
	require.NoError(t, p.UpdateStatusByService(userCtx, svcID, StatusClosed))
	require.NoError(t, p.UpdateStatusByService(userCtx, uuid.NewString(), StatusClosed))
	assert.Equal(t, before, p.fdb.View().Index(), "nothing to change")
	assert.True(t, validation.IsClientError(p.UpdateStatusByService(userCtx, svcID, StatusTriggered)))
	assert.True(t, validation.IsClientError(p.UpdateStatusByService(userCtx, "x", StatusClosed)))
	assert.True(t, permission.IsPermissionError(p.UpdateStatusByService(permission.ServiceContext(context.Background(), svcID), svcID, StatusClosed)))
}

func TestDeltaEscalate(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	for i := range 3 {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svcID})
		require.NoError(t, err)
	}
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusActive))

	require.NoError(t, p.Escalate(userCtx, 1, 0))
	row, _ := e.row(t, 1)
	assert.Equal(t, "triggered", row.Status, "trg_30_trig_alert_on_force_escalation")
	assert.Equal(t, 2, e.jobsFor(t, 1))
	assert.Equal(t, []string{"created", "acknowledged", "escalation_request"}, e.events(t, 1))
	st, err := p.State(userCtx, []int{1})
	require.NoError(t, err)
	assert.Equal(t, []State{{ID: 1, StepNumber: 2, RepeatCount: 1}}, st)

	at := start.Add(time.Minute)
	p.setState(t, 1, func(st *stateRow) { st.Force, st.LastEscalation = false, &at })
	err = p.EscalateAsOf(userCtx, 1, start)
	require.ErrorContains(t, err, "already escalated", "a reply to an older message")
	assert.True(t, validation.IsClientError(err))
	e.clock.Set(at)
	require.NoError(t, p.Escalate(userCtx, 1, 0), "a zero time is now")
	p.setState(t, 1, func(st *stateRow) { st.Force = false })
	require.NoError(t, p.EscalateAsOf(userCtx, 1, at))

	ids, err := p.EscalateMany(userCtx, []int{2})
	require.NoError(t, err)
	assert.Equal(t, []int{2}, ids)
	_, err = p.EscalateMany(userCtx, []int{2, 3})
	assert.True(t, validation.IsClientError(err), "one at a time")

	require.ErrorContains(t, p.Escalate(userCtx, 99, 0), "not found")
	require.NoError(t, p.UpdateStatus(userCtx, 2, StatusClosed))
	assert.True(t, IsAlreadyClosed(p.Escalate(userCtx, 2, 0)))
	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.states.Delete(tx, filestore.IntKey(3))
		return err
	})
	require.ErrorContains(t, p.Escalate(userCtx, 3, 0), "policy is empty")
	p.write(t, func(tx *filestore.Tx) error {
		s, _ := e.services.Get(tx, svcID)
		s.Maint = true
		return e.services.Put(tx, s)
	})
	require.ErrorContains(t, p.Escalate(userCtx, 1, 0), "maintenance")
	assert.True(t, permission.IsPermissionError(p.Escalate(permission.ServiceContext(context.Background(), svcID), 1, 0)))
	assert.Equal(t, []string{"created"}, e.events(t, 3), "a refused request leaves nothing")

	st, err = p.State(userCtx, []int{3, 1, 2, 1})
	require.NoError(t, err)
	assert.Equal(t, []State{{ID: 1, StepNumber: 2, RepeatCount: 1, LastEscalation: at}}, st)
	st, err = p.State(userCtx, []int{99})
	require.NoError(t, err)
	assert.Equal(t, []State{}, st)
}

func TestDeltaFind(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	for i := range 3 {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svcID, Dedup: NewUserDedup("a:b:" + strconv.Itoa(i))})
		require.NoError(t, err)
	}
	require.NoError(t, p.UpdateStatus(userCtx, 2, StatusClosed))
	b := e.pod(t, "b")
	for _, s := range []*pod{p, b} {
		a, err := s.FindOne(userCtx, 1)
		require.NoError(t, err)
		assert.Equal(t, &Alert{ID: 1, Status: StatusTriggered, Summary: "0", Source: SourceManual, ServiceID: svcID, CreatedAt: start, Dedup: &DedupID{Type: DedupTypeUser, Version: 1, Payload: "a:b:0"}}, a)
		many, err := s.FindMany(userCtx, []int{3, 2, 99, 2})
		require.NoError(t, err)
		require.Len(t, many, 2)
		assert.Equal(t, 2, many[0].ID)
		assert.Nil(t, many[0].Dedup)
		assert.Equal(t, 3, many[1].ID)

		epID, err := s.EPID(sysCtx, 1)
		require.NoError(t, err)
		assert.Contains(t, e.steps, epID)
		name, n, err := s.ServiceInfo(userCtx, strings.ToUpper(svcID))
		require.NoError(t, err)
		assert.Equal(t, "Web", name)
		assert.Equal(t, 2, n)
	}
	_, err := p.FindOne(userCtx, 99)
	require.ErrorIs(t, err, sql.ErrNoRows)
	many, err := p.FindMany(userCtx, []int{99})
	require.NoError(t, err)
	assert.Equal(t, []Alert{}, many)
	many, err = p.FindMany(userCtx, nil)
	require.NoError(t, err)
	assert.Nil(t, many)
	_, err = p.EPID(sysCtx, 99)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.EPID(userCtx, 1)
	assert.True(t, permission.IsPermissionError(err))
	_, _, err = p.ServiceInfo(userCtx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, _, err = p.ServiceInfo(userCtx, "x")
	assert.True(t, validation.IsClientError(err))
}

func TestDeltaMetadata(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	svcCtx := permission.ServiceContext(context.Background(), svcID)
	in := &Alert{Summary: "x", ServiceID: svcID}

	before := p.fdb.View().Index()
	n, isNew, err := p.CreateOrUpdateWithMeta(svcCtx, in, map[string]string{"k": "v", "ü": "ä"})
	require.ErrorContains(t, err, "Meta", "only ASCII keys")
	assert.Nil(t, n)
	assert.False(t, isNew)
	assert.Zero(t, e.count(t), "the alert goes with its metadata")
	_, isNew, err = p.CreateOrUpdateWithMeta(svcCtx, in, map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, before+1, p.fdb.View().Index(), "one commit")
	_, isNew, err = p.CreateOrUpdateWithMeta(svcCtx, in, map[string]string{"k": "new"})
	require.NoError(t, err)
	assert.False(t, isNew)

	b := e.pod(t, "b")
	md, err := b.Metadata(userCtx, nil, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, md, "a duplicate doesn't change it")
	require.NoError(t, b.SetMetadataTx(userCtx, nil, 1, map[string]string{"k": "w"}))
	require.NoError(t, b.SetMetadataTx(svcCtx, nil, 1, map[string]string{"k": "x"}))
	require.NoError(t, b.db.Run(svcCtx, nil, func(tx txn.Tx) error {
		require.NoError(t, b.SetMetadataTx(svcCtx, tx, 1, map[string]string{}))
		md, err := b.Metadata(userCtx, tx, 1)
		require.NoError(t, err)
		assert.Empty(t, md, "a transaction sees its own writes")
		return b.SetMetadataTx(svcCtx, tx, 1, map[string]string{"k": "y"})
	}))

	_, _, err = b.CreateOrUpdate(sysCtx, &Alert{Summary: "y", ServiceID: svcID})
	require.NoError(t, err)
	md, err = b.Metadata(userCtx, nil, 2)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, md, "never nil")
	require.NoError(t, b.SetMetadataTx(userCtx, nil, 2, map[string]string{"a": "b"}))
	many, err := b.FindManyMetadata(userCtx, nil, []int{2, 1, 99, 2})
	require.NoError(t, err)
	assert.Equal(t, []MetadataAlertID{{ID: 1, Meta: map[string]string{"k": "y"}}, {ID: 2, Meta: map[string]string{"a": "b"}}}, many)
	many, err = b.FindManyMetadata(userCtx, nil, []int{99})
	require.NoError(t, err)
	assert.Equal(t, []MetadataAlertID{}, many)
	_, err = b.FindManyMetadata(userCtx, nil, nil)
	assert.True(t, validation.IsClientError(err))

	// Not another service's alert, a closed one, or one that doesn't exist.
	err = b.SetMetadataTx(permission.ServiceContext(context.Background(), uuid.NewString()), nil, 1, map[string]string{})
	assert.True(t, permission.IsPermissionError(err))
	require.NoError(t, b.UpdateStatus(userCtx, 1, StatusClosed))
	assert.True(t, permission.IsPermissionError(b.SetMetadataTx(userCtx, nil, 1, map[string]string{})))
	assert.True(t, permission.IsPermissionError(b.SetMetadataTx(userCtx, nil, 99, map[string]string{})))
	assert.True(t, permission.IsPermissionError(b.SetMetadataTx(context.Background(), nil, 2, map[string]string{})))

	// Metadata of another kind, or none.
	b.write(t, func(tx *filestore.Tx) error {
		if err := e.tables.data.Put(tx, dataRow{ID: 1, AlertID: 1, Metadata: []byte(`{"Type":"other"}`)}); err != nil {
			return err
		}
		return e.tables.data.Put(tx, dataRow{ID: 2, AlertID: 2})
	})
	_, err = b.Metadata(userCtx, nil, 1)
	require.ErrorContains(t, err, "unsupported metadata type")
	md, err = b.Metadata(userCtx, nil, 2)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, md)
	_, err = b.FindManyMetadata(userCtx, nil, []int{2})
	require.Error(t, err)

	err = deltatxn.Write(sysCtx, b.db, nil, func(s deltatxn.Scope) error { return e.tables.data.Put(s.Tx, dataRow{ID: 1, AlertID: 2}) })
	assert.Equal(t, "alert_data_id_key", constraint(t, err))
	err = deltatxn.Write(sysCtx, b.db, nil, func(s deltatxn.Scope) error { return e.tables.data.Put(s.Tx, dataRow{ID: 9, AlertID: 99}) })
	assert.Equal(t, "alert_data_alert_id_fkey", constraint(t, err))
}

func TestDeltaFeedback(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	for i := range 3 {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svcID})
		require.NoError(t, err)
	}
	require.NoError(t, p.UpdateFeedback(userCtx, &Feedback{ID: 2, NoiseReason: "noisy"}))
	ids, err := p.UpdateManyAlertFeedback(userCtx, "duplicate", []int{3, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, []int{3, 2}, ids, "each once, in the order given")
	require.NoError(t, p.UpdateFeedback(userCtx, &Feedback{ID: 3, NoiseReason: "false positive"}))

	b := e.pod(t, "b")
	fb, err := b.Feedback(userCtx, []int{3, 1, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, []Feedback{{ID: 2, NoiseReason: "duplicate"}, {ID: 3, NoiseReason: "false positive"}}, fb)
	fb, err = b.Feedback(userCtx, []int{1})
	require.NoError(t, err)
	assert.Nil(t, fb)

	_, err = b.UpdateManyAlertFeedback(userCtx, "x", []int{1, 99})
	assert.Equal(t, "alert_feedback_alert_id_fkey", constraint(t, err))
	assert.Equal(t, "alert_feedback_alert_id_fkey", constraint(t, b.UpdateFeedback(userCtx, &Feedback{ID: 99, NoiseReason: "x"})))
	fb, err = b.Feedback(userCtx, []int{1})
	require.NoError(t, err)
	assert.Empty(t, fb, "all or nothing")
	_, err = b.UpdateManyAlertFeedback(permission.ServiceContext(context.Background(), svcID), "x", []int{1})
	assert.True(t, permission.IsPermissionError(err))
	_, err = b.UpdateManyAlertFeedback(userCtx, "x", nil)
	assert.True(t, validation.IsClientError(err))
	err = deltatxn.Write(sysCtx, b.db, nil, func(s deltatxn.Scope) error {
		return e.tables.feedback.Put(s.Tx, feedbackRow{ID: 1, AlertID: 1})
	})
	assert.Equal(t, "alert_feedback_id_key", constraint(t, err))
}

func TestDeltaAlertConstraints(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	try := func(a DeltaAlert) error {
		return deltatxn.Write(sysCtx, p.db, nil, func(s deltatxn.Scope) error {
			_, err := p.DeltaInsert(s.Tx, a)
			return err
		})
	}
	assert.Equal(t, "dedup_key_only_for_open_alerts", constraint(t, try(DeltaAlert{ServiceID: &svcID})))
	assert.Equal(t, "dedup_key_only_for_open_alerts", constraint(t, try(DeltaAlert{ServiceID: &svcID, Status: "closed", DedupKey: text("auto:1:a")})))
	require.ErrorContains(t, try(DeltaAlert{ServiceID: &svcID, Status: "bogus"}), "bogus")
	require.NoError(t, try(DeltaAlert{ID: 5, ServiceID: &svcID, DedupKey: text("auto:1:a"), CreatedAt: start.Add(-time.Hour)}))
	assert.Equal(t, "alerts_pkey", constraint(t, try(DeltaAlert{ID: 5, ServiceID: &svcID, DedupKey: text("auto:1:b")})))
	assert.Equal(t, "idx_no_alert_duplicates", constraint(t, try(DeltaAlert{ID: 6, ServiceID: &svcID, DedupKey: text("auto:1:a")})))
	require.NoError(t, try(DeltaAlert{ID: 7, ServiceID: &svcID, Status: "closed"}))
	require.NoError(t, try(DeltaAlert{ID: 8, ServiceID: &svcID, Status: "closed"}), "closed alerts have no dedup key")
	require.NoError(t, try(DeltaAlert{ID: 9, DedupKey: text("auto:1:a")}), "service_id may be NULL")
	row, hasState := e.row(t, 5)
	assert.True(t, hasState)
	assert.Equal(t, start.Add(-time.Hour), row.CreatedAt)
	_, hasState = e.row(t, 7)
	assert.False(t, hasState, "not for a closed alert")

	set := func(id int64, st Status) error {
		return deltatxn.Write(sysCtx, p.db, nil, func(s deltatxn.Scope) error { return p.DeltaSetStatus(s.Tx, id, st) })
	}
	require.NoError(t, set(5, StatusActive))
	require.NoError(t, set(5, StatusTriggered))
	require.NoError(t, set(5, StatusClosed))
	for _, st := range statuses {
		err := set(5, st)
		assert.Equal(t, "trg_prevent_reopen", constraint(t, err))
		assert.ErrorContains(t, err, "cannot change status of closed alert")
	}
	require.NoError(t, set(99, StatusClosed), "an UPDATE of no rows")
	assert.Equal(t, 3, e.jobsFor(t, 5))

	// A sequence starts after nothing here, so the next alert is 1.
	n, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID})
	require.NoError(t, err)
	assert.Equal(t, 1, n.ID)
	many, err := p.FindMany(userCtx, []int{9})
	require.NoError(t, err)
	assert.Empty(t, many[0].ServiceID)
	b := e.pod(t, "b")
	b.write(t, func(tx *filestore.Tx) error {
		a, _ := e.tables.Alerts.Get(tx, filestore.IntKey(1))
		a.DedupKey = text("bogus")
		return e.tables.Alerts.Put(tx, a)
	})
	_, err = b.FindMany(userCtx, []int{1})
	require.Error(t, err, "a dedup key that can't be parsed, as on Postgres")
}

// TestDeltaServiceCascade: deleting a service deletes its alerts and what
// belongs to them (F7), but not their log entries, as on Postgres.
func TestDeltaServiceCascade(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID, other := p.addService(t, "Web"), p.addService(t, "Other")
	chID := uuid.NewString()
	for i, svc := range []string{svcID, other} {
		_, _, err := p.CreateOrUpdateWithMeta(permission.ServiceContext(context.Background(), svc), &Alert{Summary: "x", ServiceID: svc}, map[string]string{"a": "b"})
		require.NoError(t, err)
		require.NoError(t, p.UpdateFeedback(userCtx, &Feedback{ID: i + 1, NoiseReason: "x"}))
	}
	p.write(t, func(tx *filestore.Tx) error {
		require.NoError(t, e.channels.Put(tx, idRow{ID: chID}))
		for id := range int64(2) {
			us := int64(1)
			require.NoError(t, e.tables.Subscribe(tx, DeltaSubscription{AlertID: id + 1, ChannelID: &chID, LastAlertStatus: "triggered"}))
			require.NoError(t, e.metrics.Insert(deltatxn.Scope{Tx: tx, Reader: tx, Now: tx.Now()}, alertmetrics.DeltaMetric{AlertID: id + 1, ServiceID: svcID, TimeToClose: &us, ClosedAt: start}))
		}
		return nil
	})
	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, svcID)
		return err
	})
	e.read(t, func(r filestore.Reader) {
		for name, n := range map[string]int{
			"alerts": e.tables.Alerts.Len(r), "alert_data": e.tables.data.Len(r), "alert_feedback": e.tables.feedback.Len(r),
			"alert_status_subscriptions": e.tables.Subscriptions.Len(r), "alert_metrics": e.metrics.Metrics.Len(r), "state": e.states.Len(r),
		} {
			assert.Equal(t, 1, n, name)
		}
		_, ok := e.tables.Alerts.Get(r, filestore.IntKey(2))
		assert.True(t, ok)
		assert.Equal(t, 2, e.logs.Logs.Len(r))
	})
}

func TestDeltaSubscriptions(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID, ch, cm := p.addService(t, "Web"), uuid.NewString(), uuid.NewString()
	for i := range 2 {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svcID})
		require.NoError(t, err)
	}
	p.write(t, func(tx *filestore.Tx) error {
		require.NoError(t, e.channels.Put(tx, idRow{ID: ch}))
		return e.methods.Put(tx, idRow{ID: cm})
	})
	sub := func(s DeltaSubscription) error {
		s.LastAlertStatus = "triggered"
		return deltatxn.Write(sysCtx, p.db, nil, func(sc deltatxn.Scope) error { return e.tables.Subscribe(sc.Tx, s) })
	}
	require.NoError(t, sub(DeltaSubscription{AlertID: 1, ChannelID: &ch}))
	require.NoError(t, sub(DeltaSubscription{AlertID: 1, ContactMethodID: &cm}))
	require.NoError(t, sub(DeltaSubscription{AlertID: 2, ChannelID: &ch}))
	e.clock.Advance(48 * time.Hour)
	require.NoError(t, sub(DeltaSubscription{AlertID: 1, ChannelID: &ch}), "a destination that has one")
	require.NoError(t, sub(DeltaSubscription{AlertID: 2, ContactMethodID: &cm}))

	assert.Equal(t, "alert_status_subscriptions_check", constraint(t, sub(DeltaSubscription{AlertID: 1})))
	assert.Equal(t, "alert_status_subscriptions_check", constraint(t, sub(DeltaSubscription{AlertID: 1, ChannelID: &ch, ContactMethodID: &cm})))
	assert.Equal(t, "alert_status_subscriptions_alert_id_fkey", constraint(t, sub(DeltaSubscription{AlertID: 99, ChannelID: &ch})))
	assert.Equal(t, "alert_status_subscriptions_channel_id_fkey", constraint(t, sub(DeltaSubscription{AlertID: 1, ChannelID: text(uuid.NewString())})))
	assert.Equal(t, "alert_status_subscriptions_contact_method_id_fkey", constraint(t, sub(DeltaSubscription{AlertID: 1, ContactMethodID: text(uuid.NewString())})))
	err := deltatxn.Write(sysCtx, p.db, nil, func(sc deltatxn.Scope) error {
		return e.tables.Subscriptions.Put(sc.Tx, DeltaSubscription{ID: 9, AlertID: 1, ChannelID: &ch, LastAlertStatus: "active", UpdatedAt: start})
	})
	assert.Equal(t, "alert_status_subscriptions_channel_id_contact_method_id_ale_key", constraint(t, err), "two pods can't both add one (F3)")

	ids := func(r filestore.Reader, from, to time.Time) (ids []int64) {
		e.tables.AscendSubscriptionsUpdated(r, from, to, func(s DeltaSubscription) bool {
			ids = append(ids, s.ID)
			return true
		})
		return ids
	}
	e.read(t, func(r filestore.Reader) {
		assert.Equal(t, []int64{1, 2, 3, 4}, ids(r, time.Time{}, time.Time{}))
		assert.Equal(t, []int64{4}, ids(r, start.Add(24*time.Hour), time.Time{}))
		assert.Equal(t, []int64{1, 2, 3}, ids(r, time.Time{}, start))
		var of1 []DeltaSubscription
		e.tables.SubscriptionsByAlert.Ascend(r, filestore.IntKey(1), func(s DeltaSubscription) bool {
			of1 = append(of1, s)
			return true
		})
		assert.Equal(t, []DeltaSubscription{
			{ID: 1, AlertID: 1, ChannelID: &ch, LastAlertStatus: "triggered", UpdatedAt: start},
			{ID: 2, AlertID: 1, ContactMethodID: &cm, LastAlertStatus: "triggered", UpdatedAt: start},
		}, of1)
	})

	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.channels.Delete(tx, ch)
		return err
	})
	e.read(t, func(r filestore.Reader) { assert.Equal(t, []int64{2, 4}, ids(r, time.Time{}, time.Time{})) })
	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.methods.Delete(tx, cm)
		return err
	})
	e.read(t, func(r filestore.Reader) { assert.Empty(t, ids(r, time.Time{}, time.Time{})) })
}

// TestDeltaCreateRace: pods that get the same event at once create one alert
// (F4, F8).
func TestDeltaCreateRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	svcID := pods[0].addService(t, "Web")
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, isNew, err := pods[i%2].CreateOrUpdateWithMeta(permission.ServiceContext(context.Background(), svcID),
				&Alert{Summary: "Disk full", ServiceID: svcID, Details: strconv.Itoa(i), Dedup: NewUserDedup("disk")}, map[string]string{"n": strconv.Itoa(i)})
			if !assert.NoError(t, err) || !assert.NotNil(t, n) {
				return
			}
			assert.Equal(t, 1, n.ID)
			if isNew {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "exactly one caller created it")
	assert.Equal(t, 1, e.count(t))
	assert.Equal(t, []string{"created", "duplicate_suppressed"}, e.events(t, 1))
	row, hasState := e.row(t, 1)
	assert.True(t, hasState)
	md, err := e.pod(t, "c").Metadata(userCtx, nil, 1)
	require.NoError(t, err)
	assert.Equal(t, row.Details, md["n"], "the metadata is the creator's")
}

// TestDeltaCreateTxRace: of pods that insert the same alert at once, one
// succeeds, and the others get the unique index's error.
func TestDeltaCreateTxRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	svcID := pods[0].addService(t, "Web")
	var created, refused atomic.Int32
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].CreateTx(userCtx, nil, &Alert{Summary: "x", ServiceID: svcID})
			if err == nil {
				created.Add(1)
				return
			}
			if assert.Equal(t, "idx_no_alert_duplicates", constraint(t, err)) {
				refused.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load())
	assert.EqualValues(t, 5, refused.Load())
	assert.Equal(t, 1, e.count(t))
}

// TestDeltaLimitRace: pods racing can't go over the limit (F8).
func TestDeltaLimitRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	svcID := pods[0].addService(t, "Web")
	e.limit.Store(4)
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := pods[i%2].CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svcID})
			if err == nil {
				created.Add(1)
				return
			}
			assert.Equal(t, "unacked_alerts_per_service_limit", constraint(t, err))
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 4, created.Load())
	assert.Equal(t, 4, e.count(t))
}

// TestDeltaAckCloseRace: an alert that is acknowledged and closed at once ends
// up closed, with every change logged once (F4).
func TestDeltaAckCloseRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	svcID := pods[0].addService(t, "Web")
	for round := range 5 {
		id, dedup := round+1, NewUserDedup(strconv.Itoa(round))
		_, _, err := pods[0].CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID, Dedup: dedup})
		require.NoError(t, err)
		var acked, closed atomic.Int32
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p := pods[i%2]
				switch i / 2 {
				case 0:
					err := p.UpdateStatus(userCtx, id, StatusActive)
					if err == nil {
						acked.Add(1)
					} else {
						assert.True(t, IsAlreadyAcknowledged(err) || IsAlreadyClosed(err), err)
					}
				case 1:
					err := p.UpdateStatus(userCtx, id, StatusClosed)
					if err == nil {
						closed.Add(1)
					} else {
						assert.True(t, IsAlreadyClosed(err), err)
					}
				case 2:
					ids, err := p.UpdateManyAlertStatus(userCtx, StatusClosed, []int{id}, nil)
					assert.NoError(t, err)
					closed.Add(int32(len(ids)))
				case 3:
					n, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID, Status: StatusClosed, Dedup: dedup})
					assert.NoError(t, err)
					if n != nil {
						closed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		row, hasState := e.row(t, id)
		assert.Equal(t, "closed", row.Status)
		assert.Nil(t, row.DedupKey)
		assert.False(t, hasState)
		assert.EqualValues(t, 1, closed.Load(), "exactly one caller closed it")
		var acks, closes int
		for _, ev := range e.events(t, id) {
			switch ev {
			case "acknowledged":
				acks++
			case "closed":
				closes++
			}
		}
		assert.Equal(t, 1, closes)
		assert.EqualValues(t, acked.Load(), acks)
		assert.LessOrEqual(t, acks, 1)
	}
}

// TestDeltaCreateCloseRace: an event that closes an alert while another
// creates it again leaves either one open alert or none, never a lost close.
func TestDeltaCreateCloseRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	svcID := pods[0].addService(t, "Web")
	var created, closed atomic.Int32
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				st := StatusTriggered
				if i == 1 {
					st = StatusClosed
				}
				n, isNew, err := pods[i].CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID, Status: st})
				assert.NoError(t, err)
				if isNew {
					created.Add(1)
				}
				if i == 1 && n != nil {
					closed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	e.read(t, func(r filestore.Reader) {
		open := len(e.tables.AlertIDs(r, svcID, "triggered", "active"))
		assert.LessOrEqual(t, open, 1)
		assert.EqualValues(t, created.Load()-closed.Load(), open)
		assert.EqualValues(t, created.Load(), e.tables.Alerts.Len(r))
		assert.Equal(t, open, e.states.Len(r))
	})
}

func TestDeltaServiceStats(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svc1, svc2, svc3 := p.addService(t, "1"), p.addService(t, "2"), p.addService(t, "3")
	id1, id2 := uuid.MustParse(svc1), uuid.MustParse(svc2)
	for i := range 5 {
		_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: strconv.Itoa(i), ServiceID: svc1})
		require.NoError(t, err)
	}
	require.NoError(t, p.UpdateStatus(userCtx, 1, StatusClosed))
	require.NoError(t, p.UpdateStatus(userCtx, 2, StatusActive))
	_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svc2})
	require.NoError(t, err)

	nid := func(id uuid.UUID) uuid.NullUUID { return uuid.NullUUID{UUID: id, Valid: true} }
	counts, err := p.ServiceAlertCounts(userCtx, []uuid.UUID{id2, id1, uuid.New(), id1})
	require.NoError(t, err)
	assert.Equal(t, []gadb.ServiceAlertCountsRow{
		{Count: 1, Status: "triggered", ServiceID: nid(id2)},
		{Count: 3, Status: "triggered", ServiceID: nid(id1)},
		{Count: 1, Status: "active", ServiceID: nid(id1)},
		{Count: 1, Status: "closed", ServiceID: nid(id1)},
	}, counts)
	counts, err = p.ServiceAlertCounts(userCtx, []uuid.UUID{uuid.New()})
	require.NoError(t, err)
	assert.Nil(t, counts)

	// Statistics. The first 4 are the smoke test's.
	us := func(d time.Duration) *int64 {
		n := d.Microseconds()
		return &n
	}
	jan := func(d, h, m, s int) time.Time { return time.Date(2022, 1, d, h, m, s, 0, time.UTC) }
	p.write(t, func(tx *filestore.Tx) error {
		sc := deltatxn.Scope{Tx: tx, Reader: tx, Now: tx.Now()}
		for i, m := range []alertmetrics.DeltaMetric{
			{ServiceID: svc1, TimeToAck: us(time.Hour), TimeToClose: us(2 * time.Hour), ClosedAt: jan(1, 0, 1, 0)},
			{ServiceID: svc1, TimeToAck: us(time.Hour), TimeToClose: us(2 * time.Hour), ClosedAt: jan(1, 23, 59, 59)},
			{ServiceID: svc1, TimeToClose: us(2 * time.Hour), Escalated: true, ClosedAt: jan(1, 0, 3, 0)},
			{ServiceID: svc2, TimeToClose: us(time.Second), ClosedAt: jan(1, 0, 4, 0)},
			{ServiceID: svc1, TimeToAck: us(30 * time.Minute), TimeToClose: us(3 * time.Hour), Escalated: true, ClosedAt: jan(2, 0, 5, 0)},
			{ServiceID: svc3, TimeToClose: us(time.Second), ClosedAt: jan(1, 0, 4, 0)},
		} {
			m.AlertID = int64(i + 1)
			require.NoError(t, e.metrics.Insert(sc, m))
		}
		return nil
	})
	day := pgtype.Interval{Days: 1, Valid: true}
	stats := func(ids []uuid.UUID, stride pgtype.Interval, origin, from, to time.Time) ([]gadb.ServiceAlertStatsRow, error) {
		return p.ServiceAlertStats(userCtx, gadb.ServiceAlertStatsParams{Stride: stride, Origin: origin, ServiceIds: ids, StartTime: from, EndTime: to})
	}
	rows, err := stats([]uuid.UUID{id1, id1}, day, jan(1, 0, 0, 0), jan(1, 0, 0, 0), jan(3, 0, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, []gadb.ServiceAlertStatsRow{
		{Bucket: jan(1, 0, 0, 0), AvgTimeToAckSeconds: 3600, AvgTimeToCloseSeconds: 7200, AlertCount: 3, EscalatedCount: 1, ServiceID: id1},
		{Bucket: jan(2, 0, 0, 0), AvgTimeToAckSeconds: 1800, AvgTimeToCloseSeconds: 10800, AlertCount: 1, EscalatedCount: 1, ServiceID: id1},
	}, rows, "the average time to ack leaves out alerts that were not acknowledged")
	rows, err = stats([]uuid.UUID{id1}, day, jan(1, 0, 0, 0), jan(1, 0, 3, 0), jan(1, 23, 59, 59))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.EqualValues(t, 2, rows[0].AlertCount, "BETWEEN includes both ends")
	rows, err = stats([]uuid.UUID{id1, id2}, day, jan(1, 0, 0, 0), time.Time{}, jan(1, 12, 0, 0))
	require.NoError(t, err)
	assert.Len(t, rows, 2, "a row for each service")
	rows, err = stats([]uuid.UUID{id1}, pgtype.Interval{Microseconds: 3_600_000_000, Valid: true}, time.Time{}, time.Time{}, jan(1, 0, 10, 0))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, jan(1, 0, 0, 0), rows[0].Bucket, "an origin long ago")
	rows, err = stats([]uuid.UUID{id1}, day, jan(9, 12, 0, 0), time.Time{}, jan(1, 0, 10, 0))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, jan(0, 12, 0, 0), rows[0].Bucket, "an origin in the future")
	rows, err = stats([]uuid.UUID{id1}, day, jan(1, 0, 0, 0), jan(5, 0, 0, 0), jan(6, 0, 0, 0))
	require.NoError(t, err)
	assert.Nil(t, rows)
	for _, bad := range []pgtype.Interval{{Months: 1, Valid: true}, {Valid: true}, {Days: -1, Valid: true}} {
		_, err = stats([]uuid.UUID{id1}, bad, jan(1, 0, 0, 0), time.Time{}, jan(3, 0, 0, 0))
		require.Error(t, err)
		rows, err = stats([]uuid.UUID{id1}, bad, jan(1, 0, 0, 0), jan(5, 0, 0, 0), jan(6, 0, 0, 0))
		require.NoError(t, err, "date_bin is only asked about rows")
		assert.Nil(t, rows)
	}
}

func TestDeltaPendingMessages(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, cm, nc, gone := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	e.names[u1], e.names[u2], e.names[nc] = "Alice", "Bob", "#ops"
	sms, chat := gadb.NewDestV1("sms", "phone", "+1"), gadb.NewDestV1("chat", "id", "C1")
	e.dests[cm], e.dests[nc] = sms, chat
	e.pending = []PendingMessage{
		{UserID: u1, ContactMethodID: cm},
		{UserID: u1, ContactMethodID: cm},
		{UserID: u2, ContactMethodID: cm},
		{ChannelID: nc},
		{UserID: gone, ContactMethodID: gone},
		{UserID: u1},
	}
	rows, err := p.PendingMessageDests(userCtx, gadb.AllPendingMsgDestsParams{AlertID: 1, ServiceID: uuid.New()})
	require.NoError(t, err)
	name := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	dest := func(d gadb.DestV1) gadb.NullDestV1 { return gadb.NullDestV1{Valid: true, DestV1: d} }
	assert.Equal(t, []gadb.AllPendingMsgDestsRow{
		{UserName: name("Alice"), CmDest: dest(sms)},
		{UserName: name("Bob"), CmDest: dest(sms)},
		{NcName: name("#ops"), NcDest: dest(chat)},
		{},
		{UserName: name("Alice")},
	}, rows, "each once; rows that don't exist are NULL, like LEFT JOIN")
	e.pending = nil
	rows, err = p.PendingMessageDests(userCtx, gadb.AllPendingMsgDestsParams{})
	require.NoError(t, err)
	assert.Nil(t, rows)
}

func TestDeltaNotPorted(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "x", ServiceID: svcID})
	require.NoError(t, err)
	all := p.delta.DeltaLookups
	p.delta.DeltaLookups = DeltaLookups{}
	for name, fn := range map[string]func() error{
		"create": func() error { _, err := p.CreateTx(sysCtx, nil, &Alert{Summary: "y", ServiceID: svcID}); return err },
		"upsert": func() error {
			_, _, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: "y", ServiceID: svcID})
			return err
		},
		"insert": func() error {
			return p.delta.run(sysCtx, nil, func(_ txn.Tx, s deltatxn.Scope) error { _, err := p.DeltaInsert(s.Tx, DeltaAlert{}); return err })
		},
		"status":  func() error { return p.UpdateStatus(sysCtx, 1, StatusActive) },
		"close":   func() error { _, err := p.UpdateManyAlertStatus(sysCtx, StatusClosed, []int{1}, nil); return err },
		"esc":     func() error { return p.Escalate(sysCtx, 1, 0) },
		"state":   func() error { _, err := p.State(sysCtx, []int{1}); return err },
		"info":    func() error { _, _, err := p.ServiceInfo(userCtx, svcID); return err },
		"epid":    func() error { _, err := p.EPID(sysCtx, 1); return err },
		"search":  func() error { _, err := p.Search(sysCtx, &SearchOptions{Search: "x"}); return err },
		"pending": func() error { _, err := p.PendingMessageDests(sysCtx, gadb.AllPendingMsgDestsParams{}); return err },
	} {
		require.ErrorIs(t, fn(), deltatxn.ErrNotPorted, name)
	}
	p.delta.DeltaLookups = all
	p.delta.StatusChanged = nil
	ids, err := p.UpdateManyAlertStatus(sysCtx, StatusActive, []int{1}, nil)
	require.NoError(t, err, "without the engine's jobs, the status still changes")
	assert.Equal(t, []int{1}, ids)
	row, _ := e.row(t, 1)
	assert.Equal(t, "active", row.Status)
	assert.Equal(t, 1, e.count(t))
}

func TestDeltaSearchWords(t *testing.T) {
	for _, same := range []string{
		"fail fails failed failing", "stop stopped stopping stops", "retry retries retried retrying", "create created creating creates",
		"crash crashes crashed crashing", "use used using uses", "database databases", "process processes processing processed",
		"exceed exceeds exceeded exceeding", "need needs needed", "call calls called calling", "entry entries", "deploy deploys deployed",
		"status statuses", "die dies", "timeout Timeouts TIMEOUT", "alert alerts alerting", "disk disks",
	} {
		words := strings.Fields(same)
		for _, w := range words[1:] {
			assert.Equal(t, searchWords(words[0]), searchWords(w), "%s and %s", words[0], w)
		}
	}
	for _, differ := range []string{"string strong", "ping pong", "red bed", "dns dn", "node1 node2", "users us", "thing th", "cpu cpus"} {
		words := strings.Fields(differ)
		assert.NotEqual(t, searchWords(words[0]), searchWords(words[1]), differ)
	}
	assert.Equal(t, []string{"disk", "full", "db", "exampl", "com", "95"}, searchWords("The disk is FULL on db.example.com: disk_full (95%)"))
	assert.Equal(t, []string{"größe", "größen"}, searchWords("Größe größen"), "other languages' words stay whole")
	assert.Empty(t, searchWords("the and of, ... "))
	assert.Empty(t, searchWords(strings.Repeat("x", maxWordLen+1)), "too long to be found")

	// Any summary that is valid can be stored.
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	for i, summary := range []string{strings.Repeat("𝔘", MaxSummaryLength), strings.Repeat("Ⱥ", MaxSummaryLength), strings.Repeat("x ", MaxSummaryLength/2-1) + "x"} {
		_, isNew, err := p.CreateOrUpdate(sysCtx, &Alert{Summary: summary, ServiceID: svcID, Dedup: NewUserDedup(strconv.Itoa(i) + strings.Repeat("𝔘", 600))})
		require.NoError(t, err)
		assert.True(t, isNew)
	}
}

// reference is the search template's query, without any index.
func (e *deltaEnv) reference(r filestore.Reader, opts SearchOptions, limit int) []int {
	var rows []DeltaAlert
	in := func(ids []string, id string) bool { return slices.Contains(ids, id) }
	var nameIDs []string
	words := searchWords(opts.Search)
	e.services.Ascend(r, func(s svcRow) bool {
		if len(words) > 0 && hasAll(searchWords(s.Name), words) {
			nameIDs = append(nameIDs, s.ID)
		}
		return true
	})
	var notified []int64
	e.logs.Logs.Ascend(r, func(l alertlog.DeltaLog) bool {
		if opts.NotifiedUserID != "" && l.SubUserID != nil && *l.SubUserID == opts.NotifiedUserID && (l.Event == "notification_sent" || l.Event == "no_notification_sent") {
			notified = append(notified, *l.AlertID)
		}
		return true
	})
	e.tables.Alerts.Ascend(r, func(a DeltaAlert) bool {
		ok := !slices.Contains(opts.Omit, int(a.ID))
		if opts.Search != "" {
			ok = ok && (strconv.FormatInt(a.ID, 10) == opts.Search || (len(words) > 0 && hasAll(searchWords(a.Summary), words)) || in(nameIDs, *a.ServiceID))
		}
		if len(opts.Status) > 0 {
			ok = ok && slices.Contains(opts.Status, Status(a.Status))
		}
		if opts.ServiceFilter.Valid {
			ok = ok && (in(opts.ServiceFilter.IDs, *a.ServiceID) || slices.Contains(notified, a.ID))
		}
		ok = ok && (opts.Before.IsZero() || a.CreatedAt.Before(opts.Before))
		ok = ok && (opts.NotBefore.IsZero() || !a.CreatedAt.Before(opts.NotBefore))
		if opts.After.ID != 0 {
			at, id := opts.After.Created, int64(opts.After.ID)
			so, sa := statusOrder[Status(a.Status)], statusOrder[opts.After.Status]
			switch opts.Sort {
			case SortModeDateID:
				ok = ok && (a.CreatedAt.Before(at) || (a.CreatedAt.Equal(at) && a.ID < id))
			case SortModeDateIDReverse:
				ok = ok && (a.CreatedAt.After(at) || (a.CreatedAt.Equal(at) && a.ID > id))
			default:
				ok = ok && (so > sa || (so == sa && a.ID < id))
			}
		}
		m, has := e.metrics.Metrics.Get(r, filestore.IntKey(a.ID))
		ok = ok && (opts.ClosedBefore.IsZero() || (has && m.ClosedAt.Before(opts.ClosedBefore)))
		ok = ok && (opts.NotClosedBefore.IsZero() || (has && m.ClosedAt.After(opts.NotClosedBefore)))
		if ok {
			rows = append(rows, a)
		}
		return true
	})
	slices.SortFunc(rows, func(a, b DeltaAlert) int {
		switch opts.Sort {
		case SortModeDateID:
			return cmp.Or(b.CreatedAt.Compare(a.CreatedAt), cmp.Compare(b.ID, a.ID))
		case SortModeDateIDReverse:
			return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
		}
		return cmp.Or(cmp.Compare(statusOrder[Status(a.Status)], statusOrder[Status(b.Status)]), cmp.Compare(b.ID, a.ID))
	})
	ids := []int{}
	for _, a := range rows[:min(len(rows), limit)] {
		ids = append(ids, int(a.ID))
	}
	return ids
}

// TestDeltaSearch compares searches, whichever index they use, with what the
// search template's conditions and order give when applied to every alert.
func TestDeltaSearch(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	rng := rand.New(rand.NewPCG(1, 2))
	userID := uuid.NewString()
	var svcs []string
	for _, name := range []string{"Web frontend", "Database", "Disk monitoring", "Payments web"} {
		svcs = append(svcs, p.addService(t, name))
	}
	svcs = append(svcs, uuid.NewString()) // doesn't exist
	words := []string{"disk", "disks", "full", "failed", "failing", "web", "the", "database", "down", "5"}
	day := func(n int) time.Time { return start.AddDate(0, 0, -n) }
	p.write(t, func(tx *filestore.Tx) error {
		sc := deltatxn.Scope{Tx: tx, Reader: tx, Now: tx.Now()}
		for i := range 60 {
			a := DeltaAlert{
				Summary:   words[rng.IntN(len(words))] + " " + words[rng.IntN(len(words))],
				ServiceID: &svcs[rng.IntN(4)], Status: string(statuses[rng.IntN(3)]),
				// Some at the same time, over more than a year, not in the
				// order of the IDs.
				CreatedAt: day(rng.IntN(20) * 25).Add(time.Duration(rng.IntN(3)) * time.Hour),
			}
			if a.Status != "closed" {
				a.DedupKey = text(fmt.Sprintf("auto:1:%d", i))
			}
			a, err := p.DeltaInsert(tx, a)
			require.NoError(t, err)
			if a.Status == "closed" && rng.IntN(4) > 0 {
				us := int64(1)
				require.NoError(t, e.metrics.Insert(sc, alertmetrics.DeltaMetric{AlertID: a.ID, ServiceID: *a.ServiceID, TimeToClose: &us, ClosedAt: day(rng.IntN(20) * 25)}))
			}
			if rng.IntN(5) == 0 {
				event := []string{"notification_sent", "no_notification_sent", "acknowledged"}[rng.IntN(3)]
				require.NoError(t, e.logs.Insert(tx, alertlog.DeltaLog{AlertID: &a.ID, Event: event, SubUserID: &userID}))
			}
		}
		return nil
	})
	b := e.pod(t, "b")
	r := b.fdb.View()
	ids := func(alerts []Alert) []int {
		ids := []int{}
		for _, a := range alerts {
			ids = append(ids, a.ID)
		}
		return ids
	}

	for range 3000 {
		opts := SearchOptions{Sort: SortMode(rng.IntN(3))}
		if rng.IntN(2) == 0 {
			opts.Limit = 1 + rng.IntN(20)
		}
		if rng.IntN(3) == 0 {
			opts.Search = []string{"disk", "Failing", "web", "the", "7", "5", "web disk", "x", "database full", "Web-Frontend"}[rng.IntN(10)]
		}
		for range rng.IntN(3) {
			opts.Status = append(opts.Status, statuses[rng.IntN(3)])
		}
		if rng.IntN(3) == 0 {
			opts.ServiceFilter.Valid = true
			for range rng.IntN(3) {
				opts.ServiceFilter.IDs = append(opts.ServiceFilter.IDs, svcs[rng.IntN(5)])
			}
		}
		if rng.IntN(3) == 0 {
			opts.NotifiedUserID = userID
		}
		for range rng.IntN(3) {
			opts.Omit = append(opts.Omit, rng.IntN(62))
		}
		for _, at := range []*time.Time{&opts.Before, &opts.NotBefore, &opts.ClosedBefore, &opts.NotClosedBefore} {
			if rng.IntN(4) == 0 {
				*at = day(rng.IntN(20) * 25).Add(time.Duration(rng.IntN(2)) * time.Hour)
			}
		}
		if rng.IntN(3) == 0 {
			a, ok := e.tables.Alerts.Get(r, filestore.IntKey(int64(1+rng.IntN(60))))
			require.True(t, ok)
			opts.After = SearchCursor{ID: int(a.ID), Status: Status(a.Status), Created: a.CreatedAt}
		}
		limit := cmp.Or(opts.Limit, 15)
		want := e.reference(r, opts, limit)
		got, err := b.Search(userCtx, &opts)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, want, ids(got), "%+v", opts)
	}

	// Page through everything in each order.
	for _, sort := range []SortMode{SortModeStatusID, SortModeDateID, SortModeDateIDReverse} {
		opts := SearchOptions{Sort: sort, Limit: 7}
		var all []int
		for {
			page, err := b.Search(userCtx, &opts)
			require.NoError(t, err)
			if len(page) == 0 {
				break
			}
			all = append(all, ids(page)...)
			last := page[len(page)-1]
			opts.After = SearchCursor{ID: last.ID, Status: last.Status, Created: last.CreatedAt}
		}
		assert.Len(t, all, 60)
		assert.Equal(t, e.reference(r, SearchOptions{Sort: sort}, 100), all)
	}

	got, err := b.Search(userCtx, nil)
	require.NoError(t, err)
	assert.Len(t, got, 15)
	got, err = b.Search(userCtx, &SearchOptions{ServiceFilter: IDFilter{Valid: true, IDs: []string{strings.ToUpper(svcs[0])}}, Limit: 1000})
	require.NoError(t, err)
	assert.NotEmpty(t, got, "ids in any case")
	_, err = b.Search(userCtx, &SearchOptions{Limit: 2000})
	assert.True(t, validation.IsClientError(err))
	_, err = b.Search(permission.ServiceContext(context.Background(), svcs[0]), nil)
	assert.True(t, permission.IsPermissionError(err))
	got, err = b.Search(userCtx, &SearchOptions{After: SearchCursor{ID: 1 << 60}, Status: []Status{StatusClosed}, Limit: 1000})
	require.NoError(t, err)
	assert.NotEmpty(t, got, "a cursor without a status is before every closed alert")
}

func TestDeltaAlertTables(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	svcID := p.addService(t, "Web")
	for i, st := range []string{"triggered", "closed", "active", "triggered"} {
		a := DeltaAlert{ServiceID: &svcID, Status: st, CreatedAt: start.AddDate(0, 0, -i)}
		if st != "closed" {
			a.DedupKey = text(fmt.Sprintf("auto:1:%d", i))
		}
		p.insert(t, a)
	}
	e.read(t, func(r filestore.Reader) {
		assert.Equal(t, []int64{1, 2, 3, 4}, e.tables.AlertIDs(r, svcID))
		assert.Equal(t, []int64{1, 3, 4}, e.tables.AlertIDs(r, svcID, "active", "triggered"))
		assert.Empty(t, e.tables.AlertIDs(r, uuid.NewString()))
		var open, created []int64
		e.tables.AscendOpen(r, StatusTriggered, func(a DeltaAlert) bool {
			open = append(open, a.ID)
			return true
		})
		assert.Equal(t, []int64{1, 4}, open)
		e.tables.AscendCreated(r, time.Time{}, start.AddDate(0, 0, -1), func(a DeltaAlert) bool {
			created = append(created, a.ID)
			return len(created) < 2
		})
		assert.Equal(t, []int64{4, 3}, created)
	})
	// The engine's cleanup deletes old alerts.
	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.tables.Alerts.Delete(tx, filestore.IntKey(2))
		return err
	})
	assert.Equal(t, 3, e.count(t))
}
