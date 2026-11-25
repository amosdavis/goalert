package heartbeat

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/util/sqlutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// parent stands in for services.
type parent struct {
	ID string `parquet:"id"`
}

type env struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	services *filestore.Table[parent]
	clock    *filestore.FakeClock
	limit    int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock, limit: -1}
	e.services = filestore.NewTable(e.schema, "services", func(p parent) string { return p.ID })
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.services)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

type pod struct {
	*Store
	db  deltatxn.DB
	env *env
}

// open opens the table as one GoAlert pod.
func (e *env) open(t *testing.T) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &pod{Store: NewDeltaStore(db, e.tables, func(filestore.Reader) int { return e.limit }), db: db, env: e}
}

// update runs fn on the filestore tables.
func (p *pod) update(fn func(tx *filestore.Tx) error) error {
	_, err := p.db.DB.Update(context.Background(), fn)
	return err
}

func (p *pod) addService(t *testing.T) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return p.env.services.Put(tx, parent{ID: id}) }))
	return id
}

func testCtx() context.Context {
	return permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser)
}

func (p *pod) create(t *testing.T, serviceID, name string) *Monitor {
	t.Helper()
	m, err := p.CreateTx(testCtx(), nil, &Monitor{Name: name, ServiceID: serviceID, Timeout: 5 * time.Minute})
	require.NoError(t, err)
	return m
}

func (p *pod) find(t *testing.T, id string) Monitor {
	t.Helper()
	m, err := p.FindOneTx(testCtx(), nil, id)
	require.NoError(t, err)
	return *m
}

func requireConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	require.Error(t, err)
	dbErr := sqlutil.MapError(err)
	require.NotNil(t, dbErr, "%v", err)
	assert.Equal(t, code, dbErr.Code)
	assert.Equal(t, constraint, dbErr.ConstraintName)
}

func TestDeltaMonitors(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc, other := p.addService(t), p.addService(t)

	a, err := p.CreateTx(ctx, nil, &Monitor{
		Name: "Alpha", ServiceID: strings.ToUpper(svc), Timeout: 10*time.Minute + time.Second,
		AdditionalDetails: "details", Muted: "reason",
	})
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, a.Timeout, "whole minutes")
	want := Monitor{
		ID: a.ID, Name: "Alpha", ServiceID: svc, Timeout: 10 * time.Minute,
		AdditionalDetails: "details", Muted: "reason", lastState: StateInactive,
	}
	assert.Equal(t, want, p.find(t, a.ID))
	assert.Equal(t, want, p.find(t, strings.ToUpper(a.ID)), "a UUID in another form is the same UUID")
	assert.Equal(t, want, e.open(t).find(t, a.ID), "another pod reads the same")
	b := p.create(t, svc, "beta")
	c := p.create(t, other, "alpha") // a name is unique in its service only

	// UpdateTx sets the name, timeout, details and muted, and nothing else.
	require.NoError(t, p.UpdateTx(ctx, nil, &Monitor{ID: a.ID, Name: "Alpha 2", ServiceID: other, Timeout: time.Hour}))
	assert.Equal(t, Monitor{ID: a.ID, Name: "Alpha 2", ServiceID: svc, Timeout: time.Hour, lastState: StateInactive}, p.find(t, a.ID))
	require.NoError(t, p.UpdateTx(ctx, nil, &Monitor{ID: uuid.NewString(), Name: "none", ServiceID: svc, Timeout: time.Hour}), "an UPDATE of no rows")

	_, err = p.FindOneTx(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	ids := func(monitors []Monitor) []string {
		res := []string{}
		for _, m := range monitors {
			res = append(res, m.ID)
		}
		return res
	}
	many, err := p.FindMany(ctx, []string{b.ID, c.ID, b.ID, uuid.NewString()})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{b.ID, c.ID}, ids(many), "each once")
	many, err = p.FindMany(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, []Monitor{}, many, "an empty list, not nil, as on Postgres")
	many, err = p.FindMany(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, many)

	all, err := p.FindAllByService(ctx, svc)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{a.ID, b.ID}, ids(all))
	all, err = p.FindAllByService(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Equal(t, []Monitor{}, all, "an empty list, not nil, as on Postgres")

	require.NoError(t, p.DeleteTx(ctx, nil, a.ID, uuid.NewString(), a.ID))
	require.NoError(t, p.DeleteTx(ctx, nil))
	_, err = p.FindOneTx(ctx, nil, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	p.find(t, b.ID)
}

func TestDeltaMonitorsChecks(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc := p.addService(t)
	a, b := p.create(t, svc, "Alpha"), p.create(t, svc, "beta")

	// heartbeat_monitor_name_service_id
	_, err := p.CreateTx(ctx, nil, &Monitor{Name: "ALPHA", ServiceID: svc, Timeout: time.Hour})
	requireConstraint(t, err, "23505", "heartbeat_monitor_name_service_id")
	assert.EqualError(t, errutil.MapDBError(err), validation.NewFieldError("Name", "heartbeat monitor already exists with that name").Error())
	err = p.UpdateTx(ctx, nil, &Monitor{ID: b.ID, Name: "alpha", ServiceID: svc, Timeout: time.Hour})
	requireConstraint(t, err, "23505", "heartbeat_monitor_name_service_id")
	require.NoError(t, p.UpdateTx(ctx, nil, &Monitor{ID: a.ID, Name: "ALPHA", ServiceID: svc, Timeout: time.Hour}), "its own name")

	// heartbeat_monitors_pkey
	err = p.update(func(tx *filestore.Tx) error {
		return e.tables.Monitors.Insert(tx, monitorRow{ID: a.ID, Name: "x", ServiceID: svc}, "heartbeat_monitors_pkey")
	})
	requireConstraint(t, err, "23505", "heartbeat_monitors_pkey")

	// heartbeat_monitors_service_id_fkey
	_, err = p.CreateTx(ctx, nil, &Monitor{Name: "gamma", ServiceID: uuid.NewString(), Timeout: time.Hour})
	requireConstraint(t, err, "23503", "heartbeat_monitors_service_id_fkey")
	kept := p.create(t, p.addService(t), "kept")
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, svc)
		return err
	}))
	_, err = p.FindOneTx(ctx, nil, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows, "monitors go with their service")
	p.find(t, kept.ID)

	// Checks shared with Postgres.
	_, err = p.CreateTx(ctx, nil, &Monitor{Name: "gamma", ServiceID: kept.ServiceID, Timeout: time.Second})
	assert.True(t, validation.IsValidationError(err), "%v", err)
	assert.True(t, validation.IsValidationError(p.UpdateTx(ctx, nil, &Monitor{Name: "gamma", ServiceID: kept.ServiceID, Timeout: time.Hour})))
	assert.True(t, validation.IsValidationError(p.DeleteTx(ctx, nil, "x")))
	assert.True(t, validation.IsValidationError(p.RecordHeartbeat(ctx, "x")))
	anon := context.Background()
	_, err = p.CreateTx(anon, nil, &Monitor{})
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	assert.True(t, permission.IsPermissionError(p.UpdateTx(anon, nil, &Monitor{})))
	assert.True(t, permission.IsPermissionError(p.DeleteTx(anon, nil, kept.ID)))
	_, err = p.FindOneTx(anon, nil, kept.ID)
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	_, err = p.FindMany(anon, []string{kept.ID})
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	_, err = p.FindAllByService(anon, kept.ServiceID)
	assert.True(t, permission.IsPermissionError(err), "%v", err)
}

func TestDeltaMonitorsLimit(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	e.limit = 2
	p := e.open(t)
	svc, other := p.addService(t), p.addService(t)
	a := p.create(t, svc, "one")
	p.create(t, svc, "two")
	p.create(t, other, "other service")

	_, err := p.CreateTx(ctx, nil, &Monitor{Name: "three", ServiceID: svc, Timeout: time.Hour})
	requireConstraint(t, err, "23514", "heartbeat_monitors_per_service_limit")
	assert.Equal(t, "max=2", sqlutil.MapError(err).Hint)
	assert.EqualError(t, errutil.MapDBError(err), "too many heartbeat monitors on this service")
	all, err := p.FindAllByService(ctx, svc)
	require.NoError(t, err)
	assert.Len(t, all, 2, "nothing was added")

	require.NoError(t, p.UpdateTx(ctx, nil, &Monitor{ID: a.ID, Name: "uno", ServiceID: svc, Timeout: time.Hour}), "only new monitors are limited")
	e.limit = -1
	p.create(t, svc, "three")
	e.limit = 0
	_, err = p.CreateTx(ctx, nil, &Monitor{Name: "four", ServiceID: other, Timeout: time.Hour})
	requireConstraint(t, err, "23514", "heartbeat_monitors_per_service_limit")

	assert.Panics(t, func() { NewDeltaStore(p.db, e.tables, nil) }, "a limit can't be left out by mistake (F8)")
}

func TestDeltaMonitorsTx(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	m := p.create(t, p.addService(t), "Alpha")

	boom := errors.New("boom")
	err := p.db.Run(ctx, nil, func(tx txn.Tx) error {
		found, err := p.FindOneTx(ctx, tx, m.ID)
		if err != nil {
			return err
		}
		found.Muted = "reason"
		if err := p.UpdateTx(ctx, tx, found); err != nil {
			return err
		}
		found, err = p.FindOneTx(ctx, tx, m.ID)
		if err != nil {
			return err
		}
		assert.Equal(t, "reason", found.Muted, "a transaction sees its own changes")
		if err := p.DeleteTx(ctx, tx, m.ID); err != nil {
			return err
		}
		_, err = p.CreateTx(ctx, tx, &Monitor{Name: "ALPHA", ServiceID: m.ServiceID, Timeout: time.Hour})
		if err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, "", p.find(t, m.ID).Muted, "a failed transaction changes nothing")
}

func TestDeltaRecordHeartbeat(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	p := e.open(t)
	m := p.create(t, p.addService(t), "Alpha")
	assert.True(t, p.find(t, m.ID).LastHeartbeat().IsZero())

	version := p.db.DB.View().Index()
	require.NoError(t, p.RecordHeartbeat(ctx, m.ID))
	assert.Equal(t, version+1, p.db.DB.View().Index(), "a heartbeat is one commit")
	got := p.find(t, m.ID)
	assert.Equal(t, start, got.LastHeartbeat())
	assert.Equal(t, StateInactive, got.LastState(), "the engine changes the state")

	e.clock.Advance(time.Minute)
	require.NoError(t, p.RecordHeartbeat(ctx, strings.ToUpper(m.ID)))
	assert.Equal(t, start.Add(time.Minute), p.find(t, m.ID).LastHeartbeat())
	assert.Equal(t, start.Add(time.Minute), e.open(t).find(t, m.ID).LastHeartbeat(), "another pod reads the same")

	version = p.db.DB.View().Index()
	require.NoError(t, p.RecordHeartbeat(ctx, uuid.NewString()), "an UPDATE of no rows")
	assert.Equal(t, version, p.db.DB.View().Index(), "commits nothing")

	// The engine finds monitors to check by state, once they had a heartbeat.
	p.create(t, m.ServiceID, "never")
	assert.Equal(t, 1, e.tables.ByState.Count(p.db.DB.View(), string(StateInactive)))
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		row, _ := e.tables.Monitors.Get(tx, m.ID)
		row.LastState = string(StateHealthy)
		return e.tables.Monitors.Put(tx, row)
	}))
	assert.Equal(t, 0, e.tables.ByState.Count(p.db.DB.View(), string(StateInactive)))
	assert.Equal(t, 1, e.tables.ByState.Count(p.db.DB.View(), string(StateHealthy)))
	assert.Equal(t, StateHealthy, p.find(t, m.ID).LastState())

	// An update keeps the state and the last heartbeat.
	require.NoError(t, p.UpdateTx(testCtx(), nil, &Monitor{ID: m.ID, Name: "Alpha 2", ServiceID: m.ServiceID, Timeout: time.Hour}))
	got = p.find(t, m.ID)
	assert.Equal(t, StateHealthy, got.LastState())
	assert.Equal(t, start.Add(time.Minute), got.LastHeartbeat())
}

// Heartbeats that arrive together share commits, and none is lost.
func TestDeltaRecordHeartbeatTogether(t *testing.T) {
	e := newEnv(t)
	p := e.open(t)
	svc := p.addService(t)
	const n = 40
	ids := make([]string, n)
	for i := range ids {
		ids[i] = p.create(t, svc, fmt.Sprintf("mon %d", i)).ID
	}
	e.clock.Advance(time.Minute)
	before, txsBefore := p.db.DB.Batches()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.RecordHeartbeat(context.Background(), id)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	after, txsAfter := p.db.DB.Batches()
	assert.EqualValues(t, n, txsAfter-txsBefore)
	t.Logf("%d heartbeats took %d commits", n, after-before)

	fresh := e.open(t)
	for _, id := range ids {
		assert.Equal(t, start.Add(time.Minute), fresh.find(t, id).LastHeartbeat())
	}
}

func TestDeltaMonitorsRace(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	pods := []*pod{e.open(t), e.open(t)}
	svc := pods[0].addService(t)

	race := func(fn func(i int, p *pod) error) []error {
		errs := make([]error, len(pods))
		var wg sync.WaitGroup
		for i, p := range pods {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = fn(i, p)
			}()
		}
		wg.Wait()
		return errs
	}
	oneFailed := func(errs []error) error {
		t.Helper()
		if errs[0] == nil {
			errs[0], errs[1] = errs[1], errs[0]
		}
		require.NoError(t, errs[1])
		return errs[0]
	}

	// The same name, at once.
	errs := race(func(_ int, p *pod) error {
		_, err := p.CreateTx(ctx, nil, &Monitor{Name: "Alpha", ServiceID: svc, Timeout: time.Hour})
		return err
	})
	requireConstraint(t, oneFailed(errs), "23505", "heartbeat_monitor_name_service_id")

	// The last place under the limit, at once.
	e.limit = 2
	errs = race(func(i int, p *pod) error {
		_, err := p.CreateTx(ctx, nil, &Monitor{Name: fmt.Sprintf("mon %d", i), ServiceID: svc, Timeout: time.Hour})
		return err
	})
	requireConstraint(t, oneFailed(errs), "23514", "heartbeat_monitors_per_service_limit")
	e.limit = -1

	// A heartbeat and a change of the monitor, at once: neither is lost.
	all, err := e.open(t).FindAllByService(ctx, svc)
	require.NoError(t, err)
	require.Len(t, all, 2)
	m := all[0]
	e.clock.Advance(time.Minute)
	errs = race(func(i int, p *pod) error {
		if i == 0 {
			return p.RecordHeartbeat(ctx, m.ID)
		}
		return p.UpdateTx(ctx, nil, &Monitor{ID: m.ID, Name: "renamed", ServiceID: svc, Timeout: time.Hour})
	})
	require.NoError(t, errors.Join(errs...))
	got := e.open(t).find(t, m.ID)
	assert.Equal(t, "renamed", got.Name)
	assert.Equal(t, start.Add(time.Minute), got.LastHeartbeat())
}
