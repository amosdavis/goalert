package override

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

// start is in the future, since the store refuses overrides that are over.
var start = time.Now().Add(24 * time.Hour).UTC().Truncate(time.Hour)

func at(hours float64) time.Time { return start.Add(time.Duration(hours * float64(time.Hour))) }

// idRow stands in for the users and schedules of other packages.
type idRow struct {
	ID string `parquet:"id"`
}

type deltaEnv struct {
	store     objstore.Store
	schema    *filestore.Schema
	tables    *DeltaTables
	users     *filestore.Table[idRow]
	schedules *filestore.Table[idRow]
	clock     *filestore.FakeClock
	max       atomic.Int64 // user_overrides_per_schedule
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	e.max.Store(-1)
	e.tables = DefineDeltaTables(e.schema)
	e.users = filestore.NewTable(e.schema, "users", func(r idRow) string { return r.ID })
	e.schedules = filestore.NewTable(e.schema, "schedules", func(r idRow) string { return r.ID })
	DeclareDeltaForeignKeys(e.tables, e.users, e.schedules)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// pod is one GoAlert pod on the table.
type pod struct {
	*Store
	e   *deltaEnv
	fdb *filestore.DeltaDB
	db  txn.DB
}

func (e *deltaEnv) pod(t *testing.T, name string) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	limit := func(filestore.Reader) int { return int(e.max.Load()) }
	return &pod{Store: NewDeltaStore(db, e.tables, limit), e: e, fdb: fdb, db: db}
}

func (p *pod) add(t *testing.T, table *filestore.Table[idRow]) string {
	t.Helper()
	id := uuid.NewString()
	_, err := p.fdb.Update(context.Background(), func(tx *filestore.Tx) error { return table.Put(tx, idRow{ID: id}) })
	require.NoError(t, err)
	return id
}

func (p *pod) remove(t *testing.T, table *filestore.Table[idRow], id string) {
	t.Helper()
	_, err := p.fdb.Update(context.Background(), func(tx *filestore.Tx) error {
		_, err := table.Delete(tx, id)
		return err
	})
	require.NoError(t, err)
}

func testCtx() context.Context {
	return permission.UserContext(context.Background(), "a1b2c3d4-0000-4000-8000-000000000001", permission.RoleUser)
}

// over is an override of a schedule; the times are hours from start.
func over(schedID, addID, removeID string, from, to float64) *UserOverride {
	return &UserOverride{AddUserID: addID, RemoveUserID: removeID, Start: at(from), End: at(to), Target: assignment.ScheduleTarget(schedID)}
}

func (p *pod) create(t *testing.T, o *UserOverride) string {
	t.Helper()
	n, err := p.CreateUserOverrideTx(testCtx(), nil, o)
	require.NoError(t, err)
	return n.ID
}

func (p *pod) count() int { return p.e.tables.Overrides.Len(p.fdb.View()) }

func clientError(t *testing.T, err error, want string) {
	t.Helper()
	got := errutil.MapDBError(err)
	require.Error(t, got)
	assert.True(t, validation.IsClientError(got), "a message for the user, not %v", got)
	assert.ErrorContains(t, got, want)
}

func assertSame(t *testing.T, want *UserOverride, got UserOverride) {
	t.Helper()
	assert.Equal(t, want.AddUserID, got.AddUserID)
	assert.Equal(t, want.RemoveUserID, got.RemoveUserID)
	assert.True(t, want.Start.Equal(got.Start))
	assert.True(t, want.End.Equal(got.End))
	assert.Equal(t, want.Target, got.Target)
}

func TestDeltaOverrides(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, sched := p.add(t, e.users), p.add(t, e.users), p.add(t, e.schedules)

	o := over(sched, u1, u2, 0, 8)
	n, err := p.CreateUserOverrideTx(ctx, nil, o)
	require.NoError(t, err)
	got, err := p.FindOneUserOverrideTx(ctx, nil, n.ID, false)
	require.NoError(t, err)
	assert.Equal(t, n.ID, got.ID)
	assertSame(t, o, *got)
	got, err = p.FindOneUserOverrideTx(ctx, nil, strings.ToUpper(n.ID), true)
	require.NoError(t, err)
	require.NotNil(t, got, "a uuid may be written in capitals, as on Postgres")
	assert.Equal(t, n.ID, got.ID)

	missing, err := p.FindOneUserOverrideTx(ctx, nil, uuid.NewString(), false)
	require.NoError(t, err)
	assert.Nil(t, missing, "nil for a missing override, as on Postgres")

	got.AddUserID, got.RemoveUserID, got.Start, got.End = "", strings.ToUpper(u1), at(1), at(9)
	require.NoError(t, p.UpdateUserOverride(ctx, got))
	got, err = p.FindOneUserOverrideTx(ctx, nil, n.ID, false)
	require.NoError(t, err)
	assertSame(t, over(sched, "", u1, 1, 9), *got)

	ghost := *got
	ghost.ID = uuid.NewString()
	require.NoError(t, p.UpdateUserOverride(ctx, &ghost), "an UPDATE of no rows")
	assert.Equal(t, 1, p.count())

	fresh := e.pod(t, "b")
	got2, err := fresh.FindOneUserOverrideTx(ctx, nil, n.ID, false)
	require.NoError(t, err)
	assert.Equal(t, got, got2, "a new pod reads the same override")

	// In a transaction, the changes are seen before they are committed.
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		n, err := p.CreateUserOverrideTx(ctx, tx, over(sched, u2, "", 0, 1))
		if err != nil {
			return err
		}
		o, err := p.FindOneUserOverrideTx(ctx, tx, n.ID, true)
		assert.NoError(t, err)
		assert.NotNil(t, o)
		found, err := p.Search(ctx, tx, nil)
		assert.NoError(t, err)
		assert.Len(t, found, 2)
		return errors.New("rolled back")
	})
	require.ErrorContains(t, err, "rolled back")
	assert.Equal(t, 1, p.count(), "a failed transaction changes nothing")

	require.NoError(t, p.DeleteUserOverrideTx(ctx, nil))
	require.NoError(t, p.DeleteUserOverrideTx(ctx, nil, uuid.NewString()))
	assert.Equal(t, 1, p.count())
	require.NoError(t, p.DeleteUserOverrideTx(ctx, nil, strings.ToUpper(n.ID), n.ID))
	assert.Zero(t, p.count())

	_, err = p.CreateUserOverrideTx(context.Background(), nil, o)
	assert.True(t, permission.IsPermissionError(err), "permissions are checked")
	_, err = p.CreateUserOverrideTx(ctx, nil, over(sched, u1, "", -72, -48))
	assert.True(t, validation.IsValidationError(err), "and the store still validates")
}

func TestDeltaOverrideChecks(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, sched := p.add(t, e.users), p.add(t, e.users), p.add(t, e.schedules)
	id := p.create(t, over(sched, u1, u2, 0, 8))

	same := over(sched, u1, u1, 10, 12)
	_, err := p.CreateUserOverrideTx(ctx, nil, same)
	clientError(t, err, "cannot be the same as the user being replaced")
	var cerr *filestore.CheckError
	require.ErrorAs(t, err, &cerr)
	assert.Equal(t, "user_overrides_check2", cerr.Constraint)
	same.AddUserID = strings.ToUpper(u1)
	_, err = p.CreateUserOverrideTx(ctx, nil, same)
	clientError(t, err, "cannot be the same as the user being replaced")
	same.ID = id
	clientError(t, p.UpdateUserOverride(ctx, same), "cannot be the same as the user being replaced")

	// The store's validation stops these before they get here.
	for _, c := range []struct {
		o          *UserOverride
		constraint string
	}{
		{over(sched, u1, "", 10, 10), "user_overrides_check"},
		{over(sched, u1, "", 10, 9), "user_overrides_check"},
		{over(sched, "", "", 10, 9), "user_overrides_check"},
		{over(sched, "", "", 10, 11), "user_overrides_check1"},
	} {
		c.o.ID = uuid.NewString()
		require.ErrorAs(t, p.delta.create(ctx, nil, c.o), &cerr)
		assert.Equal(t, c.constraint, cerr.Constraint)
		c.o.ID = id
		require.ErrorAs(t, p.delta.update(ctx, nil, c.o), &cerr)
		assert.Equal(t, c.constraint, cerr.Constraint)
	}
	dup := over(sched, u1, "", 20, 21)
	dup.ID = id
	var uerr *filestore.UniqueError
	require.ErrorAs(t, p.delta.create(ctx, nil, dup), &uerr)
	assert.Equal(t, "user_overrides_pkey", uerr.Index)

	// Foreign keys.
	_, err = p.CreateUserOverrideTx(ctx, nil, over(sched, uuid.NewString(), u2, 10, 12))
	clientError(t, err, "user does not exist")
	assert.ErrorContains(t, errutil.MapDBError(err), "AddUserID")
	_, err = p.CreateUserOverrideTx(ctx, nil, over(sched, u1, uuid.NewString(), 10, 12))
	clientError(t, err, "user does not exist")
	assert.ErrorContains(t, errutil.MapDBError(err), "RemoveUserID")
	_, err = p.CreateUserOverrideTx(ctx, nil, over(uuid.NewString(), u1, u2, 10, 12))
	clientError(t, err, "schedule does not exist")
	assert.Equal(t, 1, p.count(), "nothing changed")
}

func TestDeltaOverrideCascade(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, u3 := p.add(t, e.users), p.add(t, e.users), p.add(t, e.users)
	s1, s2 := p.add(t, e.schedules), p.add(t, e.schedules)
	p.create(t, over(s1, u1, u3, 0, 1))
	p.create(t, over(s1, u3, u1, 2, 3))
	p.create(t, over(s1, u2, "", 0, 1))
	p.create(t, over(s2, u2, "", 0, 1))
	kept := p.create(t, over(s2, u3, "", 0, 1))

	p.remove(t, e.users, u1)
	assert.Equal(t, 3, p.count(), "ON DELETE CASCADE for the added and the removed user")
	p.remove(t, e.schedules, s1)
	assert.Equal(t, 2, p.count(), "ON DELETE CASCADE for the schedule")
	p.remove(t, e.users, u2)
	fresh := e.pod(t, "b")
	assert.Equal(t, 1, fresh.count())
	o, err := fresh.FindOneUserOverrideTx(testCtx(), nil, kept, false)
	require.NoError(t, err)
	assert.NotNil(t, o)
}

func TestDeltaOverrideConflict(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, u3, u4 := p.add(t, e.users), p.add(t, e.users), p.add(t, e.users), p.add(t, e.users)
	sched, other := p.add(t, e.schedules), p.add(t, e.schedules)
	id := p.create(t, over(sched, u1, u2, 10, 20))

	for _, c := range []struct {
		name     string
		o        *UserOverride
		conflict bool
	}{
		{"same", over(sched, u1, u2, 10, 20), true},
		{"added twice", over(sched, u1, u3, 10, 20), true},
		{"removed twice", over(sched, u3, u2, 10, 20), true},
		{"added and removed", over(sched, u3, u1, 10, 20), true},
		{"removed and added", over(sched, u2, u3, 10, 20), true},
		{"only added", over(sched, u1, "", 10, 20), true},
		{"only removed", over(sched, "", u2, 10, 20), true},
		{"in capitals", over(strings.ToUpper(sched), strings.ToUpper(u1), "", 10, 20), true},
		{"others", over(sched, u3, u4, 10, 20), false},
		{"other schedule", over(other, u1, u2, 10, 20), false},
		{"the start", over(sched, u1, "", 9, 11), true},
		{"the end", over(sched, u1, "", 19, 21), true},
		{"inside", over(sched, u1, "", 14, 15), true},
		{"around", over(sched, u1, "", 5, 25), true},
		{"from the start", over(sched, u1, "", 10, 11), true},
		{"to the end", over(sched, u1, "", 19, 20), true},
		{"until it starts", over(sched, u1, "", 8, 10), false},
		{"once it ends", over(sched, u1, "", 20, 22), false},
		{"before", over(sched, u1, "", 2, 3), false},
		{"after", over(sched, u1, "", 30, 31), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, err := p.CreateUserOverrideTx(ctx, nil, c.o)
			if !c.conflict {
				require.NoError(t, err)
				require.NoError(t, p.DeleteUserOverrideTx(ctx, nil, n.ID))
				return
			}
			clientError(t, err, "cannot override the same user twice at the same time, check existing overrides; CONFLICTING_ID="+id)
			var cerr *filestore.CheckError
			require.ErrorAs(t, err, &cerr)
			assert.Equal(t, "user_override_no_conflict_allowed", cerr.Constraint)
			assert.Equal(t, 1, p.count(), "nothing changed")

			// The same goes for changing one.
			id2 := p.create(t, over(sched, u4, "", 100, 101))
			c.o.ID = id2
			err = p.UpdateUserOverride(ctx, c.o)
			clientError(t, err, "CONFLICTING_ID="+id)
			got, err := p.FindOneUserOverrideTx(ctx, nil, id2, false)
			require.NoError(t, err)
			assert.Equal(t, u4, got.AddUserID, "nothing changed")
			require.NoError(t, p.DeleteUserOverrideTx(ctx, nil, id2))
		})
	}

	own := over(sched, u1, u2, 12, 22)
	own.ID = id
	require.NoError(t, p.UpdateUserOverride(ctx, own), "an override doesn't conflict with itself")
}

func TestDeltaOverlaps(t *testing.T) {
	for _, c := range []struct {
		s1, e1, s2, e2 float64
		want           bool
	}{
		{0, 2, 1, 3, true},
		{1, 3, 0, 2, true},
		{0, 2, 2, 3, false}, // the end is not included
		{2, 3, 0, 2, false},
		{0, 3, 1, 2, true},
		{0, 1, 0, 5, true},
		{0, 1, 2, 3, false},
		{2, 0, 3, 1, true}, // backwards
		{0, 2, 3, 1, true},
		{0, 2, 1, 1, true}, // a moment
		{0, 2, 0, 0, true},
		{0, 2, 2, 2, false},
		{1, 1, 1, 1, true},
		{1, 1, 2, 2, false},
	} {
		assert.Equal(t, c.want, overlaps(at(c.s1), at(c.e1), at(c.s2), at(c.e2)), "%v", c)
	}
}

// TestDeltaOverrideConflictRace: on Postgres, the trigger locks the table, so
// of two conflicting overrides made at once only one succeeds.
func TestDeltaOverrideConflictRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	u1, sched := a.add(t, e.users), a.add(t, e.schedules)

	for round := range 5 {
		var created atomic.Int32
		var wg sync.WaitGroup
		for i := range 6 {
			p := []*pod{a, b}[i%2]
			wg.Add(1)
			go func() {
				defer wg.Done()
				from := float64(round*100) + float64(i)/10
				_, err := p.CreateUserOverrideTx(ctx, nil, over(sched, u1, "", from, from+8))
				if err == nil {
					created.Add(1)
					return
				}
				assert.ErrorContains(t, errutil.MapDBError(err), "CONFLICTING_ID=")
			}()
		}
		wg.Wait()
		assert.EqualValues(t, 1, created.Load(), "two pods never both make a conflicting override (F4, F8)")
	}
	assert.Equal(t, 5, e.pod(t, "c").count())
}

// TestDeltaOverrideConflictOpen is test/smoke/overrideconflict_test.go with
// two pods: a transaction that has made its override and not committed yet.
func TestDeltaOverrideConflictOpen(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	u1, sched := a.add(t, e.users), a.add(t, e.schedules)

	var once sync.Once
	opened, proceed := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- a.db.Run(ctx, nil, func(tx txn.Tx) error {
			if _, err := a.CreateUserOverrideTx(ctx, tx, over(sched, u1, "", 0, 8)); err != nil {
				return err
			}
			once.Do(func() { close(opened) })
			<-proceed
			return nil
		})
	}()
	<-opened
	_, second := b.CreateUserOverrideTx(ctx, nil, over(sched, u1, "", 4, 12))
	close(proceed)
	firstErr := <-first
	require.NoError(t, second, "the second commits first")
	clientError(t, firstErr, "CONFLICTING_ID=")
	assert.Equal(t, 1, e.pod(t, "c").count())
}

// TestDeltaOverrideUpdateRace: changing two overrides at once so that they
// conflict.
func TestDeltaOverrideUpdateRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	u1, u2, u3, sched := a.add(t, e.users), a.add(t, e.users), a.add(t, e.users), a.add(t, e.schedules)
	for round := range 5 {
		day := float64(round * 24)
		ids := []string{a.create(t, over(sched, u1, "", day, day+2)), a.create(t, over(sched, u2, "", day, day+2))}
		var ok atomic.Int32
		var wg sync.WaitGroup
		for i, id := range ids {
			o := over(sched, u3, "", day, day+2)
			o.ID = id
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := []*pod{a, b}[i].UpdateUserOverride(ctx, o)
				if err == nil {
					ok.Add(1)
					return
				}
				assert.ErrorContains(t, errutil.MapDBError(err), "CONFLICTING_ID="+ids[1-i])
			}()
		}
		wg.Wait()
		assert.EqualValues(t, 1, ok.Load(), "one of them wins (F4, F8)")
	}
}

func TestDeltaOverrideLimit(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	e.max.Store(2)
	p := e.pod(t, "a")
	u1, s1, s2 := p.add(t, e.users), p.add(t, e.schedules), p.add(t, e.schedules)
	id := p.create(t, over(s1, u1, "", 0, 1))
	p.create(t, over(s1, u1, "", 2, 3))
	p.create(t, over(s2, u1, "", 0, 1))

	_, err := p.CreateUserOverrideTx(ctx, nil, over(s1, u1, "", 4, 5))
	var le errutil.LimitError
	require.ErrorAs(t, errutil.MapDBError(err), &le, "trg_enforce_user_override_schedule_limit")
	assert.Equal(t, limit.UserOverridesPerSchedule, le.ID())
	assert.Equal(t, 2, le.Max())
	assert.Equal(t, 3, p.count(), "nothing changed")

	_, err = p.CreateUserOverrideTx(ctx, nil, over(s1, u1, "", 0.5, 1.5))
	clientError(t, err, "CONFLICTING_ID="+id) // the conflict comes first, as on Postgres

	moved := over(s2, u1, "", 10, 11)
	moved.ID = id
	require.NoError(t, p.UpdateUserOverride(ctx, moved))
	moved = over(s1, u1, "", 10, 11)
	moved.ID = id
	e.max.Store(1)
	require.NoError(t, p.UpdateUserOverride(ctx, moved), "the limit is for new overrides")

	e.max.Store(2)
	e.clock.Set(at(3)) // the one from 2 to 3 ends now, so it no longer counts
	p.create(t, over(s1, u1, "", 20, 21))
	_, err = p.CreateUserOverrideTx(ctx, nil, over(s1, u1, "", 30, 31))
	require.ErrorAs(t, errutil.MapDBError(err), &le)
	e.max.Store(0)
	_, err = p.CreateUserOverrideTx(ctx, nil, over(p.add(t, e.schedules), u1, "", 30, 31))
	require.ErrorAs(t, errutil.MapDBError(err), &le, "a limit of zero")
	e.max.Store(-1)
	p.create(t, over(s1, u1, "", 30, 31))
}

func TestDeltaOverrideLimitRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	e.max.Store(3)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	u1, sched := a.add(t, e.users), a.add(t, e.schedules)
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		p := []*pod{a, b}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.CreateUserOverrideTx(ctx, nil, over(sched, u1, "", float64(i), float64(i+1)))
			var le errutil.LimitError
			if err == nil {
				created.Add(1)
			} else {
				assert.ErrorAs(t, errutil.MapDBError(err), &le)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 3, created.Load(), "two pods never exceed the limit together (F8)")
	assert.Equal(t, 3, e.pod(t, "c").count())
}

// TestDeltaOverrideDeleteUserRace: one pod deletes a user while another adds
// an override of them.
func TestDeltaOverrideDeleteUserRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	sched := a.add(t, e.schedules)
	for range 10 {
		u := a.add(t, e.users)
		var addErr, delErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, delErr = a.fdb.Update(ctx, func(tx *filestore.Tx) error {
				_, err := e.users.Delete(tx, u)
				return err
			})
		}()
		go func() {
			defer wg.Done()
			_, addErr = b.CreateUserOverrideTx(ctx, nil, over(sched, u, "", 0, 1))
		}()
		wg.Wait()
		require.NoError(t, delErr)
		if addErr != nil {
			clientError(t, addErr, "user does not exist")
		}
		assert.Zero(t, e.pod(t, "c").count(), "no override of a deleted user (F7)")
	}
}

func TestDeltaFindAllUserOverrides(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, sched, other := p.add(t, e.users), p.add(t, e.users), p.add(t, e.schedules), p.add(t, e.schedules)
	late := p.create(t, over(sched, u1, u2, 20, 30))
	early := p.create(t, over(sched, u1, "", 0, 10))
	p.create(t, over(other, u1, "", 0, 10))

	find := func(from, to float64) []string {
		t.Helper()
		tgt := assignment.ScheduleTarget(sched)
		found, err := p.FindAllUserOverrides(ctx, at(from), at(to), tgt)
		require.NoError(t, err)
		var ids []string
		for _, o := range found {
			assert.Equal(t, tgt, o.Target)
			ids = append(ids, o.ID)
		}
		return ids
	}
	assert.Equal(t, []string{early, late}, find(-5, 50))
	assert.Equal(t, []string{early}, find(5, 20), "the end of the range is not included")
	assert.Equal(t, []string{late}, find(10, 21), "nor is the end of an override")
	assert.Nil(t, find(10, 20), "nil, as on Postgres")
	assert.Equal(t, []string{late}, find(25, 15), "a range can be given backwards, as on Postgres")
	assert.Equal(t, []string{early}, find(5, 5))

	found, err := p.FindAllUserOverrides(ctx, at(20), at(21), assignment.ScheduleTarget(strings.ToUpper(sched)))
	require.NoError(t, err)
	require.Len(t, found, 1)
	want := over(strings.ToUpper(sched), u1, u2, 20, 30) // the caller's target, as on Postgres
	assertSame(t, want, found[0])
	_, err = p.FindAllUserOverrides(ctx, at(0), at(1), assignment.RotationTarget(sched))
	assert.True(t, validation.IsValidationError(err))
}

func TestDeltaOverrideSearch(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u1, u2, u3 := p.add(t, e.users), p.add(t, e.users), p.add(t, e.users)
	s1, s2 := p.add(t, e.schedules), p.add(t, e.schedules)
	names := make(map[string]string)
	ids := make(map[string]string)
	for _, c := range []struct {
		name string
		o    *UserOverride
	}{
		{"e", over(s1, u1, "", 40, 41)},
		{"a", over(s1, u1, u2, 0, 4)},
		{"c", over(s1, "", u1, 10, 12)},
		{"d", over(s2, u2, u3, 10, 14)},
		{"b", over(s2, u3, u1, 2, 3)},
	} {
		id := p.create(t, c.o)
		names[id], ids[c.name] = c.name, id
	}
	search := func(opts *SearchOptions) string {
		t.Helper()
		found, err := p.Search(ctx, nil, opts)
		require.NoError(t, err)
		require.NotNil(t, found, "an empty list, not nil, as on Postgres")
		out := ""
		for _, o := range found {
			out += names[o.ID]
		}
		return out
	}

	assert.Equal(t, "abcde", search(nil), "by start, then end")
	assert.Equal(t, "abcde", search(&SearchOptions{}))
	assert.Equal(t, "ab", search(&SearchOptions{Limit: 2}))
	assert.Equal(t, "abcde", search(&SearchOptions{Limit: -1}), "as on Postgres")
	assert.Equal(t, "bde", search(&SearchOptions{Omit: []string{ids["a"], strings.ToUpper(ids["c"])}}))
	assert.Equal(t, "ace", search(&SearchOptions{ScheduleID: strings.ToUpper(s1)}))
	assert.Equal(t, "", search(&SearchOptions{ScheduleID: uuid.NewString()}))
	assert.Equal(t, "abce", search(&SearchOptions{AnyUserIDs: []string{u1}}))
	assert.Equal(t, "abcde", search(&SearchOptions{AnyUserIDs: []string{u1, u2}}), "each once")
	assert.Equal(t, "ae", search(&SearchOptions{AddUserIDs: []string{u1}}))
	assert.Equal(t, "abe", search(&SearchOptions{AddUserIDs: []string{u1, u3}}))
	assert.Equal(t, "bc", search(&SearchOptions{RemoveUserIDs: []string{u1}}))
	assert.Equal(t, "b", search(&SearchOptions{AddUserIDs: []string{u3}, RemoveUserIDs: []string{u1}}))
	assert.Equal(t, "ad", search(&SearchOptions{AnyUserIDs: []string{u2}, AddUserIDs: []string{u1, u2}}))
	assert.Equal(t, "d", search(&SearchOptions{AnyUserIDs: []string{u2}, ScheduleID: s2}))
	assert.Equal(t, "acde", search(&SearchOptions{Start: at(3)}), "those that end after it")
	assert.Equal(t, "abcd", search(&SearchOptions{End: at(10)}), "those that start by it")
	assert.Equal(t, "acd", search(&SearchOptions{Start: at(3), End: at(10)}))
	assert.Equal(t, "cde", search(&SearchOptions{After: SearchCursor{ID: ids["b"]}}))
	assert.Equal(t, "", search(&SearchOptions{After: SearchCursor{ID: ids["e"]}}))
	assert.Equal(t, "ce", search(&SearchOptions{After: SearchCursor{ID: ids["b"]}, ScheduleID: s1}))
	assert.Equal(t, "", search(&SearchOptions{After: SearchCursor{ID: uuid.NewString()}}), "nothing comes after a missing override")

	// Ties are in order of id, and at most 150 come back.
	s3 := p.add(t, e.schedules)
	_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		for i := range 160 {
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", (i*7)%160)
			user := uuid.NewString()
			if err := e.users.Put(tx, idRow{ID: user}); err != nil {
				return err
			}
			if err := e.tables.Overrides.Put(tx, OverrideRow{ID: id, AddUserID: &user, Start: at(0), End: at(1), ScheduleID: s3}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	found, err := p.Search(ctx, nil, &SearchOptions{ScheduleID: s3, Limit: 200})
	require.NoError(t, err)
	require.Len(t, found, 150)
	for i, o := range found {
		require.Equal(t, fmt.Sprintf("00000000-0000-4000-8000-%012d", i), o.ID)
	}
	found, err = p.Search(ctx, nil, &SearchOptions{ScheduleID: s3, Limit: 200, After: SearchCursor{ID: found[149].ID}})
	require.NoError(t, err)
	assert.Len(t, found, 10)

	_, err = p.Search(context.Background(), nil, nil)
	assert.True(t, permission.IsPermissionError(err))
	_, err = p.Search(ctx, nil, &SearchOptions{AnyUserIDs: []string{"bob"}})
	assert.True(t, validation.IsValidationError(err))
}
