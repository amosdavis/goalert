package oncall

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "time/tzdata" // like cmd/goalert; not every machine has zone data

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/override"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/timeutil"
)

var deltaNow = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// parentRow stands in for a row of another package's table.
type parentRow struct {
	ID   string `parquet:"id"`
	Name string `parquet:"name"`
}

type deltaEnv struct {
	store     objstore.Store
	schema    *filestore.Schema
	tables    *DeltaTables
	schedT    *schedule.DeltaTables
	ruleT     *rule.DeltaTables
	users     *filestore.Table[parentRow]
	rotations *filestore.Table[parentRow]
	steps     *filestore.Table[parentRow]
	clock     *filestore.FakeClock

	// What the lookups return.
	serviceSteps map[string][]DeltaStep
	rots         map[string]DeltaRotation
	overrides    map[string][]override.UserOverride
}

// deltaPod is one GoAlert pod.
type deltaPod struct {
	*Store
	db     txn.DB
	scheds *schedule.Store
	rules  *rule.Store
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(deltaNow)
	clock.SetSpeed(0)
	clock.Set(deltaNow)
	e := &deltaEnv{
		store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock,
		serviceSteps: make(map[string][]DeltaStep),
		rots:         make(map[string]DeltaRotation),
		overrides:    make(map[string][]override.UserOverride),
	}
	key := func(r parentRow) string { return r.ID }
	e.users = filestore.NewTable(e.schema, "users", key)
	e.rotations = filestore.NewTable(e.schema, "rotations", key)
	e.steps = filestore.NewTable(e.schema, "escalation_policy_steps", key)
	e.schedT = schedule.DefineDeltaTables(e.schema)
	e.ruleT = rule.DefineDeltaTables(e.schema)
	e.tables = DefineDeltaTables(e.schema)
	rule.DeclareDeltaForeignKeys(e.ruleT, e.schedT.Schedules, e.rotations, e.users)
	DeclareDeltaForeignKeys(e.tables, e.schedT.Schedules, e.steps, e.users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

func (e *deltaEnv) lookups() DeltaLookups {
	return DeltaLookups{
		ScheduleTimeZone: e.schedT.TimeZone,
		UserName: func(r filestore.Reader, userID string) (string, bool) {
			u, ok := e.users.Get(r, userID)
			return u.Name, ok
		},
		ServiceSteps: func(_ filestore.Reader, serviceID string) []DeltaStep { return e.serviceSteps[serviceID] },
		Rotation: func(_ filestore.Reader, rotationID string) (DeltaRotation, bool) {
			rot, ok := e.rots[rotationID]
			return rot, ok
		},
		Overrides: func(_ filestore.Reader, scheduleID string) []override.UserOverride { return e.overrides[scheduleID] },
	}
}

func (e *deltaEnv) pod(t *testing.T, name string) *deltaPod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	p := &deltaPod{db: deltatxn.DB{DB: fdb}}
	none := func(filestore.Reader) int { return -1 }
	p.rules = rule.NewDeltaStore(p.db, e.ruleT, rule.DeltaLimits{RulesPerSchedule: none, TargetsPerSchedule: none})
	p.scheds = schedule.NewDeltaStore(p.db, e.schedT, schedule.DeltaLookups{
		UserExists: func(r filestore.Reader, userID string) bool { _, ok := e.users.Get(r, userID); return ok },
	})
	p.Store = NewDeltaStore(p.db, e.tables, p.rules, p.scheds, e.lookups())
	return p
}

// update runs fn in a transaction.
func (p *deltaPod) update(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	require.NoError(t, p.try(fn))
}

func (p *deltaPod) try(fn func(tx *filestore.Tx) error) error {
	return p.db.Run(context.Background(), nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return fn(ftx)
	})
}

func (p *deltaPod) read(t *testing.T, fn func(r filestore.Reader)) {
	t.Helper()
	require.NoError(t, p.db.Run(context.Background(), &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, r := deltatxn.Unwrap(tx)
		fn(r)
		return nil
	}))
}

func (p *deltaPod) add(t *testing.T, table *filestore.Table[parentRow], name string) string {
	t.Helper()
	id := uuid.NewString()
	p.update(t, func(tx *filestore.Tx) error { return table.Put(tx, parentRow{ID: id, Name: name}) })
	return id
}

func (p *deltaPod) addSchedule(t *testing.T, name, tz string) string {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	require.NoError(t, err)
	s, err := p.scheds.Create(system(), &schedule.Schedule{Name: name, Description: "test", TimeZone: loc})
	require.NoError(t, err)
	return s.ID
}

func (p *deltaPod) startSchedule(t *testing.T, scheduleID, userID string) {
	t.Helper()
	p.update(t, func(tx *filestore.Tx) error { return p.delta.StartScheduleOnCall(tx, scheduleID, userID) })
}

func (p *deltaPod) endSchedule(t *testing.T, scheduleID, userID string) {
	t.Helper()
	p.update(t, func(tx *filestore.Tx) error { return p.delta.EndScheduleOnCall(tx, scheduleID, userID) })
}

func (p *deltaPod) startStep(t *testing.T, stepID, userID string) bool {
	t.Helper()
	var isNew bool
	p.update(t, func(tx *filestore.Tx) (err error) {
		isNew, err = p.delta.StartStepOnCall(tx, stepID, userID)
		return err
	})
	return isNew
}

func (p *deltaPod) endStep(t *testing.T, stepID, userID string) {
	t.Helper()
	p.update(t, func(tx *filestore.Tx) error { return p.delta.EndStepOnCall(tx, stepID, userID) })
}

func requireConstraint(t *testing.T, err error, name string) {
	t.Helper()
	var c interface{ ConstraintName() string }
	require.ErrorAs(t, err, &c, "expected a constraint error, got %v", err)
	assert.Equal(t, name, c.ConstraintName())
}

func system() context.Context { return permission.SystemContext(context.Background(), "test") }

func TestDeltaOnCallBySchedule(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, other := a.addSchedule(t, "One", "UTC"), a.addSchedule(t, "Two", "UTC")
	alice, bob := a.add(t, e.users, "Alice"), a.add(t, e.users, "Bob")

	got, err := a.OnCallUsersBySchedule(ctx, sched)
	require.NoError(t, err)
	assert.Nil(t, got, "nil, as on Postgres")

	a.startSchedule(t, sched, bob)
	a.startSchedule(t, sched, alice)
	a.startSchedule(t, other, alice)
	got, err = a.OnCallUsersBySchedule(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, []ScheduleOnCallUser{{ID: bob, Name: "Bob"}, {ID: alice, Name: "Alice"}}, got)

	e.clock.Advance(time.Hour)
	a.endSchedule(t, sched, bob)
	a.endSchedule(t, sched, bob) // again: an UPDATE of no rows
	got, err = e.pod(t, "b").OnCallUsersBySchedule(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, []ScheduleOnCallUser{{ID: alice, Name: "Alice"}}, got, "a new pod reads the same")

	// Bob's shift is kept, and he can go on call again.
	e.clock.Advance(time.Hour)
	a.startSchedule(t, sched, bob)
	a.read(t, func(r filestore.Reader) {
		var rows []ScheduleOnCallRow
		e.tables.ScheduleOnCall.Ascend(r, func(row ScheduleOnCallRow) bool { rows = append(rows, row); return true })
		require.Len(t, rows, 4)
		assert.EqualValues(t, 1, rows[0].ID, "ids from schedule_on_call_users_id_seq")
		assert.True(t, rows[0].StartTime.Equal(deltaNow))
		if assert.NotNil(t, rows[0].EndTime) {
			assert.True(t, rows[0].EndTime.Equal(deltaNow.Add(time.Hour)))
		}
		assert.Nil(t, rows[3].EndTime)
		assert.Equal(t, []string{alice, bob}, e.tables.ScheduleUsers(r, sched), "oldest shift first")

		var open []int64
		e.tables.EachOpenSchedule(r, func(row ScheduleOnCallRow) bool { open = append(open, row.ID); return true })
		assert.Equal(t, []int64{2, 3, 4}, open, "open rows of every schedule")
	})
}

func TestDeltaOnCallByService(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	alice, bob, carol := a.add(t, e.users, "Alice"), a.add(t, e.users, "Bob"), a.add(t, e.users, "Carol")
	step0, step1, step2 := a.add(t, e.steps, ""), a.add(t, e.steps, ""), a.add(t, e.steps, "")
	svc := uuid.NewString()
	e.serviceSteps[svc] = []DeltaStep{{ID: step1, Number: 1}, {ID: step0, Number: 0}} // in no order

	got, err := a.OnCallUsersByService(ctx, svc)
	require.NoError(t, err)
	assert.Nil(t, got, "nil, as on Postgres")
	got, err = a.OnCallUsersByService(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, got, "a missing service has no one on call")

	assert.True(t, a.startStep(t, step1, alice))
	e.clock.Advance(time.Minute)
	assert.True(t, a.startStep(t, step0, carol))
	e.clock.Advance(time.Minute)
	assert.True(t, a.startStep(t, step0, alice))
	assert.True(t, a.startStep(t, step0, bob))
	assert.True(t, a.startStep(t, step2, bob))
	assert.False(t, a.startStep(t, step0, bob), "already on call (ON CONFLICT DO NOTHING)")
	want := []ServiceOnCallUser{
		{StepNumber: 0, UserID: carol, UserName: "Carol"},
		{StepNumber: 0, UserID: alice, UserName: "Alice"},
		{StepNumber: 0, UserID: bob, UserName: "Bob"},
		{StepNumber: 1, UserID: alice, UserName: "Alice"},
	}
	got, err = e.pod(t, "b").OnCallUsersByService(ctx, svc)
	require.NoError(t, err)
	assert.Equal(t, want, got, "by step, then by when they went on call")

	a.read(t, func(r filestore.Reader) {
		assert.Equal(t, []string{step1, step0}, e.tables.UserSteps(r, alice))
		assert.Equal(t, []string{carol, alice, bob}, e.tables.StepUsers(r, step0))
		n := 0
		e.tables.EachOpenStep(r, func(StepOnCallRow) bool { n++; return true })
		assert.Equal(t, 5, n)
	})

	e.clock.Advance(time.Minute)
	a.endStep(t, step0, alice)
	a.endStep(t, step0, alice) // again: an UPDATE of no rows
	got, err = a.OnCallUsersByService(ctx, svc)
	require.NoError(t, err)
	assert.Equal(t, []ServiceOnCallUser{want[0], want[2], want[3]}, got)
	assert.True(t, a.startStep(t, step0, alice), "a new shift")
	a.read(t, func(r filestore.Reader) {
		assert.Equal(t, 6, e.tables.StepOnCall.Len(r), "the old shift is kept")
	})
}

func TestDeltaOnCallConstraints(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, usr, step := a.addSchedule(t, "One", "UTC"), a.add(t, e.users, "Alice"), a.add(t, e.steps, "")
	start := func(schedID, userID string) error {
		return a.try(func(tx *filestore.Tx) error { return a.delta.StartScheduleOnCall(tx, schedID, userID) })
	}
	startStep := func(stepID, userID string) error {
		return a.try(func(tx *filestore.Tx) error { _, err := a.delta.StartStepOnCall(tx, stepID, userID); return err })
	}

	// Foreign keys.
	requireConstraint(t, start(uuid.NewString(), usr), "schedule_on_call_users_schedule_id_fkey")
	requireConstraint(t, start(sched, uuid.NewString()), "schedule_on_call_users_user_id_fkey")
	requireConstraint(t, startStep(uuid.NewString(), usr), "ep_step_on_call_users_ep_step_id_fkey")
	requireConstraint(t, startStep(step, uuid.NewString()), "ep_step_on_call_users_user_id_fkey")

	// One open row for each schedule and user, and for each step and user.
	require.NoError(t, start(sched, usr))
	requireConstraint(t, start(sched, usr), "idx_schedule_on_call_once")
	require.NoError(t, startStep(step, usr))
	requireConstraint(t, a.try(func(tx *filestore.Tx) error {
		return e.tables.StepOnCall.Put(tx, StepOnCallRow{ID: 99, StepID: step, UserID: usr, StartTime: tx.Now()})
	}), "idx_ep_step_on_call")
	requireConstraint(t, a.try(func(tx *filestore.Tx) error {
		return e.tables.StepOnCall.Insert(tx, StepOnCallRow{ID: 1, StepID: step, UserID: usr, StartTime: tx.Now()}, "ep_step_on_call_users_uniq_id")
	}), "ep_step_on_call_users_uniq_id")

	// An id that is taken is refused, and its row is not replaced.
	other := a.add(t, e.users, "Bob")
	a.update(t, func(tx *filestore.Tx) error {
		row, ok := e.tables.ScheduleOnCall.Get(tx, filestore.IntKey(1))
		require.True(t, ok)
		row.ID, row.UserID = 2, other // what the sequence gives next
		return e.tables.PutScheduleOnCall(tx, row)
	})
	requireConstraint(t, start(sched, a.add(t, e.users, "Carol")), "schedule_on_call_users_uniq_id")
	a.read(t, func(r filestore.Reader) {
		row, _ := e.tables.ScheduleOnCall.Get(r, filestore.IntKey(2))
		assert.Equal(t, other, row.UserID)
	})
	a.update(t, func(tx *filestore.Tx) error {
		_, err := e.tables.ScheduleOnCall.Delete(tx, filestore.IntKey(2))
		return err
	})

	// A shift ends after it starts, even when the clock has not advanced.
	a.endSchedule(t, sched, usr)
	a.read(t, func(r filestore.Reader) {
		var ended []ScheduleOnCallRow
		e.tables.schedBySchedule.Ascend(r, sched, func(row ScheduleOnCallRow) bool {
			if row.UserID == usr && row.EndTime != nil {
				ended = append(ended, row)
			}
			return true
		})
		require.Len(t, ended, 1)
		assert.Equal(t, deltaNow.Add(time.Microsecond), *ended[0].EndTime)
	})
	for _, d := range []time.Duration{-time.Second, 0, 100 * time.Nanosecond} {
		end := deltaNow.Add(d)
		requireConstraint(t, a.try(func(tx *filestore.Tx) error {
			return e.tables.PutScheduleOnCall(tx, ScheduleOnCallRow{ID: 99, ScheduleID: sched, UserID: usr, StartTime: deltaNow, EndTime: &end})
		}), "schedule_on_call_users_check")
	}
	e.clock.Advance(time.Microsecond)
	a.endSchedule(t, sched, usr)
	a.endStep(t, step, usr)

	// Rows with an end time are outside the unique indexes.
	require.NoError(t, start(sched, usr))
	require.NoError(t, startStep(step, usr))
}

func TestDeltaOnCallCascades(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, sched2 := a.addSchedule(t, "One", "UTC"), a.addSchedule(t, "Two", "UTC")
	alice, bob := a.add(t, e.users, "Alice"), a.add(t, e.users, "Bob")
	step, step2 := a.add(t, e.steps, ""), a.add(t, e.steps, "")
	for _, usr := range []string{alice, bob} {
		a.startSchedule(t, sched, usr)
		a.startSchedule(t, sched2, usr)
		a.startStep(t, step, usr)
		a.startStep(t, step2, usr)
	}
	e.clock.Advance(time.Hour)
	a.endSchedule(t, sched, alice) // history goes too
	a.endStep(t, step, alice)
	rows := func(scheds, steps int) {
		t.Helper()
		a.read(t, func(r filestore.Reader) {
			assert.Equal(t, scheds, e.tables.ScheduleOnCall.Len(r))
			assert.Equal(t, steps, e.tables.StepOnCall.Len(r))
		})
	}
	rows(4, 4)

	a.update(t, func(tx *filestore.Tx) error { _, err := e.users.Delete(tx, alice); return err })
	rows(2, 2)
	a.update(t, func(tx *filestore.Tx) error { _, err := e.steps.Delete(tx, step); return err })
	rows(2, 1)
	require.NoError(t, a.scheds.Delete(system(), sched))
	rows(1, 1)
}

// Two engines at once (F28) can't put a user on call twice: Postgres has
// unique indexes for that.
func TestDeltaOnCallStartRace(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, usr, step := a.addSchedule(t, "One", "UTC"), a.add(t, e.users, "Alice"), a.add(t, e.steps, "")
	pods := []*deltaPod{a, e.pod(t, "b")}

	var started, stepStarted, refused atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := pods[i%2]
			err := p.try(func(tx *filestore.Tx) error { return p.delta.StartScheduleOnCall(tx, sched, usr) })
			if err == nil {
				started.Add(1)
			} else {
				requireConstraint(t, err, "idx_schedule_on_call_once")
				refused.Add(1)
			}
			var isNew bool
			assert.NoError(t, p.try(func(tx *filestore.Tx) (err error) {
				isNew, err = p.delta.StartStepOnCall(tx, step, usr)
				return err
			}))
			if isNew {
				stepStarted.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, started.Load())
	assert.EqualValues(t, 7, refused.Load())
	assert.EqualValues(t, 1, stepStarted.Load())
	fresh := e.pod(t, "reader")
	fresh.read(t, func(r filestore.Reader) {
		assert.Equal(t, 1, e.tables.ScheduleOnCall.Len(r), "one open row (F4)")
		assert.Equal(t, 1, e.tables.StepOnCall.Len(r), "one open row (F4)")
	})
}

// A pod that doesn't know yet that the user was deleted is refused (F7).
func TestDeltaOnCallDeletedUserRace(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, usr, step := a.addSchedule(t, "One", "UTC"), a.add(t, e.users, "Alice"), a.add(t, e.steps, "")
	b := e.pod(t, "b")
	a.update(t, func(tx *filestore.Tx) error { _, err := e.users.Delete(tx, usr); return err })

	err := b.try(func(tx *filestore.Tx) error { return b.delta.StartScheduleOnCall(tx, sched, usr) })
	requireConstraint(t, err, "schedule_on_call_users_user_id_fkey")
	err = b.try(func(tx *filestore.Tx) error { _, err := b.delta.StartStepOnCall(tx, step, usr); return err })
	requireConstraint(t, err, "ep_step_on_call_users_user_id_fkey")
}

// shiftTexts are the shifts in UTC, with the users' names.
func shiftTexts(shifts []Shift, names map[string]string) []string {
	var res []string
	for _, s := range shifts {
		text := fmt.Sprintf("%s %s - %s", names[s.UserID], s.Start.UTC().Format("02 15:04"), s.End.UTC().Format("02 15:04"))
		if s.Truncated {
			text += " ..."
		}
		res = append(res, text)
	}
	return res
}

func TestDeltaHistoryBySchedule(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched := a.addSchedule(t, "One", "America/Chicago") // UTC-6 on this day
	ids, names := make(map[string]string), make(map[string]string)
	for _, n := range []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace"} {
		ids[n] = a.add(t, e.users, n)
		names[ids[n]] = n
	}
	at := func(day, hour, min int) time.Time { return time.Date(2026, 11, day, hour, min, 0, 0, time.UTC) }

	// History: one shift ended, one is too short to show, one is still open.
	history := func(id int64, user string, start time.Time, end *time.Time) {
		a.update(t, func(tx *filestore.Tx) error {
			return e.tables.PutScheduleOnCall(tx, ScheduleOnCallRow{ID: id, ScheduleID: sched, UserID: ids[user], StartTime: start, EndTime: end})
		})
	}
	seven, nine, short, old := at(25, 7, 0), at(25, 9, 0), at(25, 9, 1), at(24, 3, 0)
	history(1, "grace", at(24, 2, 0), &old) // before the range
	history(2, "alice", seven, &nine)
	history(3, "bob", nine, &short)
	history(4, "alice", at(25, 11, 0), nil)

	// Rules: carol from 8am to 5pm, and a rotation that hands off at midnight,
	// both in Chicago. The rotation started before daylight saving time ended
	// on November 1, so its handoff was at 05:00 UTC then, and is at 06:00 now.
	chicago, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	rot, empty := a.add(t, e.rotations, "daily"), a.add(t, e.rotations, "empty")
	e.rots[rot] = DeltaRotation{
		Type: rotation.TypeDaily, ShiftLength: 1, TimeZone: "America/Chicago",
		Start:    time.Date(2026, 10, 20, 0, 0, 0, 0, chicago).UTC(),
		Position: 0, ShiftStart: at(25, 6, 0), UserIDs: []string{ids["dave"], ids["erin"]},
	}
	carol := rule.NewAlwaysActive(sched, assignment.UserTarget(ids["carol"]))
	carol.Start, carol.End = timeutil.NewClock(8, 0), timeutil.NewClock(17, 0)
	for _, r := range []*rule.Rule{
		carol,
		rule.NewAlwaysActive(sched, assignment.RotationTarget(rot)),
		rule.NewAlwaysActive(sched, assignment.RotationTarget(empty)), // has no state: no one
	} {
		_, err := a.rules.Add(ctx, r)
		require.NoError(t, err)
	}

	e.overrides[sched] = []override.UserOverride{
		{ID: "c", Start: at(26, 13, 0), End: at(26, 14, 0), AddUserID: ids["bob"]},                                          // after the range
		{ID: "b", Start: at(25, 15, 0), End: at(25, 16, 0), RemoveUserID: ids["carol"], AddUserID: ids["frank"]},            // replace
		{ID: "a", Start: at(25, 8, 0), End: at(25, 11, 30), RemoveUserID: ids["alice"], AddUserID: ids["bob"]},              // has ended
		{ID: "d", Start: at(25, 22, 0), End: at(26, 0, 0), AddUserID: ids["bob"], Target: assignment.ScheduleTarget(sched)}, // add
	}
	a.update(t, func(tx *filestore.Tx) error {
		return e.schedT.UpdateData(tx, uuid.MustParse(sched), func(data *schedule.Data) error {
			data.V1.TemporarySchedules = []schedule.TemporarySchedule{{
				Start: at(25, 18, 0), End: at(25, 20, 0),
				Shifts: []schedule.FixedShift{
					{Start: at(25, 18, 0), End: at(25, 19, 0), UserID: ids["grace"]},
					{Start: at(25, 18, 0), End: at(25, 20, 0), UserID: uuid.NewString()}, // no such user
				},
			}}
			return nil
		})
	})

	want := []string{
		"alice 25 07:00 - 25 09:00",
		"alice 25 11:00 - 25 12:01", // history is used up to the current minute
		"dave 25 12:01 - 25 18:00",
		"carol 25 14:00 - 25 15:00",
		"frank 25 15:00 - 25 16:00",
		"carol 25 16:00 - 25 18:00",
		"grace 25 18:00 - 25 19:00",
		"carol 25 20:00 - 25 23:00",
		"dave 25 20:00 - 26 06:00",
		"bob 25 22:00 - 26 00:00",
		"erin 26 06:00 - 26 12:00 ...",
	}
	for _, p := range []*deltaPod{a, e.pod(t, "b")} {
		for range 2 { // reading changes nothing
			shifts, err := p.HistoryBySchedule(ctx, sched, at(25, 6, 0), at(26, 12, 0))
			require.NoError(t, err)
			assert.ElementsMatch(t, want, shiftTexts(shifts, names))
		}
	}
	assert.Equal(t, 0, e.rots[rot].Position)
	assert.Equal(t, "c", e.overrides[sched][0].ID, "what a lookup returned is left alone")

	// Time passes: the clock is the store's, not the machine's.
	e.clock.Set(at(25, 14, 30))
	shifts, err := a.HistoryBySchedule(ctx, sched, at(25, 10, 0), at(25, 15, 30))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"alice 25 11:00 - 25 14:31", // still open
		"carol 25 14:31 - 25 15:00",
		"dave 25 14:31 - 25 15:30 ...",
		"frank 25 15:00 - 25 15:30 ...",
	}, shiftTexts(shifts, names))
}

func TestDeltaHistoryByScheduleErrors(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched := a.addSchedule(t, "One", "UTC")

	_, err := a.HistoryBySchedule(ctx, uuid.NewString(), deltaNow, deltaNow.Add(time.Hour))
	assert.ErrorIs(t, err, sql.ErrNoRows)
	_, err = a.HistoryBySchedule(ctx, sched, deltaNow, deltaNow.Add(-time.Hour))
	assert.ErrorContains(t, err, "range lower bound", "as on Postgres")
	shifts, err := a.HistoryBySchedule(ctx, sched, deltaNow, deltaNow.Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, shifts)
	shifts, err = a.HistoryBySchedule(ctx, sched, deltaNow, deltaNow)
	require.NoError(t, err)
	assert.Empty(t, shifts)

	rot := a.add(t, e.rotations, "broken")
	e.rots[rot] = DeltaRotation{Type: rotation.TypeDaily, ShiftLength: 1, TimeZone: "Nowhere/Special"}
	_, err = a.rules.Add(ctx, rule.NewAlwaysActive(sched, assignment.RotationTarget(rot)))
	require.NoError(t, err)
	_, err = a.HistoryBySchedule(ctx, sched, deltaNow, deltaNow.Add(time.Hour))
	assert.ErrorContains(t, err, "load time zone info")

	bare := NewDeltaStore(a.db, e.tables, a.rules, a.scheds, DeltaLookups{})
	_, err = bare.HistoryBySchedule(ctx, sched, deltaNow, deltaNow.Add(time.Hour))
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted)
	_, err = bare.OnCallUsersBySchedule(ctx, sched)
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted)
	_, err = bare.OnCallUsersByService(ctx, uuid.NewString())
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted)
}

// The bounds of the history query (tstzrange && tstzrange).
func TestDeltaShiftsBounds(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.pod(t, "a")
	sched, usr := a.addSchedule(t, "One", "UTC"), a.add(t, e.users, "Alice")
	at := func(min int) time.Time { return deltaNow.Add(time.Duration(min) * time.Minute) }
	end := at(10)
	a.update(t, func(tx *filestore.Tx) error {
		return e.tables.PutScheduleOnCall(tx, ScheduleOnCallRow{ID: 1, ScheduleID: sched, UserID: usr, StartTime: at(0), EndTime: &end})
	})
	a.update(t, func(tx *filestore.Tx) error {
		return e.tables.PutScheduleOnCall(tx, ScheduleOnCallRow{ID: 2, ScheduleID: sched, UserID: usr, StartTime: at(60)})
	})
	for _, c := range []struct{ start, end, want int }{
		{-10, 0, 0}, {-10, 1, 1}, {0, 10, 1}, {9, 20, 1}, {10, 20, 0}, {5, 5, 0},
		{20, 60, 0}, {20, 61, 1}, {0, 100, 2}, {1000, 2000, 1}, // an open row has no end
	} {
		a.read(t, func(r filestore.Reader) {
			shifts, err := e.tables.shifts(r, sched, at(c.start), at(c.end))
			require.NoError(t, err)
			assert.Len(t, shifts, c.want, "%d to %d", c.start, c.end)
		})
	}
}

// The SQL operator OVERLAPS, with values checked against its definition.
func TestDeltaOverlaps(t *testing.T) {
	at := func(min int) time.Time { return deltaNow.Add(time.Duration(min) * time.Minute) }
	for _, c := range []struct {
		aStart, aEnd, bStart, bEnd int
		want                       bool
	}{
		{0, 10, 5, 15, true},
		{0, 10, 10, 15, false}, // half open
		{10, 15, 0, 10, false},
		{0, 10, 2, 3, true},
		{2, 3, 0, 10, true},
		{10, 0, 5, 15, true}, // either order
		{5, 5, 0, 10, true},  // an instant
		{0, 0, 0, 10, true},
		{10, 10, 0, 10, false},
		{0, 10, 0, 5, true},
		{20, 30, 0, 10, false},
	} {
		got := overlaps(at(c.aStart), at(c.aEnd), at(c.bStart), at(c.bEnd))
		assert.Equal(t, c.want, got, "%+v", c)
	}
}
