package rule

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata" // like cmd/goalert; not every machine has zone data

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/util/timeutil"
)

var start = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// parentRow stands in for a row of another package's table.
type parentRow struct {
	ID string `parquet:"id"`
}

type deltaEnv struct {
	store     objstore.Store
	schema    *filestore.Schema
	tables    *DeltaTables
	schedules *filestore.Table[parentRow]
	rotations *filestore.Table[parentRow]
	users     *filestore.Table[parentRow]
	clock     *filestore.FakeClock
	limits    DeltaLimits
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	key := func(r parentRow) string { return r.ID }
	e.schedules = filestore.NewTable(e.schema, "schedules", key)
	e.rotations = filestore.NewTable(e.schema, "rotations", key)
	e.users = filestore.NewTable(e.schema, "users", key)
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.schedules, e.rotations, e.users)
	none := func(filestore.Reader) int { return -1 }
	e.limits = DeltaLimits{RulesPerSchedule: none, TargetsPerSchedule: none}
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) (*Store, txn.DB) {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return NewDeltaStore(db, e.tables, e.limits), db
}

// add adds a row to a stand-in table, and returns its id.
func add(t *testing.T, db txn.DB, table *filestore.Table[parentRow]) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, db.Run(context.Background(), nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return table.Put(ftx, parentRow{ID: id})
	}))
	return id
}

func remove(t *testing.T, db txn.DB, table *filestore.Table[parentRow], id string) {
	t.Helper()
	require.NoError(t, db.Run(context.Background(), nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		_, err := table.Delete(ftx, id)
		return err
	}))
}

func requireConstraint(t *testing.T, err error, name string) {
	t.Helper()
	var ce interface{ ConstraintName() string }
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, name, ce.ConstraintName())
}

func system() context.Context { return permission.SystemContext(context.Background(), "test") }

func ids(rules []Rule) []string {
	res := make([]string, len(rules))
	for i, r := range rules {
		res[i] = r.ID
	}
	return res
}

func TestDeltaRules(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, other := add(t, db, e.schedules), add(t, db, e.schedules)
	usr, rot := add(t, db, e.users), add(t, db, e.rotations)

	weekdays := timeutil.WeekdayFilter{0, 1, 1, 1, 1, 1, 0}
	first, err := s.Add(ctx, &Rule{
		ScheduleID: strings.ToUpper(sched), WeekdayFilter: weekdays,
		Start: timeutil.NewClock(8, 30), End: timeutil.NewClock(17, 0),
		Target: assignment.UserTarget(strings.ToUpper(usr)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.ID)
	e.clock.Advance(time.Minute)
	second, err := s.CreateRuleTx(ctx, nil, NewAlwaysActive(sched, assignment.RotationTarget(rot)))
	require.NoError(t, err)
	e.clock.Advance(time.Minute)
	_, err = s.Add(ctx, NewAlwaysActive(other, assignment.UserTarget(usr)))
	require.NoError(t, err)

	all, err := s.FindAll(ctx, strings.ToUpper(sched))
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, Rule{
		ID: first.ID, ScheduleID: sched, WeekdayFilter: weekdays,
		Start: timeutil.NewClock(8, 30), End: timeutil.NewClock(17, 0), Target: assignment.UserTarget(usr),
	}, all[0], "oldest first, with ids as Postgres returns them, and no created_at")
	assert.Equal(t, second.ID, all[1].ID)
	assert.True(t, all[1].AlwaysActive())
	assert.Equal(t, assignment.RotationTarget(rot), all[1].Target)

	none, err := s.FindAll(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, none, "nil, as on Postgres")

	byUser, err := s.FindByTargetTx(ctx, nil, sched, assignment.UserTarget(strings.ToUpper(usr)))
	require.NoError(t, err)
	assert.Equal(t, []string{first.ID}, ids(byUser), "only this schedule's")
	byRot, err := s.FindByTargetTx(ctx, nil, sched, assignment.RotationTarget(rot))
	require.NoError(t, err)
	assert.Equal(t, []string{second.ID}, ids(byRot))
	byRot, err = s.FindByTargetTx(ctx, nil, sched, assignment.RotationTarget(usr))
	require.NoError(t, err)
	assert.Nil(t, byRot, "a user's id is not a rotation")

	// An update replaces the target and the times, but not the order.
	change := all[0]
	change.Target = assignment.RotationTarget(rot)
	change.WeekdayFilter = timeutil.EveryDay()
	change.Start, change.End = timeutil.NewClock(0, 0), timeutil.NewClock(23, 59)
	require.NoError(t, s.UpdateTx(ctx, nil, &change))
	all, err = s.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, []string{first.ID, second.ID}, ids(all))
	assert.Equal(t, change, all[0])

	missing := change
	missing.ID = uuid.NewString()
	require.NoError(t, s.UpdateTx(ctx, nil, &missing), "an UPDATE of no rows")
	missing.ID = "nonsense"
	require.Error(t, s.UpdateTx(ctx, nil, &missing))

	// A transaction sees its own changes.
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		r, err := s.CreateRuleTx(ctx, tx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
		require.NoError(t, err)
		got, err := s.FindAllTx(ctx, tx, sched)
		require.NoError(t, err)
		assert.Len(t, got, 3)
		return s.DeleteManyTx(ctx, tx, []string{r.ID})
	}))

	fresh, _ := e.pod(t, "b")
	got, err := fresh.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, all, got, "a new pod reads the same rules")

	require.NoError(t, s.DeleteManyTx(ctx, nil, nil))
	require.NoError(t, s.DeleteManyTx(ctx, nil, []string{strings.ToUpper(first.ID), uuid.NewString()}))
	all, err = s.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, []string{second.ID}, ids(all))
}

// Rules added in one transaction have the same created_at.
func TestDeltaRulesOrder(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, usr := add(t, db, e.schedules), add(t, db, e.users)
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		for range 5 {
			if _, err := s.CreateRuleTx(ctx, tx, NewAlwaysActive(sched, assignment.UserTarget(usr))); err != nil {
				return err
			}
		}
		return nil
	}))
	e.clock.Advance(time.Second)
	last, err := s.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	require.NoError(t, err)
	all, err := s.FindAll(ctx, sched)
	require.NoError(t, err)
	require.Len(t, all, 6)
	assert.IsIncreasing(t, ids(all[:5]), "by id")
	assert.Equal(t, last.ID, all[5].ID)
}

// The row has every column, with Postgres's defaults, and reads the times of
// rows with seconds, such as the default end_time.
func TestDeltaRuleRow(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, usr := add(t, db, e.schedules), add(t, db, e.users)
	r, err := s.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	require.NoError(t, err)
	get := func() (row ruleRow) {
		require.NoError(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
			_, reader := deltatxn.Unwrap(tx)
			row, _ = e.tables.Rules.Get(reader, r.ID)
			return nil
		}))
		return row
	}
	row := get()
	assert.Equal(t, ruleRow{
		ID: r.ID, ScheduleID: sched, TgtUserID: &usr, CreatedAt: start,
		Sunday: true, Monday: true, Tuesday: true, Wednesday: true, Thursday: true, Friday: true, Saturday: true,
		StartTime: "00:00:00", EndTime: "00:00:00",
	}, row)

	// The columns that the store doesn't write survive an update.
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		row.IsActive, row.EndTime = true, "23:59:59"
		return e.tables.Rules.Put(ftx, row)
	}))
	all, err := s.FindAll(ctx, sched)
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, timeutil.NewClock(23, 59), all[0].End, "to the minute, as on Postgres")
	e.clock.Advance(time.Hour)
	require.NoError(t, s.UpdateTx(ctx, nil, &all[0]))
	row = get()
	assert.True(t, row.IsActive)
	assert.True(t, row.CreatedAt.Equal(start))
	assert.Equal(t, "23:59:00", row.EndTime)

	for _, c := range []timeutil.Clock{timeutil.NewClock(24, 0), timeutil.NewClock(-1, 0)} {
		bad := NewAlwaysActive(sched, assignment.UserTarget(usr))
		bad.End = c
		_, err = s.Add(ctx, bad)
		assert.Error(t, err, "%v is not a time of day", c)
	}
}

func TestDeltaRuleConstraints(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, usr, rot := add(t, db, e.schedules), add(t, db, e.users), add(t, db, e.rotations)
	r, err := s.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	require.NoError(t, err)

	bad := func(schedID string, tgt assignment.Target, constraint string) {
		t.Helper()
		_, err := s.Add(ctx, NewAlwaysActive(schedID, tgt))
		requireConstraint(t, err, constraint)
		change := *r
		change.ScheduleID, change.Target = schedID, tgt
		requireConstraint(t, s.UpdateTx(ctx, nil, &change), constraint)
	}
	bad(uuid.NewString(), assignment.UserTarget(usr), "schedule_rules_schedule_id_fkey")
	bad(sched, assignment.UserTarget(uuid.NewString()), "schedule_rules_tgt_user_id_fkey")
	bad(sched, assignment.RotationTarget(uuid.NewString()), "schedule_rules_tgt_rotation_id_fkey")
	bad(sched, assignment.ServiceTarget(uuid.NewString()), "schedule_rules_check")
	_, err = s.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget("nobody")))
	require.Error(t, err, "not a uuid")
	requireConstraint(t, s.delta.add(ctx, nil, r), "schedule_rules_pkey")

	// Users see the same messages as on Postgres.
	_, err = s.Add(ctx, NewAlwaysActive(sched, assignment.RotationTarget(uuid.NewString())))
	assert.ErrorContains(t, errutil.MapDBError(err), "rotation does not exist")

	all, err := s.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Equal(t, []string{r.ID}, ids(all), "nothing was added or changed")
	assert.Equal(t, assignment.UserTarget(usr), all[0].Target)

	// The check takes exactly one target.
	assert.Error(t, ruleRow{}.check())
	assert.Error(t, ruleRow{TgtUserID: &usr, TgtRotationID: &rot}.check())
	assert.NoError(t, ruleRow{TgtRotationID: &rot}.check())
	_, err = ruleRow{StartTime: "00:00:00", EndTime: "00:00:00"}.Rule()
	assert.ErrorIs(t, err, errNoKnownTarget)
}

func TestDeltaRuleCascades(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, sched2 := add(t, db, e.schedules), add(t, db, e.schedules)
	usr, rot := add(t, db, e.users), add(t, db, e.rotations)
	for _, tgt := range []assignment.Target{assignment.UserTarget(usr), assignment.RotationTarget(rot)} {
		for _, id := range []string{sched, sched2} {
			_, err := s.Add(ctx, NewAlwaysActive(id, tgt))
			require.NoError(t, err)
		}
	}
	count := func(id string) int {
		all, err := s.FindAll(ctx, id)
		require.NoError(t, err)
		return len(all)
	}

	remove(t, db, e.users, usr)
	assert.Equal(t, 1, count(sched), "a user's rules go with the user")
	assert.Equal(t, 1, count(sched2))
	remove(t, db, e.schedules, sched)
	assert.Equal(t, 0, count(sched), "and a schedule's with the schedule")
	assert.Equal(t, 1, count(sched2))
	remove(t, db, e.rotations, rot)
	assert.Equal(t, 0, count(sched2), "and a rotation's with the rotation")
}

func TestDeltaRuleLimits(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	maxRules, maxTargets := 3, -1
	e.limits = DeltaLimits{
		RulesPerSchedule:   func(filestore.Reader) int { return maxRules },
		TargetsPerSchedule: func(filestore.Reader) int { return maxTargets },
	}
	s, db := e.pod(t, "a")
	sched, other := add(t, db, e.schedules), add(t, db, e.schedules)
	usr, rot := add(t, db, e.users), add(t, db, e.rotations)
	addRule := func(id string, tgt assignment.Target) error {
		_, err := s.Add(ctx, NewAlwaysActive(id, tgt))
		return err
	}
	isLimit := func(err error, name string) {
		t.Helper()
		requireConstraint(t, err, name)
		assert.True(t, errutil.IsLimitError(errutil.MapDBError(err)), "users get the limit message")
	}

	for range 3 {
		require.NoError(t, addRule(sched, assignment.UserTarget(usr)))
	}
	isLimit(addRule(sched, assignment.UserTarget(usr)), "rules_per_schedule_limit")
	require.NoError(t, addRule(other, assignment.UserTarget(usr)), "another schedule has its own limit")

	maxRules = -1 // no limit
	maxTargets = 1
	require.NoError(t, addRule(sched, assignment.UserTarget(usr)), "one target, however many rules")
	isLimit(addRule(sched, assignment.RotationTarget(rot)), "targets_per_schedule_limit")
	isLimit(addRule(sched, assignment.UserTarget(add(t, db, e.users))), "targets_per_schedule_limit")
	maxTargets = 2
	require.NoError(t, addRule(sched, assignment.RotationTarget(rot)))
	maxTargets = 0
	isLimit(addRule(add(t, db, e.schedules), assignment.UserTarget(usr)), "targets_per_schedule_limit")

	// The triggers are for new rules only.
	all, err := s.FindAll(ctx, other)
	require.NoError(t, err)
	require.Len(t, all, 1)
	maxRules, maxTargets = 1, 1
	all[0].ScheduleID = sched
	require.NoError(t, s.UpdateTx(ctx, nil, &all[0]), "a rule can move past the limits, as on Postgres")

	// A transaction whose rule is refused fails, even if it ignores that, and
	// leaves nothing behind.
	isLimit(db.Run(ctx, nil, func(tx txn.Tx) error {
		_, err := s.CreateRuleTx(ctx, tx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
		assert.Error(t, err)
		return nil
	}), "rules_per_schedule_limit")
	all, err = s.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Len(t, all, 6)

	bare := NewDeltaStore(db, e.tables, DeltaLimits{})
	_, err = bare.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted, "limits are never dropped quietly (F8)")
}

// Pods adding rules at once stay within the limit (F8): one that commits second
// runs again, and counts the rules of the first (F4).
func TestDeltaRuleLimitRace(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	e.limits.RulesPerSchedule = func(filestore.Reader) int { return 4 }
	a, db := e.pod(t, "a")
	sched, usr := add(t, db, e.schedules), add(t, db, e.users)
	b, _ := e.pod(t, "b")
	pods := []*Store{a, b}

	errs := make(chan error, 10)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	added := 0
	for err := range errs {
		if err == nil {
			added++
			continue
		}
		requireConstraint(t, err, "rules_per_schedule_limit")
	}
	assert.Equal(t, 4, added)
	fresh, _ := e.pod(t, "reader")
	all, err := fresh.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Len(t, all, 4, "two pods can't go over the limit (F8)")
}

// A pod that doesn't know yet that another pod deleted the parent is refused
// (F4, F7).
func TestDeltaRuleParentRace(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	a, db := e.pod(t, "a")
	sched, usr := add(t, db, e.schedules), add(t, db, e.users)
	r, err := a.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	require.NoError(t, err)
	b, _ := e.pod(t, "b")

	remove(t, db, e.users, usr)
	_, err = b.Add(ctx, NewAlwaysActive(sched, assignment.UserTarget(usr)))
	requireConstraint(t, err, "schedule_rules_tgt_user_id_fkey")
	assert.NoError(t, b.UpdateTx(ctx, nil, r), "the rule is gone with the user")
	fresh, _ := e.pod(t, "reader")
	all, err := fresh.FindAll(ctx, sched)
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestDeltaRuleLookups(t *testing.T) {
	ctx := system()
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched, sched2 := add(t, db, e.schedules), add(t, db, e.schedules)
	usr, rot, rot2 := add(t, db, e.users), add(t, db, e.rotations), add(t, db, e.rotations)
	for _, r := range []*Rule{
		NewAlwaysActive(sched, assignment.UserTarget(usr)),
		NewAlwaysActive(sched, assignment.UserTarget(usr)),
		NewAlwaysActive(sched, assignment.RotationTarget(rot)),
		NewAlwaysActive(sched2, assignment.RotationTarget(rot)),
		NewAlwaysActive(sched2, assignment.RotationTarget(rot)),
	} {
		_, err := s.Add(ctx, r)
		require.NoError(t, err)
	}
	require.NoError(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, r := deltatxn.Unwrap(tx)
		assert.Equal(t, []string{sched}, e.tables.ScheduleIDsByTarget(r, assignment.UserTarget(usr)))
		assert.ElementsMatch(t, []string{sched, sched2}, e.tables.ScheduleIDsByTarget(r, assignment.RotationTarget(rot)))
		both := e.tables.ScheduleIDsByTarget(r, assignment.UserTarget(usr), assignment.RotationTarget(rot), assignment.RotationTarget(rot2))
		assert.Len(t, both, 2)
		assert.IsIncreasing(t, both, "each once, sorted")
		assert.Empty(t, e.tables.ScheduleIDsByTarget(r, assignment.RotationTarget(rot2)))
		return nil
	}))
}
