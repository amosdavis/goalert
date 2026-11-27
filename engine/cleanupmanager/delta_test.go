package cleanupmanager_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/cleanupmanager"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/override"
	"github.com/target/goalert/schedule"
)

const day = 24 * time.Hour

type env struct {
	in     *deltastorestest.Install
	p      *deltastorestest.Pod
	runner *enginetables.Runner
	db     *cleanupmanager.DeltaDB
	cfg    config.Config
}

func newEnv(t *testing.T) *env {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	e := &env{in: in, p: p, runner: enginetables.NewRunner(p.Delta, p.Tables.Engine.NewLease("test", time.Minute))}
	e.db = cleanupmanager.NewDeltaDB(p.Delta, p.Tables, p.Alert, e.runner, slog.New(slog.DiscardHandler))
	_, err := e.runner.Run(p.Ctx())
	require.NoError(t, err)
	return e
}

func (e *env) ctx() context.Context { return e.cfg.Context(e.p.Ctx()) }

func (e *env) run(name string) error {
	e.p.T.Helper()
	i := slices.IndexFunc(e.db.Tasks(), func(t enginetables.Task) bool { return t.Name == name })
	require.GreaterOrEqual(e.p.T, i, 0, name)
	return e.db.Tasks()[i].Run(e.ctx())
}

func TestDeltaAlerts(t *testing.T) {
	e := newEnv(t)
	p := e.p
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	al := func(name string) int64 { t.Helper(); return p.AddAlert(svc, name) }
	status := func(id int64) alert.Status { s, _ := p.Tables.Alert.Status(p.View(), id); return s }
	old, acked, busy, policy, closed := al("old"), al("acked"), al("busy"), al("policy"), al("closed")
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(acked), alert.StatusActive))
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(closed), alert.StatusClosed))

	// Without days nothing is closed or deleted.
	e.in.Clock.Advance(10 * day)
	require.NoError(t, e.run("cleanup-alerts"))
	assert.Equal(t, alert.StatusTriggered, status(old))
	fresh := al("fresh")
	p.Update(func(tx *filestore.Tx) error {
		for id, event := range map[int64]alertlog.Type{busy: alertlog.TypeEscalated, policy: alertlog.TypePolicyUpdated} {
			if err := p.Tables.AlertLog.Insert(tx, alertlog.DeltaLog{AlertID: &id, Event: string(event)}); err != nil {
				return err
			}
		}
		return nil
	})
	e.cfg.Maintenance.AlertAutoCloseDays = 9
	require.NoError(t, e.run("cleanup-alerts"))
	for id, want := range map[int64]alert.Status{
		old: alert.StatusClosed, policy: alert.StatusClosed, busy: alert.StatusTriggered, fresh: alert.StatusTriggered, acked: alert.StatusActive,
	} {
		assert.Equal(t, want, status(id), id)
	}
	_, ok := p.Tables.Escalation.AlertState(p.View(), old)
	assert.False(t, ok, "with all that closing one means")
	entries, err := p.AlertLog.Search(p.Ctx(), &alertlog.SearchOptions{FilterAlertIDs: []int{int(old)}})
	require.NoError(t, err)
	assert.Contains(t, entries[0].String(p.Ctx()), "Closed")
	e.cfg.Maintenance.AutoCloseAckedAlerts = true
	require.NoError(t, e.run("cleanup-alerts"))
	assert.Equal(t, alert.StatusClosed, status(acked))

	e.cfg.Maintenance.AlertCleanupDays = 10
	require.NoError(t, e.run("cleanup-alerts"))
	assert.Equal(t, alert.StatusClosed, status(old), "not yet")
	e.in.Clock.Advance(time.Second)
	require.NoError(t, e.run("cleanup-alerts"))
	for _, id := range []int64{old, acked, policy, closed} {
		_, ok := p.Tables.Alert.Alerts.Get(p.View(), filestore.IntKey(id))
		assert.False(t, ok, id)
	}
	assert.Equal(t, alert.StatusTriggered, status(busy))

	// Their log entries go later.
	before := p.Tables.AlertLog.Logs.Len(p.View())
	require.NoError(t, e.run("cleanup-alert-logs"))
	assert.Less(t, p.Tables.AlertLog.Logs.Len(p.View()), before)
	p.Tables.AlertLog.Logs.Ascend(p.View(), func(l alertlog.DeltaLog) bool {
		assert.Contains(t, []int64{busy, fresh}, *l.AlertID)
		return true
	})
}

// TestDeltaBatches checks that more than fits in a transaction is all done.
func TestDeltaBatches(t *testing.T) {
	e := newEnv(t)
	p := e.p
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	p.Update(func(tx *filestore.Tx) error {
		for range 250 {
			if _, err := p.Alert.DeltaInsert(tx, alert.DeltaAlert{Summary: "x", ServiceID: &svc, Status: "closed"}); err != nil {
				return err
			}
		}
		gone := int64(999999)
		for i := range 12000 {
			l := alertlog.DeltaLog{AlertID: &gone, Event: string(alertlog.TypeEscalated)}
			if i%100 == 0 {
				id := int64(1)
				l.AlertID = &id
			}
			if err := p.Tables.AlertLog.Insert(tx, l); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, e.run("cleanup-alert-logs"))
	assert.Equal(t, 120, p.Tables.AlertLog.Logs.Len(p.View()))
	e.in.Clock.Advance(2 * day)
	e.cfg.Maintenance.AlertCleanupDays = 1
	require.NoError(t, e.run("cleanup-alerts"))
	assert.Zero(t, p.Tables.Alert.Alerts.Len(p.View()))
}

func TestDeltaSchedules(t *testing.T) {
	e := newEnv(t)
	p := e.p
	alice := p.AddUser("alice")
	sched := p.AddSchedule("sched")
	pol := p.AddPolicy("policy", 0)
	step := p.AddStep(pol, 5)
	now := e.in.Clock.Now()
	ended := now.Add(time.Hour)
	p.Update(func(tx *filestore.Tx) error {
		for i, end := range []*time.Time{&ended, nil} {
			user := alice
			if err := p.Tables.OnCall.PutScheduleOnCall(tx, oncall.ScheduleOnCallRow{ID: int64(i + 1), ScheduleID: sched, UserID: user, StartTime: now, EndTime: end}); err != nil {
				return err
			}
			if err := p.Tables.OnCall.StepOnCall.Put(tx, oncall.StepOnCallRow{ID: int64(i + 1), StepID: step, UserID: user, StartTime: now, EndTime: end}); err != nil {
				return err
			}
		}
		for _, end := range []time.Time{ended, now.Add(100 * day)} {
			err := p.Tables.Override.Overrides.Put(tx, override.OverrideRow{ID: uuid.NewString(), AddUserID: &alice, Start: now, End: end, ScheduleID: sched})
			if err != nil {
				return err
			}
		}
		return p.Tables.Schedule.UpdateData(tx, uuid.MustParse(sched), func(d *schedule.Data) error {
			d.V1.TemporarySchedules = []schedule.TemporarySchedule{
				{Start: now, End: ended, Shifts: []schedule.FixedShift{{Start: now, End: ended, UserID: alice}}},
				{Start: now, End: now.Add(100 * day), Shifts: []schedule.FixedShift{
					{Start: now, End: ended, UserID: alice},
					{Start: now, End: ended, UserID: uuid.NewString()},
				}},
			}
			return nil
		})
	})
	counts := func() string {
		r := p.View()
		data, err := p.Tables.Schedule.ScheduleData(r, uuid.MustParse(sched))
		require.NoError(t, err)
		temps := data.V1.TemporarySchedules
		return fmt.Sprintf("%d shifts, %d step shifts, %d overrides, %d temporary schedules, the last with %d shifts",
			p.Tables.OnCall.ScheduleOnCall.Len(r), p.Tables.OnCall.StepOnCall.Len(r), p.Tables.Override.Overrides.Len(r),
			len(temps), len(temps[len(temps)-1].Shifts))
	}
	run := func() {
		t.Helper()
		require.NoError(t, e.run("cleanup-shifts"))
		require.NoError(t, e.run("cleanup-schedule-data"))
	}
	const all = "2 shifts, 2 step shifts, 2 overrides, 2 temporary schedules, the last with 2 shifts"
	e.in.Clock.Advance(50 * day)
	run()
	assert.Equal(t, all, counts(), "nothing unless it is configured")
	e.cfg.Maintenance.ScheduleCleanupDays = 50
	run()
	assert.Equal(t, "2 shifts, 2 step shifts, 2 overrides, 1 temporary schedules, the last with 1 shifts", counts())
	e.in.Clock.Advance(2 * time.Hour)
	run()
	assert.Equal(t, "1 shifts, 1 step shifts, 1 overrides, 1 temporary schedules, the last with 1 shifts", counts())
	before := p.Delta.View().Index()
	run()
	assert.Equal(t, before, p.Delta.View().Index(), "a schedule's data is looked at once a month")

	// Data that can't be read does not keep others from being looked at.
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.Schedule.Data.Get(tx, sched)
		row.Data, row.LastCleanupAt = []byte("{"), nil
		return p.Tables.Schedule.Data.Put(tx, row)
	})
	run()
	row, _ := p.Tables.Schedule.Data.Get(p.View(), sched)
	assert.NotNil(t, row.LastCleanupAt)
	assert.Equal(t, "{", string(row.Data))
}

// TestDeltaLease checks that nothing is deleted without the lease.
func TestDeltaLease(t *testing.T) {
	e := newEnv(t)
	e.cfg.Maintenance.AlertAutoCloseDays, e.cfg.Maintenance.AlertCleanupDays = 1, 1
	e.cfg.Maintenance.ScheduleCleanupDays = 1
	for _, task := range e.db.Tasks() {
		require.NoError(t, task.Run(e.ctx()), task.Name)
	}
	require.NoError(t, e.runner.Release(e.p.Ctx()))
	for _, task := range e.db.Tasks() {
		require.ErrorIs(t, task.Run(e.ctx()), enginetables.ErrNotHolder, task.Name)
	}
}
