package schedulemanager_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/schedulemanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/override"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/util/timeutil"
)

func update(p *deltastorestest.Pod, db *schedulemanager.DeltaDB) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return db.UpdateAll(p.Ctx(), tx) })
}

func onCall(p *deltastorestest.Pod, scheduleID string) []string {
	p.T.Helper()
	return p.Tables.OnCall.ScheduleUsers(p.View(), scheduleID)
}

func setData(p *deltastorestest.Pod, scheduleID string, fn func(d *schedule.Data)) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error {
		return p.Tables.Schedule.UpdateData(tx, uuid.MustParse(scheduleID), func(d *schedule.Data) error { fn(d); return nil })
	})
}

func addOverride(p *deltastorestest.Pod, scheduleID string, add, remove string, start, end time.Time) string {
	p.T.Helper()
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	id := uuid.NewString()
	p.Update(func(tx *filestore.Tx) error {
		return p.Tables.Override.Overrides.Put(tx, override.OverrideRow{
			ID: id, ScheduleID: scheduleID, AddUserID: opt(add), RemoveUserID: opt(remove), Start: start, End: end,
		})
	})
	return id
}

func TestDeltaOnCall(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	db := schedulemanager.NewDeltaDB(p.Tables)
	alice, bob, carol, dave := p.AddUser("alice"), p.AddUser("bob"), p.AddUser("carol"), p.AddUser("dave")
	rot := p.AddRotation("rot", rotation.TypeHourly, 1, in.Clock.Now(), bob, carol)
	empty := p.AddRotation("empty", rotation.TypeHourly, 1, in.Clock.Now())
	sched := p.AddSchedule("sched")
	other := p.AddSchedule("other")
	p.AddScheduleRule(sched, assignment.UserTarget(alice))
	p.AddScheduleRule(sched, assignment.RotationTarget(rot))
	p.AddScheduleRule(sched, assignment.RotationTarget(empty))

	update(p, db)
	assert.ElementsMatch(t, []string{alice, bob}, onCall(p, sched))
	assert.Empty(t, onCall(p, other))
	before := p.Delta.View().Index()
	update(p, db)
	assert.Equal(t, before, p.Delta.View().Index(), "nothing to do is no commit")

	// Overrides replace, remove and add, while they last.
	start := in.Clock.Now().Add(time.Minute)
	addOverride(p, sched, dave, alice, start, start.Add(time.Hour))
	addOverride(p, sched, "", bob, start, start.Add(time.Hour))
	addOverride(p, sched, carol, dave, start.Add(time.Hour), start.Add(2*time.Hour)) // dave is not on call then
	update(p, db)
	assert.ElementsMatch(t, []string{alice, bob}, onCall(p, sched))
	in.Clock.Advance(time.Minute)
	update(p, db)
	assert.Equal(t, []string{dave}, onCall(p, sched))
	in.Clock.Advance(time.Hour + time.Minute)
	update(p, db)
	assert.ElementsMatch(t, []string{alice, bob}, onCall(p, sched))

	// The history has the times.
	rows := 0
	p.Tables.OnCall.ScheduleOnCall.Ascend(p.View(), func(r oncall.ScheduleOnCallRow) bool {
		rows++
		if r.UserID == dave {
			assert.Equal(t, start, r.StartTime)
			assert.Equal(t, start.Add(time.Hour+time.Minute), *r.EndTime)
		}
		return true
	})
	assert.Equal(t, 5, rows)

	// A temporary schedule is all that counts, and users it names who were
	// deleted don't.
	now := in.Clock.Now()
	setData(p, sched, func(d *schedule.Data) {
		d.V1.TemporarySchedules = []schedule.TemporarySchedule{{
			Start: now, End: now.Add(time.Hour),
			Shifts: []schedule.FixedShift{
				{Start: now, End: now.Add(time.Hour), UserID: carol},
				{Start: now, End: now.Add(30 * time.Minute), UserID: dave},
				{Start: now, End: now.Add(time.Hour), UserID: uuid.NewString()},
			},
		}}
	})
	update(p, db)
	assert.ElementsMatch(t, []string{carol, dave}, onCall(p, sched))
	before = p.Delta.View().Index()
	update(p, db)
	assert.Equal(t, before, p.Delta.View().Index(), "a deleted user does not start and stop over and over")
	in.Clock.Advance(30 * time.Minute)
	update(p, db)
	assert.Equal(t, []string{carol}, onCall(p, sched))
	in.Clock.Advance(30 * time.Minute)
	update(p, db)
	assert.ElementsMatch(t, []string{alice, bob}, onCall(p, sched))

	// Deleting a user on call takes them off it.
	require.NoError(t, p.User.DeleteManyTx(p.Admin(), nil, []string{alice}))
	update(p, db)
	assert.Equal(t, []string{bob}, onCall(p, sched))
}

func TestDeltaNotify(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	db := schedulemanager.NewDeltaDB(p.Tables)
	alice, bob := p.AddUser("alice"), p.AddUser("bob")
	sched := p.AddSchedule("sched")
	p.AddScheduleRule(sched, assignment.UserTarget(alice))
	channel := func(name string) uuid.UUID {
		ctx, err := p.NotificationChannel.ResolveDests(p.Admin(), deltastorestest.Dest(name))
		require.NoError(t, err)
		id, err := p.NotificationChannel.MapDestToID(ctx, nil, deltastorestest.Dest(name))
		require.NoError(t, err)
		return id
	}
	change, daily := channel("change"), channel("daily")
	clock, every := timeutil.NewClock(13, 0), timeutil.EveryDay()
	setData(p, sched, func(d *schedule.Data) {
		d.V1.OnCallNotificationRules = []schedule.OnCallNotificationRule{
			{ChannelID: change},
			{ChannelID: daily, Time: &clock, WeekdayFilter: &every},
			{ChannelID: uuid.New()}, // since deleted
		}
	})
	notified := func() map[uuid.UUID]int {
		out := map[uuid.UUID]int{}
		for _, m := range p.Messages() {
			require.Equal(t, string(gadb.EnumOutgoingMessagesTypeScheduleOnCallNotification), m.MessageType)
			require.Equal(t, sched, *m.ScheduleID)
			require.Nil(t, m.UserID)
			out[uuid.MustParse(*m.ChannelID)]++
		}
		return out
	}

	update(p, db)
	assert.Equal(t, map[uuid.UUID]int{change: 1}, notified(), "alice goes on call")
	data, err := p.Tables.Schedule.ScheduleData(p.View(), uuid.MustParse(sched))
	require.NoError(t, err)
	assert.Equal(t, deltastorestest.Start.Add(time.Hour), data.V1.OnCallNotificationRules[1].NextNotification.UTC())
	in.Clock.Advance(59 * time.Minute)
	update(p, db)
	assert.Equal(t, map[uuid.UUID]int{change: 1}, notified())
	in.Clock.Advance(time.Minute)
	update(p, db)
	update(p, db)
	assert.Equal(t, map[uuid.UUID]int{change: 1, daily: 1}, notified())
	in.Clock.Advance(25 * time.Hour)
	addOverride(p, sched, bob, alice, in.Clock.Now(), in.Clock.Now().Add(time.Hour))
	update(p, db)
	update(p, db)
	assert.Equal(t, map[uuid.UUID]int{change: 2, daily: 2}, notified())
}

// TestDeltaBadSchedule checks that a schedule that can't be updated does not
// keep the others from being (F53), and that data that can't be read does not
// take everybody off call.
func TestDeltaBadSchedule(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	db := schedulemanager.NewDeltaDB(p.Tables)
	alice := p.AddUser("alice")
	var scheds []string
	for _, name := range []string{"first", "second", "third"} {
		id := p.AddSchedule(name)
		p.AddScheduleRule(id, assignment.UserTarget(alice))
		scheds = append(scheds, id)
	}
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.Schedule.Schedules.Get(tx, scheds[0])
		row.TimeZone = "Nowhere/Else"
		return p.Tables.Schedule.Schedules.Put(tx, row)
	})
	setData(p, scheds[1], func(*schedule.Data) {})
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.Schedule.Data.Get(tx, scheds[1])
		row.Data = []byte("{")
		return p.Tables.Schedule.Data.Put(tx, row)
	})
	update(p, db)
	assert.Empty(t, onCall(p, scheds[0]))
	assert.Equal(t, []string{alice}, onCall(p, scheds[1]))
	assert.Equal(t, []string{alice}, onCall(p, scheds[2]))
}

// TestDeltaSameTime checks a shift that ends at the instant it started, as the
// transactions of two pods can, if one's clock is ahead (F13).
func TestDeltaSameTime(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := in.Pod(t), in.Pod(t)
	alice, bob := a.AddUser("alice"), a.AddUser("bob")
	sched := a.AddSchedule("sched")
	a.AddScheduleRule(sched, assignment.UserTarget(alice))
	update(a, schedulemanager.NewDeltaDB(a.Tables))
	b.Sync()
	addOverride(b, sched, bob, alice, in.Clock.Now(), in.Clock.Now().Add(time.Hour))
	update(b, schedulemanager.NewDeltaDB(b.Tables))
	assert.Equal(t, []string{bob}, onCall(b, sched))
	assert.Equal(t, []string{bob}, onCall(a, sched))
}
