package escalationmanager_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/engine/escalationmanager"
	"github.com/target/goalert/engine/schedulemanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/schedule/rotation"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error {
		return escalationmanager.NewDeltaDB(p.Tables, p.AlertLog).UpdateAll(p.Ctx(), tx)
	})
}

// cycles are the users being notified of an alert, and how often.
func cycles(p *deltastorestest.Pod, alertID int64) map[string]int {
	p.T.Helper()
	out := map[string]int{}
	p.Tables.Engine.CyclesByAlert.Ascend(p.View(), filestore.IntKey(alertID), func(c enginetables.CycleRow) bool {
		out[c.UserID]++
		return true
	})
	return out
}

func channelMessages(p *deltastorestest.Pod, alertID int64) int {
	p.T.Helper()
	n := 0
	for _, m := range p.Messages() {
		if m.AlertID != nil && *m.AlertID == alertID && m.ChannelID != nil {
			require.Equal(p.T, "alert_notification", m.MessageType)
			require.NotNil(p.T, m.ServiceID)
			require.NotNil(p.T, m.EscalationPolicyID)
			n++
		}
	}
	return n
}

// escalations are the metadata of an alert's escalations, oldest first.
func escalations(p *deltastorestest.Pod, alertID int64) []alertlog.EscalationMetaData {
	p.T.Helper()
	p.Sync()
	entries, err := p.AlertLog.Search(p.Ctx(), &alertlog.SearchOptions{FilterAlertIDs: []int{int(alertID)}, Limit: 50})
	require.NoError(p.T, err)
	var out []alertlog.EscalationMetaData
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Type() == alertlog.TypeEscalated {
			meta, ok := entries[i].Meta(p.Ctx()).(*alertlog.EscalationMetaData)
			require.True(p.T, ok)
			out = append(out, *meta)
		}
	}
	return out
}

type meta = alertlog.EscalationMetaData

// TestDeltaEscalate follows an alert through its policy, which repeats once.
func TestDeltaEscalate(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob := p.AddUser("alice"), p.AddUser("bob")
	pol := p.AddPolicy("policy", 1)
	p.AddStep(pol, 5, deltastorestest.UserDest(alice))
	p.AddStep(pol, 10, deltastorestest.UserDest(bob), deltastorestest.Dest("channel"))
	p.AddStep(pol, 1)
	svc := p.AddService("service", pol)
	al := p.AddAlert(svc, "down")

	update(p)
	assert.Equal(t, map[string]int{alice: 1}, cycles(p, al))
	assert.Equal(t, []meta{{}}, escalations(p, al))
	before := p.Delta.View().Index()
	update(p)
	in.Clock.Advance(5 * time.Minute)
	update(p)
	assert.Equal(t, before, p.Delta.View().Index(), "it is due after that")

	in.Clock.Advance(time.Second)
	update(p)
	assert.Equal(t, map[string]int{alice: 1, bob: 1}, cycles(p, al))
	assert.Equal(t, 1, channelMessages(p, al))
	in.Clock.Advance(10*time.Minute + time.Second)
	update(p)
	in.Clock.Advance(time.Minute + time.Second)
	update(p) // repeats
	assert.Equal(t, map[string]int{alice: 2, bob: 1}, cycles(p, al))
	for _, d := range []time.Duration{5 * time.Minute, 10 * time.Minute, time.Minute} {
		in.Clock.Advance(d + time.Second)
		update(p)
	}
	assert.Equal(t, map[string]int{alice: 2, bob: 2}, cycles(p, al))
	assert.Equal(t, 2, channelMessages(p, al))
	in.Clock.Advance(time.Hour)
	update(p)
	assert.Equal(t, []meta{
		{},
		{NewStepIndex: 1, OldDelayMinutes: 5},
		{NewStepIndex: 2, OldDelayMinutes: 10, NoOneOnCall: true},
		{NewStepIndex: 0, OldDelayMinutes: 1, Repeat: true},
		{NewStepIndex: 1, OldDelayMinutes: 5},
		{NewStepIndex: 2, OldDelayMinutes: 10, NoOneOnCall: true},
	}, escalations(p, al), "and no more")

	// Asking for it goes on.
	require.NoError(t, p.Alert.EscalateAsOf(p.Admin(), int(al), in.Clock.Now()))
	update(p)
	last := escalations(p, al)
	assert.Equal(t, meta{Forced: true, Repeat: true, OldDelayMinutes: 1}, last[len(last)-1])
	st, _ := p.Tables.Escalation.AlertState(p.View(), al)
	assert.False(t, st.ForceEscalation)
	assert.EqualValues(t, 2, st.LoopCount)

	// An alert that is acknowledged waits, until somebody asks. One that is
	// closed has no state.
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusActive))
	in.Clock.Advance(time.Hour)
	n := len(escalations(p, al))
	update(p)
	assert.Len(t, escalations(p, al), n)
	require.NoError(t, p.Alert.EscalateAsOf(p.Admin(), int(al), in.Clock.Now()))
	update(p)
	assert.Len(t, escalations(p, al), n+1)
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusClosed))
	in.Clock.Advance(time.Hour)
	update(p)
	assert.Len(t, escalations(p, al), n+1)
}

func TestDeltaRepeatForever(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5, deltastorestest.UserDest(p.AddUser("alice")))
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.Escalation.Policies.Get(tx, pol)
		row.Repeat = -1 // only old installs have it
		return p.Tables.Escalation.Policies.Put(tx, row)
	})
	al := p.AddAlert(p.AddService("service", pol), "down")
	update(p)
	for range 10 {
		in.Clock.Advance(5*time.Minute + time.Second)
		update(p)
	}
	assert.Len(t, escalations(p, al), 11)
}

// TestDeltaSteps checks alerts whose policy changes under them.
func TestDeltaSteps(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob, carol := p.AddUser("alice"), p.AddUser("bob"), p.AddUser("carol")
	pol, empty := p.AddPolicy("policy", 0), p.AddPolicy("empty", 0)
	first := p.AddStep(pol, 5, deltastorestest.UserDest(alice))
	p.AddStep(pol, 5, deltastorestest.UserDest(bob))
	last := p.AddStep(pol, 5, deltastorestest.UserDest(carol))
	svc := p.AddService("service", pol)
	al := p.AddAlert(svc, "down")
	update(p)

	// The step it is on is deleted: the one now in its place is next.
	_, err := p.Escalation.DeleteStepTx(p.Admin(), nil, uuid.MustParse(first))
	require.NoError(t, err)
	update(p)
	assert.Equal(t, map[string]int{alice: 1, bob: 1}, cycles(p, al))
	in.Clock.Advance(5*time.Minute + time.Second)
	update(p)
	assert.Equal(t, map[string]int{alice: 1, bob: 1, carol: 1}, cycles(p, al))
	// The last step is deleted: it starts over, whatever the policy repeats.
	_, err = p.Escalation.DeleteStepTx(p.Admin(), nil, uuid.MustParse(last))
	require.NoError(t, err)
	update(p)
	assert.Equal(t, map[string]int{alice: 1, bob: 2, carol: 1}, cycles(p, al))
	got := escalations(p, al)
	assert.Equal(t, []meta{{}, {}, {NewStepIndex: 1, OldDelayMinutes: 5}, {Repeat: true}}, got)

	// A policy without steps: there is nothing to escalate to.
	svc2 := p.AddService("service2", empty)
	al2 := p.AddAlert(svc2, "down2")
	update(p)
	_, ok := p.Tables.Escalation.AlertState(p.View(), al2)
	assert.False(t, ok)
	s, err := p.Service.FindOne(p.Admin(), svc)
	require.NoError(t, err)
	s.EscalationPolicyID = empty
	require.NoError(t, p.Service.UpdateTx(p.Admin(), nil, s))
	update(p)
	_, ok = p.Tables.Escalation.AlertState(p.View(), al)
	assert.False(t, ok)
	// With steps again, it starts (F2).
	s.EscalationPolicyID = pol
	require.NoError(t, p.Service.UpdateTx(p.Admin(), nil, s))
	update(p)
	assert.Equal(t, map[string]int{alice: 1, bob: 3, carol: 1}, cycles(p, al))
}

func TestDeltaMaintenance(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5, deltastorestest.UserDest(p.AddUser("alice")))
	svc := p.AddService("service", pol)
	maintenance := func(until *time.Time) {
		p.Update(func(tx *filestore.Tx) error {
			row, _ := p.Tables.Service.Services.Get(tx, svc)
			row.MaintenanceExpiresAt = until
			return p.Tables.Service.Services.Put(tx, row)
		})
	}
	until := in.Clock.Now().Add(time.Hour)
	maintenance(&until)
	al := p.AddAlert(svc, "down")
	update(p)
	assert.Empty(t, cycles(p, al))
	in.Clock.Advance(time.Hour - time.Second)
	update(p)
	assert.Empty(t, cycles(p, al))
	in.Clock.Advance(time.Second)
	update(p)
	assert.Len(t, cycles(p, al), 1)
	row, _ := p.Tables.Service.Services.Get(p.View(), svc)
	assert.Nil(t, row.MaintenanceExpiresAt)
}

// TestDeltaOnCall checks who is on call for a step: the users of its actions,
// and those on call for its schedules and rotations.
func TestDeltaOnCall(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob, carol := p.AddUser("alice"), p.AddUser("bob"), p.AddUser("carol")
	rot := p.AddRotation("rot", rotation.TypeHourly, 1, in.Clock.Now(), bob, carol)
	sched := p.AddSchedule("sched")
	rule := p.AddScheduleRule(sched, assignment.UserTarget(carol))
	pol := p.AddPolicy("policy", 0)
	step := p.AddStep(pol, 5, deltastorestest.UserDest(alice), deltastorestest.RotationDest(rot), deltastorestest.ScheduleDest(sched))
	other := p.AddStep(pol, 5, deltastorestest.UserDest(alice))
	users := func(id string) []string { return p.Tables.OnCall.StepUsers(p.View(), id) }

	update(p)
	assert.ElementsMatch(t, []string{alice, bob}, users(step), "nobody is on call for the schedule yet")
	p.Update(func(tx *filestore.Tx) error { return schedulemanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
	update(p)
	assert.ElementsMatch(t, []string{alice, bob, carol}, users(step))
	assert.Equal(t, []string{alice}, users(other))
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index())

	// On call in two ways is on call once, until neither is left.
	require.NoError(t, p.Rotation.SetActiveIndexTx(p.Admin(), nil, rot, 1))
	update(p)
	assert.ElementsMatch(t, []string{alice, carol}, users(step))
	require.NoError(t, p.Rule.DeleteManyTx(p.Admin(), nil, []string{rule}))
	in.Clock.Advance(time.Minute)
	p.Update(func(tx *filestore.Tx) error { return schedulemanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
	update(p)
	assert.ElementsMatch(t, []string{alice, carol}, users(step))
	require.NoError(t, p.Rotation.SetActiveIndexTx(p.Admin(), nil, rot, 0))
	update(p)
	assert.ElementsMatch(t, []string{alice, bob}, users(step))
}

// TestDeltaMany checks that alerts escalate in an order, so many at a time.
func TestDeltaMany(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5, deltastorestest.Dest("channel"))
	var alerts []int64
	for _, name := range []string{"first", "second", "third", "fourth", "fifth", "sixth"} {
		svc := p.AddService(name, pol)
		p.Update(func(tx *filestore.Tx) error {
			for range 190 { // the limit is 200
				id := svc
				a, err := p.Alert.DeltaInsert(tx, alert.DeltaAlert{Summary: "down", ServiceID: &id, DedupKey: ptr("user:1:" + uuid.NewString())})
				if err != nil {
					return err
				}
				alerts = append(alerts, a.ID)
			}
			return nil
		})
	}
	escalated := func() (n int) {
		for _, id := range alerts {
			if st, _ := p.Tables.Escalation.AlertState(p.Delta.View(), id); st.LastEscalation != nil {
				n++
			}
		}
		return n
	}
	update(p)
	assert.Equal(t, 1000, escalated())
	update(p)
	assert.Equal(t, len(alerts), escalated())
	assert.Len(t, p.Messages(), len(alerts))
}

func ptr[T any](v T) *T { return &v }
