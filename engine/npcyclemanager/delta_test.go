package npcyclemanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/engine/npcyclemanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error {
		return npcyclemanager.NewDeltaDB(p.Tables, p.AlertLog).UpdateAll(p.Ctx(), tx)
	})
}

// sent is how many messages there are for each contact method.
func sent(p *deltastorestest.Pod) map[string]int {
	p.T.Helper()
	out := map[string]int{}
	for _, m := range p.Messages() {
		require.Equal(p.T, "alert_notification", m.MessageType)
		require.NotNil(p.T, m.AlertID)
		require.NotNil(p.T, m.UserID)
		require.NotNil(p.T, m.ServiceID)
		require.NotNil(p.T, m.EscalationPolicyID)
		out[*m.ContactMethodID]++
	}
	return out
}

// deliver sends what is queued: a message that was sent does not depend on its
// cycle any more.
func deliver(p *deltastorestest.Pod) {
	p.T.Helper()
	for _, m := range p.Messages() {
		m.CycleID, m.LastStatus, m.SentAt = nil, "sent", &m.CreatedAt
		p.Update(func(tx *filestore.Tx) error { return p.Tables.Notification.SetMessageStatus(tx, m) })
	}
}

func cycleCount(p *deltastorestest.Pod) int {
	p.T.Helper()
	return p.Tables.Engine.Cycles.Len(p.View())
}

func logged(p *deltastorestest.Pod, alertID int64, typ alertlog.Type) (n int) {
	p.T.Helper()
	p.Tables.AlertLog.AscendEvent(p.View(), alertID, typ, func(alertlog.DeltaLog) bool { n++; return true })
	return n
}

func startCycle(p *deltastorestest.Pod, alertID int64, userID string, multiAck bool) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return p.Tables.Engine.InsertCycle(tx, alertID, userID, multiAck) })
}

type env struct {
	in  *deltastorestest.Install
	p   *deltastorestest.Pod
	svc string
}

func newEnv(t *testing.T) env {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	return env{in, p, p.AddService("service", pol)}
}

func TestDeltaRules(t *testing.T) {
	e := newEnv(t)
	p := e.p
	user := p.AddUser("alice")
	now, soon, late := p.AddMethod(user, "now"), p.AddMethod(user, "soon"), p.AddMethod(user, "late")
	p.AddRule(user, now, 0)
	p.AddRule(user, soon, 1)
	p.AddRule(user, soon, 2)
	p.AddRule(user, late, 10)
	bob := p.AddUser("bob")
	p.AddRule(bob, p.AddMethod(bob, "bob"), 0)
	al := p.AddAlert(e.svc, "down")
	startCycle(p, al, user, false)

	update(p)
	assert.Equal(t, map[string]int{now: 1}, sent(p))
	before := p.Delta.View().Index()
	update(p)
	e.in.Clock.Advance(time.Minute)
	update(p)
	assert.Equal(t, before, p.Delta.View().Index(), "it is looked at once a minute")
	e.in.Clock.Advance(time.Second)
	update(p)
	assert.Equal(t, map[string]int{now: 1, soon: 1}, sent(p))
	e.in.Clock.Advance(30 * time.Second)
	update(p)
	assert.Equal(t, map[string]int{now: 1, soon: 1}, sent(p))
	e.in.Clock.Advance(time.Minute)
	update(p)
	assert.Equal(t, map[string]int{now: 1, soon: 2}, sent(p))

	// A rule that is added after it would have been due is not.
	p.AddRule(user, now, 1)
	e.in.Clock.Advance(2 * time.Minute)
	update(p)
	assert.Equal(t, map[string]int{now: 1, soon: 2}, sent(p))
	// After a long pause all that came due are, each contact method once.
	p.AddRule(user, late, 11)
	e.in.Clock.Advance(time.Hour)
	update(p)
	assert.Equal(t, map[string]int{now: 1, soon: 2, late: 1}, sent(p))
	assert.Equal(t, 1, cycleCount(p))
	assert.Zero(t, logged(p, al, alertlog.TypeNoNotificationSent))
}

// TestDeltaNothingToSend checks that a user who can't be notified at once is
// logged, once.
func TestDeltaNothingToSend(t *testing.T) {
	e := newEnv(t)
	p := e.p
	user := p.AddUser("alice")
	method := p.AddMethod(user, "late")
	p.AddRule(user, method, 5)
	al := p.AddAlert(e.svc, "down")
	startCycle(p, al, user, false)
	for range 3 {
		update(p)
		e.in.Clock.Advance(90 * time.Second)
	}
	assert.Empty(t, sent(p))
	assert.Equal(t, 1, logged(p, al, alertlog.TypeNoNotificationSent))
	p.Tables.AlertLog.AscendEvent(p.View(), al, alertlog.TypeNoNotificationSent, func(l alertlog.DeltaLog) bool {
		assert.Equal(t, user, *l.SubUserID)
		return true
	})
	e.in.Clock.Advance(2 * time.Minute)
	update(p)
	assert.Equal(t, map[string]int{method: 1}, sent(p))
}

// TestDeltaEnd checks when a user is not notified any more.
func TestDeltaEnd(t *testing.T) {
	e := newEnv(t)
	p := e.p
	alice, aliceCM := p.AddNotifiedUser("alice")
	bob, bobCM := p.AddNotifiedUser("bob")
	p.AddRule(alice, aliceCM, 5)
	p.AddRule(bob, bobCM, 5)

	// Acknowledged: nobody is, and what was queued is not sent.
	al := p.AddAlert(e.svc, "queued")
	startCycle(p, al, alice, false)
	update(p)
	assert.Equal(t, map[string]int{aliceCM: 1}, sent(p))
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusActive))
	e.in.Clock.Advance(2 * time.Minute)
	update(p)
	assert.Empty(t, sent(p))
	al = p.AddAlert(e.svc, "ack")
	startCycle(p, al, alice, false)
	startCycle(p, al, bob, false)
	update(p)
	deliver(p)
	assert.Equal(t, map[string]int{aliceCM: 1, bobCM: 1}, sent(p))
	require.NoError(t, p.Alert.UpdateStatus(p.As(alice, permission.RoleUser), int(al), alert.StatusActive))
	e.in.Clock.Advance(6 * time.Minute)
	update(p)
	assert.Equal(t, map[string]int{aliceCM: 1, bobCM: 1}, sent(p))
	assert.Zero(t, cycleCount(p))

	// On a step where everybody has to: those who have not.
	multi := p.AddAlert(e.svc, "multi")
	startCycle(p, multi, alice, true)
	startCycle(p, multi, bob, true)
	update(p)
	deliver(p)
	assert.Equal(t, map[string]int{aliceCM: 2, bobCM: 2}, sent(p))
	e.in.Clock.Advance(time.Second)
	require.NoError(t, p.Alert.UpdateStatus(p.As(alice, permission.RoleUser), int(multi), alert.StatusActive))
	e.in.Clock.Advance(6 * time.Minute)
	update(p)
	assert.Equal(t, map[string]int{aliceCM: 2, bobCM: 3}, sent(p))
	assert.Equal(t, 1, cycleCount(p))
	// Closed: nobody.
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(multi), alert.StatusClosed))
	e.in.Clock.Advance(2 * time.Minute)
	update(p)
	assert.Zero(t, cycleCount(p))

	// Deleting the alert or the user ends it too.
	gone := p.AddAlert(e.svc, "gone")
	startCycle(p, gone, alice, false)
	startCycle(p, gone, bob, false)
	require.NoError(t, p.User.DeleteManyTx(p.Admin(), nil, []string{bob}))
	assert.Equal(t, 1, cycleCount(p))
	p.Update(func(tx *filestore.Tx) error {
		_, err := p.Tables.Alert.Alerts.Delete(tx, filestore.IntKey(gone))
		return err
	})
	assert.Zero(t, cycleCount(p))
	update(p)
}

// TestDeltaBadCycle checks that a cycle that can't be handled does not keep the
// others from being (F53).
func TestDeltaBadCycle(t *testing.T) {
	e := newEnv(t)
	p := e.p
	alice, aliceCM := p.AddNotifiedUser("alice")
	bob, bobCM := p.AddNotifiedUser("bob")
	al := p.AddAlert(e.svc, "down")
	startCycle(p, al, alice, false)
	startCycle(p, al, bob, false)
	p.Update(func(tx *filestore.Tx) error {
		// A rule for a contact method there is none of, which only a broken
		// table could have.
		var bad enginetables.CycleRow
		p.Tables.Engine.Cycles.Ascend(tx, func(c enginetables.CycleRow) bool { bad = c; return c.UserID != alice })
		bad.ID = "not a uuid"
		return p.Tables.Engine.Cycles.Put(tx, bad)
	})
	update(p)
	assert.GreaterOrEqual(t, sent(p)[aliceCM], 1)
	assert.Equal(t, 1, sent(p)[bobCM])
}
