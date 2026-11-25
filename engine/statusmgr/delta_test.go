package statusmgr_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/statusmgr"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/permission"
)

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

func (e env) update() {
	e.p.T.Helper()
	e.p.Update(func(tx *filestore.Tx) error {
		return statusmgr.NewDeltaDB(e.p.Tables, e.p.Registry).UpdateAll(e.p.Ctx(), tx)
	})
}

func (e env) subscribe(alertID int64, methodID, channelID string) {
	e.p.T.Helper()
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	e.p.Update(func(tx *filestore.Tx) error {
		return e.p.Tables.Alert.Subscribe(tx, alert.DeltaSubscription{
			AlertID: alertID, ContactMethodID: opt(methodID), ChannelID: opt(channelID), LastAlertStatus: "triggered",
		})
	})
}

func (e env) subscriptions() int {
	e.p.T.Helper()
	return e.p.Tables.Alert.Subscriptions.Len(e.p.View())
}

// newest is the last message, of how many.
func (e env) newest() (notification.MessageRow, int) {
	e.p.T.Helper()
	msgs := e.p.Messages()
	if len(msgs) == 0 {
		return notification.MessageRow{}, 0
	}
	m := msgs[len(msgs)-1]
	require.Equal(e.p.T, "alert_status_update", m.MessageType)
	require.NotNil(e.p.T, m.AlertLogID)
	return m, len(msgs)
}

func TestDeltaUpdates(t *testing.T) {
	e := newEnv(t)
	p := e.p
	alice, aliceCM := p.AddNotifiedUser("alice")
	ctx := p.NotificationChannel.ResolveDests(p.Admin(), deltastorestest.Dest("channel"))
	channel, err := p.NotificationChannel.MapDestToID(ctx, nil, deltastorestest.Dest("channel"))
	require.NoError(t, err)
	al := p.AddAlert(e.svc, "down")
	e.subscribe(al, aliceCM, "")
	e.update()
	_, n := e.newest()
	assert.Zero(t, n, "nothing changed")

	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusActive))
	e.update()
	m, n := e.newest()
	assert.Equal(t, 1, n)
	assert.Equal(t, aliceCM, *m.ContactMethodID)
	assert.Equal(t, alice, *m.UserID)
	assert.Equal(t, al, *m.AlertID)
	log, _ := p.Tables.AlertLog.Logs.Get(p.View(), filestore.IntKey(*m.AlertLogID))
	assert.Equal(t, "acknowledged", log.Event)
	before := p.Delta.View().Index()
	e.update()
	assert.Equal(t, before, p.Delta.View().Index())

	// A channel is told, too. The user is not told what they did themselves.
	e.subscribe(al, "", channel.String())
	e.update()
	_, n = e.newest()
	require.Equal(t, 2, n, "it was subscribed when it was triggered")
	e.in.Clock.Advance(time.Second) // the clock is stopped: this makes the next message the newest
	require.NoError(t, p.Alert.UpdateStatus(p.As(alice, permission.RoleUser), int(al), alert.StatusClosed))
	e.update()
	m, n = e.newest()
	assert.Equal(t, 3, n)
	assert.Equal(t, channel.String(), *m.ChannelID)
	assert.Nil(t, m.UserID)
	assert.Zero(t, e.subscriptions(), "nothing happens to a closed alert")
}

func TestDeltaEnd(t *testing.T) {
	e := newEnv(t)
	p := e.p
	_, disabled := p.AddNotifiedUser("disabled")
	_, old := p.AddNotifiedUser("older")
	al := p.AddAlert(e.svc, "down")
	e.subscribe(al, disabled, "")
	e.subscribe(al, old, "")
	p.Update(func(tx *filestore.Tx) error {
		_, err := p.Tables.ContactMethod.SetDisabled(tx, disabled, true)
		return err
	})
	e.update()
	assert.Equal(t, 2, e.subscriptions())
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusActive))
	e.update()
	_, n := e.newest()
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, e.subscriptions())
	e.in.Clock.Advance(7 * 24 * time.Hour)
	e.update()
	assert.Equal(t, 1, e.subscriptions())
	e.in.Clock.Advance(time.Second)
	e.update()
	assert.Zero(t, e.subscriptions())
}

// TestDeltaBadSubscription checks that a subscription that can't be handled does
// not keep the others from being (F53).
func TestDeltaBadSubscription(t *testing.T) {
	e := newEnv(t)
	p := e.p
	_, bad := p.AddNotifiedUser("bad")
	_, good := p.AddNotifiedUser("good")
	al := p.AddAlert(e.svc, "down")
	e.subscribe(al, bad, "")
	e.subscribe(al, good, "")
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.ContactMethod.Methods.Get(tx, bad)
		row.Dest = []byte(`{"Type":"nobody knows it","Args":{}}`)
		return p.Tables.ContactMethod.Methods.Put(tx, row)
	})
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusActive))
	e.update()
	m, n := e.newest()
	assert.Equal(t, 1, n)
	assert.Equal(t, good, *m.ContactMethodID)
	assert.Equal(t, 2, e.subscriptions())
}
