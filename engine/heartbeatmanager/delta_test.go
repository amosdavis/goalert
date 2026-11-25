package heartbeatmanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/heartbeatmanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/heartbeat"
	"github.com/target/goalert/limit"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error {
		return heartbeatmanager.NewDeltaDB(p.Tables, p.Alert).UpdateAll(p.Ctx(), tx)
	})
}

// open are the summaries of a service's open alerts.
func open(p *deltastorestest.Pod, serviceID string) []string {
	p.T.Helper()
	var out []string
	for _, id := range p.Tables.Alert.AlertIDs(p.View(), serviceID, string(alert.StatusTriggered), string(alert.StatusActive)) {
		a, _ := p.Tables.Alert.Alerts.Get(p.Delta.View(), filestore.IntKey(id))
		out = append(out, a.Summary)
	}
	return out
}

func addMonitor(p *deltastorestest.Pod, serviceID, name string, muted string) string {
	p.T.Helper()
	m, err := p.Heartbeat.CreateTx(p.Admin(), nil, &heartbeat.Monitor{
		Name: name, ServiceID: serviceID, Timeout: 10 * time.Minute, AdditionalDetails: "call home", Muted: muted,
	})
	require.NoError(p.T, err)
	return m.ID
}

func TestDeltaMonitors(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	mon := addMonitor(p, svc, "mon", "")
	muted := addMonitor(p, svc, "muted", "we know")
	addMonitor(p, svc, "never", "")
	beat := func(id string) { t.Helper(); require.NoError(t, p.Heartbeat.RecordHeartbeat(p.Ctx(), id)) }
	state := func(id string) string {
		m, _ := p.Tables.Heartbeat.Monitors.Get(p.View(), id)
		return m.LastState
	}

	update(p)
	assert.Equal(t, "inactive", state(mon))
	beat(mon)
	beat(muted)
	update(p)
	assert.Equal(t, "healthy", state(mon))
	before := p.Delta.View().Index()
	update(p)
	in.Clock.Advance(10*time.Minute - time.Second)
	update(p)
	assert.Equal(t, before, p.Delta.View().Index())

	in.Clock.Advance(time.Second)
	update(p)
	assert.Equal(t, []string{"Heartbeat monitor 'mon' expired."}, open(p, svc), "one that is muted says nothing")
	assert.Equal(t, "unhealthy", state(mon))
	assert.Equal(t, "unhealthy", state(muted))
	ids := p.Tables.Alert.AlertIDs(p.Delta.View(), svc)
	a, _ := p.Tables.Alert.Alerts.Get(p.Delta.View(), filestore.IntKey(ids[0]))
	assert.Contains(t, a.Details, "Last heartbeat: Wed Nov 25 12:00:00 UTC 2026")
	assert.Contains(t, a.Details, "call home")
	_, ok := p.Tables.Escalation.AlertState(p.Delta.View(), ids[0])
	assert.True(t, ok, "it escalates (F2)")
	update(p)
	assert.Len(t, open(p, svc), 1)

	beat(mon)
	update(p)
	assert.Empty(t, open(p, svc))
	assert.Equal(t, "healthy", state(mon))
}

// TestDeltaBadMonitor checks that a monitor whose alert can't be opened is tried
// again, and does not keep the others from being handled (F53).
func TestDeltaBadMonitor(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	full, other := p.AddService("full", pol), p.AddService("other", pol)
	mon, good := addMonitor(p, full, "mon", ""), addMonitor(p, other, "good", "")
	require.NoError(t, p.Limit.SetMax(p.Admin(), limit.UnackedAlertsPerService, 1))
	al := p.AddAlert(full, "something else")
	for _, id := range []string{mon, good} {
		require.NoError(t, p.Heartbeat.RecordHeartbeat(p.Ctx(), id))
	}
	in.Clock.Advance(time.Hour)
	update(p)
	assert.Equal(t, []string{"something else"}, open(p, full))
	assert.Len(t, open(p, other), 1)
	update(p)
	assert.Equal(t, []string{"something else"}, open(p, full))
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusClosed))
	update(p)
	assert.Equal(t, []string{"Heartbeat monitor 'mon' expired."}, open(p, full))
}
