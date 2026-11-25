package metricsmanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/metricsmanager"
	"github.com/target/goalert/filestore"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return metricsmanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
}

func metric(p *deltastorestest.Pod, alertID int64) (alertmetrics.DeltaMetric, bool) {
	p.T.Helper()
	return p.Tables.AlertMetrics.Metrics.Get(p.View(), filestore.IntKey(alertID))
}

func TestDeltaMetrics(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	al, other := p.AddAlert(svc, "down"), p.AddAlert(svc, "other")
	set := func(id int64, s alert.Status) {
		t.Helper()
		require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(id), s))
	}
	escalated := func(id int64) {
		p.Update(func(tx *filestore.Tx) error {
			return p.Tables.AlertLog.Insert(tx, alertlog.DeltaLog{AlertID: &id, Event: string(alertlog.TypeEscalated)})
		})
	}
	update(p)
	in.Clock.Advance(time.Minute)
	set(al, alert.StatusActive)
	escalated(al)
	escalated(al)
	escalated(other)
	in.Clock.Advance(time.Minute)
	set(al, alert.StatusClosed)
	update(p)
	m, ok := metric(p, al)
	require.True(t, ok)
	assert.Equal(t, svc, m.ServiceID)
	assert.Equal(t, time.Minute.Microseconds(), *m.TimeToAck)
	assert.Equal(t, 2*time.Minute.Microseconds(), *m.TimeToClose)
	assert.True(t, m.Escalated)
	assert.Equal(t, in.Clock.Now(), m.ClosedAt)
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index())

	// Never acknowledged, and closed at the same time as the last one.
	set(other, alert.StatusClosed)
	update(p)
	m, ok = metric(p, other)
	require.True(t, ok)
	assert.Nil(t, m.TimeToAck)
	assert.False(t, m.Escalated)
}

// TestDeltaMany checks that a lot of closed alerts are all measured, over days,
// and that deleted ones don't stop it.
func TestDeltaMany(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	var ids []int64
	for day := range 3 {
		at := in.Clock.Now().Add(time.Duration(day-5) * 24 * time.Hour)
		p.Update(func(tx *filestore.Tx) error {
			for i := range 400 {
				a, err := p.Alert.DeltaInsert(tx, alert.DeltaAlert{Summary: "down", ServiceID: &svc, Status: "closed", CreatedAt: at})
				if err != nil {
					return err
				}
				// Later ids can have earlier times.
				closed := at.Add(time.Duration(400-i) * time.Second)
				if err := p.Tables.AlertLog.Insert(tx, alertlog.DeltaLog{AlertID: &a.ID, Event: string(alertlog.TypeClosed), Timestamp: &closed}); err != nil {
					return err
				}
				if i%3 == 0 { // deleted since
					if _, err := p.Tables.Alert.Alerts.Delete(tx, filestore.IntKey(a.ID)); err != nil {
						return err
					}
					continue
				}
				ids = append(ids, a.ID)
			}
			return nil
		})
	}
	for range 3 {
		update(p)
	}
	for _, id := range ids {
		_, ok := metric(p, id)
		require.True(t, ok, id)
	}
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index())
}
