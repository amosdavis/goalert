package signalmgr_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/signalmgr"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return signalmgr.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
}

func TestDeltaSignals(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc, other := p.AddService("service", pol), p.AddService("other", pol)
	channel := func(name string) uuid.UUID {
		ctx, err := p.NotificationChannel.ResolveDests(p.Admin(), deltastorestest.Dest(name))
		require.NoError(t, err)
		id, err := p.NotificationChannel.MapDestToID(ctx, nil, deltastorestest.Dest(name))
		require.NoError(t, err)
		return id
	}
	one, two := channel("one"), channel("two")
	signal := func(serviceID string, dest uuid.UUID, params string) {
		t.Helper()
		require.NoError(t, p.IntegrationKey.InsertSignalMessage(p.Ctx(), nil, gadb.IntKeyInsertSignalMessageParams{
			DestID: dest, ServiceID: uuid.MustParse(serviceID), Params: []byte(params),
		}))
	}
	// params are those of the messages, in order.
	params := func() []string {
		var out []string
		for _, m := range p.Messages() {
			require.Equal(t, "signal_message", m.MessageType)
			raw, ok := p.Tables.IntegrationKey.SignalParams(p.Delta.View(), m.ID)
			require.True(t, ok)
			out = append(out, string(raw))
		}
		return out
	}
	signal(svc, one, `{"n":"1"}`)
	signal(svc, one, `{"n":"2"}`)
	signal(svc, two, `{"n":"3"}`)
	signal(other, one, `{"n":"4"}`)
	update(p)
	assert.ElementsMatch(t, []string{`{"n":"1"}`, `{"n":"3"}`, `{"n":"4"}`}, params(), "one at a time for each service and destination")
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index())

	// Once it is sent the next one follows.
	for _, m := range p.Messages() {
		m.LastStatus, m.SentAt = "sent", &m.CreatedAt
		p.Update(func(tx *filestore.Tx) error { return p.Tables.Notification.SetMessageStatus(tx, m) })
	}
	update(p)
	assert.ElementsMatch(t, []string{`{"n":"1"}`, `{"n":"3"}`, `{"n":"4"}`, `{"n":"2"}`}, params())

	// One that waited too long is dropped.
	signal(svc, one, `{"n":"5"}`)
	in.Clock.Advance(time.Hour + time.Second)
	update(p)
	assert.Len(t, params(), 4)
	assert.Equal(t, 4, p.Tables.IntegrationKey.Signals.Len(p.View()))
}
