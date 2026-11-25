package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfymsg"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/schedule/rotation"
)

// deltaPod is a pod that runs an engine.
type deltaPod struct {
	*deltastorestest.Pod
	e *Engine
}

func newDeltaPod(t testing.TB, in *deltastorestest.Install) deltaPod {
	t.Helper()
	return withEngine(t, in.Pod(t))
}

func withEngine(t testing.TB, p *deltastorestest.Pod) deltaPod {
	t.Helper()
	e, err := NewDeltaEngine(p.Ctx(), DeltaConfig{DB: p.Delta, Tables: p.Tables, Name: "pod"}, &Config{
		AlertLogStore: p.AlertLog, AlertStore: p.Alert, ContactMethodStore: p.ContactMethod,
		NotificationManager: notification.NewManager(p.Registry), UserStore: p.User, NotificationStore: p.Notification,
		NCStore: p.NotificationChannel, OnCallStore: p.OnCall, ScheduleStore: p.Schedule, AuthLinkStore: p.AuthLink,
		DestRegistry: p.Registry, ConfigSource: p.Config, Logger: slog.New(slog.DiscardHandler),
		CycleTime: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.delta.stop(context.Background()) })
	return deltaPod{p, e}
}

func (p deltaPod) cycle() { p.e.deltaCycle(p.Ctx()) }

func sent[M nfymsg.Message](in *deltastorestest.Install) (out []M) {
	for _, m := range in.Sent() {
		if m, ok := m.(M); ok {
			out = append(out, m)
		}
	}
	return out
}

func (p deltaPod) status(alertID int64) alert.Status {
	p.T.Helper()
	s, _ := p.Tables.Alert.Status(p.View(), alertID)
	return s
}

// TestDeltaPage follows an alert from its creation to the page and back.
func TestDeltaPage(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	alice, method := a.AddNotifiedUser("alice")
	a.Update(func(tx *filestore.Tx) error { // she wants to know what becomes of alerts
		row, _ := a.Tables.ContactMethod.Methods.Get(tx, method)
		row.EnableStatusUpdates = true
		return a.Tables.ContactMethod.Methods.Put(tx, row)
	})
	rot := a.AddRotation("rot", rotation.TypeDaily, 1, in.Clock.Now(), alice)
	sched := a.AddSchedule("sched")
	a.AddScheduleRule(sched, assignment.RotationTarget(rot))
	pol := a.AddPolicy("policy", 0)
	a.AddStep(pol, 5, deltastorestest.ScheduleDest(sched))
	svc := a.AddService("service", pol)
	al := a.AddAlert(svc, "down")

	// One cycle, with one commit before the page is sent and one after.
	before := a.Delta.View().Index()
	a.cycle()
	pages := sent[notification.Alert](in)
	require.Len(t, pages, 1)
	assert.Equal(t, before+2, a.Delta.View().Index())
	assert.Equal(t, int(al), pages[0].AlertID)
	assert.Equal(t, "down", pages[0].Summary)
	assert.Equal(t, "service", pages[0].ServiceName)
	assert.Nil(t, pages[0].OriginalStatus)
	assert.Equal(t, deltastorestest.Dest("alice"), pages[0].Dest)
	entries, err := a.AlertLog.Search(a.Ctx(), &alertlog.SearchOptions{FilterAlertIDs: []int{int(al)}})
	require.NoError(t, err)
	assert.Equal(t, alertlog.TypeNotificationSent, entries[0].Type())
	assert.Contains(t, entries[0].String(a.Ctx()), "alice")
	assert.Equal(t, 1, a.Tables.Alert.Subscriptions.Len(a.Delta.View()))
	a.cycle()
	assert.Len(t, in.Sent(), 1)

	// The answer comes in on another pod, which has not heard of any of it.
	msgID := pages[0].MsgID()
	_, ok := b.Tables.Notification.Messages.Get(b.Delta.View(), msgID)
	require.False(t, ok)
	require.NoError(t, b.e.Receive(b.Ctx(), msgID, notification.ResultAcknowledge))
	assert.Equal(t, alert.StatusActive, b.status(al))
	require.ErrorContains(t, b.e.Receive(b.Ctx(), uuid.NewString(), notification.ResultAcknowledge), "no rows")

	// Somebody else closes it, and the user is told.
	require.NoError(t, b.Alert.UpdateStatus(b.Admin(), int(al), alert.StatusClosed))
	in.Clock.Advance(time.Minute)
	a.cycle()
	updates := sent[notification.AlertStatus](in)
	require.Len(t, updates, 1)
	assert.Equal(t, notification.AlertStateClosed, updates[0].NewAlertState)
	assert.Equal(t, msgID, updates[0].OriginalStatus.ID)
	assert.Zero(t, a.Tables.Alert.Subscriptions.Len(a.View()))

	// What the provider reports is recorded, on any pod.
	require.NoError(t, b.e.SetSendResult(context.Background(), &notification.SendResult{
		ID: msgID, Status: notification.Status{State: notification.StateDelivered, Details: "delivered"},
	}))
	m, _ := a.Tables.Notification.Messages.Get(a.View(), msgID)
	assert.Equal(t, "delivered", m.LastStatus)
}

// TestDeltaDestinations checks what is known of a destination, and that answers
// can turn one off and on.
func TestDeltaDestinations(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	a.AddNotifiedUser("alice")
	ctx, err := a.NotificationChannel.ResolveDests(a.Admin(), deltastorestest.Dest("channel"))
	require.NoError(t, err)
	_, err = a.NotificationChannel.MapDestToID(ctx, nil, deltastorestest.Dest("channel"))
	require.NoError(t, err)
	known := func(addr string) bool {
		ok, err := b.e.IsKnownDest(b.Ctx(), deltastorestest.Dest(addr))
		require.NoError(t, err)
		return ok
	}
	assert.True(t, known("alice"))
	assert.True(t, known("channel"))
	assert.False(t, known("nobody"))
	require.NoError(t, a.e.Stop(a.Ctx(), deltastorestest.Dest("alice")))
	b.Sync()
	assert.False(t, known("alice"))
	require.NoError(t, b.e.Start(b.Ctx(), deltastorestest.Dest("alice")))
	assert.True(t, known("alice"))
	require.NoError(t, b.e.Stop(b.Ctx(), deltastorestest.Dest("nobody")))
}

// TestDeltaSignal checks that the parameters of a signal are sent.
func TestDeltaSignal(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	ctx, err := p.NotificationChannel.ResolveDests(p.Admin(), deltastorestest.Dest("channel"))
	require.NoError(t, err)
	channel, err := p.NotificationChannel.MapDestToID(ctx, nil, deltastorestest.Dest("channel"))
	require.NoError(t, err)
	messageID := uuid.NewString()
	p.Update(func(tx *filestore.Tx) error {
		id := channel.String()
		_, err := p.Tables.Notification.InsertMessage(tx, notification.MessageRow{
			ID: messageID, MessageType: string(gadb.EnumOutgoingMessagesTypeSignalMessage), ServiceID: &svc, ChannelID: &id,
		})
		return err
	})
	require.NoError(t, p.IntegrationKey.InsertSignalMessage(p.Ctx(), nil, gadb.IntKeyInsertSignalMessageParams{
		DestID: channel, ServiceID: uuid.MustParse(svc), Params: []byte(`{"k":"v"}`),
	}))
	p.Update(func(tx *filestore.Tx) error { return p.Tables.IntegrationKey.SetSignalMessage(tx, 1, messageID) })
	p.cycle()
	signals := sent[notification.SignalMessage](in)
	require.Len(t, signals, 1)
	assert.Equal(t, map[string]string{"k": "v"}, signals[0].Params)
}

// onCallService adds a service whose alerts page a user.
func onCallService(p deltaPod, name string) (serviceID string) {
	p.T.Helper()
	userID, _ := p.AddNotifiedUser("user-" + name)
	pol := p.AddPolicy("policy-"+name, 0)
	p.AddStep(pol, 5, deltastorestest.UserDest(userID))
	return p.AddService("service-"+name, pol)
}

// TestDeltaOneEngine checks that several pods page once, and that another goes
// on when the one that does stops, or disappears (F28, F55).
func TestDeltaOneEngine(t *testing.T) {
	in := deltastorestest.New(t)
	pods := []deltaPod{newDeltaPod(t, in), newDeltaPod(t, in), newDeltaPod(t, in)}
	svc := onCallService(pods[0], "svc")
	page := func(name string) {
		t.Helper()
		before := len(in.Sent())
		in.Clock.Advance(time.Minute)
		pods[0].AddAlert(svc, name)
		for _, p := range pods {
			p.Sync()
		}
		var wg sync.WaitGroup
		for _, p := range pods {
			wg.Go(func() {
				for range 3 {
					p.cycle()
				}
			})
		}
		wg.Wait()
		require.Len(t, in.Sent(), before+1, name)
	}
	page("first")
	holder := -1
	for i, p := range pods {
		if p.e.delta.runner.Holds() {
			assert.Equal(t, -1, holder)
			holder = i
		}
	}
	require.GreaterOrEqual(t, holder, 0)

	// A clean stop hands over at once.
	require.NoError(t, pods[holder].e.delta.stop(t.Context()))
	stopped := pods[holder]
	pods = append(append([]deltaPod{}, pods[:holder]...), pods[holder+1:]...)
	page("after a clean stop")
	page("again")
	assert.False(t, stopped.e.delta.runner.Holds())

	// One that disappears is replaced once its lease is over.
	var gone, left deltaPod
	for _, p := range pods {
		if p.e.delta.runner.Holds() {
			gone = p
		} else {
			left = p
		}
	}
	require.NotNil(t, gone.e)
	al := left.AddAlert(svc, "while nobody is there")
	left.cycle()
	n := len(in.Sent())
	assert.NotEqual(t, al, int64(0))
	in.Clock.Advance(defaultLeaseTime + time.Second)
	left.cycle()
	assert.Len(t, in.Sent(), n+1)
	gone.cycle()
	assert.Len(t, in.Sent(), n+1)
	assert.False(t, gone.e.delta.runner.Holds(), "it has found out")
}

// TestDeltaKilledWhileSending checks that a page whose pod is cut off while it
// sends it is sent again, and that what the first one reports later does not
// count (F3, F54).
func TestDeltaKilledWhileSending(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	svc := onCallService(a, "svc")
	al := a.AddAlert(svc, "down")
	started, release := make(chan struct{}), make(chan struct{})
	var n atomic.Int32
	in.OnSend = func(nfymsg.Message) error {
		if n.Add(1) == 1 {
			close(started)
			<-release
			return errors.New("the connection was lost")
		}
		return nil
	}
	done := make(chan struct{})
	go func() { defer close(done); a.cycle() }()
	<-started
	in.Clock.Advance(time.Minute)
	b.Sync()
	for range 3 {
		b.cycle()
		in.Clock.Advance(20 * time.Second)
	}
	assert.EqualValues(t, 2, n.Load())
	close(release)
	<-done
	a.Sync()
	msgs := b.Messages()
	require.Len(t, msgs, 1)
	assert.Equal(t, "sent", msgs[0].LastStatus)
	assert.EqualValues(t, 1, msgs[0].RetryCount)
	for range 3 {
		a.cycle()
		b.cycle()
		in.Clock.Advance(20 * time.Second)
	}
	assert.EqualValues(t, 2, n.Load())
	require.NoError(t, b.Alert.UpdateStatus(b.Admin(), int(al), alert.StatusClosed))
}

type badModule struct{ err error }

func (badModule) Name() string { return "Engine.Bad" }
func (m badModule) UpdateAll(context.Context, *filestore.Tx) error {
	if m.err == nil {
		panic("oops")
	}
	return m.err
}

// TestDeltaBadModules checks that modules that fail don't keep the others from
// working, or messages from being sent (F53).
func TestDeltaBadModules(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	p.e.delta.modules = append([]deltaModule{badModule{errors.New("boom")}, badModule{}}, p.e.delta.modules...)
	p.AddAlert(onCallService(p, "svc"), "down")
	p.cycle()
	assert.Len(t, in.Sent(), 1)
	assert.True(t, p.e.delta.runner.Holds())
	require.NoError(t, p.e.Health())
}

// TestDeltaIdle checks what an engine with nothing to do costs: a commit now and
// then, to keep its lease (F37).
func TestDeltaIdle(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	onCallService(p, "svc")
	p.cycle()
	before := p.Delta.View().Index()
	for range 120 { // ten minutes
		in.Clock.Advance(5 * time.Second)
		p.cycle()
	}
	assert.LessOrEqual(t, p.Delta.View().Index()-before, uint64(40))
	other := newDeltaPod(t, in)
	before = p.Delta.View().Index()
	for range 5 {
		other.cycle()
	}
	assert.Equal(t, before, other.Delta.View().Index(), "a pod without it costs none")
}

// TestDeltaHealth checks that an engine that does not work is noticed (F55).
func TestDeltaHealth(t *testing.T) {
	in := deltastorestest.New(t)
	var down atomic.Bool
	in.Wrap = func(s objstore.Store) objstore.Store { return &flakyStore{Store: s, down: &down} }
	in.Options.CommitAttempts = 1
	p := newDeltaPod(t, in)
	p.cycle()
	require.NoError(t, p.e.Health())
	down.Store(true)
	in.Clock.Advance(time.Minute)
	p.cycle()
	require.NoError(t, p.e.Health(), "once is nothing")
	p.e.delta.failing = time.Now().Add(-2 * healthGrace)
	p.cycle()
	require.ErrorContains(t, p.e.Health(), "the engine has not worked since")
	down.Store(false)
	p.cycle()
	require.NoError(t, p.e.Health())

	// Another pod's lease that nobody renews, and that can't be taken.
	b := newDeltaPod(t, in)
	in.Clock.Advance(5 * time.Minute)
	down.Store(true)
	b.cycle()
	b.e.delta.failing = time.Now().Add(-2 * healthGrace)
	b.cycle()
	require.Error(t, b.e.Health())
	b.e.cfg.DisableCycle = true
	require.NoError(t, b.e.Health(), "it does not run one")
	var none *Engine
	require.NoError(t, none.Health())
}

type flakyStore struct {
	objstore.Store
	down *atomic.Bool
}

func (s *flakyStore) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if s.down.Load() && strings.HasPrefix(name, "_delta_log/") {
		return errors.New("storage is down")
	}
	return s.Store.PutIfAbsent(ctx, name, data)
}

// TestDeltaLifecycle checks that an engine runs, pauses, goes on and stops, and
// that it gives its lease back when it does.
func TestDeltaLifecycle(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	svc := onCallService(a, "svc")
	ctx, cancel := context.WithCancel(a.Ctx())
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- a.e.Run(ctx) }()
	wait := func() {
		t.Helper()
		id := a.e.NextCycleID()
		a.e.Trigger()
		require.NoError(t, a.e.WaitCycleID(ctx, id))
	}
	a.AddAlert(svc, "first")
	wait()
	assert.Len(t, in.Sent(), 1)

	require.NoError(t, a.e.Pause(ctx))
	assert.False(t, a.e.delta.runner.Holds())
	b.Sync()
	in.Clock.Advance(time.Minute)
	b.AddAlert(svc, "second")
	b.cycle()
	assert.Len(t, in.Sent(), 2, "another pod goes on")
	require.NoError(t, a.e.Resume(ctx))
	require.NoError(t, b.e.delta.stop(ctx))
	in.Clock.Advance(time.Minute)
	a.AddAlert(svc, "third")
	wait()
	assert.Len(t, in.Sent(), 3)

	require.NoError(t, a.e.Shutdown(ctx))
	require.NoError(t, <-ran)
	assert.False(t, a.e.delta.runner.Holds())
	b.Sync()
	b.cycle()
	assert.True(t, b.e.delta.runner.Holds())
}

// TestDeltaMaintenance checks that the engine does what is due beside its cycles.
func TestDeltaMaintenance(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	svc := onCallService(p, "svc")
	al := p.AddAlert(svc, "old")
	require.NoError(t, p.Alert.UpdateStatus(p.Admin(), int(al), alert.StatusClosed))
	in.Clock.Advance(48 * time.Hour)
	cfg := config.Config{}
	cfg.Maintenance.AlertCleanupDays = 1
	ctx := cfg.Context(p.Ctx())
	p.e.delta.maintain(ctx)
	assert.Nil(t, p.e.delta.maintDone, "without the lease it does not")
	p.cycle()
	p.e.delta.maintain(ctx)
	p.e.delta.maintain(ctx)
	<-p.e.delta.maintDone
	_, ok := p.Tables.Alert.Alerts.Get(p.View(), filestore.IntKey(al))
	assert.False(t, ok)
	require.NoError(t, p.e.delta.stop(t.Context()))
}

// BenchmarkDeltaCycle measures a cycle of a large installation in which nothing
// is due (F15).
func BenchmarkDeltaCycle(b *testing.B) {
	in := deltastorestest.New(b)
	p := newDeltaPod(b, in)
	const users, scheds, policies, alertsPer = 500, 500, 1000, 5
	var userIDs, schedIDs []string
	for i := range users {
		userIDs = append(userIDs, p.AddUser(fmt.Sprintf("user%d", i)))
	}
	for i := range scheds {
		rot := p.AddRotation(fmt.Sprintf("rot%d", i), rotation.TypeDaily, 1, in.Clock.Now(), userIDs[i%users], userIDs[(i+1)%users])
		id := p.AddSchedule(fmt.Sprintf("sched%d", i))
		p.AddScheduleRule(id, assignment.RotationTarget(rot))
		p.AddScheduleRule(id, assignment.UserTarget(userIDs[i%users]))
		schedIDs = append(schedIDs, id)
	}
	for i := range policies {
		pol := p.AddPolicy(fmt.Sprintf("policy%d", i), 3)
		p.AddStep(pol, 30, deltastorestest.ScheduleDest(schedIDs[i%scheds]))
		p.AddStep(pol, 30, deltastorestest.UserDest(userIDs[i%users]))
		svc := p.AddService(fmt.Sprintf("service%d", i), pol)
		p.Update(func(tx *filestore.Tx) error {
			for range alertsPer {
				key := "user:1:" + uuid.NewString()
				if _, err := p.Alert.DeltaInsert(tx, alert.DeltaAlert{Summary: "down", ServiceID: &svc, DedupKey: &key}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	for range 10 {
		p.cycle()
	}
	b.Logf("%d alerts, %d cycles", p.Tables.Alert.Alerts.Len(p.Delta.View()), p.Tables.Engine.Cycles.Len(p.Delta.View()))
	b.ResetTimer()
	for range b.N {
		p.cycle()
	}
}
