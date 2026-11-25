package message

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/objstore"
)

// deltaPod is a pod that sends messages.
type deltaPod struct {
	*deltastorestest.Pod
	db *DeltaDB
}

func newDeltaPod(t *testing.T, in *deltastorestest.Install) deltaPod {
	p := in.Pod(t)
	return deltaPod{p, NewDeltaDB(p.Delta, p.Tables, p.AlertLog)}
}

// claimAll commits the first half of a cycle, as the engine does.
func (p deltaPod) claimAll(ctx context.Context) (*Batch, error) {
	var b Batch
	_, err := p.Delta.Update(ctx, func(tx *filestore.Tx) error { return p.db.UpdateAll(ctx, tx, &b) })
	switch {
	case err == nil:
		p.db.Done(&b)
	case errors.Is(err, objstore.ErrUnknown):
		p.db.Unclear(&b)
	}
	return &b, err
}

// cycle runs a cycle.
func (p deltaPod) cycle(send DeltaSendFunc) *Batch {
	p.T.Helper()
	b, err := p.claimAll(p.Ctx())
	require.NoError(p.T, err)
	p.db.Send(p.Ctx(), b, send, nil)
	return b
}

// sender is a provider that counts.
type sender struct {
	mu    sync.Mutex
	sent  map[string]int
	res   func(m *Message) (*notification.SendResult, error)
	after FollowUp
}

func (s *sender) send(_ context.Context, m *Message) (*notification.SendResult, FollowUp, error) {
	s.mu.Lock()
	if s.sent == nil {
		s.sent = map[string]int{}
	}
	s.sent[m.ID]++
	s.mu.Unlock()
	if s.res == nil {
		return result(m.ID, notification.StateSent, 0), s.after, nil
	}
	res, err := s.res(m)
	return res, s.after, err
}

func (s *sender) count() (n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.sent {
		n += v
	}
	return n
}

func result(id string, state notification.State, seq int) *notification.SendResult {
	return &notification.SendResult{
		ID: id, ProviderMessageID: notification.ProviderMessageID{ProviderName: "test", ExternalID: "ext-" + id},
		Status: notification.Status{State: state, Sequence: seq},
	}
}

// add adds a message to a new user.
func (p deltaPod) add(name string, change ...func(m *notification.MessageRow)) string {
	p.T.Helper()
	userID := p.AddUser(name)
	return p.addTo(userID, p.AddMethod(userID, name), change...)
}

func (p deltaPod) addTo(userID, methodID string, change ...func(m *notification.MessageRow)) string {
	p.T.Helper()
	m := notification.MessageRow{MessageType: "test_notification", UserID: &userID, ContactMethodID: &methodID}
	for _, fn := range change {
		fn(&m)
	}
	p.Update(func(tx *filestore.Tx) (err error) {
		m, err = p.Tables.Notification.InsertMessage(tx, m)
		return err
	})
	return m.ID
}

func (p deltaPod) get(id string) notification.MessageRow {
	p.T.Helper()
	m, ok := p.Tables.Notification.Messages.Get(p.View(), id)
	require.True(p.T, ok)
	return m
}

func (p deltaPod) history(id string) (out []string) {
	p.T.Helper()
	p.Tables.Notification.History.Ascend(p.View(), func(h notification.StatusHistoryRow) bool {
		if h.MessageID == id {
			out = append(out, h.Status)
		}
		return true
	})
	return out
}

func (p deltaPod) logged(alertID int64) (n int) {
	p.T.Helper()
	p.Tables.AlertLog.AscendEvent(p.View(), alertID, alertlog.TypeNotificationSent, func(alertlog.DeltaLog) bool { n++; return true })
	return n
}

// TestDeltaSend checks that a message is claimed by a commit before it is sent,
// and that what follows from sending it is in the commit that records it.
func TestDeltaSend(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	id := p.add("alice")
	var s sender
	s.res = func(m *Message) (*notification.SendResult, error) {
		row, _ := p.Tables.Notification.Messages.Get(p.Delta.View(), m.ID)
		assert.Equal(t, string(StatusSending), row.LastStatus)
		assert.Equal(t, "addr", firstArg(m.Dest.Args))
		res := result(m.ID, notification.StateSent, 7)
		res.Details, res.SrcValue = "accepted", "+15551234567"
		return res, nil
	}
	s.after = func(_ context.Context, tx *filestore.Tx) error {
		row, _ := p.Tables.Notification.Messages.Get(tx, id)
		assert.Equal(t, string(StatusSent), row.LastStatus)
		return nil
	}
	before := p.Delta.View().Index()
	p.cycle(s.send)
	assert.Equal(t, before+2, p.Delta.View().Index())
	assert.Equal(t, 1, s.count())
	m := p.get(id)
	assert.Equal(t, string(StatusSent), m.LastStatus)
	assert.Equal(t, "accepted", m.StatusDetails)
	assert.EqualValues(t, 7, m.ProviderSeq)
	assert.Equal(t, "test:ext-"+id, *m.ProviderMsgID)
	assert.Equal(t, "+15551234567", *m.SrcValue)
	assert.Equal(t, in.Clock.Now(), *m.SentAt)
	assert.Nil(t, m.FiredAt)
	assert.Nil(t, m.SendingDeadline)
	assert.Equal(t, []string{"pending", "sending", "sent"}, p.history(id))
	before = p.Delta.View().Index()
	p.cycle(s.send)
	assert.Equal(t, before, p.Delta.View().Index())
	assert.Equal(t, 1, s.count())
}

func firstArg(args map[string]string) string {
	for k := range args {
		return k
	}
	return ""
}

// TestDeltaOnePodSends checks that however many pods run the engine at once,
// each message is sent once (F3, F28).
func TestDeltaOnePodSends(t *testing.T) {
	in := deltastorestest.New(t)
	pods := []deltaPod{newDeltaPod(t, in), newDeltaPod(t, in), newDeltaPod(t, in)}
	var ids []string
	for i := range 5 {
		ids = append(ids, pods[0].add(fmt.Sprintf("user%d", i)))
	}
	var s sender
	var wg sync.WaitGroup
	for _, p := range pods {
		wg.Go(func() {
			for range 3 {
				b, err := p.claimAll(p.Ctx())
				if assert.NoError(t, err) {
					p.db.Send(p.Ctx(), b, s.send, nil)
				}
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		assert.Equal(t, 1, s.sent[id])
		assert.Equal(t, string(StatusSent), pods[0].get(id).LastStatus)
	}
}

// TestDeltaCrash checks that a message whose pod stopped before it recorded what
// came of it is sent again (F3), and how often.
func TestDeltaCrash(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	id := a.add("alice")
	var s sender
	for attempt := range 4 {
		_, err := a.claimAll(a.Ctx()) // and no more is heard of it
		require.NoError(t, err)
		require.Equal(t, string(StatusSending), a.get(id).LastStatus, attempt)
		in.Clock.Advance(claimTime - time.Second)
		b.cycle(s.send)
		require.Equal(t, string(StatusSending), b.get(id).LastStatus)
		in.Clock.Advance(time.Second)
		b.cycle(s.send)
		m := b.get(id)
		require.Equal(t, string(StatusFailed), m.LastStatus)
		require.Equal(t, "send deadline expired", m.StatusDetails)
		in.Clock.Advance(retryDelay + time.Second)
	}
	b.cycle(s.send)
	assert.Zero(t, s.count())
	m := b.get(id)
	assert.Equal(t, string(StatusFailed), m.LastStatus)
	assert.Nil(t, m.NextRetryAt)
	assert.EqualValues(t, maxRetries, m.RetryCount)

	// With a retry left, the next pod sends it.
	id = a.add("bob")
	_, err := a.claimAll(a.Ctx())
	require.NoError(t, err)
	in.Clock.Advance(claimTime)
	b.cycle(s.send)
	in.Clock.Advance(retryDelay + time.Second)
	batch := b.cycle(s.send)
	assert.Equal(t, 1, batch.Retried)
	assert.Equal(t, 1, s.sent[id])
	assert.Equal(t, string(StatusSent), b.get(id).LastStatus)
}

// TestDeltaLateResult checks that what came of a claim that has expired does not
// count for the one that followed (F54).
func TestDeltaLateResult(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	id := a.add("alice")
	late, err := a.claimAll(a.Ctx())
	require.NoError(t, err)
	in.Clock.Advance(claimTime)
	var s sender
	b.cycle(s.send)
	in.Clock.Advance(retryDelay + time.Second)
	now, err := b.claimAll(b.Ctx())
	require.NoError(t, err)
	require.Len(t, now.claims, 1)

	followed := 0
	follow := func(context.Context, *filestore.Tx) error { followed++; return nil }
	failed := result(id, notification.StateFailedPerm, 0)
	a.db.finish(a.Ctx(), outcome{claim: late.claims[0], res: failed, follow: follow})
	assert.Equal(t, string(StatusSending), a.get(id).LastStatus)
	assert.Equal(t, 1, followed, "it was sent, after all")
	b.db.Send(b.Ctx(), now, s.send, nil)
	assert.Equal(t, string(StatusSent), b.get(id).LastStatus)
	assert.Equal(t, 1, s.count())

	// A claim that took too long to commit is not used.
	id = a.add("bob")
	slow, err := a.claimAll(a.Ctx())
	require.NoError(t, err)
	in.Clock.Advance(claimTime - sendTimeout + time.Second)
	a.db.Send(a.Ctx(), slow, s.send, nil)
	assert.Zero(t, s.sent[id])
	assert.Equal(t, string(StatusSending), a.get(id).LastStatus)
}

// failingStore fails commits on purpose.
type failingStore struct {
	objstore.Store
	// before may return an error instead of writing, after one although it was
	// written.
	before, after atomic.Pointer[error]
}

func (s *failingStore) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	commit := strings.HasPrefix(name, "_delta_log/")
	if err := s.before.Load(); commit && err != nil {
		return *err
	}
	err := s.Store.PutIfAbsent(ctx, name, data)
	if after := s.after.Load(); commit && after != nil && err == nil {
		return *after
	}
	return err
}

func (s *failingStore) Get(ctx context.Context, name string) ([]byte, error) {
	if err := s.after.Load(); err != nil && strings.HasPrefix(name, "_delta_log/") {
		return nil, errors.New("storage is down")
	}
	return s.Store.Get(ctx, name)
}

// TestDeltaRecordFails checks that a message that was sent is not sent again,
// because that could not be recorded at first.
func TestDeltaRecordFails(t *testing.T) {
	for name, unclear := range map[string]bool{"not recorded": false, "recorded after all": true} {
		t.Run(name, func(t *testing.T) {
			in := deltastorestest.New(t)
			fs := &failingStore{Store: in.Store}
			in.Wrap = func(objstore.Store) objstore.Store { return fs }
			in.Options.CommitAttempts = 1
			p := newDeltaPod(t, in)
			id := p.add("alice")
			followed := 0
			var s sender
			s.after = func(_ context.Context, tx *filestore.Tx) error {
				_, err := tx.NextID("followed")
				followed++
				return err
			}
			b, err := p.claimAll(p.Ctx())
			require.NoError(t, err)
			boom := errors.New("storage is down")
			unknown := objstore.Unknown(boom)
			if unclear {
				fs.after.Store(&unknown)
			} else {
				fs.before.Store(&boom)
			}
			p.db.finish(p.Ctx(), outcome{claim: b.claims[0], res: result(id, notification.StateSent, 0), follow: s.after})
			require.NotEmpty(t, p.db.unrecorded)
			fs.before.Store(nil)
			fs.after.Store(nil)

			// Even if the claim has expired meanwhile.
			in.Clock.Advance(claimTime + retryDelay + time.Second)
			p.cycle(s.send)
			p.cycle(s.send)
			assert.Zero(t, s.count())
			assert.Equal(t, string(StatusSent), p.get(id).LastStatus)
			assert.Empty(t, p.db.unrecorded)
			var n int64
			p.Update(func(tx *filestore.Tx) (err error) { n, err = tx.NextID("followed"); return err })
			assert.EqualValues(t, 2, n, "what follows from it is done once")
		})
	}
}

// TestDeltaFollowFails checks that what follows from sending a message can't
// keep it from being recorded.
func TestDeltaFollowFails(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	first, second := p.add("alice"), p.add("bob")
	var s sender
	s.after = func(_ context.Context, tx *filestore.Tx) error {
		_, err := tx.NextID("half done")
		require.NoError(t, err)
		return errors.New("no")
	}
	s.res = func(m *Message) (*notification.SendResult, error) {
		if m.ID == second {
			panic("oops")
		}
		return result(m.ID, notification.StateSent, 0), nil
	}
	p.cycle(s.send)
	assert.Equal(t, string(StatusSent), p.get(first).LastStatus)
	m := p.get(second)
	assert.Equal(t, string(StatusFailed), m.LastStatus)
	assert.NotNil(t, m.NextRetryAt)
	p.Update(func(tx *filestore.Tx) error {
		n, err := tx.NextID("half done")
		assert.EqualValues(t, 1, n)
		return err
	})
}

func TestDeltaRetry(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	id := p.add("alice")
	var s sender
	s.res = func(m *Message) (*notification.SendResult, error) {
		return result(m.ID, notification.StateFailedTemp, 0), errors.New("busy")
	}
	for i := range 4 {
		p.cycle(s.send)
		assert.Equal(t, i+1, s.count())
		m := p.get(id)
		assert.Equal(t, string(StatusFailed), m.LastStatus)
		assert.Equal(t, "busy", m.StatusDetails)
		assert.Equal(t, "test:ext-"+id, *m.ProviderMsgID)
		p.cycle(s.send)
		assert.Equal(t, i+1, s.count(), "it waits")
		in.Clock.Advance(retryDelay + time.Second)
	}
	p.cycle(s.send)
	assert.Equal(t, 4, s.count())
	assert.Nil(t, p.get(id).NextRetryAt)

	// One that the provider refuses is not tried again.
	id = p.add("bob")
	s.res = func(m *Message) (*notification.SendResult, error) {
		return result(m.ID, notification.StateFailedPerm, 0), nil
	}
	p.cycle(s.send)
	in.Clock.Advance(time.Hour)
	p.cycle(s.send)
	assert.Equal(t, 1, s.sent[id])
}

// TestDeltaUnsendable checks the messages that can't be sent, or need not be.
func TestDeltaUnsendable(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	al := p.AddAlert(svc, "down")
	p.Update(func(tx *filestore.Tx) error {
		return p.Tables.AlertLog.Insert(tx, alertlog.DeltaLog{AlertID: &al, Event: string(alertlog.TypeEscalated)})
	})
	var logID int64
	p.Tables.AlertLog.AscendEvent(p.View(), al, alertlog.TypeEscalated, func(l alertlog.DeltaLog) bool { logID = l.ID; return true })
	about := func(m *notification.MessageRow) {
		m.MessageType, m.AlertID, m.ServiceID, m.EscalationPolicyID = "alert_notification", &al, &svc, &pol
	}
	setMethod := func(id string, fn func(disabled, updates *bool, typ *string)) {
		p.Update(func(tx *filestore.Tx) error {
			row, _ := p.Tables.ContactMethod.Methods.Get(tx, *p.mustGet(tx, id).ContactMethodID)
			fn(&row.Disabled, &row.EnableStatusUpdates, &row.Type)
			return p.Tables.ContactMethod.Methods.Put(tx, row)
		})
	}
	disabled := p.add("disabled", about)
	setMethod(disabled, func(d, _ *bool, _ *string) { *d = true })
	verify := p.add("verify", func(m *notification.MessageRow) {
		code := uuid.NewString()
		p.Update(func(tx *filestore.Tx) error {
			return p.Tables.Notification.Codes.Put(tx, notification.VerificationCodeRow{ID: code, ContactMethodID: *m.ContactMethodID, ExpiresAt: in.Clock.Now()})
		})
		m.MessageType, m.UserVerificationCodeID = "verification_message", &code
	})
	setMethod(verify, func(d, _ *bool, _ *string) { *d = true })
	sms := p.add("phone", about)
	setMethod(sms, func(_, _ *bool, typ *string) { *typ = "SMS" })
	update := func(m *notification.MessageRow) {
		m.MessageType, m.AlertID, m.AlertLogID = "alert_status_update", &al, &logID
	}
	optedOut, optedIn := p.add("out", update), p.add("in", update)
	setMethod(optedIn, func(_, updates *bool, _ *string) { *updates = true })

	var s sender
	p.cycle(s.send)
	assert.Equal(t, map[string]int{verify: 1, optedIn: 1}, s.sent)
	assert.Equal(t, "contact method disabled", p.get(disabled).StatusDetails)
	assert.Equal(t, "SMS/Voice support not enabled by administrator", p.get(sms).StatusDetails)
	for _, id := range []string{disabled, sms} {
		m := p.get(id)
		assert.Equal(t, string(StatusFailed), m.LastStatus)
		assert.Nil(t, m.NextRetryAt)
	}
	_, ok := p.Tables.Notification.Messages.Get(p.View(), optedOut)
	assert.False(t, ok)
	assert.Equal(t, 2, p.logged(al), "that they were not notified")

	// With Twilio it would be sent.
	sms = p.add("phone2", about)
	setMethod(sms, func(_, _ *bool, typ *string) { *typ = "SMS" })
	cfg := config.Config{}
	cfg.Twilio.Enable = true
	ctx := cfg.Context(p.Ctx())
	b, err := p.claimAll(ctx)
	require.NoError(t, err)
	p.db.Send(ctx, b, s.send, nil)
	assert.Equal(t, 1, s.sent[sms])
}

func (p deltaPod) mustGet(r filestore.Reader, id string) notification.MessageRow {
	m, ok := p.Tables.Notification.Messages.Get(r, id)
	require.True(p.T, ok)
	return m
}

// TestDeltaBundles checks that messages about alerts of one service to the same
// destination are one.
func TestDeltaBundles(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	pol := p.AddPolicy("policy", 0)
	p.AddStep(pol, 5)
	svc := p.AddService("service", pol)
	userID := p.AddUser("alice")
	methodID := p.AddMethod(userID, "alice")
	var ids []string
	var alerts []int64
	for i := range 3 {
		alerts = append(alerts, p.AddAlert(svc, fmt.Sprintf("down %d", i)))
	}
	for _, al := range append(alerts, alerts[0]) {
		ids = append(ids, p.addTo(userID, methodID, func(m *notification.MessageRow) {
			m.MessageType, m.AlertID, m.ServiceID, m.EscalationPolicyID = "alert_notification", &al, &svc, &pol
		}))
		in.Clock.Advance(time.Second)
	}
	var s sender
	var bundle Message
	s.res = func(m *Message) (*notification.SendResult, error) {
		bundle = *m
		return result(m.ID, notification.StateSent, 0), nil
	}
	p.cycle(s.send)
	assert.Equal(t, 1, s.count())
	assert.Equal(t, notification.MessageTypeAlertBundle, bundle.Type)
	assert.Zero(t, bundle.AlertID)
	assert.Equal(t, svc, bundle.ServiceID)
	assert.Equal(t, userID, bundle.UserID)
	for i, id := range ids {
		m := p.get(id)
		assert.Equal(t, "bundled", m.LastStatus)
		if i < 3 {
			assert.Equal(t, bundle.ID, m.StatusDetails)
		} else {
			assert.Equal(t, ids[0], m.StatusDetails, "a duplicate")
		}
	}
	assert.Equal(t, []string{"pending", "sending", "sent"}, p.history(bundle.ID))

	// Without bundles, each is sent.
	for _, al := range alerts[:2] {
		p.addTo(userID, methodID, func(m *notification.MessageRow) {
			m.MessageType, m.AlertID, m.ServiceID, m.EscalationPolicyID = "alert_notification", &al, &svc, &pol
		})
	}
	cfg := config.Config{}
	cfg.General.DisableMessageBundles = true
	ctx := cfg.Context(p.Ctx())
	b, err := p.claimAll(ctx)
	require.NoError(t, err)
	p.db.Send(ctx, b, s.send, nil)
	assert.Equal(t, 3, s.count())
}

// TestDeltaLimits checks that what was sent lately counts for the limits, on
// whichever pod it was.
func TestDeltaLimits(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := newDeltaPod(t, in), newDeltaPod(t, in)
	for i := range 8 {
		a.add(fmt.Sprintf("user%d", i))
	}
	var s sender
	a.cycle(s.send)
	assert.Equal(t, 5, s.count(), "for a type of destination")
	b.cycle(s.send)
	assert.Equal(t, 5, s.count())
	in.Clock.Advance(6 * time.Second)
	b.cycle(s.send)
	assert.Equal(t, 8, s.count())
}

// TestDeltaRemote checks messages that a provider has taken and not reported on.
func TestDeltaRemote(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	ids := []string{p.add("sent"), p.add("gone"), p.add("unsupported"), p.add("silent")}
	var s sender
	s.res = func(m *Message) (*notification.SendResult, error) {
		return result(m.ID, notification.StateSending, 8), nil
	}
	p.cycle(s.send)
	var mu sync.Mutex
	asked := map[string]int{}
	status := func(_ context.Context, id notification.ProviderMessageID) (*notification.Status, error) {
		mu.Lock()
		defer mu.Unlock()
		asked[strings.TrimPrefix(id.ExternalID, "ext-")]++
		switch id.ExternalID {
		case "ext-" + ids[0]:
			return &notification.Status{State: notification.StateFailedTemp, Sequence: 1, Details: "final"}, nil
		case "ext-" + ids[1]:
			return nil, notification.ErrStatusNotFound
		case "ext-" + ids[2]:
			return nil, notification.ErrStatusUnsupported
		}
		return nil, errors.New("down")
	}
	run := func() {
		t.Helper()
		b, err := p.claimAll(p.Ctx())
		require.NoError(t, err)
		p.db.Send(p.Ctx(), b, s.send, status)
	}
	run()
	assert.Empty(t, asked, "not yet")
	in.Clock.Advance(time.Minute + time.Second)
	run()
	assert.Len(t, asked, 4)
	m := p.get(ids[0])
	assert.Equal(t, string(StatusFailed), m.LastStatus)
	assert.Equal(t, "final", m.StatusDetails)
	assert.Nil(t, m.NextRetryAt, "it is not sent again")
	assert.Equal(t, "message no longer exists at provider; final status unknown", p.get(ids[1]).StatusDetails)
	for _, id := range ids[2:] {
		assert.Equal(t, string(StatusQueuedRemotely), p.get(id).LastStatus)
	}
	run()
	assert.Equal(t, 1, asked[ids[3]], "once a minute")
	in.Clock.Advance(24 * time.Hour)
	run()
	m = p.get(ids[3])
	assert.Equal(t, string(StatusFailed), m.LastStatus)
	assert.Contains(t, m.StatusDetails, "timed out waiting for final status from provider")
	assert.Equal(t, 4, s.count())
}

// TestDeltaStatus checks which reports of a provider count.
func TestDeltaStatus(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	id := p.add("alice")
	other := newDeltaPod(t, in)
	var s sender
	s.res = func(m *Message) (*notification.SendResult, error) {
		return result(m.ID, notification.StateSending, 2), nil
	}
	p.cycle(s.send)
	sentAt := *p.get(id).SentAt
	in.Clock.Advance(time.Second)
	report := func(state notification.State, seq int, details string) string {
		t.Helper()
		res := result(id, state, seq)
		res.Details = details
		if seq == 3 {
			res.ID = "" // only the provider's
		}
		require.NoError(t, other.db.UpdateMessageStatus(other.Ctx(), res), "on any pod")
		m := p.get(id)
		return m.LastStatus + " " + m.StatusDetails
	}
	assert.Equal(t, "queued_remotely ", report(notification.StateDelivered, 1, "older"))
	assert.Equal(t, "delivered newer", report(notification.StateDelivered, 3, "newer"))
	assert.Equal(t, "read same", report(notification.StateRead, 3, "same"))
	assert.Equal(t, "delivered polled", report(notification.StateDelivered, -1, "polled"))
	m := p.get(id)
	assert.EqualValues(t, 3, m.ProviderSeq)
	assert.Equal(t, sentAt, *m.SentAt)
	assert.Equal(t, "failed busy", report(notification.StateFailedTemp, 4, "busy"))
	assert.NotNil(t, p.get(id).NextRetryAt)
	assert.Equal(t, "failed busy", report(notification.StateSent, 5, "too late"))
	assert.Equal(t, "failed no", report(notification.StateFailedPerm, 6, "no"))
	assert.Nil(t, p.get(id).NextRetryAt)
	require.NoError(t, p.db.UpdateMessageStatus(p.Ctx(), nil))
	require.NoError(t, p.db.UpdateMessageStatus(p.Ctx(), &notification.SendResult{ID: uuid.NewString()}))
	require.Error(t, p.db.UpdateMessageStatus(context.Background(), result(id, notification.StateSent, 9)))

	// A report that comes before what came of sending it is recorded.
	id = p.add("bob")
	b, err := p.claimAll(p.Ctx())
	require.NoError(t, err)
	assert.Equal(t, "delivered fast", report(notification.StateDelivered, 4, "fast"))
	followed := 0
	p.db.finish(p.Ctx(), outcome{claim: b.claims[0], res: result(id, notification.StateSending, 0), follow: func(context.Context, *filestore.Tx) error {
		followed++
		return nil
	}})
	assert.Equal(t, string(StatusDelivered), p.get(id).LastStatus)
	assert.Equal(t, 1, followed)
}

// TestDeltaBadMessages checks that messages that can't be handled don't keep the
// others from being sent (F53).
func TestDeltaBadMessages(t *testing.T) {
	in := deltastorestest.New(t)
	p := newDeltaPod(t, in)
	bad, good := p.add("bad"), p.add("good")
	p.Update(func(tx *filestore.Tx) error {
		row, _ := p.Tables.ContactMethod.Methods.Get(tx, *p.mustGet(tx, bad).ContactMethodID)
		row.Dest = []byte("{")
		return p.Tables.ContactMethod.Methods.Put(tx, row)
	})
	remote := p.add("remote", func(m *notification.MessageRow) {
		id := "no provider"
		m.LastStatus, m.SentAt, m.ProviderMsgID = "queued_remotely", ptr(in.Clock.Now()), &id
	})
	in.Clock.Advance(2 * time.Minute)
	var s sender
	b, err := p.claimAll(p.Ctx())
	require.NoError(t, err)
	p.db.Send(p.Ctx(), b, s.send, nil)
	assert.Equal(t, map[string]int{good: 1}, s.sent)
	assert.Equal(t, string(StatusPending), p.get(bad).LastStatus)
	assert.Equal(t, string(StatusQueuedRemotely), p.get(remote).LastStatus)
}

func ptr[T any](v T) *T { return &v }
