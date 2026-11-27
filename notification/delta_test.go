package notification

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// Stand-ins for the tables of other packages.
type (
	deltaInt struct {
		ID int64 `parquet:"id"`
	}
	deltaNamed struct {
		ID   string `parquet:"id"`
		Name string `parquet:"name"`
	}
	deltaMethod struct {
		ID               string     `parquet:"id"`
		UserID           string     `parquet:"user_id"`
		DestType         string     `parquet:"dest_type"`
		Type             string     `parquet:"type"`
		Value            string     `parquet:"value"`
		Disabled         bool       `parquet:"disabled"`
		LastTestVerifyAt *time.Time `parquet:"last_test_verify_at,optional,timestamp(microsecond:utc)"`
	}
	deltaChannel struct {
		ID       string `parquet:"id"`
		Name     string `parquet:"name"`
		DestType string `parquet:"dest_type"`
		Type     string `parquet:"type"`
	}
)

var deltaStart = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	clock  *filestore.FakeClock

	alerts, logs                      *filestore.Table[deltaInt]
	channels                          *filestore.Table[deltaChannel]
	methods                           *filestore.Table[deltaMethod]
	cycles, policies, schedules, svcs *filestore.Table[deltaNamed]
	users                             *filestore.Table[deltaNamed]
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(deltaStart)}
	e.clock.SetSpeed(0)
	e.clock.Set(deltaStart)
	intTable := func(name string) *filestore.Table[deltaInt] {
		return filestore.NewTable(e.schema, name, func(r deltaInt) string { return filestore.IntKey(r.ID) })
	}
	named := func(name string) *filestore.Table[deltaNamed] {
		return filestore.NewTable(e.schema, name, func(r deltaNamed) string { return r.ID })
	}
	e.alerts, e.logs = intTable("alerts"), intTable("alert_logs")
	e.cycles, e.policies, e.schedules = named("notification_policy_cycles"), named("escalation_policies"), named("schedules")
	e.svcs, e.users = named("services"), named("users")
	e.methods = filestore.NewTable(e.schema, "user_contact_methods", func(m deltaMethod) string { return m.ID })
	e.channels = filestore.NewTable(e.schema, "notification_channels", func(c deltaChannel) string { return c.ID })
	filestore.References(e.methods, "user_contact_methods_user_id_fkey", e.users,
		func(m deltaMethod) (string, bool) { return m.UserID, true }, filestore.Cascade, nil)
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.alerts, e.logs, e.channels, e.methods, e.policies, e.schedules, e.svcs, e.users)
	DeclareDeltaCycleForeignKey(e.tables, e.cycles)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	return e
}

func (e *deltaEnv) lookups() DeltaLookups {
	return DeltaLookups{
		MethodOwner: func(r filestore.Reader, id string) (string, bool) {
			m, ok := e.methods.Get(r, id)
			return m.UserID, ok
		},
		MethodDest: func(r filestore.Reader, id string) (gadb.DestV1, error) {
			m, ok := e.methods.Get(r, id)
			if !ok {
				return gadb.DestV1{}, sql.ErrNoRows
			}
			return gadb.DestV1{Type: m.DestType}, nil
		},
		MethodDisabled: func(r filestore.Reader, id string) (bool, bool) {
			m, ok := e.methods.Get(r, id)
			return m.Disabled, ok
		},
		MethodTypeValue: func(r filestore.Reader, id string) (string, string, bool) {
			m, ok := e.methods.Get(r, id)
			return m.Type, m.Value, ok
		},
		SetMethodDisabled: func(tx *filestore.Tx, id string, disabled bool) (bool, error) {
			m, ok := e.methods.Get(tx, id)
			if !ok {
				return false, nil
			}
			m.Disabled = disabled
			return true, e.methods.Put(tx, m)
		},
		SetMethodLastTestVerify: func(tx *filestore.Tx, id string, minAge time.Duration) (bool, error) {
			m, ok := e.methods.Get(tx, id)
			now := tx.Now()
			if !ok || m.LastTestVerifyAt != nil && !m.LastTestVerifyAt.Add(minAge).Before(now) {
				return false, nil
			}
			m.LastTestVerifyAt = &now
			return true, e.methods.Put(tx, m)
		},
		ChannelDest: func(r filestore.Reader, id string) (gadb.DestV1, error) {
			c, ok := e.channels.Get(r, id)
			if !ok {
				return gadb.DestV1{}, sql.ErrNoRows
			}
			return gadb.DestV1{Type: c.DestType}, nil
		},
		ChannelNameType: func(r filestore.Reader, id string) (string, string, bool) {
			c, ok := e.channels.Get(r, id)
			return c.Name, c.Type, ok
		},
		UserName: func(r filestore.Reader, id string) (string, bool) {
			u, ok := e.users.Get(r, id)
			return u.Name, ok
		},
		ServiceName: func(r filestore.Reader, id string) (string, bool) {
			s, ok := e.svcs.Get(r, id)
			return s.Name, ok
		},
	}
}

type deltaPod struct {
	*Store
	e  *deltaEnv
	db txn.DB
	f  *filestore.DeltaDB
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) *deltaPod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &deltaPod{Store: NewDeltaStore(db, e.tables, e.lookups()), e: e, db: db, f: fdb}
}

func (p *deltaPod) try(fn func(tx *filestore.Tx) error) error {
	return deltatxn.Write(context.Background(), p.db, nil, func(s deltatxn.Scope) error { return fn(s.Tx) })
}

func (p *deltaPod) write(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	require.NoError(t, p.try(fn))
}

func (p *deltaPod) addUser(t *testing.T, name string) string {
	t.Helper()
	id := uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error { return p.e.users.Put(tx, deltaNamed{ID: id, Name: name}) })
	return id
}

func (p *deltaPod) addMethod(t *testing.T, userID string) string {
	t.Helper()
	id := uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error {
		return p.e.methods.Put(tx, deltaMethod{ID: id, UserID: userID, DestType: "builtin-smtp-email", Type: "EMAIL", Value: "a@example.com"})
	})
	return id
}

func (p *deltaPod) addChannel(t *testing.T) string {
	t.Helper()
	id := uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error {
		return p.e.channels.Put(tx, deltaChannel{ID: id, Name: "#ops", DestType: "builtin-slack-channel", Type: "SLACK"})
	})
	return id
}

// parents are rows that messages refer to.
type parents struct {
	user, cm, channel, service, policy, schedule, cycle, code string
	alert, log                                                int64
}

func (p *deltaPod) addParents(t *testing.T) parents {
	t.Helper()
	x := parents{
		user: p.addUser(t, "Alice"), channel: p.addChannel(t), service: uuid.NewString(), policy: uuid.NewString(),
		schedule: uuid.NewString(), cycle: uuid.NewString(), code: uuid.NewString(), alert: 1, log: 2,
	}
	x.cm = p.addMethod(t, x.user)
	p.write(t, func(tx *filestore.Tx) error {
		for _, err := range []error{
			p.e.alerts.Put(tx, deltaInt{x.alert}),
			p.e.logs.Put(tx, deltaInt{x.log}),
			p.e.svcs.Put(tx, deltaNamed{ID: x.service, Name: "Service"}),
			p.e.policies.Put(tx, deltaNamed{ID: x.policy}),
			p.e.schedules.Put(tx, deltaNamed{ID: x.schedule}),
			p.e.cycles.Put(tx, deltaNamed{ID: x.cycle}),
			p.e.tables.Codes.Put(tx, VerificationCodeRow{ID: x.code, ContactMethodID: x.cm, Code: 123456, ExpiresAt: tx.Now().Add(time.Hour)}),
		} {
			if err != nil {
				return err
			}
		}
		return nil
	})
	return x
}

// alertMessage is a valid alert notification to the user.
func (x parents) alertMessage() MessageRow {
	return MessageRow{
		MessageType: typeAlert, UserID: &x.user, ContactMethodID: &x.cm,
		AlertID: &x.alert, ServiceID: &x.service, EscalationPolicyID: &x.policy,
	}
}

func (p *deltaPod) insert(t *testing.T, m MessageRow) MessageRow {
	t.Helper()
	p.write(t, func(tx *filestore.Tx) error {
		var err error
		m, err = p.e.tables.InsertMessage(tx, m)
		return err
	})
	return m
}

// set changes a message through SetMessageStatus.
func (p *deltaPod) set(t *testing.T, id string, change func(m *MessageRow)) {
	t.Helper()
	p.write(t, func(tx *filestore.Tx) error {
		m, ok := p.e.tables.Messages.Get(tx, id)
		require.True(t, ok)
		change(&m)
		return p.e.tables.SetMessageStatus(tx, m)
	})
}

func (p *deltaPod) message(t *testing.T, id string) MessageRow {
	t.Helper()
	m, ok := p.e.tables.Messages.Get(p.f.View(), id)
	require.True(t, ok)
	return m
}

func (p *deltaPod) history(t *testing.T, id string) []gadb.MessageStatusHistory {
	t.Helper()
	hist, err := p.MessageStatusHistory(context.Background(), uuid.MustParse(id))
	require.NoError(t, err)
	return hist
}

func ptr[T any](v T) *T { return &v }

func constraint(t *testing.T, err error) string {
	t.Helper()
	var ce interface{ ConstraintName() string }
	require.ErrorAs(t, err, &ce)
	return ce.ConstraintName()
}

func asUser(id string) context.Context {
	return permission.UserContext(context.Background(), id, permission.RoleUser)
}

func TestDeltaMessageChecks(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	now := deltaStart

	for name, want := range map[string]struct {
		change func(m *MessageRow)
		errs   bool
	}{
		"om_alert_svc_ep_ids/alert":   {func(m *MessageRow) { m.AlertID = nil }, true},
		"om_alert_svc_ep_ids/service": {func(m *MessageRow) { m.ServiceID = nil }, true},
		"om_alert_svc_ep_ids/policy":  {func(m *MessageRow) { m.EscalationPolicyID = nil }, true},
		"om_alert_svc_ep_ids/all":     {func(m *MessageRow) {}, false},
		"om_alert_svc_ep_ids/otherType": {func(m *MessageRow) {
			*m = MessageRow{MessageType: typeTest, UserID: m.UserID, ContactMethodID: m.ContactMethodID}
		}, false},

		"om_no_status_bundles/pending": {func(m *MessageRow) { m.MessageType, m.StatusAlertIDs = typeStatusBundle, []int64{1} }, true},
		"om_no_status_bundles/failed": {func(m *MessageRow) {
			m.MessageType, m.StatusAlertIDs, m.LastStatus = typeStatusBundle, []int64{1}, statusFailed
		}, false},

		"om_pending_no_fired_no_sent/fired": {func(m *MessageRow) { m.FiredAt = &now }, true},
		"om_pending_no_fired_no_sent/sent":  {func(m *MessageRow) { m.SentAt = &now }, true},

		"om_processed_no_fired_sent/notSent": {func(m *MessageRow) { m.LastStatus = "delivered" }, true},
		"om_processed_no_fired_sent/fired":   {func(m *MessageRow) { m.LastStatus, m.SentAt, m.FiredAt = "sent", &now, &now }, true},
		"om_processed_no_fired_sent/queued":  {func(m *MessageRow) { m.LastStatus = statusQueuedRemotely }, true},
		"om_processed_no_fired_sent/read":    {func(m *MessageRow) { m.LastStatus = "read" }, true},
		"om_processed_no_fired_sent/sent":    {func(m *MessageRow) { m.LastStatus, m.SentAt = "sent", &now }, false},
		"om_processed_no_fired_sent/bundled": {func(m *MessageRow) { m.LastStatus = statusBundled }, false},
		"om_processed_no_fired_sent/failed":  {func(m *MessageRow) { m.LastStatus, m.FiredAt = statusFailed, &now }, false},

		"om_sending_deadline_reqd/none":     {func(m *MessageRow) { m.LastStatus, m.FiredAt = statusSending, &now }, true},
		"om_sending_fired_no_sent/notFired": {func(m *MessageRow) { m.LastStatus, m.SendingDeadline = statusSending, &now }, true},
		"om_sending_fired_no_sent/sent": {func(m *MessageRow) {
			m.LastStatus, m.SendingDeadline, m.FiredAt, m.SentAt = statusSending, &now, &now, &now
		}, true},
		"om_sending_fired_no_sent/valid": {func(m *MessageRow) { m.LastStatus, m.SendingDeadline, m.FiredAt = statusSending, &now, &now }, false},

		"om_status_alert_ids/null": {func(m *MessageRow) { m.MessageType, m.LastStatus = typeStatusBundle, statusFailed }, true},
		"om_status_alert_ids/empty": {func(m *MessageRow) {
			m.MessageType, m.LastStatus, m.StatusAlertIDs = typeStatusBundle, statusFailed, []int64{}
		}, false},

		"om_status_update_log_id/none":  {func(m *MessageRow) { m.MessageType = typeAlertStatus }, true},
		"om_status_update_log_id/valid": {func(m *MessageRow) { m.MessageType, m.AlertLogID = typeAlertStatus, &x.log }, false},

		"om_user_cm_or_channel/all":     {func(m *MessageRow) { m.ChannelID = &x.channel }, true},
		"om_user_cm_or_channel/none":    {func(m *MessageRow) { m.UserID, m.ContactMethodID = nil, nil }, true},
		"om_user_cm_or_channel/noCM":    {func(m *MessageRow) { m.ContactMethodID = nil }, true},
		"om_user_cm_or_channel/noUser":  {func(m *MessageRow) { m.UserID = nil }, true},
		"om_user_cm_or_channel/channel": {func(m *MessageRow) { m.UserID, m.ContactMethodID, m.ChannelID = nil, nil, &x.channel }, false},

		"verify_needs_id/none":  {func(m *MessageRow) { m.MessageType = typeVerification }, true},
		"verify_needs_id/valid": {func(m *MessageRow) { m.MessageType, m.UserVerificationCodeID = typeVerification, &x.code }, false},
	} {
		t.Run(name, func(t *testing.T) {
			m := x.alertMessage()
			want.change(&m)
			err := p.try(func(tx *filestore.Tx) error {
				_, err := e.tables.InsertMessage(tx, m)
				return err
			})
			if !want.errs {
				require.NoError(t, err)
				return
			}
			checkName, _, _ := strings.Cut(name, "/")
			assert.Equal(t, checkName, constraint(t, err))

			// An update is checked too.
			ok := p.insert(t, x.alertMessage())
			m.ID, m.CreatedAt, m.LastStatusAt = ok.ID, ok.CreatedAt, ok.LastStatusAt
			if m.LastStatus == "" {
				m.LastStatus = statusPending
			}
			assert.Equal(t, checkName, constraint(t, p.try(func(tx *filestore.Tx) error { return e.tables.SetMessageStatus(tx, m) })))
			if m.LastStatus == statusPending {
				assert.Equal(t, checkName, constraint(t, p.try(func(tx *filestore.Tx) error { return e.tables.UpdateMessage(tx, m) })))
			}
		})
	}

	for _, m := range []MessageRow{{MessageType: "other"}, {MessageType: typeTest, LastStatus: "other"}} {
		m.UserID, m.ContactMethodID = &x.user, &x.cm
		assert.ErrorContains(t, p.try(func(tx *filestore.Tx) error {
			_, err := e.tables.InsertMessage(tx, m)
			return err
		}), "unknown", "only the values of the enums")
	}
}

func TestDeltaMessageRoundTrip(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	at := func(s int) *time.Time {
		return ptr(deltaStart.Add(time.Duration(s)*time.Second + 123456*time.Microsecond))
	}
	m := p.insert(t, MessageRow{
		ID: uuid.NewString(), MessageType: typeAlert, UserID: &x.user, ContactMethodID: &x.cm,
		AlertID: &x.alert, AlertLogID: &x.log, ServiceID: &x.service, EscalationPolicyID: &x.policy,
		ScheduleID: &x.schedule, CycleID: &x.cycle, UserVerificationCodeID: &x.code, StatusAlertIDs: []int64{3, 1, 2},
		LastStatus: statusFailed, LastStatusAt: at(1), StatusDetails: "details", ProviderMsgID: ptr("Twilio-SMS:SM1"),
		ProviderSeq: 4, SrcValue: ptr("+17635550100"), RetryCount: 2, NextRetryAt: at(2), FiredAt: at(3),
		SendingDeadline: at(4), SentAt: at(5), CreatedAt: *at(6),
	})
	empty := p.insert(t, MessageRow{MessageType: typeStatusBundle, ChannelID: &x.channel, LastStatus: statusFailed, StatusAlertIDs: []int64{}})
	null := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel})

	for name, pod := range map[string]*deltaPod{"the pod that wrote it": p, "a fresh pod": e.pod(t, "b")} {
		assert.Equal(t, m, pod.message(t, m.ID), name)
		got := pod.message(t, empty.ID).StatusAlertIDs
		assert.NotNil(t, got, "an empty list is not null (%s)", name)
		assert.Empty(t, got, name)
		assert.Nil(t, pod.message(t, null.ID).StatusAlertIDs, name)
		h, ok := e.tables.History.Get(pod.f.View(), filestore.IntKey(1))
		require.True(t, ok, name)
		assert.Equal(t, StatusHistoryRow{ID: 1, MessageID: m.ID, Status: statusFailed, StatusDetails: "details", Timestamp: *at(1)}, h, name)
	}
}

func TestDeltaMessageHistory(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)

	m := p.insert(t, x.alertMessage())
	assert.NotEmpty(t, m.ID)
	got := p.message(t, m.ID)
	assert.Equal(t, statusPending, got.LastStatus, "the columns' defaults")
	assert.Equal(t, deltaStart, got.CreatedAt)
	assert.Equal(t, deltaStart, *got.LastStatusAt)
	assert.Zero(t, got.RetryCount)
	assert.Zero(t, got.ProviderSeq)
	assert.Empty(t, got.StatusDetails)

	hist := p.history(t, m.ID)
	require.Len(t, hist, 1, "trg_insert_message_status_history")
	assert.Equal(t, gadb.MessageStatusHistory{ID: 1, MessageID: uuid.MustParse(m.ID), Status: "pending", Timestamp: deltaStart}, hist[0])

	// Every UPDATE that sets last_status is recorded, with the row's
	// last_status_at and details.
	sending := deltaStart.Add(time.Second)
	p.set(t, m.ID, func(m *MessageRow) {
		m.LastStatus, m.LastStatusAt, m.FiredAt, m.SendingDeadline = statusSending, &sending, &sending, ptr(sending.Add(time.Minute))
	})
	sent := deltaStart.Add(2 * time.Second)
	p.set(t, m.ID, func(m *MessageRow) {
		m.LastStatus, m.LastStatusAt, m.SentAt, m.FiredAt, m.SendingDeadline = "sent", &sent, &sent, nil, nil
		m.StatusDetails = "queued"
	})
	p.set(t, m.ID, func(m *MessageRow) { m.StatusDetails = "sent again" })
	hist = p.history(t, m.ID)
	require.Len(t, hist, 4)
	var got2 []string
	for _, h := range hist {
		got2 = append(got2, fmt.Sprintf("%s %s %s", h.Status, h.StatusDetails, h.Timestamp.Sub(deltaStart)))
	}
	assert.Equal(t, []string{"sent sent again 2s", "sent queued 2s", "sending  1s", "pending  0s"}, got2,
		"newest first, also one that set the status to what it was")

	// Other UPDATEs are not.
	p.write(t, func(tx *filestore.Tx) error {
		m, _ := e.tables.Messages.Get(tx, m.ID)
		m.LastStatusAt = ptr(deltaStart.Add(time.Minute))
		return e.tables.UpdateMessage(tx, m)
	})
	assert.Len(t, p.history(t, m.ID), 4)
	assert.ErrorContains(t, p.try(func(tx *filestore.Tx) error {
		m, _ := e.tables.Messages.Get(tx, m.ID)
		m.LastStatus = statusFailed
		return e.tables.UpdateMessage(tx, m)
	}), "can't change the status")
	assert.Equal(t, "sent", p.message(t, m.ID).LastStatus, "a refused change changes nothing")

	// A status without a time can't be recorded (the column is not null).
	assert.ErrorContains(t, p.try(func(tx *filestore.Tx) error {
		m, _ := e.tables.Messages.Get(tx, m.ID)
		m.LastStatusAt = nil
		return e.tables.SetMessageStatus(tx, m)
	}), "must not be null")

	// A message that was deleted doesn't come back.
	gone := x.alertMessage()
	gone.ID, gone.LastStatus, gone.LastStatusAt, gone.CreatedAt = uuid.NewString(), statusPending, &deltaStart, deltaStart
	require.ErrorIs(t, p.try(func(tx *filestore.Tx) error { return e.tables.SetMessageStatus(tx, gone) }), sql.ErrNoRows)
	require.ErrorIs(t, p.try(func(tx *filestore.Tx) error { return e.tables.UpdateMessage(tx, gone) }), sql.ErrNoRows)
	_, ok := e.tables.Messages.Get(p.f.View(), gone.ID)
	assert.False(t, ok)

	none, err := p.MessageStatusHistory(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Nil(t, none)
}

func TestDeltaMessageUniques(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	m := x.alertMessage()
	m.ProviderMsgID = ProviderMsgIDColumn(ProviderMessageID{ProviderName: "builtin-twilio-sms", ExternalID: "SM1"})
	require.NotNil(t, m.ProviderMsgID)
	assert.Equal(t, "Twilio-SMS:SM1", *m.ProviderMsgID, "stored as Postgres stores it")
	assert.Nil(t, ProviderMsgIDColumn(ProviderMessageID{}))
	m = p.insert(t, m)

	again := x.alertMessage()
	again.ProviderMsgID = m.ProviderMsgID
	assert.Equal(t, "idx_outgoing_messages_provider_msg_id", constraint(t, p.try(func(tx *filestore.Tx) error {
		_, err := e.tables.InsertMessage(tx, again)
		return err
	})))
	again = x.alertMessage()
	again.ID = m.ID
	assert.Equal(t, "outgoing_messages_pkey", constraint(t, p.try(func(tx *filestore.Tx) error {
		_, err := e.tables.InsertMessage(tx, again)
		return err
	})))
	assert.Equal(t, "message_status_history_pkey", constraint(t, p.try(func(tx *filestore.Tx) error {
		return e.tables.History.Insert(tx, StatusHistoryRow{ID: 1, MessageID: m.ID, Timestamp: deltaStart}, "message_status_history_pkey")
	})))

	p.insert(t, x.alertMessage())
	p.insert(t, x.alertMessage()) // none is not a duplicate
	found, ok := e.tables.MessagesByProviderMsgID.Get(p.f.View(), "Twilio-SMS:SM1")
	require.True(t, ok)
	assert.Equal(t, m.ID, found.ID)

	assert.Equal(t, "user_verification_codes_contact_method_id_key", constraint(t, p.try(func(tx *filestore.Tx) error {
		return e.tables.Codes.Put(tx, VerificationCodeRow{ID: uuid.NewString(), ContactMethodID: x.cm, ExpiresAt: deltaStart})
	})))
	other := p.addMethod(t, x.user)
	assert.Equal(t, "user_verification_codes_pkey", constraint(t, p.try(func(tx *filestore.Tx) error {
		return e.tables.Codes.Insert(tx, VerificationCodeRow{ID: x.code, ContactMethodID: other, ExpiresAt: deltaStart}, "user_verification_codes_pkey")
	})))
}

func TestDeltaMessageForeignKeys(t *testing.T) {
	missing := uuid.NewString()
	for name, fk := range map[string]struct {
		change func(m *MessageRow, x parents)
		remove func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error)
	}{
		"alert_id": {func(m *MessageRow, x parents) { m.AlertID = ptr(int64(99)) }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) {
			return e.alerts.Delete(tx, filestore.IntKey(x.alert))
		}},
		"alert_log_id": {func(m *MessageRow, x parents) { m.AlertLogID = ptr(int64(99)) }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) {
			return e.logs.Delete(tx, filestore.IntKey(x.log))
		}},
		"channel_id":           {func(m *MessageRow, x parents) { m.UserID, m.ContactMethodID, m.ChannelID = nil, nil, &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.channels.Delete(tx, x.channel) }},
		"contact_method_id":    {func(m *MessageRow, x parents) { m.ContactMethodID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.methods.Delete(tx, x.cm) }},
		"cycle_id":             {func(m *MessageRow, x parents) { m.CycleID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.cycles.Delete(tx, x.cycle) }},
		"escalation_policy_id": {func(m *MessageRow, x parents) { m.EscalationPolicyID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.policies.Delete(tx, x.policy) }},
		"schedule_id": {func(m *MessageRow, x parents) { m.ScheduleID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) {
			return e.schedules.Delete(tx, x.schedule)
		}},
		"service_id":                {func(m *MessageRow, x parents) { m.ServiceID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.svcs.Delete(tx, x.service) }},
		"user_id":                   {func(m *MessageRow, x parents) { m.UserID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.users.Delete(tx, x.user) }},
		"user_verification_code_id": {func(m *MessageRow, x parents) { m.UserVerificationCodeID = &missing }, func(e *deltaEnv, tx *filestore.Tx, x parents) (bool, error) { return e.tables.Codes.Delete(tx, x.code) }},
	} {
		t.Run(name, func(t *testing.T) {
			e := newDeltaEnv(t)
			p := e.pod(t, "a")
			x := p.addParents(t)

			full := x.alertMessage()
			full.AlertLogID, full.CycleID, full.ScheduleID, full.UserVerificationCodeID = &x.log, &x.cycle, &x.schedule, &x.code
			bad := full
			fk.change(&bad, x)
			err := p.try(func(tx *filestore.Tx) error {
				_, err := e.tables.InsertMessage(tx, bad)
				return err
			})
			assert.Equal(t, "outgoing_messages_"+name+"_fkey", constraint(t, err))
			assert.Zero(t, e.tables.Messages.Len(p.f.View()))
			assert.Zero(t, e.tables.History.Len(p.f.View()))

			if name == "channel_id" {
				full = MessageRow{MessageType: typeTest, ChannelID: &x.channel}
			}
			full = p.insert(t, full)
			p.set(t, full.ID, func(m *MessageRow) {})
			kept := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: ptr(p.addChannel(t))})

			p.write(t, func(tx *filestore.Tx) error {
				ok, err := fk.remove(e, tx, x)
				assert.True(t, ok)
				return err
			})
			fresh := e.pod(t, "b")
			_, ok := e.tables.Messages.Get(fresh.f.View(), full.ID)
			assert.False(t, ok, "ON DELETE CASCADE")
			assert.Empty(t, fresh.history(t, full.ID), "with its history")
			assert.Len(t, fresh.history(t, kept.ID), 1)
			_, ok = e.tables.Codes.Get(fresh.f.View(), x.code)
			switch name {
			case "contact_method_id", "user_id", "user_verification_code_id":
				assert.False(t, ok, "a verification code goes with its contact method")
			default:
				assert.True(t, ok)
			}
		})
	}

	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	assert.Equal(t, "user_verification_codes_contact_method_id_fkey", constraint(t, p.try(func(tx *filestore.Tx) error {
		return e.tables.Codes.Put(tx, VerificationCodeRow{ID: uuid.NewString(), ContactMethodID: missing, ExpiresAt: deltaStart})
	})))
	assert.Equal(t, "message_status_history_message_id_fkey", constraint(t, p.try(func(tx *filestore.Tx) error {
		return e.tables.History.Put(tx, StatusHistoryRow{ID: 1, MessageID: missing, Timestamp: deltaStart})
	})))
}

// The indexes the engine uses.
func TestDeltaMessageIndexes(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	ids := func(ix *filestore.Index[MessageRow], key string) []string {
		var ids []string
		ix.Ascend(p.f.View(), key, func(m MessageRow) bool { ids = append(ids, m.ID); return true })
		return ids
	}

	withCycle := x.alertMessage()
	withCycle.CycleID = &x.cycle
	a := p.insert(t, withCycle).ID
	b := p.insert(t, MessageRow{MessageType: string(MessageTypeScheduleOnCallUsers), ChannelID: &x.channel, ScheduleID: &x.schedule}).ID

	assert.ElementsMatch(t, []string{a, b}, ids(e.tables.MessagesUnfinished, statusPending))
	assert.Equal(t, []string{a}, ids(e.tables.MessagesPending, PendingTypeKey(MessageTypeAlert)))
	assert.Equal(t, []string{a}, ids(e.tables.MessagesPending, PendingDestKey(MessageTypeAlert, x.cm)))
	assert.Equal(t, []string{b}, ids(e.tables.MessagesPending, PendingDestKey(MessageTypeScheduleOnCallUsers, x.channel)))
	assert.Empty(t, ids(e.tables.MessagesPending, PendingDestKey(MessageTypeScheduleOnCallUsers, x.cm)))
	assert.Equal(t, []string{a}, ids(e.tables.MessagesByAlert, filestore.IntKey(x.alert)))
	assert.Equal(t, []string{a}, ids(e.tables.MessagesByCycle, x.cycle))

	now := deltaStart
	p.set(t, a, func(m *MessageRow) {
		m.LastStatus, m.FiredAt, m.SendingDeadline = statusSending, &now, &now
	})
	assert.Equal(t, []string{a}, ids(e.tables.MessagesUnfinished, statusSending))
	assert.Equal(t, []string{b}, ids(e.tables.MessagesUnfinished, statusPending))
	assert.Empty(t, ids(e.tables.MessagesPending, PendingTypeKey(MessageTypeAlert)))

	p.set(t, a, func(m *MessageRow) { m.LastStatus, m.FiredAt = statusFailed, nil })
	assert.Equal(t, []string{a}, ids(e.tables.MessagesUnfinished, statusFailed), "failed, with a cycle to clear")
	p.set(t, a, func(m *MessageRow) { m.CycleID, m.NextRetryAt = nil, &now })
	assert.Equal(t, []string{a}, ids(e.tables.MessagesUnfinished, statusFailed), "failed, to be tried again")
	assert.Empty(t, ids(e.tables.MessagesByCycle, x.cycle))
	p.set(t, a, func(m *MessageRow) { m.NextRetryAt = nil })
	assert.Empty(t, ids(e.tables.MessagesUnfinished, statusFailed), "failed for good")

	p.set(t, b, func(m *MessageRow) { m.LastStatus, m.SentAt = statusQueuedRemotely, &now })
	assert.Equal(t, []string{b}, ids(e.tables.MessagesUnfinished, statusQueuedRemotely))
	p.set(t, b, func(m *MessageRow) { m.LastStatus = "delivered" })
	for _, s := range messageStatuses {
		assert.Empty(t, ids(e.tables.MessagesUnfinished, s), "nothing left for the engine to look at")
	}
}

func TestDeltaMessagesSent(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	want := map[time.Duration]string{}
	for _, d := range []time.Duration{-90 * time.Minute, -time.Hour, -time.Microsecond, 0, time.Minute, 59 * time.Minute, time.Hour, 100 * 24 * time.Hour} {
		want[d] = p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel, LastStatus: "sent", SentAt: ptr(deltaStart.Add(d))}).ID
	}
	p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel}) // not sent

	for _, r := range []struct{ from, to time.Duration }{
		{0, time.Hour}, {-time.Hour, time.Microsecond}, {-time.Hour, 0}, {time.Minute, time.Minute}, {-2 * time.Hour, 2 * time.Hour},
		{-time.Hour, 365 * 24 * time.Hour}, {time.Hour, 0},
	} {
		var ids []string
		for d, id := range want {
			if d >= r.from && d < r.to {
				ids = append(ids, id)
			}
		}
		var got []string
		e.tables.MessagesSent(p.f.View(), deltaStart.Add(r.from), deltaStart.Add(r.to), func(m MessageRow) bool {
			got = append(got, m.ID)
			return true
		})
		assert.ElementsMatch(t, ids, got, "%s to %s", r.from, r.to)
	}

	for _, to := range []time.Duration{2 * time.Hour, 365 * 24 * time.Hour} {
		n := 0
		e.tables.MessagesSent(p.f.View(), deltaStart.Add(-2*time.Hour), deltaStart.Add(to), func(MessageRow) bool {
			n++
			return n < 2
		})
		assert.Equal(t, 2, n, "stops when told to")
	}
}

func TestDeltaPendingForAlert(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	other := uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error { return e.svcs.Put(tx, deltaNamed{ID: other}) })
	old := deltaStart.Add(-time.Minute)
	bundle := func(svc string, created time.Time) MessageRow {
		return MessageRow{MessageType: typeAlertBundle, UserID: &x.user, ContactMethodID: &x.cm, ServiceID: &svc, CreatedAt: created}
	}
	mine := x.alertMessage()
	mine.CreatedAt = old
	a := p.insert(t, mine).ID
	p.insert(t, x.alertMessage()) // too new
	sent := mine
	sent.LastStatus, sent.SentAt = "sent", &deltaStart
	p.insert(t, sent)
	b := p.insert(t, bundle(x.service, old)).ID
	p.insert(t, bundle(x.service, deltaStart))
	p.insert(t, bundle(other, old))
	both := bundle(x.service, old)
	both.AlertID = &x.alert
	c := p.insert(t, both).ID

	var got []string
	for _, m := range e.tables.PendingForAlert(p.f.View(), x.alert, x.service, deltaStart.Add(-15*time.Second)) {
		got = append(got, m.ID)
	}
	assert.ElementsMatch(t, []string{a, b, c}, got, "each once")
	assert.Empty(t, e.tables.PendingForAlert(p.f.View(), 99, uuid.NewString(), deltaStart))
}

// Two pods claim a message: one gets it (F3).
func TestDeltaMessageClaimRace(t *testing.T) {
	e := newDeltaEnv(t)
	setup := e.pod(t, "setup")
	x := setup.addParents(t)
	m := setup.insert(t, x.alertMessage())
	pods := []*deltaPod{e.pod(t, "a"), e.pod(t, "b")}

	var claims atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var claimed bool
			err := pods[i%2].try(func(tx *filestore.Tx) error {
				claimed = false
				row, ok := e.tables.Messages.Get(tx, m.ID)
				if !ok || row.LastStatus != statusPending {
					return nil
				}
				now := tx.Now()
				row.LastStatus, row.LastStatusAt, row.FiredAt, row.SendingDeadline = statusSending, &now, &now, &now
				claimed = true
				return e.tables.SetMessageStatus(tx, row)
			})
			if assert.NoError(t, err) && claimed {
				claims.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, claims.Load())
	assert.Len(t, e.pod(t, "check").history(t, m.ID), 2)
}

func TestDeltaOriginalMessageStatus(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	toCM := DestID{CMID: uuid.NullUUID{UUID: uuid.MustParse(x.cm), Valid: true}}
	toChannel := DestID{NCID: uuid.NullUUID{UUID: uuid.MustParse(x.channel), Valid: true}}

	res, err := p.OriginalMessageStatus(sys, 1, toCM)
	require.NoError(t, err)
	assert.Nil(t, res)

	pending := p.insert(t, x.alertMessage())
	res, err = p.OriginalMessageStatus(sys, 1, toCM)
	require.NoError(t, err)
	assert.Equal(t, &SendResult{ID: pending.ID, DestType: "builtin-smtp-email", Status: Status{State: StatePending}}, res)

	sentMsg := func(after time.Duration, provider string) MessageRow {
		m := x.alertMessage()
		m.LastStatus, m.SentAt, m.LastStatusAt = "delivered", ptr(deltaStart.Add(after)), ptr(deltaStart.Add(after+time.Second))
		m.ProviderMsgID, m.ProviderSeq, m.SrcValue, m.StatusDetails = ptr(provider), 3, ptr("src"), "done"
		return p.insert(t, m)
	}
	sentMsg(2*time.Minute, "Twilio-SMS:SM2")
	first := sentMsg(time.Minute, "Twilio-SMS:SM1")
	other := x.alertMessage()
	other.MessageType, other.AlertLogID = typeAlertStatus, &x.log
	other.LastStatus, other.SentAt = "sent", &deltaStart
	p.insert(t, other) // another type
	ch := x.alertMessage()
	ch.UserID, ch.ContactMethodID, ch.ChannelID = nil, nil, &x.channel
	chMsg := p.insert(t, ch)

	res, err = e.pod(t, "b").OriginalMessageStatus(sys, 1, toCM)
	require.NoError(t, err)
	assert.Equal(t, &SendResult{
		ID: first.ID, DestType: "builtin-smtp-email",
		ProviderMessageID: ProviderMessageID{ProviderName: "builtin-twilio-sms", ExternalID: "SM1"},
		Status:            Status{State: StateDelivered, Details: "done", Sequence: 3, SrcValue: "src", Age: 61 * time.Second},
	}, res, "the first that was sent")
	res, err = p.OriginalMessageStatus(sys, 1, toChannel)
	require.NoError(t, err)
	assert.Equal(t, chMsg.ID, res.ID)
	assert.Equal(t, "builtin-slack-channel", res.DestType)

	for _, dst := range []DestID{{}, {CMID: uuid.NullUUID{UUID: uuid.New(), Valid: true}}, {NCID: toCM.CMID}, {CMID: toChannel.NCID}} {
		res, err = p.OriginalMessageStatus(sys, 1, dst)
		require.NoError(t, err)
		assert.Nil(t, res)
	}
	res, err = p.OriginalMessageStatus(sys, 2, toCM)
	require.NoError(t, err)
	assert.Nil(t, res)
	_, err = p.OriginalMessageStatus(asUser(x.user), 1, toCM)
	assert.True(t, permission.IsPermissionError(err))
}

func TestDeltaMessageStatuses(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	ctx := asUser(x.user)

	failed := func(retry *time.Time) string {
		m := x.alertMessage()
		m.LastStatus, m.NextRetryAt = statusFailed, retry
		return p.insert(t, m).ID
	}
	a, b := failed(&deltaStart), failed(nil)
	c := p.insert(t, MessageRow{MessageType: typeTest, ChannelID: &x.channel}).ID

	got, err := p.FindManyMessageStatuses(ctx, []string{a, strings.ToUpper(b), c, a, uuid.NewString()})
	require.NoError(t, err)
	require.Len(t, got, 3, "each message once, like `where id = any($1)`")
	assert.Equal(t, a, got[0].ID)
	assert.Equal(t, StateFailedTemp, got[0].State, "with a retry to come")
	assert.Equal(t, b, got[1].ID)
	assert.Equal(t, StateFailedPerm, got[1].State)
	assert.Equal(t, c, got[2].ID)
	assert.Equal(t, "builtin-slack-channel", got[2].DestType)

	none, err := p.FindManyMessageStatuses(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, none)
	none, err = p.FindManyMessageStatuses(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.Nil(t, none)
	_, err = p.FindManyMessageStatuses(ctx, []string{"nope"})
	assert.True(t, validation.IsClientError(err))

	// A provider id that can't be parsed is an error, as on Postgres.
	m := x.alertMessage()
	m.ProviderMsgID = ptr("no colon")
	bad := p.insert(t, m).ID
	_, err = p.FindManyMessageStatuses(ctx, []string{bad})
	assert.ErrorContains(t, err, "invalid provider id format")
}

func TestDeltaLastMessageStatus(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	x := p.addParents(t)
	ctx := asUser(x.user)
	test := func(at time.Time) string {
		return p.insert(t, MessageRow{MessageType: typeTest, UserID: &x.user, ContactMethodID: &x.cm, CreatedAt: at}).ID
	}
	first := test(deltaStart)
	last := test(deltaStart.Add(time.Minute))
	test(deltaStart.Add(-time.Minute))

	res, created, err := p.LastMessageStatus(ctx, MessageTypeTest, strings.ToUpper(x.cm), deltaStart)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, last, res.ID)
	assert.Equal(t, deltaStart.Add(time.Minute), created)

	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.tables.Messages.Delete(tx, last)
		return err
	})
	res, _, err = p.LastMessageStatus(ctx, MessageTypeTest, x.cm, deltaStart.Add(999))
	require.NoError(t, err)
	require.NotNil(t, res, "from is inclusive, at the precision of created_at")
	assert.Equal(t, first, res.ID)

	for name, args := range map[string]struct {
		typ  gadb.EnumOutgoingMessagesType
		cm   string
		from time.Time
	}{
		"too late":               {MessageTypeTest, x.cm, deltaStart.Add(time.Microsecond)},
		"another type":           {MessageTypeVerification, x.cm, deltaStart},
		"another contact method": {MessageTypeTest, uuid.NewString(), deltaStart},
	} {
		res, created, err = p.LastMessageStatus(ctx, args.typ, args.cm, args.from)
		require.NoError(t, err, name)
		assert.Nil(t, res, name)
		assert.True(t, created.IsZero(), name)
	}
}

func TestDeltaSendTest(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	user, other := p.addUser(t, "Alice"), p.addUser(t, "Bob")
	cm := p.addMethod(t, user)
	ctx := asUser(user)

	err := p.SendContactMethodTest(ctx, uuid.NewString())
	require.ErrorContains(t, err, "does not exist")
	assert.True(t, permission.IsPermissionError(p.SendContactMethodTest(asUser(other), cm)))
	assert.True(t, permission.IsPermissionError(p.SendContactMethodTest(permission.UserContext(context.Background(), other, permission.RoleAdmin), cm)),
		"an admin can't send tests to someone else's")

	require.NoError(t, p.SendContactMethodTest(ctx, strings.ToUpper(cm)))
	var msgs []MessageRow
	e.tables.Messages.Ascend(p.f.View(), func(m MessageRow) bool { msgs = append(msgs, m); return true })
	require.Len(t, msgs, 1)
	assert.Equal(t, MessageRow{
		ID: msgs[0].ID, MessageType: typeTest, UserID: &user, ContactMethodID: &cm,
		LastStatus: statusPending, LastStatusAt: &deltaStart, CreatedAt: deltaStart,
	}, msgs[0])
	assert.Len(t, p.history(t, msgs[0].ID), 1)
	m, _ := e.methods.Get(p.f.View(), cm)
	assert.Equal(t, deltaStart, *m.LastTestVerifyAt)

	e.clock.Set(deltaStart.Add(time.Minute))
	err = p.SendContactMethodTest(ctx, cm)
	require.ErrorContains(t, err, "rate-limit")
	assert.True(t, validation.IsClientError(err))
	e.clock.Set(deltaStart.Add(time.Minute + time.Microsecond))
	require.NoError(t, p.SendContactMethodTest(ctx, cm), "more than a minute later")

	e.clock.Advance(time.Hour)
	p.write(t, func(tx *filestore.Tx) error {
		m, _ := e.methods.Get(tx, cm)
		m.Disabled = true
		return e.methods.Put(tx, m)
	})
	err = p.SendContactMethodTest(ctx, cm)
	require.ErrorContains(t, err, "contact method disabled")
	assert.Equal(t, 2, e.tables.Messages.Len(p.f.View()), "a refused test sends nothing")
}

// Two pods are asked for a test message at once: one sends it (F4).
func TestDeltaSendTestRace(t *testing.T) {
	e := newDeltaEnv(t)
	setup := e.pod(t, "setup")
	user := setup.addUser(t, "Alice")
	cm := setup.addMethod(t, user)
	pods := []*deltaPod{e.pod(t, "a"), e.pod(t, "b")}

	var sent atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pods[i%2].SendContactMethodTest(asUser(user), cm); err == nil {
				sent.Add(1)
			} else {
				assert.ErrorContains(t, err, "rate-limit")
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, sent.Load())
	assert.Equal(t, 1, e.tables.Messages.Len(e.pod(t, "check").f.View()))
}

func (p *deltaPod) codeRow(t *testing.T, cm string) VerificationCodeRow {
	t.Helper()
	row, ok := p.e.tables.CodesByMethod.Get(p.f.View(), cm)
	require.True(t, ok)
	return row
}

func TestDeltaSendVerification(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	user, other := p.addUser(t, "Alice"), p.addUser(t, "Bob")
	cm := p.addMethod(t, user)
	ctx := asUser(user)

	require.ErrorContains(t, p.SendContactMethodVerification(ctx, uuid.NewString()), "does not exist")
	assert.True(t, permission.IsPermissionError(p.SendContactMethodVerification(asUser(other), cm)))

	require.NoError(t, p.SendContactMethodVerification(ctx, strings.ToUpper(cm)))
	row := p.codeRow(t, cm)
	assert.False(t, row.Sent)
	assert.Equal(t, deltaStart.Add(15*time.Minute), row.ExpiresAt)
	assert.GreaterOrEqual(t, row.Code, int32(100000))
	assert.LessOrEqual(t, row.Code, int32(999999))
	m, _ := e.methods.Get(p.f.View(), cm)
	assert.Equal(t, deltaStart, *m.LastTestVerifyAt)

	code, err := e.pod(t, "b").Code(sys, strings.ToUpper(row.ID))
	require.NoError(t, err)
	assert.EqualValues(t, row.Code, code)
	_, err = p.Code(sys, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.Code(ctx, row.ID)
	assert.True(t, permission.IsPermissionError(err), "users can't read codes")

	err = p.SendContactMethodVerification(ctx, cm)
	require.ErrorContains(t, err, "Too many messages! Please try again in 1 minute(s)")
	assert.True(t, validation.IsClientError(err))
	assert.Equal(t, row, p.codeRow(t, cm))

	// The engine has sent it, and the user asks for it again.
	p.write(t, func(tx *filestore.Tx) error {
		row, _ := e.tables.Codes.Get(tx, row.ID)
		row.Sent = true
		return e.tables.Codes.Put(tx, row)
	})
	e.clock.Advance(2 * time.Minute)
	require.NoError(t, p.SendContactMethodVerification(ctx, cm))
	again := p.codeRow(t, cm)
	assert.Equal(t, row.ID, again.ID, "the same code, to be sent again")
	assert.Equal(t, row.Code, again.Code)
	assert.False(t, again.Sent)
	assert.Equal(t, deltaStart.Add(17*time.Minute), again.ExpiresAt)

	// Once it has expired, there is a new one.
	e.clock.Set(again.ExpiresAt)
	require.NoError(t, p.SendContactMethodVerification(ctx, cm))
	renewed := p.codeRow(t, cm)
	assert.NotEqual(t, row.ID, renewed.ID)
	assert.Equal(t, again.ExpiresAt.Add(15*time.Minute), renewed.ExpiresAt)
	assert.Equal(t, 1, e.tables.Codes.Len(p.f.View()))
}

func TestDeltaSendVerificationRace(t *testing.T) {
	e := newDeltaEnv(t)
	setup := e.pod(t, "setup")
	user := setup.addUser(t, "Alice")
	cm := setup.addMethod(t, user)
	pods := []*deltaPod{e.pod(t, "a"), e.pod(t, "b")}

	var sent atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pods[i%2].SendContactMethodVerification(asUser(user), cm); err == nil {
				sent.Add(1)
			} else {
				assert.ErrorContains(t, err, "Too many messages!")
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, sent.Load())
	assert.Equal(t, 1, e.tables.Codes.Len(e.pod(t, "check").f.View()))
}

func TestDeltaVerify(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	user, other := p.addUser(t, "Alice"), p.addUser(t, "Bob")
	cm := p.addMethod(t, user)
	ctx := asUser(user)
	p.write(t, func(tx *filestore.Tx) error {
		m, _ := e.methods.Get(tx, cm)
		m.Disabled = true
		return e.methods.Put(tx, m)
	})
	disabled := func() bool {
		m, ok := e.methods.Get(e.pod(t, "reader").f.View(), cm)
		require.True(t, ok)
		return m.Disabled
	}
	invalid := func(err error) {
		t.Helper()
		require.ErrorContains(t, err, "invalid code")
		assert.True(t, validation.IsClientError(err))
	}

	invalid(p.VerifyContactMethod(ctx, cm, 123456))
	require.NoError(t, p.SendContactMethodVerification(ctx, cm))
	row := p.codeRow(t, cm)
	code := int(row.Code)
	msg := p.insert(t, MessageRow{MessageType: typeVerification, UserID: &user, ContactMethodID: &cm, UserVerificationCodeID: &row.ID})

	require.ErrorContains(t, p.VerifyContactMethod(ctx, uuid.NewString(), code), "does not exist")
	assert.True(t, permission.IsPermissionError(p.VerifyContactMethod(asUser(other), cm, code)))
	invalid(p.VerifyContactMethod(ctx, cm, code+1))
	assert.True(t, disabled())
	assert.Equal(t, row, p.codeRow(t, cm), "a wrong code uses nothing up")

	// Another contact method's code doesn't count.
	cm2 := p.addMethod(t, user)
	invalid(p.VerifyContactMethod(ctx, cm2, code))

	require.NoError(t, p.VerifyContactMethod(ctx, strings.ToUpper(cm), code))
	assert.False(t, disabled())
	fresh := e.pod(t, "b")
	assert.Zero(t, e.tables.Codes.Len(fresh.f.View()), "the code is gone")
	_, ok := e.tables.Messages.Get(fresh.f.View(), msg.ID)
	assert.False(t, ok, "and the message with it (ON DELETE CASCADE)")
	assert.Zero(t, e.tables.History.Len(fresh.f.View()))
	invalid(fresh.VerifyContactMethod(ctx, cm, code))

	// An admin can verify anyone's.
	e.clock.Advance(time.Hour)
	admin := permission.UserContext(context.Background(), other, permission.RoleAdmin)
	require.NoError(t, p.SendContactMethodVerification(admin, cm))
	require.NoError(t, p.VerifyContactMethod(admin, cm, int(p.codeRow(t, cm).Code)))
}

func TestDeltaVerifyExpiry(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	user := p.addUser(t, "Alice")
	cm := p.addMethod(t, user)
	ctx := asUser(user)

	require.NoError(t, p.SendContactMethodVerification(ctx, cm))
	row := p.codeRow(t, cm)
	e.clock.Set(row.ExpiresAt)
	require.ErrorContains(t, p.VerifyContactMethod(ctx, cm, int(row.Code)), "invalid code")

	require.NoError(t, p.SendContactMethodVerification(ctx, cm))
	row = p.codeRow(t, cm)
	e.clock.Set(row.ExpiresAt.Add(-time.Microsecond))
	require.NoError(t, p.VerifyContactMethod(ctx, cm, int(row.Code)))
}

// Two pods verify one code at once: one succeeds (F4).
func TestDeltaVerifyRace(t *testing.T) {
	e := newDeltaEnv(t)
	setup := e.pod(t, "setup")
	user := setup.addUser(t, "Alice")
	cm := setup.addMethod(t, user)
	ctx := asUser(user)
	require.NoError(t, setup.SendContactMethodVerification(ctx, cm))
	code := int(setup.codeRow(t, cm).Code)
	pods := []*deltaPod{e.pod(t, "a"), e.pod(t, "b")}

	var verified atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pods[i%2].VerifyContactMethod(ctx, cm, code); err == nil {
				verified.Add(1)
			} else {
				assert.ErrorContains(t, err, "invalid code")
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, verified.Load())
	check := e.pod(t, "check")
	assert.Zero(t, e.tables.Codes.Len(check.f.View()))
	m, _ := e.methods.Get(check.f.View(), cm)
	assert.False(t, m.Disabled)
}

func TestDeltaLookupsRequired(t *testing.T) {
	assert.Panics(t, func() { NewDeltaStore(nil, nil, DeltaLookups{}) }, "a missing lookup shows at startup, not at the first page")
}
