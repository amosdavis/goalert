package fixture

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
)

const (
	u1  = "11111111-1111-1111-1111-111111111111"
	u2  = "22222222-2222-2222-2222-222222222222"
	cm1 = "33333333-3333-3333-3333-333333333333"
	ep  = "44444444-4444-4444-4444-444444444444"
	st1 = "55555555-5555-5555-5555-555555555555"
	st2 = "66666666-6666-6666-6666-666666666666"
	svc = "77777777-7777-7777-7777-777777777777"
	rot = "88888888-8888-8888-8888-888888888888"
	sch = "99999999-9999-9999-9999-999999999999"
	p1  = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	p2  = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	nc  = "cccccccc-cccc-cccc-cccc-cccccccccccc"
)

func newDB(t *testing.T) (*deltastorestest.Pod, *DB) {
	t.Helper()
	p := deltastorestest.New(t).Pod(t)
	return p, &DB{Delta: p.Delta, Stores: p.Stores}
}

func exec(t *testing.T, db *DB, sql string) {
	t.Helper()
	require.NoError(t, db.Exec(context.Background(), strings.NewReplacer(
		"$u1", "'"+u1+"'", "$u2", "'"+u2+"'", "$cm1", "'"+cm1+"'", "$ep", "'"+ep+"'", "$st1", "'"+st1+"'", "$st2", "'"+st2+"'",
		"$svc", "'"+svc+"'", "$rot", "'"+rot+"'", "$sch", "'"+sch+"'", "$p1", "'"+p1+"'", "$p2", "'"+p2+"'", "$nc", "'"+nc+"'",
	).Replace(sql)))
}

// basics is what most fixtures start with.
const basics = `
	-- a comment; with a semicolon, and 'quotes
	INSERT INTO users (id, name, email, role)
	values
		($u1, 'bob''s', 'bob@example.com', 'admin'),
		($u2, 'joe', '', 'user');
	insert into user_contact_methods (id, user_id, name, type, value) values ($cm1, $u1, 'personal', 'SMS', '+17633000000');
	insert into user_notification_rules (user_id, contact_method_id, delay_minutes) values ($u1, $cm1, 5);
	insert into escalation_policies (id, name, repeat) values ($ep, 'policy', -1);
	insert into escalation_policy_steps (id, escalation_policy_id, delay) values ($st1, $ep, 60), ($st2, $ep, 1);
	insert into escalation_policy_actions (escalation_policy_step_id, user_id) values ($st1, $u1);
	insert into services (id, escalation_policy_id, name) values ($svc, $ep, 'service');
`

func TestInsert(t *testing.T) {
	p, db := newDB(t)
	exec(t, db, basics+`
		insert into alerts (id, service_id, summary, status, dedup_key, created_at)
		values (5, $svc, 'old', 'closed', null, now() - '1 hour 30 minutes'::interval);
		insert into alerts (service_id, summary, dedup_key) values ($svc, 'new', 'auto:1:x');
		insert into notification_channels (id, type, name, value) values ($nc, 'WEBHOOK', 'webhook', 'https://example.com');
	`)
	ctx, v := p.Admin(), p.View()

	u, err := p.User.FindOne(ctx, u1)
	require.NoError(t, err)
	assert.Equal(t, "bob's", u.Name)
	assert.Equal(t, permission.RoleAdmin, u.Role)
	cm, ok := p.Tables.ContactMethod.Methods.Get(v, cm1)
	require.True(t, ok)
	assert.JSONEq(t, `{"Type":"builtin-twilio-sms","Args":{"phone_number":"+17633000000"}}`, string(cm.Dest))
	assert.True(t, cm.Pending)
	rules, err := p.NotificationRule.FindAll(ctx, u1)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, 5, rules[0].DelayMinutes)
	pol, _ := p.Tables.Escalation.Policies.Get(v, ep)
	assert.Equal(t, escalation.PolicyRow{ID: ep, Name: "policy", Repeat: -1, StepCount: 2}, pol)
	s2, _ := p.Tables.Escalation.Steps.Get(v, st2)
	assert.Equal(t, escalation.StepRow{ID: st2, PolicyID: ep, Delay: 1, StepNumber: 1}, s2)

	a, ok := p.Tables.Alert.Alerts.Get(v, filestore.IntKey(5))
	require.True(t, ok)
	assert.Equal(t, deltastorestest.Start.Add(-90*time.Minute), a.CreatedAt.UTC())
	_, ok = p.Tables.Escalation.AlertState(v, 5)
	assert.False(t, ok)
	a, ok = p.Tables.Alert.Alerts.Get(v, filestore.IntKey(1))
	require.True(t, ok)
	assert.Equal(t, "triggered", a.Status)
	state, ok := p.Tables.Escalation.AlertState(v, 1)
	require.True(t, ok)
	assert.Nil(t, state.NextEscalation)
	ch, ok := p.Tables.NotificationChannel.Channels.Get(v, nc)
	require.True(t, ok)
	assert.JSONEq(t, `{"Type":"builtin-webhook","Args":{"webhook_url":"https://example.com"}}`, string(ch.Dest))
}

func TestDroppedColumns(t *testing.T) {
	p, db := newDB(t)
	exec(t, db, basics+`
		insert into alerts (id, service_id, summary, dedup_key) values (1, $svc, 'test', 'auto:1:x');
		insert into notification_policy_cycles (alert_id, user_id, checked) values (1, $u1, true);
	`)
	assert.Equal(t, 1, p.Tables.Engine.Cycles.Len(p.View()), "a fixture with the old checked column still starts a cycle")
}

func TestSchedules(t *testing.T) {
	p, db := newDB(t)
	exec(t, db, basics+`
		set timezone = 'America/Chicago';
		insert into rotations (id, name, type, start_time, time_zone) values ($rot, 'rot', 'daily', now() - '3 days'::interval + '5 minutes'::interval, 'UTC');
		insert into rotation_participants (id, rotation_id, user_id, position) values ($p1, $rot, $u1, 0), ($p2, $rot, $u2, 1);
		insert into schedules (id, name, time_zone) values ($sch, 'sched', 'America/Chicago');
		insert into schedule_rules (schedule_id, tgt_rotation_id, start_time, end_time, monday) values ($sch, $rot, '09:00', '10:30:00', false);
		insert into schedule_rules (schedule_id, tgt_user_id, start_time, end_time) values
			($sch, $u1, cast((now()-'2 hours'::interval) at time zone 'America/Chicago' as time without time zone), '00:00');
		insert into schedule_on_call_users (schedule_id, start_time, end_time, user_id) values ($sch, now()-'2 days'::interval, now()-interval '1 day', $u1);
		insert into schedule_data (schedule_id, data) values ($sch, '{"V1": {}}');
		insert into user_favorites (user_id, tgt_schedule_id) values ($u1, $sch);
	`)
	v := p.View()
	r, ok := p.Tables.Rotation.Rotations.Get(v, rot)
	require.True(t, ok)
	assert.Equal(t, deltastorestest.Start.AddDate(0, 0, -3).Add(5*time.Minute), r.Start.UTC())
	assert.EqualValues(t, 1, r.ShiftLength)
	assert.EqualValues(t, 2, r.ParticipantCount)
	rs, ok := p.Tables.Rotation.State.Get(v, rot)
	require.True(t, ok)
	assert.Equal(t, p1, rs.ParticipantID)
	rules, err := p.Rule.FindAll(p.Admin(), sch)
	require.NoError(t, err)
	require.Len(t, rules, 2)
	byType := map[assignment.TargetType]int{rules[0].Target.TargetType(): 0, rules[1].Target.TargetType(): 1}
	rr, ur := rules[byType[assignment.TargetTypeRotation]], rules[byType[assignment.TargetTypeUser]]
	assert.Equal(t, "09:00", rr.Start.String())
	assert.Equal(t, "10:30", rr.End.String())
	assert.False(t, rr.Day(time.Monday))
	assert.True(t, rr.Day(time.Tuesday))
	assert.Equal(t, "04:00", ur.Start.String()) // noon in UTC is 6 in Chicago

	// Updates have the triggers too.
	exec(t, db, `update rotation_state set position = 7, rotation_participant_id = $p2, shift_start = now() - '1 minute'::interval where rotation_id = $rot`)
	rs, _ = p.Tables.Rotation.State.Get(p.View(), rot)
	assert.Equal(t, p2, rs.ParticipantID)
	assert.EqualValues(t, 1, rs.Position)
	assert.Equal(t, deltastorestest.Start.Add(-time.Minute), rs.ShiftStart.UTC())
}

func TestUpdateDelete(t *testing.T) {
	p, db := newDB(t)
	exec(t, db, basics+`
		insert into alerts (service_id, summary, dedup_key) values ($svc, 'new', 'auto:1:x'), ($svc, 'other', 'auto:1:y');
		update users set alert_status_log_contact_method_id = $cm1 where id = $u1;
		update escalation_policy_state set escalation_policy_step_id = $st1, last_escalation = now(), next_escalation = now() + '5 minutes'::interval where alert_id = 1;
		update config_limits set max = -1 where id = 'unacked_alerts_per_service';
		insert into alert_logs (alert_id, event, message) values (1, 'created', ''), (2, 'created', 'x');
		insert into outgoing_messages (id, message_type, user_id, contact_method_id, alert_id, service_id, escalation_policy_id, last_status, sent_at)
		values ($nc, 'alert_notification', $u1, $cm1, '1', $svc, $ep, 'sent', now());
	`)
	v := p.View()
	s, ok := p.Tables.Escalation.AlertState(v, 1)
	require.True(t, ok)
	assert.Equal(t, st1, *s.StepID)
	assert.Equal(t, deltastorestest.Start.Add(5*time.Minute), s.NextEscalation.UTC())
	s, _ = p.Tables.Escalation.AlertState(v, 2)
	assert.Nil(t, s.StepID)
	m, ok := p.Tables.Notification.Messages.Get(v, nc)
	require.True(t, ok)
	assert.Equal(t, "sent", m.LastStatus)
	logs := func() (n int) {
		p.Tables.AlertLog.Logs.Ascend(p.View(), func(alertlog.DeltaLog) bool { n++; return true })
		return n
	}
	assert.Equal(t, 2, logs())
	exec(t, db, `delete from alert_logs where alert_id = 2`)
	assert.Equal(t, 1, logs())
	exec(t, db, `delete from alert_logs;`)
	assert.Equal(t, 0, logs())
}

func TestBasicAuth(t *testing.T) {
	p, db := newDB(t)
	exec(t, db, basics+`
		insert into auth_basic_users (user_id, username, password_hash)
		values ($u1, 'cypress', '$2y$04$KfvjkXmDPYhrmSukrJWQ.OmmeofzMkJA8SPmkQL2lUOxE8fXwu9LG');
	`)
	id, err := p.AuthBasic.Validate(context.Background(), "cypress", "hunter2")
	require.NoError(t, err)
	assert.Equal(t, u1, id)
}

func TestErrors(t *testing.T) {
	_, db := newDB(t)
	exec(t, db, basics)
	for sql, want := range map[string]string{
		`select 1`:                                  `unexpected "select"`,
		`insert into users (id) values (1`:          "want \")\"",
		`insert into users (id, name) values ('a')`: "2 columns, but 1 values",
		`insert into nothing (id) values ('a')`:     "there is no table nothing",
		`insert into _seq (name) values ('a')`:      "there is no table _seq",
		`insert into users (color) values ('a')`:    "there is no column color",
		`insert into users (name) values (5)`:       "name: a int64 can't be",
		`insert into users (name) values (null)`:    "can't be null",
		`insert into users (name) values ('a' 'b')`: "want",
		`insert into users (name) values ('a`:       "has no end",
		`insert into users (name) values (now())`:   "name: a time.Time can't be",
		`insert into user_contact_methods (user_id, name, type, value) values ('` + u1 + `', 'x', 'PIGEON', 'x')`:                 "unknown contact method type",
		`insert into escalation_policy_steps (escalation_policy_id) values ('` + u1 + `')`:                                        "escalation_policy_steps_escalation_policy_id_fkey",
		`insert into escalation_policy_actions (escalation_policy_step_id) values ('` + st1 + `')`:                                "epa_there_can_only_be_one",
		`insert into users (id, name) values ('` + u1 + `', 'x')`:                                                                 "users_pkey",
		`insert into keyring (id) values ('x')`:                                                                                   "can't insert into this table",
		`update services set name = 'x'`:                                                                                          "can't update this table",
		`update users set id = 'x'`:                                                                                               "can't change a primary key",
		`delete from users`:                                                                                                       "can't delete from this table",
		`set timezone = 'Nowhere/Land'`:                                                                                           "unknown time zone",
		`insert into rotations (name, type, start_time, time_zone) values ('r', 'daily', 'noon', 'UTC')`:                          "can't read the time",
		`insert into rotations (name, type, start_time, time_zone) values ('r', 'daily', now() + '1 fortnight'::interval, 'UTC')`: "can't read the interval",
	} {
		err := db.Exec(context.Background(), sql)
		assert.ErrorContains(t, err, want, sql)
	}
}

// TestAtomic checks that a fixture is one transaction.
func TestAtomic(t *testing.T) {
	p, db := newDB(t)
	before := p.Delta.View().Index()
	require.Error(t, db.Exec(context.Background(), `
		insert into users (name) values ('a');
		insert into users (name) values (null);`))
	assert.Equal(t, before, p.Delta.View().Index())
	exec(t, db, basics)
	assert.Equal(t, before+1, p.Delta.View().Index())
}

func TestValues(t *testing.T) {
	zone, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	at := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	for s, want := range map[string]time.Time{
		"2026-03-01T10:00:00Z":                at,
		"2026-03-01T04:00:00-06:00":           at,
		"2026-03-01 04:00:00":                 at,
		"2026-03-01 04:00":                    at,
		"2026-03-01 10:00:00 UTC":             at,
		"2026-03-01 04:00:00 America/Chicago": at,
		"2026-03-01 10:00:00+00":              at,
		"2026-03-01":                          at.Add(-4 * time.Hour),
	} {
		got, err := parseTime(s, zone)
		require.NoError(t, err, s)
		assert.True(t, want.Equal(got), "%s: %s", s, got)
	}
	for s, want := range map[string]interval{
		"1 hour 50 minutes": {d: 110 * time.Minute},
		"-1 day":            {days: -1},
		"2 weeks 1 month":   {days: 14, months: 1},
		"01:30:00":          {d: 90 * time.Minute},
		"10 minute":         {d: 10 * time.Minute},
		"1 year 5 secs":     {months: 12, d: 5 * time.Second},
	} {
		got, err := parseInterval(s)
		require.NoError(t, err, s)
		assert.Equal(t, want, got, s)
	}
	for _, s := range []string{"1", "hour", "one hour", "1 blink", "25:00"} {
		_, err := parseInterval(s)
		assert.Error(t, err, s)
	}
	assert.EqualValues(t, 26*time.Hour/time.Microsecond, interval{days: 1, d: 2 * time.Hour}.micros())
}
