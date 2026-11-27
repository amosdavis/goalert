package smoke

import (
	"testing"
	"time"

	"github.com/target/goalert/test/smoke/harness"
)

// TestOnCallNotifyTOD will test that time-of-day on-call notifications work.
func TestOnCallNotifyTOD(t *testing.T) {
	t.Parallel()

	webhookURL1, ch1 := newOnCallWebhook(t)
	webhookURL2, ch2 := newOnCallWebhook(t)

	sql := `
	insert into users (id, name, email)
	values
		({{uuid "uid"}}, 'bob', 'bob@example.com');

	insert into schedules (id, name, time_zone) 
	values
		({{uuid "sid"}}, 'testschedule', 'UTC');

	insert into schedule_rules (id, schedule_id, sunday, monday, tuesday, wednesday, thursday, friday, saturday, start_time, end_time, tgt_user_id)
	values
		({{uuid "ruleID"}}, {{uuid "sid"}}, true, true, true, true, true, true, true, '00:00:00', '00:00:00', {{uuid "uid"}});

	insert into notification_channels (id, type, name, value)
	values
		({{uuid "chan1"}}, 'WEBHOOK', 'webhook 1', '` + webhookURL1 + `'),
		({{uuid "chan2"}}, 'WEBHOOK', 'webhook 2', '` + webhookURL2 + `');
	
	insert into schedule_data (schedule_id, data)
	values
		({{uuid "sid"}}, '{"V1":{"OnCallNotificationRules": [{"ChannelID": {{uuidJSON "chan1"}}, "Time": "00:00" },{"ChannelID": {{uuidJSON "chan1"}}, "Time": "01:00" },{"ChannelID": {{uuidJSON "chan2"}}, "Time": "00:00" }]}}');
`
	h := harness.NewHarness(t, sql, "outgoing-messages-schedule-id")
	defer h.Close()

	h.Trigger()

	h.FastForward(24 * time.Hour)

	// should only send 1 notification to each webhook; cleanup checks for extras
	expectOnCallNotification(t, h, ch1, "testschedule", "bob")
	expectOnCallNotification(t, h, ch2, "testschedule", "bob")
}
