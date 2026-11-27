package smoke

import (
	"testing"

	"github.com/target/goalert/test/smoke/harness"
)

// TestOnCallNotifyTempSchedMissingUser will validate that a temporary schedule shift for a user that
// no longer exists does not cause repeated on-change notifications
func TestOnCallNotifyTempSchedMissingUser(t *testing.T) {
	t.Parallel()

	webhookURL, ch := newOnCallWebhook(t)

	// "deleted-user" is intentionally not inserted into the users table
	sql := `
	insert into users (id, name, email)
	values
		({{uuid "uid"}}, 'bob', 'bob@example.com');

	insert into schedules (id, name, time_zone)
	values
		({{uuid "sid"}}, 'testschedule', 'UTC');

	insert into notification_channels (id, type, name, value)
	values
		({{uuid "chan1"}}, 'WEBHOOK', 'webhook', '` + webhookURL + `');

	insert into schedule_data (schedule_id, data)
	values
		({{uuid "sid"}}, '{"V1":{
			"OnCallNotificationRules": [{"ChannelID": {{uuidJSON "chan1"}} }],
			"TemporarySchedules": [{
				"Start": "0000-08-24T21:03:54Z",
				"End": "9999-08-24T21:03:54Z",
				"Shifts": [
					{"Start": "0000-08-24T21:03:54Z", "End": "9999-08-24T21:03:54Z", "UserID": {{uuidJSON "uid"}} },
					{"Start": "0000-08-24T21:03:54Z", "End": "9999-08-24T21:03:54Z", "UserID": {{uuidJSON "deleted-user"}} }
				]
			}]
		}}');
`
	h := harness.NewHarness(t, sql, "outgoing-messages-schedule-id")
	defer h.Close()

	expectOnCallNotification(t, h, ch, "testschedule", "bob")

	// On-call is not changing, so additional engine cycles must not produce more notifications;
	// the webhook cleanup checks that no additional notifications arrive.
	h.Trigger()
	h.Trigger()
	h.Trigger()
}
