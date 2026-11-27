package smoke

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/test/smoke/harness"
	"github.com/target/goalert/util/timeutil"
)

func newOnCallWebhook(t *testing.T) (string, <-chan POSTDataOnCallNotification) {
	t.Helper()

	ch := make(chan POSTDataOnCallNotification, 8)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var notification POSTDataOnCallNotification
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&notification)) {
			return
		}
		ch <- notification
	}))
	t.Cleanup(func() {
		assert.Empty(t, ch, "unexpected on-call webhook notifications")
		ts.Close()
	})

	return ts.URL, ch
}

func nextOnCallNotification(t *testing.T, h *harness.Harness, ch <-chan POSTDataOnCallNotification) POSTDataOnCallNotification {
	t.Helper()
	var notification POSTDataOnCallNotification
	require.Eventually(t, func() bool {
		h.Trigger()
		select {
		case notification = <-ch:
			return true
		default:
			return false
		}
	}, 30*time.Second, 100*time.Millisecond)
	return notification
}

func expectOnCallNotification(t *testing.T, h *harness.Harness, ch <-chan POSTDataOnCallNotification, scheduleName string, userNames ...string) {
	t.Helper()

	notification := nextOnCallNotification(t, h, ch)
	assert.Equal(t, "ScheduleOnCallUsers", notification.Type)
	assert.Equal(t, scheduleName, notification.ScheduleName)
	names := make([]string, 0, len(notification.Users))
	for _, user := range notification.Users {
		names = append(names, user.Name)
	}
	assert.ElementsMatch(t, userNames, names)
}

// TestOnCallNotify will validate that on-change notifications are sent for schedules.
func TestOnCallNotify(t *testing.T) {
	t.Parallel()

	webhookURL1, ch1 := newOnCallWebhook(t)
	webhookURL2, ch2 := newOnCallWebhook(t)

	sql := `
	insert into users (id, name, email)
	values
		({{uuid "uid"}}, 'bob', 'bob@example.com'),
		({{uuid "uid2"}}, 'joe', 'joe@example.com');

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
		({{uuid "sid"}}, '{"V1":{"OnCallNotificationRules": [{"ChannelID": {{uuidJSON "chan1"}} }, {"ChannelID": {{uuidJSON "chan2"}} }]}}');
`
	h := harness.NewHarness(t, sql, "outgoing-messages-schedule-id")
	defer h.Close()

	expectOnCallNotification(t, h, ch1, "testschedule", "bob")
	expectOnCallNotification(t, h, ch2, "testschedule", "bob")

	h.FastForward(time.Hour)

	ctx := permission.SystemContext(context.Background(), "Test")
	_, err := h.App().ScheduleRuleStore.CreateRuleTx(ctx, nil, &rule.Rule{
		ID:            uuid.New().String(),
		ScheduleID:    h.UUID("sid"),
		WeekdayFilter: timeutil.EveryDay(),
		Target:        assignment.UserTarget(h.UUID("uid2")),
	})
	require.NoError(t, err)

	expectOnCallNotification(t, h, ch1, "testschedule", "bob", "joe")
	expectOnCallNotification(t, h, ch2, "testschedule", "bob", "joe")
}
