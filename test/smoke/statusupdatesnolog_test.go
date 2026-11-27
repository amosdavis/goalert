package smoke

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/test/smoke/harness"
)

// TestStatusUpdatesNoLog tests status updates continue to work when no logs are present.
func TestStatusUpdatesNoLog(t *testing.T) {
	t.Parallel()

	webhookURL, ch := newAlertStatusWebhook(t)

	sql := `
	insert into escalation_policies (id, name) 
	values
		({{uuid "eid"}}, 'esc policy');
	insert into escalation_policy_steps (id, escalation_policy_id) 
	values
		({{uuid "esid"}}, {{uuid "eid"}});

	insert into notification_channels (id, type, name, value)
	values
		({{uuid "chan"}}, 'WEBHOOK', 'webhook', '` + webhookURL + `');

	insert into escalation_policy_actions (escalation_policy_step_id, channel_id) 
	values 
		({{uuid "esid"}}, {{uuid "chan"}});

	insert into services (id, escalation_policy_id, name) 
	values
		({{uuid "sid"}}, {{uuid "eid"}}, 'service');
`
	h := harness.NewHarness(t, sql, "webhook-notification-channel-type")
	defer h.Close()

	a := h.CreateAlertWithDetails(h.UUID("sid"), "testing", "details")
	notification := nextAlertNotification(t, h, ch)
	assert.Equal(t, "Alert", notification.Type)
	assert.Equal(t, "testing", notification.Summary)

	// ensure no processing happens
	err := h.App().Pause(context.Background())
	require.NoError(t, err)
	a.Ack()

	h.Exec("delete from alert_logs;")

	h.IgnoreErrorsWith("no log entry found for alert #1 status update (acknowledged), skipping")

	err = h.App().Resume(context.Background())
	require.NoError(t, err)

	h.Trigger()
	// first update should be skipped

	a.Close()

	// The next status notification must be the close, not the acknowledgement
	// whose log entry was deleted.
	expectAlertStatusNotification(t, h, ch, "Closed")
}
