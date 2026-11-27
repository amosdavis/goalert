package smoke

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/test/smoke/harness"
)

type webhookAlertNotification struct {
	Type     string
	Summary  string
	LogEntry string
}

func newAlertStatusWebhook(t *testing.T) (string, <-chan webhookAlertNotification) {
	t.Helper()

	ch := make(chan webhookAlertNotification, 8)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var notification webhookAlertNotification
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&notification)) {
			return
		}
		ch <- notification
	}))
	t.Cleanup(func() {
		assert.Empty(t, ch, "unexpected alert webhook notifications")
		ts.Close()
	})

	return ts.URL, ch
}

func nextAlertNotification(t *testing.T, h *harness.Harness, ch <-chan webhookAlertNotification) webhookAlertNotification {
	t.Helper()
	var notification webhookAlertNotification
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

func expectAlertStatusNotification(t *testing.T, h *harness.Harness, ch <-chan webhookAlertNotification, logEntry string) {
	t.Helper()

	notification := nextAlertNotification(t, h, ch)
	assert.Equal(t, "AlertStatus", notification.Type)
	assert.Equal(t, "testing", notification.Summary)
	assert.Contains(t, notification.LogEntry, logEntry)
}

// TestStatusUpdatesChannel tests status updates to notification channels.
func TestStatusUpdatesChannel(t *testing.T) {
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

	a.Ack()
	expectAlertStatusNotification(t, h, ch, "Ack")

	a.Escalate()
	first, second := nextAlertNotification(t, h, ch), nextAlertNotification(t, h, ch)
	assert.ElementsMatch(t, []string{"Alert", "AlertStatus"}, []string{first.Type, second.Type})
	for _, notification := range []webhookAlertNotification{first, second} {
		assert.Equal(t, "testing", notification.Summary)
		if notification.Type == "AlertStatus" {
			assert.Contains(t, notification.LogEntry, "Escalated")
		}
	}

	a.Close()
	expectAlertStatusNotification(t, h, ch, "Closed")
}
