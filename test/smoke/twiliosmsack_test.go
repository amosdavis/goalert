package smoke

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/test/smoke/harness"
)

// TestTwilioSMSAck checks that an SMS ack message is processed.
func TestTwilioSMSAck(t *testing.T) {
	t.Parallel()

	sql := `
	insert into users (id, name, email, role) 
	values 
		({{uuid "user"}}, 'bob', 'joe', 'user');
	insert into user_contact_methods (id, user_id, name, type, value) 
	values
		({{uuid "cm1"}}, {{uuid "user"}}, 'personal', 'SMS', {{phone "1"}});

	insert into user_notification_rules (user_id, contact_method_id, delay_minutes) 
	values
		({{uuid "user"}}, {{uuid "cm1"}}, 0),
		({{uuid "user"}}, {{uuid "cm1"}}, 30);

	insert into escalation_policies (id, name) 
	values
		({{uuid "eid"}}, 'esc policy');
	insert into escalation_policy_steps (id, escalation_policy_id) 
	values
		({{uuid "esid"}}, {{uuid "eid"}});
	insert into escalation_policy_actions (escalation_policy_step_id, user_id) 
	values 
		({{uuid "esid"}}, {{uuid "user"}});

	insert into services (id, escalation_policy_id, name) 
	values
		({{uuid "sid"}}, {{uuid "eid"}}, 'service');

	insert into alerts (id, service_id, summary, dedup_key) 
	values
		(198, {{uuid "sid"}}, 'testing', {{autoDedupKey "testing"}});

	-- The alert starts out logged as created and already escalated to its
	-- first step, with its notification cycle started.
	insert into alert_logs (alert_id, event, message)
	values
		(198, 'created', 'Created via: manual');
	update escalation_policy_state
	set escalation_policy_step_id = {{uuid "esid"}}, last_escalation = now(), next_escalation = now() + '1 minute'::interval
	where alert_id = 198;
	insert into ep_step_on_call_users (ep_step_id, user_id)
	values
		({{uuid "esid"}}, {{uuid "user"}});
	insert into notification_policy_cycles (alert_id, user_id, checked)
	values
		(198, {{uuid "user"}}, true);
`
	h := harness.NewHarness(t, sql, "ids-to-uuids")
	defer h.Close()

	tw := h.Twilio(t)
	d1 := tw.Device(h.Phone("1"))

	d1.ExpectSMS("testing").
		ThenReply("ack198").
		ThenExpect("acknowledged")

	h.FastForward(time.Hour)

	resp := h.GraphQLQuery2(`{alert(id: 198) {recentEvents{nodes{message, state{ details, status, formattedSrcValue}}}}}`)
	var respData struct {
		Alert struct {
			RecentEvents struct {
				Nodes []struct {
					Message string
					State   struct {
						Details           string
						Status            string
						FormattedSrcValue string
					}
				}
			}
		}
	}
	err := json.Unmarshal(resp.Data, &respData)
	require.NoError(t, err)
	msgs := respData.Alert.RecentEvents.Nodes
	require.Len(t, msgs, 3)

	t.Logf("msgs: %+v", msgs)

	// note: log is in reverse order
	assert.Contains(t, msgs[0].Message, "Acknowledged by bob")
	assert.Contains(t, msgs[1].Message, "Notification sent to bob")
	assert.Contains(t, msgs[1].State.Details, "delivered")
	assert.Equal(t, "OK", msgs[1].State.Status)
	assert.Contains(t, msgs[2].Message, "Created")
}
