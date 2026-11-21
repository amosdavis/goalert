package smoke

import (
	"testing"
	"time"

	"github.com/target/goalert/test/smoke/harness"
)

// TestDedupNotifications tests that if a single contact method is
// used multiple times in a user's notification rules and if engine
// experiences a disruption and resumes after the notification rule delay,
// that only a single notification is generated.
func TestDedupNotifications(t *testing.T) {
	t.Parallel()

	sql := `
	insert into users (id, name, email) 
	values 
		({{uuid "user"}}, 'bob', 'joe');
	insert into user_contact_methods (id, user_id, name, type, value) 
	values
		({{uuid "cm1"}}, {{uuid "user"}}, 'personal', 'SMS', {{phone "1"}});

	insert into user_notification_rules (user_id, contact_method_id, delay_minutes) 
	values
		({{uuid "user"}}, {{uuid "cm1"}}, 1),
		({{uuid "user"}}, {{uuid "cm1"}}, 2);

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

	insert into alerts (service_id, summary, dedup_key)
	values
		({{uuid "sid"}}, 'testing', {{autoDedupKey "testing"}});

	-- The alert starts out logged as created and already escalated to its
	-- first step, with its notification cycle started, so both rules come
	-- due together.
	insert into alert_logs (alert_id, event, message)
	values
		(1, 'created', 'Created via: manual');
	update escalation_policy_state
	set escalation_policy_step_id = {{uuid "esid"}}, last_escalation = now(), next_escalation = now() + '1 minute'::interval
	where alert_id = 1;
	insert into ep_step_on_call_users (ep_step_id, user_id)
	values
		({{uuid "esid"}}, {{uuid "user"}});
	insert into notification_policy_cycles (alert_id, user_id, checked)
	values
		(1, {{uuid "user"}}, true);
`

	h := harness.NewHarness(t, sql, "escalation-policy-step-reorder")
	defer h.Close()

	// Test that after 3 minutes, only 1 notification is generated
	h.FastForward(time.Minute * 3)

	h.Twilio(t).Device(h.Phone("1")).ExpectSMS("testing")
}
