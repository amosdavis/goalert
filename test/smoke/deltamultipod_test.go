package smoke

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/test/smoke/harness"
)

// TestDeltaMultiPod checks that three pods on one table serve reads and writes,
// and that another pod delivers pages after the first lease holder stops.
func TestDeltaMultiPod(t *testing.T) {
	t.Parallel()
	h := harness.NewHarness(t, `
		insert into users (id, name, email) values ({{uuid "user"}}, 'bob', 'bob@example.com');
		insert into user_contact_methods (id, user_id, name, type, value)
		values ({{uuid "cm"}}, {{uuid "user"}}, 'personal', 'SMS', {{phone "1"}});
		insert into user_notification_rules (user_id, contact_method_id, delay_minutes)
		values ({{uuid "user"}}, {{uuid "cm"}}, 0);
		insert into escalation_policies (id, name) values ({{uuid "ep"}}, 'policy');
		insert into escalation_policy_steps (id, escalation_policy_id) values ({{uuid "step"}}, {{uuid "ep"}});
		insert into escalation_policy_actions (escalation_policy_step_id, user_id)
		values ({{uuid "step"}}, {{uuid "user"}});
		insert into services (id, escalation_policy_id, name) values ({{uuid "svc"}}, {{uuid "ep"}}, 'service');
	`, "ids-to-uuids")
	defer h.Close()

	pods := h.StartDeltaPods(3)
	defer pods.Close()
	d := h.Twilio(t).Device(h.Phone("1"))
	create := func(summary string) {
		res := h.GraphQLQuery2(fmt.Sprintf(`mutation{createAlert(input:{serviceID:%q,summary:%q}){id}}`, h.UUID("svc"), summary))
		require.Empty(t, res.Errors)
	}
	for i := range 3 {
		summary := fmt.Sprintf("pod-%d", i)
		create(summary)
		d.ExpectSMS(summary)
	}

	// Each pod reads what the others wrote, including the session created by the
	// first pod.
	for i := range 3 {
		res := h.GraphQLQuery2(fmt.Sprintf(`query{alert(id:%d){summary}}`, i+1))
		require.Empty(t, res.Errors)
		var out struct {
			Alert struct{ Summary string }
		}
		require.NoError(t, json.Unmarshal(res.Data, &out))
		assert.Equal(t, fmt.Sprintf("pod-%d", i), out.Alert.Summary)
	}
	assert.Equal(t, []int{2, 2, 2}, pods.Hits())

	// The first pod acquired the lease before the others started. Stopping it
	// releases the lease, and a remaining pod sends the next page.
	pods.Stop(0)
	create("after-stop")
	d.ExpectSMS("after-stop")
	hits := pods.Hits()
	assert.Equal(t, 2, hits[0], "a stopped pod keeps its earlier hit count")
	assert.Greater(t, hits[1]+hits[2], 4)
}
