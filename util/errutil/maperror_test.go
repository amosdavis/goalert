package errutil_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

// TestMapDBErrorDelta checks that broken constraints on the Delta backend
// give users the same messages as on Postgres (F22).
func TestMapDBErrorDelta(t *testing.T) {
	check := func(err error, want string) {
		t.Helper()
		got := errutil.MapDBError(fmt.Errorf("wrapped: %w", err))
		assert.True(t, validation.IsClientError(got), "%v", got)
		assert.ErrorContains(t, got, want)
	}
	check(&filestore.UniqueError{Table: "auth_basic_users", Index: "auth_basic_users_username_key", Key: "bob"}, "already in use")
	check(&filestore.UniqueError{Table: "services", Index: "services_name_key", Key: "svc"}, "already in use")
	check(&filestore.ForeignKeyError{Table: "alerts", ForeignKey: "alerts_services_id_fkey", Missing: true}, "service does not exist")
	check(&filestore.ForeignKeyError{Table: "escalation_policies", ForeignKey: "services_escalation_policy_id_fkey"}, "is currently in use")
	check(&filestore.ForeignKeyError{Table: "services", ForeignKey: "services_escalation_policy_id_fkey", Missing: true}, "does not exist")
	check(&filestore.CheckError{Table: "users", Constraint: "alert_status_user_id_match"}, "contact method is for wrong user")

	err := errutil.MapDBError(&filestore.CheckError{
		Table: "user_contact_methods", Constraint: "contact_methods_per_user_limit", Message: "limit exceeded", HintText: "max=5",
	})
	var le errutil.LimitError
	require.ErrorAs(t, err, &le)
	assert.Equal(t, limit.ContactMethodsPerUser, le.ID())
	assert.Equal(t, 5, le.Max())
}
