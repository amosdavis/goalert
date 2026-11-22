package smoke

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/test/smoke/harness"
)

// TestGraphQLUnsetFavoriteKeepsOthers checks that unsetting a favorite
// removes only the user's own. The query used to remove every user's
// favorite of a schedule, rotation, escalation policy or user.
func TestGraphQLUnsetFavoriteKeepsOthers(t *testing.T) {
	t.Parallel()

	sql := `
	insert into users (id, name, email, role)
	values
		({{uuid "u1"}}, 'one', 'one@example.com', 'user'),
		({{uuid "u2"}}, 'two', 'two@example.com', 'user');

	insert into schedules (id, name, time_zone)
	values
		({{uuid "sid"}}, 'schedule', 'UTC');

	insert into user_favorites (user_id, tgt_schedule_id)
	values
		({{uuid "u1"}}, {{uuid "sid"}}),
		({{uuid "u2"}}, {{uuid "sid"}});
	`

	h := harness.NewHarness(t, sql, "add-schedule-favorites")
	defer h.Close()

	query := func(user, q string) json.RawMessage {
		t.Helper()
		resp := h.GraphQLQueryUserT(t, h.UUID(user), q)
		for _, err := range resp.Errors {
			t.Error("GraphQL error:", err.Message)
		}
		require.Empty(t, resp.Errors)
		return resp.Data
	}
	isFavorite := func(user string) bool {
		t.Helper()
		var res struct {
			Schedule struct {
				IsFavorite bool `json:"isFavorite"`
			}
		}
		require.NoError(t, json.Unmarshal(query(user, fmt.Sprintf(`query { schedule(id: "%s") { isFavorite } }`, h.UUID("sid"))), &res))
		return res.Schedule.IsFavorite
	}
	require.True(t, isFavorite("u1"))
	require.True(t, isFavorite("u2"))

	query("u1", fmt.Sprintf(`mutation { setFavorite(input: { target: { id: "%s", type: schedule }, favorite: false }) }`, h.UUID("sid")))

	assert.False(t, isFavorite("u1"))
	assert.True(t, isFavorite("u2"), "unsetting a favorite leaves other users' favorites alone")
}
