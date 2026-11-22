package user

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

// OnCallOverview returns the services a user is on call for, through which
// schedules and escalation policies. Permissions are checked by the caller.
func (s *Store) OnCallOverview(ctx context.Context, userID uuid.UUID) ([]gadb.GQLUserOnCallOverviewRow, error) {
	if s.delta != nil {
		return nil, errNotPorted
	}
	return gadb.New(s.db).GQLUserOnCallOverview(ctx, userID)
}
