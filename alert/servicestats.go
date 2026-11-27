package alert

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

// ServiceAlertCounts returns how many alerts each service has, by status.
// Permissions are checked by the caller.
func (s *Store) ServiceAlertCounts(ctx context.Context, serviceIDs []uuid.UUID) ([]gadb.ServiceAlertCountsRow, error) {
	return s.delta.serviceAlertCounts(ctx, serviceIDs)
}

// PendingMessageDests returns where an alert's messages that are still
// waiting to be sent will go. Permissions are checked by the caller.
func (s *Store) PendingMessageDests(ctx context.Context, p gadb.AllPendingMsgDestsParams) ([]gadb.AllPendingMsgDestsRow, error) {
	return s.delta.pendingMessageDests(ctx, p)
}
