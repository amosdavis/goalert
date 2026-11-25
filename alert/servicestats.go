package alert

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

// ServiceAlertCounts returns how many alerts each service has, by status.
// Permissions are checked by the caller.
func (s *Store) ServiceAlertCounts(ctx context.Context, serviceIDs []uuid.UUID) ([]gadb.ServiceAlertCountsRow, error) {
	if s.delta != nil {
		return s.delta.serviceAlertCounts(ctx, serviceIDs)
	}
	return gadb.New(s.db).ServiceAlertCounts(ctx, serviceIDs)
}

// ServiceAlertStats returns alert statistics for services over a time range.
// Permissions are checked by the caller.
func (s *Store) ServiceAlertStats(ctx context.Context, p gadb.ServiceAlertStatsParams) ([]gadb.ServiceAlertStatsRow, error) {
	if s.delta != nil {
		return s.delta.serviceAlertStats(ctx, p)
	}
	return gadb.New(s.db).ServiceAlertStats(ctx, p)
}

// PendingMessageDests returns where an alert's messages that are still
// waiting to be sent will go. Permissions are checked by the caller.
func (s *Store) PendingMessageDests(ctx context.Context, p gadb.AllPendingMsgDestsParams) ([]gadb.AllPendingMsgDestsRow, error) {
	if s.delta != nil {
		return s.delta.pendingMessageDests(ctx, p)
	}
	return gadb.New(s.db).AllPendingMsgDests(ctx, p)
}
