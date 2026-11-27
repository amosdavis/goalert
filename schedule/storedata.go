package schedule

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/txn"
)

func (store *Store) scheduleData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID) (*Data, error) {
	return store.delta.scheduleData(ctx, tx, scheduleID)
}

func (store *Store) updateScheduleData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID, apply func(data *Data) error) error {
	return store.delta.updateData(ctx, tx, scheduleID, apply)
}
