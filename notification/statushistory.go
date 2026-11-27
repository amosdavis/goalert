package notification

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

// MessageStatusHistory returns the status changes of a message, newest
// first. Permissions are checked by the caller.
func (s *Store) MessageStatusHistory(ctx context.Context, messageID uuid.UUID) ([]gadb.MessageStatusHistory, error) {
	return s.delta.messageStatusHistory(ctx, messageID)
}
