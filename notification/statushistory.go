package notification

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
)

// MessageStatusHistory returns the status changes of a message, oldest
// first. Permissions are checked by the caller.
func (s *Store) MessageStatusHistory(ctx context.Context, messageID uuid.UUID) ([]gadb.MessageStatusHistory, error) {
	return gadb.New(s.db).GraphQL_MessageStatusHistory(ctx, messageID)
}
