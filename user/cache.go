package user

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/permission"
)

// ExistanceChecker allows checking if various users exist.
type ExistanceChecker interface {
	UserExistsString(id string) bool
	UserExistsUUID(id uuid.UUID) bool
}

// UserExists returns an ExistanceChecker.
func (s *Store) UserExists(ctx context.Context) (ExistanceChecker, error) {
	err := permission.LimitCheckAny(ctx)
	if err != nil {
		return nil, err
	}
	return s.delta.userExists(ctx)
}
