package favorite

import (
	"context"
	"slices"

	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store allows the lookup and management of Favorites on the Delta backend.
type Store struct {
	delta *deltaFavorites
}

// Set will store the target as a favorite of the given user. Must be authorized as System or the same user.
// It is safe to call multiple times.
func (s *Store) Set(ctx context.Context, tx txn.Tx, userID string, tgt assignment.Target) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.MatchUser(userID))
	if err != nil {
		return err
	}
	err = validate.Many(
		validate.UUID("TargetID", tgt.TargetID()),
		validate.UUID("UserID", userID),
		validate.OneOf("TargetType", tgt.TargetType(), assignment.TargetTypeService,
			assignment.TargetTypeSchedule, assignment.TargetTypeRotation, assignment.TargetTypeEscalationPolicy, assignment.TargetTypeUser),
	)
	if err != nil {
		return err
	}

	return s.delta.set(ctx, tx, userID, tgt)
}

// Unset will remove the target as a favorite of the given user. Must be authorized as System or the same user.
// It is safe to call multiple times.
func (s *Store) Unset(ctx context.Context, tx txn.Tx, userID string, tgt assignment.Target) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.MatchUser(userID))
	if err != nil {
		return err
	}

	err = validate.Many(
		validate.UUID("TargetID", tgt.TargetID()),
		validate.UUID("UserID", userID),
		validate.OneOf("TargetType", tgt.TargetType(), assignment.TargetTypeService,
			assignment.TargetTypeSchedule, assignment.TargetTypeRotation, assignment.TargetTypeEscalationPolicy, assignment.TargetTypeUser),
	)
	if err != nil {
		return err
	}

	return s.delta.unset(ctx, tx, userID, tgt)
}

func (s *Store) FindAll(ctx context.Context, tx txn.Tx, userID string, filter []assignment.TargetType) ([]assignment.Target, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.MatchUser(userID))
	if err != nil {
		return nil, err
	}

	err = validate.Many(
		validate.UUID("UserID", userID),
		validate.Range("Filter", len(filter), 0, 50),
	)
	if err != nil {
		return nil, err
	}

	return s.delta.findAll(ctx, tx, userID, func(t assignment.TargetType) bool {
		return len(filter) == 0 || slices.Contains(filter, t)
	})
}
