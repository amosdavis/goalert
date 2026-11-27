package override

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// Store manages active overrides on the Delta backend.
type Store struct {
	delta *deltaOverrides
}

func (s *Store) FindOneUserOverrideTx(ctx context.Context, tx txn.Tx, id string, _ bool) (*UserOverride, error) {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("OverrideID", id)
	if err != nil {
		return nil, err
	}
	return s.delta.findOne(ctx, tx, id)
}

// UpdateUserOverrideTx updates an existing UserOverride, inside an optional transaction.
func (s *Store) UpdateUserOverrideTx(ctx context.Context, tx txn.Tx, o *UserOverride) error {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return err
	}
	n, err := o.Normalize()
	if err != nil {
		return err
	}
	err = validate.UUID("ID", n.ID)
	if err != nil {
		return err
	}
	if !n.End.After(time.Now()) {
		return validation.NewFieldError("End", "must be in the future")
	}
	return s.delta.update(ctx, tx, n)
}

// UpdateUserOverride updates an existing UserOverride.
func (s *Store) UpdateUserOverride(ctx context.Context, o *UserOverride) error {
	return s.UpdateUserOverrideTx(ctx, nil, o)
}

// CreateUserOverrideTx adds a UserOverride to the DB with a new ID.
func (s *Store) CreateUserOverrideTx(ctx context.Context, tx txn.Tx, o *UserOverride) (*UserOverride, error) {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return nil, err
	}
	n, err := o.Normalize()
	if err != nil {
		return nil, err
	}
	if !n.End.After(time.Now()) {
		return nil, validation.NewFieldError("End", "must be in the future")
	}
	n.ID = uuid.New().String()
	if err := s.delta.create(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

// DeleteUserOverride removes a UserOverride from the DB matching the given ID.
func (s *Store) DeleteUserOverrideTx(ctx context.Context, tx txn.Tx, ids ...string) error {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	err = validate.ManyUUID("UserOverrideID", ids, 50)
	if err != nil {
		return err
	}
	return s.delta.delete(ctx, tx, ids)
}

// FindAllUserOverrides will return all UserOverrides that belong to the provided Target within the provided time range.
func (s *Store) FindAllUserOverrides(ctx context.Context, start, end time.Time, t assignment.Target) ([]UserOverride, error) {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return nil, err
	}
	err = validate.Many(
		validate.OneOf("TargetType", t.TargetType(), assignment.TargetTypeSchedule),
		validate.UUID("TargetID", t.TargetID()),
	)
	if err != nil {
		return nil, err
	}
	return s.delta.findAll(ctx, start, end, t)
}
