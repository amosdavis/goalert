package limit

import (
	"context"

	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// A Store allows getting and setting system limits on the Delta backend.
type Store struct {
	delta *deltaLimits
}

// UpdateLimitsTx updates all configurable limits.
func (s *Store) UpdateLimitsTx(ctx context.Context, tx txn.Tx, id string, max int) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return err
	}
	return s.delta.update(ctx, tx, id, max)
}

// ResetAll will reset all configurable limits to the default (no-limit).
func (s *Store) ResetAll(ctx context.Context) error {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return err
	}
	return s.delta.resetAll(ctx)
}

// Max will return the current max value for the given limit.
func (s *Store) Max(ctx context.Context, id ID) (int, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return 0, err
	}
	err = id.Valid()
	if err != nil {
		return 0, err
	}
	return s.delta.max(ctx, id)
}

// All will get the current value of all limits.
func (s *Store) All(ctx context.Context) (Limits, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin)
	if err != nil {
		return nil, err
	}
	return s.delta.all(ctx)
}

// SetMax allows setting the max value for a limit.
func (s *Store) SetMax(ctx context.Context, id ID, max int) error {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return err
	}
	err = validate.Many(id.Valid(), validate.Range("Max", max, -1, 9000))
	if err != nil {
		return err
	}
	return s.delta.set(ctx, id, max)
}
