package rule

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

type ScheduleTriggerFunc func(string)

// Store provides schedule rule operations on the Delta backend.
type Store struct {
	delta *deltaRules
}

func (s *Store) _Add(ctx context.Context, tx txn.Tx, r *Rule) (*Rule, error) {
	n, err := r.Normalize()
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()
	if err := s.delta.add(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (s *Store) Add(ctx context.Context, r *Rule) (*Rule, error) {
	r, err := s._Add(ctx, nil, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Store) CreateRuleTx(ctx context.Context, tx txn.Tx, r *Rule) (*Rule, error) {
	return s._Add(ctx, tx, r)
}

func (s *Store) FindByTargetTx(ctx context.Context, tx txn.Tx, scheduleID string, target assignment.Target) ([]Rule, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.Many(
		validate.UUID("ScheduleID", scheduleID),
		validate.OneOf("TargetType", target.TargetType(), assignment.TargetTypeUser, assignment.TargetTypeRotation),
		validate.UUID("TargetID", target.TargetID()),
	)
	if err != nil {
		return nil, err
	}

	return s.delta.findByTarget(ctx, tx, scheduleID, target)
}

func (s *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ruleIDs []string) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}
	if len(ruleIDs) == 0 {
		return nil
	}
	err = validate.ManyUUID("RuleIDs", ruleIDs, 50)
	if err != nil {
		return err
	}
	return s.delta.deleteMany(ctx, tx, ruleIDs)
}

func (s *Store) UpdateTx(ctx context.Context, tx txn.Tx, r *Rule) error {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}
	n, err := r.Normalize()
	if err != nil {
		return err
	}
	return s.delta.update(ctx, tx, n)
}

func (s *Store) FindAll(ctx context.Context, scheduleID string) ([]Rule, error) {
	return s.FindAllTx(ctx, nil, scheduleID)
}

func (s *Store) FindAllTx(ctx context.Context, tx txn.Tx, scheduleID string) ([]Rule, error) {
	err := validate.UUID("ScheduleID", scheduleID)
	if err != nil {
		return nil, err
	}
	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return s.delta.findAll(ctx, tx, scheduleID)
}
