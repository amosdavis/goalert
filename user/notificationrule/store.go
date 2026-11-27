package notificationrule

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store allows the lookup and management of NotificationRules on the Delta backend.
type Store struct {
	delta *deltaRules
}

// Insert implements the NotificationRuleStore interface by inserting the new NotificationRule into the database.
// A new ID is always created.
func (s *Store) Insert(ctx context.Context, n *NotificationRule) (*NotificationRule, error) {
	return s.CreateTx(ctx, nil, n)
}

// CreateTx implements the NotificationRuleStore interface by inserting the new NotificationRule into the database.
// A new ID is always created.
func (s *Store) CreateTx(ctx context.Context, tx txn.Tx, n *NotificationRule) (*NotificationRule, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.MatchUser(n.UserID))
	if err != nil {
		return nil, err
	}

	n, err = n.Normalize(false)
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()

	if err := s.delta.insert(ctx, tx, n); err != nil {
		return nil, err
	}

	return n, nil
}

// DeleteTx will delete notification rules with the provided ids.
func (s *Store) DeleteTx(ctx context.Context, tx txn.Tx, ids ...string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		return nil
	}

	err = validate.ManyUUID("NotificationRuleID", ids, 50)
	if err != nil {
		return err
	}
	return s.delta.delete(ctx, tx, ids)
}

// FindAll implements the NotificationRuleStore interface.
func (s *Store) FindAll(ctx context.Context, userID string) ([]NotificationRule, error) {
	err := validate.UUID("UserID", userID)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findByUsers(ctx, userID)
}

// FindAllByUserIDs finds all notification rules associated with any of the given user IDs.
func (s *Store) FindAllByUserIDs(ctx context.Context, userIDs []string) ([]NotificationRule, error) {
	err := validate.ManyUUID("UserID", userIDs, 100)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findByUsers(ctx, userIDs...)
}
