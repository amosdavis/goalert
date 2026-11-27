package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store provides service operations on the Delta backend.
type Store struct {
	delta *deltaServices
}

func (s *Store) FindOneForUpdate(ctx context.Context, tx txn.Tx, id string) (*Service, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ServiceID", id)
	if err != nil {
		return nil, err
	}
	return s.delta.findOneForUpdate(ctx, tx, uuid.MustParse(id))
}

// FindMany returns slice of Service objects given a slice of serviceIDs
func (s *Store) FindMany(ctx context.Context, ids []string) ([]Service, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	err = validate.ManyUUID("ServiceIDs", ids, 100)
	if err != nil {
		return nil, err
	}
	return s.delta.find(ctx, permission.UserNullUUID(ctx), parseIDs(ids))
}

func (s *Store) CreateServiceTx(ctx context.Context, tx txn.Tx, svc *Service) (*Service, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	n, err := svc.Normalize()
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()
	if err := s.delta.insert(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (s *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	err = validate.ManyUUID("ServiceID", ids, 50)
	if err != nil {
		return err
	}
	return s.delta.delete(ctx, tx, parseIDs(ids))
}

func (s *Store) UpdateTx(ctx context.Context, tx txn.Tx, svc *Service) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	n, err := svc.Normalize()
	if err != nil {
		return err
	}

	err = validate.UUID("ServiceID", n.ID)
	if err != nil {
		return err
	}
	return s.delta.update(ctx, tx, n)
}

func (s *Store) FindOneForUser(ctx context.Context, userID, serviceID string) (*Service, error) {
	err := validate.UUID("ServiceID", serviceID)
	if err != nil {
		return nil, err
	}

	userCheck := permission.User
	if userID != "" {
		err := validate.UUID("UserID", userID)
		if err != nil {
			return nil, err
		}
		userCheck = permission.MatchUser(userID)
	}

	err = permission.LimitCheckAny(ctx, userCheck, permission.System)
	if err != nil {
		return nil, err
	}
	return s.delta.findOne(ctx, parseUserID(userID), uuid.MustParse(serviceID))
}

func (s *Store) FindOne(ctx context.Context, id string) (*Service, error) {
	return s.FindOneForUser(ctx, "", id)
}

func (s *Store) FindAllByEP(ctx context.Context, epID string) ([]Service, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}
	return s.delta.findAllByEP(ctx, epID)
}
