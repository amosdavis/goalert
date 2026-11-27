package integrationkey

import (
	"context"
	"database/sql"

	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Store provides integration key operations on the Delta backend.
type Store struct {
	delta *deltaKeys
}

func (s *Store) Authorize(ctx context.Context, tok authtoken.Token, t Type) (context.Context, error) {
	var serviceID string
	var err error
	permission.SudoContext(ctx, func(c context.Context) {
		serviceID, err = s.GetServiceID(c, tok.ID.String(), t)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ctx, permission.Unauthorized()
	}
	if err != nil {
		return ctx, errors.Wrap(err, "lookup serviceID")
	}
	ctx = permission.ServiceSourceContext(ctx, serviceID, &permission.SourceInfo{
		Type: permission.SourceTypeIntegrationKey,
		ID:   tok.ID.String(),
	})
	return ctx, nil
}

func (s *Store) GetServiceID(ctx context.Context, id string, t Type) (string, error) {
	keyUUID, err := validate.ParseUUID("IntegrationKeyID", id)
	err = validate.Many(
		err,
		validate.OneOf("IntegrationType", t, TypeGeneric),
	)
	if err != nil {
		return "", err
	}
	err = permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.User)
	if err != nil {
		return "", err
	}

	return s.delta.serviceID(ctx, keyUUID, t)
}

func (s *Store) Create(ctx context.Context, tx txn.Tx, i *IntegrationKey) (*IntegrationKey, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	n, err := i.Normalize()
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()
	err = s.delta.create(ctx, tx, n)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (s *Store) Delete(ctx context.Context, tx txn.Tx, id string) error {
	return s.DeleteMany(ctx, tx, []string{id})
}

func (s *Store) DeleteMany(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	uuids, err := validate.ParseManyUUID("IntegrationKeyID", ids, 50)
	if err != nil {
		return err
	}

	return s.delta.delete(ctx, tx, uuids)
}

func (s *Store) FindOne(ctx context.Context, id string) (*IntegrationKey, error) {
	keyUUID, err := validate.ParseUUID("IntegrationKeyID", id)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.findOne(ctx, keyUUID)
}

func (s *Store) FindAllByService(ctx context.Context, serviceID string) ([]IntegrationKey, error) {
	serviceUUID, err := validate.ParseUUID("ServiceID", serviceID)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllByService(ctx, serviceUUID)
}
