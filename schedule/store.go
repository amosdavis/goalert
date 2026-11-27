package schedule

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/validation/validate"
)

// Store provides schedule operations on the Delta backend.
type Store struct {
	delta *deltaSchedules
}

func (store *Store) FindManyTx(ctx context.Context, tx txn.Tx, ids []string) ([]Schedule, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.ManyUUID("ScheduleID", ids, 200)
	if err != nil {
		return nil, err
	}

	// Convert string IDs to UUIDs
	uuids := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		uuids[i], err = uuid.Parse(id)
		if err != nil {
			return nil, err
		}
	}

	return store.delta.findMany(ctx, tx, uuids)
}

func (store *Store) FindMany(ctx context.Context, ids []string) ([]Schedule, error) {
	return store.FindManyTx(ctx, nil, ids)
}

func (store *Store) FindManyByUserID(ctx context.Context, tx txn.Tx, userID uuid.NullUUID) ([]Schedule, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return store.delta.findManyByUser(ctx, tx, userID)
}

func (store *Store) Create(ctx context.Context, s *Schedule) (*Schedule, error) {
	return store.CreateScheduleTx(ctx, nil, s)
}

func (store *Store) CreateScheduleTx(ctx context.Context, tx txn.Tx, s *Schedule) (*Schedule, error) {
	n, err := s.Normalize()
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return nil, err
	}

	id := uuid.New()
	if err := store.delta.create(ctx, tx, id, n); err != nil {
		return nil, err
	}

	n.ID = id.String()
	return n, nil
}

func (store *Store) Update(ctx context.Context, s *Schedule) error {
	n, err := s.Normalize()
	if err != nil {
		return err
	}

	err = validate.UUID("ScheduleID", s.ID)
	if err != nil {
		return err
	}

	err = permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	id, err := uuid.Parse(n.ID)
	if err != nil {
		return err
	}
	return store.delta.update(ctx, nil, id, n)
}

func (store *Store) UpdateTx(ctx context.Context, tx txn.Tx, s *Schedule) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	n, err := s.Normalize()
	if err != nil {
		return err
	}

	err = validate.UUID("ScheduleID", n.ID)
	if err != nil {
		return err
	}

	id, err := uuid.Parse(n.ID)
	if err != nil {
		return err
	}
	return store.delta.update(ctx, tx, id, n)
}

func (store *Store) FindAll(ctx context.Context) ([]Schedule, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return store.delta.findAll(ctx)
}

func (store *Store) FindOneForUpdate(ctx context.Context, tx txn.Tx, id string) (*Schedule, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.UUID("ScheduleID", id)
	if err != nil {
		return nil, err
	}

	schedID, err := uuid.Parse(id)
	if err != nil {
		return nil, err
	}
	return store.delta.findOne(ctx, tx, schedID, false)
}

func (store *Store) FindOne(ctx context.Context, id string) (*Schedule, error) {
	err := validate.UUID("ScheduleID", id)
	if err != nil {
		return nil, err
	}
	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	schedID, err := uuid.Parse(id)
	if err != nil {
		return nil, err
	}
	return store.delta.findOne(ctx, nil, schedID, true)
}

func (store *Store) Delete(ctx context.Context, id string) error {
	return store.DeleteTx(ctx, nil, id)
}

func (store *Store) DeleteTx(ctx context.Context, tx txn.Tx, id string) error {
	return store.DeleteManyTx(ctx, tx, []string{id})
}

func (store *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	err = validate.ManyUUID("ScheduleID", ids, 50)
	if err != nil {
		return err
	}

	// Convert string IDs to UUIDs
	uuids := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		uuids[i], err = uuid.Parse(id)
		if err != nil {
			return err
		}
	}
	return store.delta.deleteMany(ctx, tx, uuids)
}

// userExists returns a checker of the users visible in tx.
func (store *Store) userExists(ctx context.Context, tx txn.Tx) (user.ExistanceChecker, error) {
	return store.delta.userExists(ctx, tx)
}
