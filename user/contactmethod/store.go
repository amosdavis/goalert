package contactmethod

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation/validate"
)

// Store implements the lookup and management of ContactMethods on the Delta backend.
type Store struct {
	delta *deltaMethods
}

func (s *Store) MetadataByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (*Metadata, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return nil, err
	}

	data, fetchedAt, err := s.delta.metadataByDest(ctx, tx, dest)
	if err != nil {
		return nil, err
	}

	var m Metadata
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	m.FetchedAt = fetchedAt

	return &m, nil
}

func (s *Store) SetCarrierV1MetadataByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1, newM *Metadata) error {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return err
	}
	return s.delta.setCarrierV1(ctx, tx, dest, newM)
}

func (s *Store) FindDestByID(ctx context.Context, tx txn.Tx, id uuid.UUID) (gadb.DestV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return gadb.DestV1{}, err
	}
	return s.delta.findDest(ctx, tx, id)
}

func (s *Store) EnableByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1) error {
	err := permission.LimitCheckAny(ctx, permission.System)
	if err != nil {
		return err
	}

	id, err := s.setDisabledByDest(ctx, tx, dest, false)

	if err == nil {
		// NOTE: maintain a record of consent/dissent
		logCtx := log.WithFields(ctx, log.Fields{
			"contactMethodID": id,
		})

		log.Logf(logCtx, "Contact method START code received.")
	}

	return err
}

func (s *Store) DisableByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1) error {
	err := permission.LimitCheckAny(ctx, permission.System)
	if err != nil {
		return err
	}

	id, err := s.setDisabledByDest(ctx, tx, dest, true)

	if err == nil {
		// NOTE: maintain a record of consent/dissent
		logCtx := log.WithFields(ctx, log.Fields{
			"contactMethodID": id,
		})

		log.Logf(logCtx, "Contact method STOP code received.")
	}

	return err
}

// setDisabledByDest enables or disables the contact method with the given
// destination and returns its ID.
func (s *Store) setDisabledByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1, disabled bool) (uuid.UUID, error) {
	return s.delta.setDisabledByDest(ctx, tx, dest, disabled)
}

// Create inserts the new ContactMethod into the database. A new ID is always created.
func (s *Store) Create(ctx context.Context, tx txn.Tx, c *ContactMethod) (*ContactMethod, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.MatchUser(c.UserID))
	if err != nil {
		return nil, err
	}
	return s.delta.create(ctx, tx, c)
}

// Delete removes the ContactMethod from the database using the provided ID within a transaction.
func (s *Store) Delete(ctx context.Context, tx txn.Tx, ids ...string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		return nil
	}

	uids, err := validate.ParseManyUUID("ContactMethodID", ids, 50)
	if err != nil {
		return err
	}
	return s.delta.delete(ctx, tx, uids)
}

// FindOne finds the contact method from the database using the provided ID within a transaction.
func (s *Store) FindOne(ctx context.Context, tx txn.Tx, id uuid.UUID) (*ContactMethod, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return s.delta.findOne(ctx, tx, id)
}

// Update updates the contact method with the newly provided values within a transaction.
func (s *Store) Update(ctx context.Context, tx txn.Tx, c *ContactMethod) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	return s.delta.update(ctx, tx, c)
}

// FindMany will fetch all contact methods matching the given ids.
func (s *Store) FindMany(ctx context.Context, tx txn.Tx, ids []string) ([]ContactMethod, error) {
	uids, err := validate.ParseManyUUID("ContactMethodID", ids, 50)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	return s.delta.findMany(ctx, tx, uids)
}

// FindAll finds all contact methods from the database associated with the given user ID along with the number of omitted (private) entries.
func (s *Store) FindAll(ctx context.Context, tx txn.Tx, userID string) ([]ContactMethod, int, error) {
	uid, err := validate.ParseUUID("ContactMethodID", userID)
	if err != nil {
		return nil, 0, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, 0, err
	}
	return s.delta.findAll(ctx, tx, uid)
}

// FindAllByUserIDs finds all contact methods from the database associated with any of the
// given user IDs. Unlike FindAll, private contact methods are not filtered here --
// callers are expected to apply that check themselves, the same way FindOneCM does
// for a single contact method.
func (s *Store) FindAllByUserIDs(ctx context.Context, tx txn.Tx, userIDs []string) ([]ContactMethod, error) {
	uids, err := validate.ParseManyUUID("UserID", userIDs, 100)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return s.delta.findByUsers(ctx, tx, uids...)
}
