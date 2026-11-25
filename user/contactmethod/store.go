package contactmethod

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// Store implements the lookup and management of ContactMethods against a *sql.Store backend.
type Store struct {
	delta *deltaMethods // set when the store runs on the Delta backend

	db  *sql.DB
	reg *nfydest.Registry
}

func NewStore(db *sql.DB, reg *nfydest.Registry) *Store {
	return &Store{db: db, reg: reg}
}

func (s *Store) MetadataByDest(ctx context.Context, tx txn.Tx, dest gadb.DestV1) (*Metadata, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return nil, err
	}

	var data []byte
	var fetchedAt time.Time
	if s.delta != nil {
		data, fetchedAt, err = s.delta.metadataByDest(ctx, tx, dest)
		if err != nil {
			return nil, err
		}
	} else {
		row, err := gadb.New(sqltxn.DBTX(s.db, tx)).ContactMethodMetaDest(ctx, gadb.NullDestV1{Valid: true, DestV1: dest})
		if err != nil {
			return nil, err
		}
		data, fetchedAt = row.Metadata, row.Now
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
	if s.delta != nil {
		return s.delta.setCarrierV1(ctx, tx, dest, newM)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	data, err := json.Marshal(newM.CarrierV1)
	if err != nil {
		return err
	}

	err = gadb.New(dbtx).ContactMethodUpdateMetaDest(ctx, gadb.ContactMethodUpdateMetaDestParams{Dest: gadb.NullDestV1{Valid: true, DestV1: dest}, CarrierV1: data})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) FindDestByID(ctx context.Context, tx txn.Tx, id uuid.UUID) (gadb.DestV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return gadb.DestV1{}, err
	}
	if s.delta != nil {
		return s.delta.findDest(ctx, tx, id)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	row, err := gadb.New(dbtx).ContactMethodFineOne(ctx, id)
	if err != nil {
		return gadb.DestV1{}, err
	}

	return row.Dest.DestV1, nil
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
	if s.delta != nil {
		return s.delta.setDisabledByDest(ctx, tx, dest, disabled)
	}

	return gadb.New(sqltxn.DBTX(s.db, tx)).ContactMethodEnableDisable(ctx, gadb.ContactMethodEnableDisableParams{
		Dest:     gadb.NullDestV1{Valid: true, DestV1: dest},
		Disabled: disabled,
	})
}

// CreateTx inserts the new ContactMethod into the database. A new ID is always created.
func (s *Store) Create(ctx context.Context, tx txn.Tx, c *ContactMethod) (*ContactMethod, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.Admin, permission.MatchUser(c.UserID))
	if err != nil {
		return nil, err
	}
	if s.delta != nil {
		return s.delta.create(ctx, tx, c)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	n, err := c.Normalize(ctx, s.reg)
	if err != nil {
		return nil, err
	}

	err = gadb.New(dbtx).ContactMethodAdd(ctx, gadb.ContactMethodAddParams{
		ID:                  n.ID,
		Name:                n.Name,
		Dest:                gadb.NullDestV1{Valid: true, DestV1: n.Dest},
		Disabled:            n.Disabled,
		UserID:              uuid.MustParse(n.UserID),
		EnableStatusUpdates: n.StatusUpdates,
		Private:             n.Private,
	})
	if err != nil {
		return nil, err
	}

	return n, nil
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
	if s.delta != nil {
		return s.delta.delete(ctx, tx, uids)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	if permission.Admin(ctx) {
		err = gadb.New(dbtx).DeleteContactMethod(ctx, uids)
		return err
	}

	rows, err := gadb.New(dbtx).ContactMethodLookupUserID(ctx, uids)
	if err != nil {
		return err
	}

	var checks []permission.Checker
	for _, id := range rows {
		checks = append(checks, permission.MatchUser(id.String()))
	}

	err = permission.LimitCheckAny(ctx, checks...)
	if err != nil {
		return err
	}

	err = gadb.New(dbtx).DeleteContactMethod(ctx, uids)
	return err
}

// FindOneTx finds the contact method from the database using the provided ID within a transaction.
func (s *Store) FindOne(ctx context.Context, tx txn.Tx, id uuid.UUID) (*ContactMethod, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	if s.delta != nil {
		return s.delta.findOne(ctx, tx, id)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	row, err := gadb.New(dbtx).ContactMethodFindOneUpdate(ctx, id)
	if err != nil {
		return nil, err
	}

	c := ContactMethod{
		ID:               row.ID,
		Name:             row.Name,
		Dest:             row.Dest.DestV1,
		Disabled:         row.Disabled,
		UserID:           row.UserID.String(),
		Pending:          row.Pending,
		StatusUpdates:    row.EnableStatusUpdates,
		Private:          row.Private,
		lastTestVerifyAt: row.LastTestVerifyAt,
	}

	return &c, nil
}

// UpdateTx updates the contact method with the newly provided values within a transaction.
func (s *Store) Update(ctx context.Context, tx txn.Tx, c *ContactMethod) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	if s.delta != nil {
		return s.delta.update(ctx, tx, c)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	n, err := c.Normalize(ctx, s.reg)
	if err != nil {
		return err
	}

	cm, err := s.FindOne(ctx, tx, c.ID)
	if err != nil {
		return err
	}
	if !n.Dest.Equal(cm.Dest) {
		return validation.NewFieldError("Dest", "cannot update destination of contact method")
	}
	if n.UserID != cm.UserID {
		return validation.NewFieldError("UserID", "cannot update owner of contact method")
	}

	if permission.Admin(ctx) {
		err = gadb.New(dbtx).ContactMethodUpdate(ctx, gadb.ContactMethodUpdateParams{ID: n.ID, Name: n.Name, Disabled: n.Disabled, EnableStatusUpdates: n.StatusUpdates, Private: n.Private})
		return err
	}

	err = permission.LimitCheckAny(ctx, permission.MatchUser(cm.UserID))
	if err != nil {
		return err
	}

	err = gadb.New(dbtx).ContactMethodUpdate(ctx, gadb.ContactMethodUpdateParams{ID: n.ID, Name: n.Name, Disabled: n.Disabled, EnableStatusUpdates: n.StatusUpdates, Private: n.Private})

	return err
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
	if s.delta != nil {
		return s.delta.findMany(ctx, tx, uids)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	rows, err := gadb.New(dbtx).ContactMethodFindMany(ctx, uids)
	if err != nil {
		return nil, err
	}

	cms := make([]ContactMethod, len(rows))
	for i, row := range rows {
		cms[i] = ContactMethod{
			ID:               row.ID,
			Name:             row.Name,
			Dest:             row.Dest.DestV1,
			Disabled:         row.Disabled,
			UserID:           row.UserID.String(),
			Pending:          row.Pending,
			StatusUpdates:    row.EnableStatusUpdates,
			Private:          row.Private,
			lastTestVerifyAt: row.LastTestVerifyAt,
		}
	}

	return cms, nil
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
	if s.delta != nil {
		return s.delta.findAll(ctx, tx, uid)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	rows, err := gadb.New(dbtx).ContactMethodFindAll(ctx, uid)
	if err != nil {
		return nil, 0, err
	}
	authID := permission.UserNullUUID(ctx).UUID

	result := make([]ContactMethod, 0, len(rows))
	var omitted int
	for _, row := range rows {
		if row.Private && row.UserID != authID {
			omitted++
			continue
		}

		result = append(result, ContactMethod{
			ID:               row.ID,
			Name:             row.Name,
			Dest:             row.Dest.DestV1,
			Disabled:         row.Disabled,
			UserID:           row.UserID.String(),
			Pending:          row.Pending,
			StatusUpdates:    row.EnableStatusUpdates,
			lastTestVerifyAt: row.LastTestVerifyAt,
			Private:          row.Private,
		})
	}

	return result, omitted, nil
}

// FindAllByUserIDs finds all contact methods from the database associated with any of the
// given user IDs, batched into a single query. Unlike FindAll, private contact methods are
// not filtered here -- callers are expected to apply that check themselves, the same way
// FindOneCM does for a single contact method.
func (s *Store) FindAllByUserIDs(ctx context.Context, tx txn.Tx, userIDs []string) ([]ContactMethod, error) {
	uids, err := validate.ParseManyUUID("UserID", userIDs, 100)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	if s.delta != nil {
		return s.delta.findByUsers(ctx, tx, uids...)
	}
	dbtx := sqltxn.DBTX(s.db, tx)

	rows, err := gadb.New(dbtx).ContactMethodFindAllByUserIDs(ctx, uids)
	if err != nil {
		return nil, err
	}

	result := make([]ContactMethod, len(rows))
	for i, row := range rows {
		result[i] = ContactMethod{
			ID:               row.ID,
			Name:             row.Name,
			Dest:             row.Dest.DestV1,
			Disabled:         row.Disabled,
			UserID:           row.UserID.String(),
			Pending:          row.Pending,
			StatusUpdates:    row.EnableStatusUpdates,
			lastTestVerifyAt: row.LastTestVerifyAt,
			Private:          row.Private,
		}
	}

	return result, nil
}
