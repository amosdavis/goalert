package notificationchannel

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/util"
	"github.com/target/goalert/validation/validate"
)

type Store struct {
	delta *deltaChannels // set when the store runs on the Delta backend

	db  *sql.DB
	reg *nfydest.Registry
}

func NewStore(ctx context.Context, db *sql.DB, reg *nfydest.Registry) (*Store, error) {
	p := &util.Prepare{DB: db, Ctx: ctx}

	return &Store{
		db:  db,
		reg: reg,
	}, p.Err
}

func (s *Store) FindMany(ctx context.Context, ids []string) ([]Channel, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	uuids, err := validate.ParseManyUUID("ID", ids, search.MaxResults)
	if err != nil {
		return nil, err
	}

	if s.delta != nil {
		return s.delta.findMany(ctx, uuids)
	}

	rows, err := gadb.New(s.db).NotifChanFindMany(ctx, uuids)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	channels := make([]Channel, len(rows))
	for i, r := range rows {
		channels[i].fromRow(r)
	}

	return channels, nil
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

	row, err := gadb.New(dbtx).NotifChanFindOne(ctx, id)
	if err != nil {
		return gadb.DestV1{}, err
	}

	return row.Dest.DestV1, nil
}

func (s *Store) LookupDestID(ctx context.Context, tx txn.Tx, d gadb.DestV1) (uuid.UUID, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return uuid.UUID{}, err
	}
	if s.delta != nil {
		return s.delta.lookupDestID(ctx, tx, d)
	}
	stx := sqltxn.Unwrap(tx)

	return gadb.New(stx).NotifChanFindDestID(ctx, gadb.NullDestV1{Valid: true, DestV1: d})
}

// ResolveDests validates destinations and looks up their names, which can call
// a provider's API, such as Slack's. A caller that uses MapDestToID in a
// transaction resolves the destinations before it starts, and runs it with the
// returned context. If one can't be resolved, MapDestToID returns why.
//
// On Postgres, MapDestToID does this itself, and the context is returned
// unchanged.
func (s *Store) ResolveDests(ctx context.Context, dests ...gadb.DestV1) context.Context {
	if s.delta == nil || permission.LimitCheckAny(ctx, permission.User) != nil {
		return ctx
	}

	return s.delta.resolveDests(ctx, dests)
}

// destName validates d and returns its display name.
func destName(ctx context.Context, reg *nfydest.Registry, d gadb.DestV1) (string, error) {
	err := reg.ValidateDest(ctx, d)
	if err != nil {
		return "", err
	}
	info, err := reg.DisplayInfo(ctx, d)
	if err != nil {
		return "", err
	}

	return info.Text, nil
}

// MapDestToID returns the ID of the channel with the given destination, adding
// it if there is none, and updates its name. On the Delta backend, a
// destination mapped in a transaction must be resolved with ResolveDests
// first, unless it has a channel already, whose name then stays.
func (s *Store) MapDestToID(ctx context.Context, tx txn.Tx, d gadb.DestV1) (uuid.UUID, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return uuid.UUID{}, err
	}
	if s.delta != nil {
		return s.delta.mapDestToID(ctx, tx, d)
	}
	dbtx := sqltxn.DBTX(s.db, tx)
	name, err := destName(ctx, s.reg, d)
	if err != nil {
		return uuid.UUID{}, err
	}

	return gadb.New(dbtx).NotifChanUpsertDest(ctx, gadb.NotifChanUpsertDestParams{
		ID:   uuid.New(),
		Dest: gadb.NullDestV1{Valid: true, DestV1: d},
		Name: name,
	})
}

func (s *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return err
	}

	uuids, err := validate.ParseManyUUID("ID", ids, 100)
	if err != nil {
		return err
	}
	if s.delta != nil {
		return s.delta.deleteMany(ctx, tx, uuids)
	}
	stx := sqltxn.Unwrap(tx)

	db := gadb.New(s.db)
	if stx != nil {
		db = db.WithTx(stx)
	}

	return db.NotifChanDeleteMany(ctx, uuids)
}

func (s *Store) FindOne(ctx context.Context, id uuid.UUID) (*Channel, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	if s.delta != nil {
		return s.delta.findOne(ctx, id)
	}

	var c Channel
	row, err := gadb.New(s.db).NotifChanFindOne(ctx, id)
	if err != nil {
		return nil, err
	}
	c.fromRow(row)

	return &c, nil
}
