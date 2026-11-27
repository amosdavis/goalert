package notificationchannel

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// Store provides notification channel operations on the Delta backend.
type Store struct {
	delta *deltaChannels
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

	return s.delta.findMany(ctx, uuids)
}

func (s *Store) FindDestByID(ctx context.Context, tx txn.Tx, id uuid.UUID) (gadb.DestV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return gadb.DestV1{}, err
	}
	return s.delta.findDest(ctx, tx, id)
}

func (s *Store) LookupDestID(ctx context.Context, tx txn.Tx, d gadb.DestV1) (uuid.UUID, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return uuid.UUID{}, err
	}
	return s.delta.lookupDestID(ctx, tx, d)
}

// ResolveDests validates destinations and looks up their names, which can call
// a provider's API. A caller that uses MapDestToID in a transaction resolves the
// destinations before it starts, and runs it with the returned context. If one
// can't be resolved, MapDestToID returns why.
func (s *Store) ResolveDests(ctx context.Context, dests ...gadb.DestV1) context.Context {
	if permission.LimitCheckAny(ctx, permission.User) != nil {
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
// it if there is none, and updates its name. A destination mapped in a
// transaction must be resolved with ResolveDests first, unless it has a channel
// already, whose name then stays.
func (s *Store) MapDestToID(ctx context.Context, tx txn.Tx, d gadb.DestV1) (uuid.UUID, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return uuid.UUID{}, err
	}
	return s.delta.mapDestToID(ctx, tx, d)
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
	return s.delta.deleteMany(ctx, tx, uuids)
}

func (s *Store) FindOne(ctx context.Context, id uuid.UUID) (*Channel, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.findOne(ctx, id)
}
