package nonce

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

// maxAge is how long a used nonce is remembered.
const maxAge = 7 * 24 * time.Hour

// nonceRow is an auth_nonce row on the Delta backend.
type nonceRow struct {
	ID      string    `parquet:"id"`
	Created time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
}

// DeltaTables are the nonce store's tables on the Delta backend.
type DeltaTables struct {
	nonces *filestore.Table[nonceRow]
}

// DefineDeltaTables adds the nonce store's tables to the Delta backend's
// schema: auth_nonce, keyed by its id (auth_nonce_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		nonces: filestore.NewTable(s, "auth_nonce", func(n nonceRow) string { return n.ID }),
	}
}

// NewDeltaStore returns a nonce store on the Delta backend.
func NewDeltaStore(logger *log.Logger, db txn.DB, tables *DeltaTables) *Store {
	s := &Store{
		logger:   logger,
		shutdown: make(chan context.Context),
		delta:    &deltaStore{db: db, DeltaTables: tables},
	}
	go s.loop()
	return s
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
}

// consume records a nonce and reports whether it was new. If two pods
// consume the same nonce at once, the one whose commit loses runs again
// and finds it used, so only one sees it as new (F4).
func (d *deltaStore) consume(ctx context.Context, id uuid.UUID) (bool, error) {
	var fresh bool
	err := d.db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		if _, used := d.nonces.Get(r, id.String()); used {
			fresh = false
			return nil
		}
		fresh = true
		return d.nonces.Put(ftx, nonceRow{ID: id.String(), Created: ftx.Now()})
	})
	return fresh, err
}

// cleanup forgets nonces older than maxAge.
func (d *deltaStore) cleanup(ctx context.Context) error {
	return d.db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		cutoff := ftx.Now().Add(-maxAge)
		var old []string
		d.nonces.Ascend(r, func(n nonceRow) bool {
			if n.Created.Before(cutoff) {
				old = append(old, n.ID)
			}
			return true
		})
		for _, id := range old {
			if _, err := d.nonces.Delete(ftx, id); err != nil {
				return err
			}
		}
		return nil
	})
}
