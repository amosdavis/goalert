package config

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// configRow is a config row on the Delta backend: one version of the
// encrypted config.
type configRow struct {
	ID      int64     `parquet:"id"`
	Schema  int32     `parquet:"schema"`
	Data    []byte    `parquet:"data"`
	Created time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
}

// DeltaTables are the config store's tables on the Delta backend.
type DeltaTables struct {
	config *filestore.Table[configRow]
}

// DefineDeltaTables adds the config store's tables to the Delta backend's
// schema: config, keyed by id (config_pkey). Keys are zero-padded, so the
// newest version sorts last.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	return &DeltaTables{
		config: filestore.NewTable(s, "config", func(r configRow) string { return configKey(r.ID) }),
	}
}

func configKey(id int64) string { return fmt.Sprintf("%020d", id) }

// NewDeltaStore returns a config store on the Delta backend; cfg.DB is not
// used. changes, from filestore.DeltaDB.Changes("config"), makes every pod
// reload a new config version at once, as trg_config_update's NOTIFY does on
// Postgres; without it, pods pick it up within a minute.
func NewDeltaStore(ctx context.Context, cfg StoreConfig, db txn.DB, tables *DeltaTables, changes <-chan struct{}) (*Store, error) {
	s := newStore(cfg, db)
	s.delta, s.changes = tables, changes
	if err := s.start(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

var errReadOnly = errors.New("config: cannot write in a read-only transaction")

// inDeltaTx runs fn with the parts of tx, or in a transaction of its own
// when tx is nil.
func (s *Store) inDeltaTx(ctx context.Context, tx txn.Tx, readOnly bool, fn func(ftx *filestore.Tx, r filestore.Reader) error) error {
	if tx != nil {
		ftx, r := deltatxn.Unwrap(tx)
		return fn(ftx, r)
	}
	return s.txdb.Run(ctx, &txn.Options{ReadOnly: readOnly}, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		return fn(ftx, r)
	})
}

// deltaLatest returns the newest config this version can read, like
// `where schema <= $1 order by id desc limit 1`.
func (s *Store) deltaLatest(ctx context.Context, tx txn.Tx) (id, schemaVersion int, data []byte, found bool, err error) {
	err = s.inDeltaTx(ctx, tx, true, func(_ *filestore.Tx, r filestore.Reader) error {
		s.delta.config.Descend(r, func(c configRow) bool {
			if int(c.Schema) > SchemaVersion {
				return true
			}
			id, schemaVersion, data, found = int(c.ID), int(c.Schema), c.Data, true
			return false
		})
		return nil
	})
	return id, schemaVersion, data, found, err
}

// deltaInsert adds a config version and returns its id.
func (s *Store) deltaInsert(ctx context.Context, tx txn.Tx, data []byte) (int, error) {
	var id int64
	err := s.inDeltaTx(ctx, tx, false, func(ftx *filestore.Tx, _ filestore.Reader) error {
		if ftx == nil {
			return errReadOnly
		}
		var err error
		id, err = ftx.NextID("config_id_seq")
		if err != nil {
			return err
		}
		return s.delta.config.Put(ftx, configRow{ID: id, Schema: SchemaVersion, Data: data, Created: ftx.Now()})
	})
	return int(id), err
}
