package config

import (
	"context"
	"fmt"
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
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
		config: filestore.NewTable(s, "config", func(r configRow) string { return filestore.IntKey(r.ID) }),
	}
}

// ReEncrypt encrypts every config with the first of keys. The others can decrypt
// them too.
func (t *DeltaTables) ReEncrypt(tx *filestore.Tx, keys keyring.Keys) error {
	var rows []configRow
	t.config.Ascend(tx, func(r configRow) bool { rows = append(rows, r); return true })
	for _, r := range rows {
		dec, label, err := keys.Decrypt(r.Data)
		if err != nil {
			return fmt.Errorf("decrypt config payload for config #%d: %w", r.ID, err)
		}
		if r.Data, err = keys.Encrypt(label, dec); err != nil {
			return fmt.Errorf("encrypt config payload for #%d: %w", r.ID, err)
		}
		if err := t.config.Put(tx, r); err != nil {
			return fmt.Errorf("update config payload for #%d: %w", r.ID, err)
		}
	}
	return nil
}

// NewDeltaStore returns a config store on the Delta backend. changes, from
// filestore.DeltaDB.Changes("config"), makes every pod reload a new config
// version at once, as trg_config_update's NOTIFY did on Postgres; without it,
// pods pick it up within a minute.
func NewDeltaStore(ctx context.Context, cfg StoreConfig, db txn.DB, tables *DeltaTables, changes <-chan struct{}) (*Store, error) {
	s := newStore(cfg, db)
	s.delta, s.changes = tables, changes
	if err := s.start(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// deltaLatest returns the newest config this version can read, like
// `where schema <= $1 order by id desc limit 1`.
func (s *Store) deltaLatest(ctx context.Context, tx txn.Tx) (id, schemaVersion int, data []byte, found bool, err error) {
	err = deltatxn.Read(ctx, s.txdb, tx, func(sc deltatxn.Scope) error {
		id, schemaVersion, data, found = 0, 0, nil, false
		s.delta.config.Descend(sc.Reader, func(c configRow) bool {
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
	err := deltatxn.Write(ctx, s.txdb, tx, func(sc deltatxn.Scope) error {
		var err error
		id, err = sc.Tx.NextID("config_id_seq")
		if err != nil {
			return err
		}
		return s.delta.config.Insert(sc.Tx, configRow{ID: id, Schema: SchemaVersion, Data: data, Created: sc.Now}, "config_pkey")
	})
	return int(id), err
}
