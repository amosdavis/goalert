package config

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	keys   keyring.Keys
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), keys: keyring.Keys{[]byte("test key")}}
	e.tables = DefineDeltaTables(e.schema)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

// pod opens the table as one GoAlert pod and returns its config store.
func (e *deltaEnv) pod(t *testing.T, name string) (*Store, txn.DB) {
	t.Helper()
	ctx := permission.SystemContext(context.Background(), "test")
	fdb, err := filestore.OpenDelta(ctx, e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	db := deltatxn.DB{DB: fdb}
	s, err := NewDeltaStore(ctx, StoreConfig{Keys: e.keys}, db, e.tables)
	require.NoError(t, err)
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(sctx)
		_ = fdb.Close()
	})
	return s, db
}

func TestDeltaConfigData(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")

	id, schema, data, err := s.ConfigData(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, id)
	assert.Equal(t, SchemaVersion, schema)
	assert.Equal(t, "{}", string(data), "no config yet")

	id, err = s.SetConfigData(ctx, nil, []byte(`{"General":{"ApplicationName":"one"}}`))
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	id, err = s.SetConfigData(ctx, nil, []byte(`{"General":{"ApplicationName":"two"}}`))
	require.NoError(t, err)
	assert.Equal(t, 2, id, "each version gets the next id")

	id, _, data, err = s.ConfigData(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, id)
	assert.JSONEq(t, `{"General":{"ApplicationName":"two"}}`, string(data))

	// The stored data is encrypted.
	require.NoError(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, r := deltatxn.Unwrap(tx)
		row, ok := e.tables.config.Get(r, configKey(2))
		require.True(t, ok)
		assert.NotContains(t, string(row.Data), "two")
		return nil
	}))

	// A transaction sees its own write.
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		id, err := s.SetConfigData(ctx, tx, []byte(`{"General":{"ApplicationName":"three"}}`))
		require.NoError(t, err)
		got, _, data, err := s.ConfigData(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, id, got)
		assert.JSONEq(t, `{"General":{"ApplicationName":"three"}}`, string(data))
		return nil
	}))
}

func TestDeltaConfigIgnoresNewerSchemas(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	_, err := s.SetConfigData(ctx, nil, []byte(`{"General":{"ApplicationName":"current"}}`))
	require.NoError(t, err)

	// A newer GoAlert wrote a config this version can't read.
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return e.tables.config.Put(ftx, configRow{ID: 99, Schema: SchemaVersion + 1, Data: []byte("x"), Created: ftx.Now()})
	}))
	id, _, data, err := s.ConfigData(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, id, "like `where schema <= $1`")
	assert.JSONEq(t, `{"General":{"ApplicationName":"current"}}`, string(data))
}

func TestDeltaUpdateConfigRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	b, _ := e.pod(t, "b")

	const each = 5
	var wg sync.WaitGroup
	for _, s := range []*Store{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				err := s.UpdateConfig(ctx, func(cfg Config) (Config, error) {
					cfg.General.ApplicationName += "x"
					return cfg, nil
				})
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	// Read through a pod that opens now: a and b don't follow the log here,
	// so each sees only up to its own last commit (F29).
	c, _ := e.pod(t, "c")
	id, _, data, err := c.ConfigData(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2*each, id)
	assert.Contains(t, string(data), `"`+strings.Repeat("x", 2*each)+`"`, "no update is lost (F4)")
}
