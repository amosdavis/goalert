package keyring

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	clock  *filestore.FakeClock
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	e.tables = DefineDeltaTables(e.schema)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// open opens the table as one GoAlert pod.
func (e *deltaEnv) open(t *testing.T, name string) *filestore.DeltaDB {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	return fdb
}

func (e *deltaEnv) keyring(t *testing.T, fdb *filestore.DeltaDB, cfg Config) *DB {
	t.Helper()
	k, err := NewDeltaDB(context.Background(), log.NewLogger(), deltatxn.DB{DB: fdb}, e.tables, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Shutdown(context.Background()) })
	return k
}

var testCfg = Config{Name: "test", RotationDays: 1, Keys: Keys{[]byte("secret")}}

func TestDeltaKeyringShared(t *testing.T) {
	e := newDeltaEnv(t)
	a := e.keyring(t, e.open(t, "a"), testCfg)
	sig, err := a.Sign([]byte("hello"))
	require.NoError(t, err)

	b := e.keyring(t, e.open(t, "b"), testCfg)
	valid, old := b.Verify([]byte("hello"), sig)
	assert.True(t, valid, "every pod uses the same keys")
	assert.False(t, old)
	valid, _ = b.Verify([]byte("goodbye"), sig)
	assert.False(t, valid)
}

func TestDeltaKeyringCreateRace(t *testing.T) {
	e := newDeltaEnv(t)
	dbs := []*filestore.DeltaDB{e.open(t, "a"), e.open(t, "b")}
	keyrings := make([]*DB, len(dbs))
	var wg sync.WaitGroup
	for i, fdb := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyrings[i] = e.keyring(t, fdb, testCfg)
		}()
	}
	wg.Wait()

	sig, err := keyrings[0].Sign([]byte("hello"))
	require.NoError(t, err)
	valid, _ := keyrings[1].Verify([]byte("hello"), sig)
	assert.True(t, valid, "two pods creating the keyring at once end up with the same keys (F4)")
}

func TestDeltaKeyringRotation(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	a := e.keyring(t, e.open(t, "a"), testCfg)
	before, err := a.Sign([]byte("hello"))
	require.NoError(t, err)

	require.NoError(t, a.refreshAndRotateKeys(ctx, false))
	assert.Equal(t, 0, a.rotationCount, "not due yet")

	e.clock.Set(start.Add(25 * time.Hour))
	require.NoError(t, a.refreshAndRotateKeys(ctx, false))
	assert.Equal(t, 1, a.rotationCount, "due after a day")

	after, err := a.Sign([]byte("hello"))
	require.NoError(t, err)
	valid, old := a.Verify([]byte("hello"), before)
	assert.True(t, valid, "an old signature stays valid")
	assert.True(t, old)
	valid, old = a.Verify([]byte("hello"), after)
	assert.True(t, valid)
	assert.False(t, old)

	b := e.keyring(t, e.open(t, "b"), testCfg)
	assert.Equal(t, 1, b.rotationCount)
	valid, _ = b.Verify([]byte("hello"), after)
	assert.True(t, valid)
}

func TestDeltaKeyringRotationRace(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	dbs := []*filestore.DeltaDB{e.open(t, "a"), e.open(t, "b")}
	keyrings := []*DB{e.keyring(t, dbs[0], testCfg), e.keyring(t, dbs[1], testCfg)}

	race := func(force bool) {
		var wg sync.WaitGroup
		for _, k := range keyrings {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, k.refreshAndRotateKeys(ctx, force))
			}()
		}
		wg.Wait()
	}

	// Both find a rotation due: the second to commit finds it done (F4).
	e.clock.Set(start.Add(25 * time.Hour))
	race(false)
	fresh := e.keyring(t, e.open(t, "c"), testCfg)
	assert.Equal(t, 1, fresh.rotationCount, "a due rotation happens once")

	// Both force one: both happen, one after the other, as on Postgres.
	race(true)
	fresh = e.keyring(t, e.open(t, "d"), testCfg)
	assert.Equal(t, 3, fresh.rotationCount, "each forced rotation happens")
}
