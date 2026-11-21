package nonce

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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

// pod opens the table as one GoAlert pod and returns its nonce store.
func (e *deltaEnv) pod(t *testing.T, name string) *Store {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	s := NewDeltaStore(log.NewLogger(), deltatxn.DB{DB: db}, e.tables)
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
		_ = db.Close()
	})
	return s
}

func TestDeltaConsume(t *testing.T) {
	ctx := context.Background()
	s := newDeltaEnv(t).pod(t, "a")
	n1, n2 := s.New(), s.New()

	fresh, err := s.Consume(ctx, n1)
	require.NoError(t, err)
	assert.True(t, fresh)
	fresh, err = s.Consume(ctx, n1)
	require.NoError(t, err)
	assert.False(t, fresh, "a nonce works once")
	fresh, err = s.Consume(ctx, n2)
	require.NoError(t, err)
	assert.True(t, fresh)
}

func TestDeltaConsumeRace(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	pods := []*Store{e.pod(t, "a"), e.pod(t, "b")}
	n := uuid.New()

	var fresh atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := pods[i%2].Consume(ctx, n)
			if assert.NoError(t, err) && ok {
				fresh.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, fresh.Load(), "pods racing on one nonce: exactly one consumes it (F4)")
}

func TestDeltaCleanup(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	s := e.pod(t, "a")
	n := s.New()
	_, err := s.Consume(ctx, n)
	require.NoError(t, err)

	e.clock.Set(start.Add(6 * 24 * time.Hour))
	require.NoError(t, s.delta.cleanup(ctx))
	fresh, err := s.Consume(ctx, n)
	require.NoError(t, err)
	assert.False(t, fresh, "kept for a week")

	e.clock.Set(start.Add(8 * 24 * time.Hour))
	require.NoError(t, s.delta.cleanup(ctx))
	fresh, err = s.Consume(ctx, n)
	require.NoError(t, err)
	assert.True(t, fresh, "forgotten after a week, as on Postgres")
}
