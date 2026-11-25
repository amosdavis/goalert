// Package tablelogtest is the conformance suite every tablelog.Log
// implementation must pass. It checks atomic publication, replay, snapshots,
// and cleanup independently of the physical backend.
package tablelogtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

var start = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// Run runs the suite. newStore returns an empty object store for data and any
// backend state that uses one. newLog returns a writer with writerID over the
// same backend; it must be independent of earlier writers, as another pod is.
func Run(t *testing.T, newStore func(t *testing.T) objstore.Store, newLog func(t *testing.T, store objstore.Store, writerID string) tablelog.Log) {
	t.Helper()
	open := func(t *testing.T) (objstore.Store, tablelog.Log, tablelog.Log) {
		store := newStore(t)
		a, b := newLog(t, store, "a"), newLog(t, store, "b")
		for _, l := range []tablelog.Log{a, b} {
			l.SetRetry(3, time.Millisecond)
			if c, ok := l.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = c.Close() })
			}
		}
		return store, a, b
	}
	meta := func() tablelog.MetaData {
		return tablelog.MetaData{ID: "store-id", Name: "goalert", SchemaString: `{"type":"struct","fields":[]}`}
	}
	add := func(path string) tablelog.Action {
		return tablelog.Action{Add: &tablelog.Add{Path: path, Size: 1, DataChange: true, Stats: `{"numRecords":1}`, PartitionValues: map[string]string{"tbl": "users", "bucket": "0"}}}
	}
	remove := func(path string) tablelog.Action {
		return tablelog.Action{Remove: &tablelog.Remove{Path: path, DeletionTimestamp: start.UnixMilli(), DataChange: true}}
	}

	t.Run("CreateRequiresExplicitTable", func(t *testing.T) {
		_, a, _ := open(t)
		_, err := a.Load(context.Background())
		require.ErrorIs(t, err, tablelog.ErrNoTable)
	})

	t.Run("CreateAndRead", func(t *testing.T) {
		_, a, b := open(t)
		ctx := context.Background()
		require.NoError(t, a.Create(ctx, meta(), start))
		require.ErrorIs(t, b.Create(ctx, meta(), start), tablelog.ErrTableExists)

		info, err := a.Commit(ctx, 1, "store-id", "WRITE", start.Add(time.Second), []tablelog.Action{add("tbl=users/bucket=0/a.parquet")})
		require.NoError(t, err)
		assert.NotEmpty(t, info.GoAlertCommitID)
		assert.Equal(t, "a", info.GoAlertWriterID)
		got, actions, err := b.Read(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, info.GoAlertCommitID, got.GoAlertCommitID)
		require.Len(t, actions, 1)
		assert.Equal(t, "tbl=users/bucket=0/a.parquet", actions[0].Add.Path)

		snap, err := b.Load(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 1, snap.Version)
		assert.Equal(t, info.GoAlertCommitID, snap.CommitID)
		assert.Contains(t, snap.Files, "tbl=users/bucket=0/a.parquet")
		_, _, err = b.Read(ctx, 2)
		require.ErrorIs(t, err, objstore.ErrNotFound)
	})

	t.Run("OneWinnerPerVersion", func(t *testing.T) {
		_, a, b := open(t)
		ctx := context.Background()
		require.NoError(t, a.Create(ctx, meta(), start))
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, l := range []tablelog.Log{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = l.Commit(ctx, 1, "store-id", "WRITE", start.Add(time.Second), []tablelog.Action{add("tbl=users/bucket=0/a.parquet")})
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil && !errors.Is(err, tablelog.ErrConflict) {
				require.NoError(t, err, "the race must not produce an unrelated error")
			}
		}
		assert.NotEqual(t, errs[0] == nil, errs[1] == nil, "exactly one writer wins")

		snap, err := a.Load(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 1, snap.Version)
	})

	t.Run("ReplaceAndCleanup", func(t *testing.T) {
		_, a, b := open(t)
		ctx := context.Background()
		require.NoError(t, a.Create(ctx, meta(), start))
		for v := int64(1); v <= 2; v++ {
			actions := []tablelog.Action{add("tbl=users/bucket=0/b.parquet")}
			if v == 2 {
				actions = append(actions, remove("tbl=users/bucket=0/a.parquet"))
			} else {
				actions[0].Add.Path = "tbl=users/bucket=0/a.parquet"
			}
			_, err := a.Commit(ctx, v, "store-id", "WRITE", start.Add(time.Duration(v)*time.Second), actions)
			require.NoError(t, err)
		}
		snap, err := b.Load(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 2, snap.Version)
		assert.NotContains(t, snap.Files, "tbl=users/bucket=0/a.parquet")
		assert.Contains(t, snap.Files, "tbl=users/bucket=0/b.parquet")
		require.NoError(t, b.WriteCheckpoint(ctx, snap, start.Add(3*time.Second), time.Hour))

		_, err = b.Cleanup(ctx, start.Add(3*time.Hour), time.Hour)
		require.NoError(t, err)
		fresh, err := a.Load(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 2, fresh.Version)
		_, _, err = a.Read(ctx, 1)
		require.ErrorIs(t, err, objstore.ErrNotFound)
		_, _, err = a.Read(ctx, 2)
		require.NoError(t, err, "the load boundary stays available")
	})
}
