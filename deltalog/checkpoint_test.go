package deltalog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

func dataFile(path string) Action {
	return Action{Add: &Add{
		Path: path, PartitionValues: map[string]string{"tbl": "alerts", "bucket": "1"},
		Size: 10, ModificationTime: 5, DataChange: true, Stats: `{"numRecords":1}`,
	}}
}

// buildLog creates a table with commits 1 to n. Each adds a file, removes
// the one before and records a txn version.
func buildLog(t *testing.T, store objstore.Store, n int64) *Log {
	t.Helper()
	l := newTestLog(t, store)
	create(t, l)
	for v := int64(1); v <= n; v++ {
		commitN(t, l, v)
	}
	return l
}

func commitN(t *testing.T, l *Log, v int64) {
	t.Helper()
	actions := []Action{dataFile(fmt.Sprintf("f%d.parquet", v)), {Txn: &Txn{AppID: "goalert", Version: v}}}
	if v > 1 {
		actions = append(actions, Action{Remove: &Remove{
			Path: fmt.Sprintf("f%d.parquet", v-1), DeletionTimestamp: testTime.UnixMilli(), DataChange: true,
		}})
	}
	_, err := l.Commit(context.Background(), v, testTable, "WRITE", testTime, actions)
	require.NoError(t, err)
}

func load(t *testing.T, l *Log) *Snapshot {
	t.Helper()
	s, err := l.Load(context.Background())
	require.NoError(t, err)
	return s
}

func checkpoint(t *testing.T, l *Log, s *Snapshot) {
	t.Helper()
	require.NoError(t, l.WriteCheckpoint(context.Background(), s, testTime, DefaultTombstoneRetention))
}

func TestCheckpointMatchesReplay(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	l := buildLog(t, store, 5)
	replayed := load(t, l)
	assert.Equal(t, []string{"f5.parquet"}, replayed.FilePaths())
	assert.Len(t, replayed.Removed, 4)
	checkpoint(t, l, replayed)

	// Log cleanup will delete the commits a checkpoint covers. Load then
	// has only the checkpoint to go on.
	for v := int64(0); v <= 5; v++ {
		require.NoError(t, store.Delete(ctx, CommitName(v)))
	}
	assert.Equal(t, replayed, load(t, l), "a snapshot loaded from a checkpoint equals the replayed one (F25)")

	commitN(t, l, 6)
	s := load(t, l)
	assert.EqualValues(t, 6, s.Version)
	assert.Equal(t, []string{"f6.parquet"}, s.FilePaths())
	assert.EqualValues(t, 6, s.Txns["goalert"])
}

func TestCheckpointCheckedAgainstTheLog(t *testing.T) {
	// The commit a checkpoint summarizes is still there, and agrees.
	store := objstore.NewMem()
	l := buildLog(t, store, 3)
	s := load(t, l)
	checkpoint(t, l, s)
	var warnings []error
	l.Warn = func(err error) { warnings = append(warnings, err) }
	assert.Equal(t, s, load(t, l))
	assert.Empty(t, warnings)
}

func TestLoadSkipsCheckpointsItCannotTrust(t *testing.T) {
	ctx := context.Background()
	replace := func(t *testing.T, store *objstore.Mem, s *Snapshot, footer map[string]string) {
		t.Helper()
		data, err := encodeCheckpoint(checkpointRows(s, testTime, DefaultTombstoneRetention), footer)
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, CheckpointName(s.Version), data))
	}
	for name, damage := range map[string]func(t *testing.T, store *objstore.Mem, s *Snapshot){
		"unreadable": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			require.NoError(t, store.Put(ctx, CheckpointName(s.Version), []byte("not parquet")))
		},
		"missing": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			require.NoError(t, store.Delete(ctx, CheckpointName(s.Version)))
		},
		"not written by GoAlert": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			replace(t, store, s, nil)
		},
		"from another history": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			other := *s
			other.CommitID = "another-commit"
			replace(t, store, &other, checkpointFooter(&other))
		},
		"labeled for another table": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			footer := checkpointFooter(s)
			footer[keyStoreID] = "another-table"
			replace(t, store, s, footer)
		},
		"newer GoAlert format": func(t *testing.T, store *objstore.Mem, s *Snapshot) {
			footer := checkpointFooter(s)
			footer[keyFormat] = "2"
			replace(t, store, s, footer)
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := objstore.NewMem()
			l := buildLog(t, store, 3)
			s := load(t, l)
			checkpoint(t, l, s)
			damage(t, store, s)

			var warnings []error
			l.Warn = func(err error) { warnings = append(warnings, err) }
			assert.Equal(t, s, load(t, l), "the whole log is still there to replay")
			assert.Len(t, warnings, 1, "a skipped checkpoint is reported (F21)")

			// Without the start of the log there is nothing to fall back on.
			require.NoError(t, store.Delete(ctx, CommitName(0)))
			_, err := l.Load(ctx)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestLoadDoesNotSkipStorageErrors(t *testing.T) {
	// An outage while reading the checkpoint must not be mistaken for a
	// damaged checkpoint, which could end in a false ErrCorrupt (F21).
	ctx := context.Background()
	store := objstore.NewMem()
	l := buildLog(t, store, 2)
	checkpoint(t, l, load(t, l))
	outage := objstore.Unknown(fmt.Errorf("connection reset"))
	failing := &failingGets{Store: store, fail: CheckpointName(2), err: outage}
	_, err := newTestLog(t, failing).Load(ctx)
	require.ErrorIs(t, err, objstore.ErrUnknown)
	assert.NotErrorIs(t, err, ErrCorrupt)
}

type failingGets struct {
	objstore.Store
	fail string
	err  error
}

func (f *failingGets) Get(ctx context.Context, name string) ([]byte, error) {
	if name == f.fail {
		return nil, f.err
	}
	return f.Store.Get(ctx, name)
}

func TestLastCheckpoint(t *testing.T) {
	ctx := context.Background()

	t.Run("left behind by a crash", func(t *testing.T) {
		store := objstore.NewMem()
		l := buildLog(t, store, 2)
		checkpoint(t, l, load(t, l))
		hint, err := store.Get(ctx, lastCheckpointName)
		require.NoError(t, err)

		commitN(t, l, 3)
		commitN(t, l, 4)
		s4 := load(t, l)
		checkpoint(t, l, s4)
		// A crash between writing the checkpoint and _last_checkpoint.
		require.NoError(t, store.Put(ctx, lastCheckpointName, hint))
		assert.Equal(t, s4, load(t, l))
	})

	t.Run("never moves back", func(t *testing.T) {
		store := objstore.NewMem()
		l := buildLog(t, store, 2)
		s2 := load(t, l)
		commitN(t, l, 3)
		checkpoint(t, l, load(t, l))
		checkpoint(t, l, s2)
		v, err := l.readLastCheckpoint(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 3, v)
	})

	t.Run("unreadable", func(t *testing.T) {
		store := objstore.NewMem()
		l := buildLog(t, store, 3)
		s := load(t, l)
		checkpoint(t, l, s)
		require.NoError(t, store.Put(ctx, lastCheckpointName, []byte("{")))
		for v := int64(0); v <= 3; v++ {
			require.NoError(t, store.Delete(ctx, CommitName(v)))
		}
		var warnings []error
		l.Warn = func(err error) { warnings = append(warnings, err) }
		assert.Equal(t, s, load(t, l), "the checkpoint is found by listing the log")
		assert.Len(t, warnings, 1)
	})

	t.Run("checkpointed by two pods", func(t *testing.T) {
		store := objstore.NewMem()
		l := buildLog(t, store, 2)
		s := load(t, l)
		checkpoint(t, l, s)
		require.NoError(t, New(store, "other-pod").WriteCheckpoint(ctx, s, testTime, DefaultTombstoneRetention))
		assert.Equal(t, s, load(t, l))
	})
}

func TestCheckpointTombstones(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t, objstore.NewMem())
	create(t, l)
	_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{dataFile("a.parquet"), dataFile("b.parquet")})
	require.NoError(t, err)
	_, err = l.Commit(ctx, 2, testTable, "WRITE", testTime, []Action{
		{Remove: &Remove{Path: "a.parquet", DeletionTimestamp: testTime.UnixMilli(), DataChange: true}},
		{Remove: &Remove{Path: "b.parquet", DataChange: true}},
	})
	require.NoError(t, err)
	s := load(t, l)

	removed := func(now time.Time) []string {
		var paths []string
		for _, r := range checkpointRows(s, now, DefaultTombstoneRetention) {
			if r.Remove != nil {
				paths = append(paths, r.Remove.Path)
			}
		}
		return paths
	}
	assert.Equal(t, []string{"a.parquet", "b.parquet"}, removed(testTime.Add(time.Hour)))
	assert.Equal(t, []string{"b.parquet"}, removed(testTime.Add(8*24*time.Hour)),
		"expired tombstones go; one without a deletion time stays, so VACUUM can't delete a file in use (F6)")
}

func TestCleanup(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	l := newTestLog(t, store)
	create(t, l)
	const day = 24 * time.Hour
	// Version v is committed on day v, with checkpoints at 3, 6 and 9.
	for v := int64(1); v <= 9; v++ {
		at := testTime.Add(time.Duration(v) * day)
		_, err := l.Commit(ctx, v, testTable, "WRITE", at, []Action{dataFile(fmt.Sprintf("f%d.parquet", v))})
		require.NoError(t, err)
		if v%3 == 0 {
			require.NoError(t, l.WriteCheckpoint(ctx, load(t, l), at, DefaultTombstoneRetention))
		}
	}
	want := load(t, l)

	n, err := l.Cleanup(ctx, testTime.Add(9*day), 7*day)
	require.NoError(t, err)
	assert.Zero(t, n, "no checkpoint is older than the retention yet")

	// On day 14 with seven days' retention, version 6 (day 6) is the newest
	// checkpoint old enough, so everything before it goes.
	n, err = l.Cleanup(ctx, testTime.Add(14*day), 7*day)
	require.NoError(t, err)
	assert.Equal(t, 7, n, "commits 0 to 5 and checkpoint 3")
	for v := int64(0); v < 6; v++ {
		_, err := store.Get(ctx, CommitName(v))
		require.ErrorIs(t, err, objstore.ErrNotFound)
	}
	for _, name := range []string{CommitName(6), CheckpointName(6), CommitName(9), CheckpointName(9), lastCheckpointName} {
		_, err := store.Get(ctx, name)
		require.NoError(t, err, "%s stays", name)
	}
	assert.Equal(t, want, load(t, l), "the log still loads the same state")

	// A boundary that can't be loaded is skipped: nothing older goes.
	require.NoError(t, store.Put(ctx, CheckpointName(9), []byte("not parquet")))
	n, err = l.Cleanup(ctx, testTime.Add(30*day), 7*day)
	require.NoError(t, err)
	assert.Zero(t, n, "checkpoint 6 is now the newest usable one, and nothing before it is left")
}
