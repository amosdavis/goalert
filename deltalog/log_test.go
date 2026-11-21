package deltalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

const testTable = "6f3c2c3e-0000-4000-8000-000000000001"

var testTime = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func newTestLog(t *testing.T, store objstore.Store) *Log {
	t.Helper()
	l := New(store, "test-writer")
	l.Backoff = time.Millisecond
	return l
}

func create(t *testing.T, l *Log) {
	t.Helper()
	require.NoError(t, l.Create(context.Background(), MetaData{
		ID:               testTable,
		SchemaString:     `{"type":"struct","fields":[]}`,
		PartitionColumns: []string{"tbl", "bucket"},
	}, testTime))
}

func add(path string) Action {
	return Action{Add: &Add{Path: path, PartitionValues: map[string]string{}, Size: 1, DataChange: true}}
}

func remove(path string) Action {
	return Action{Remove: &Remove{Path: path, DataChange: true}}
}

func TestCreateAndLoad(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t, objstore.NewMem())

	_, err := l.Load(ctx)
	require.ErrorIs(t, err, ErrNoTable, "no table is ever created implicitly (F27)")

	create(t, l)
	require.ErrorIs(t, l.Create(ctx, MetaData{ID: testTable}, testTime), ErrTableExists)

	s, err := l.Load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, s.Version)
	assert.Equal(t, Protocol{MinReaderVersion: 1, MinWriterVersion: 2}, s.Protocol)
	assert.Equal(t, testTable, s.MetaData.ID)
	assert.Equal(t, "1", s.MetaData.Configuration[FormatKey])
	assert.Equal(t, "parquet", s.MetaData.Format.Provider)
}

func TestCommitAndReplay(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t, objstore.NewMem())
	create(t, l)

	commit := func(v int64, actions ...Action) {
		t.Helper()
		_, err := l.Commit(ctx, v, testTable, "WRITE", testTime, actions)
		require.NoError(t, err)
	}
	commit(1, add("a.parquet"), add("b.parquet"))
	commit(2, remove("a.parquet"), add("c.parquet"))
	commit(3, Action{Txn: &Txn{AppID: "goalert", Version: 7}})

	s, err := l.Load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, s.Version)
	assert.Equal(t, []string{"b.parquet", "c.parquet"}, s.FilePaths())
	assert.EqualValues(t, 7, s.Txns["goalert"])

	// Following the log one version at a time gives the same snapshot.
	f, err := l.Load(ctx)
	require.NoError(t, err)
	commit(4, add("d.parquet"))
	more, err := l.Next(ctx, f)
	require.NoError(t, err)
	assert.True(t, more)
	more, err = l.Next(ctx, f)
	require.NoError(t, err)
	assert.False(t, more, "nothing after the latest version")
	assert.Equal(t, []string{"b.parquet", "c.parquet", "d.parquet"}, f.FilePaths())
}

func TestConcurrentCommitsOneWins(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	create(t, newTestLog(t, store))

	const writers = 10
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := New(store, fmt.Sprintf("writer-%d", i))
			_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add(fmt.Sprintf("w%d.parquet", i))})
			if err == nil {
				wins.Add(1)
				return
			}
			assert.ErrorIs(t, err, ErrConflict)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, wins.Load(), "exactly one writer commits a version (F4)")

	s, err := newTestLog(t, store).Load(ctx)
	require.NoError(t, err)
	assert.Len(t, s.Files, 1)
}

// TestUnclearResults covers commits whose outcome the store could not report
// (F32): nothing may be reported committed unless it is, or reported failed if
// it might still be.
func TestUnclearResults(t *testing.T) {
	ctx := context.Background()

	t.Run("applied, then the reply was lost", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		store.BeforeWrite = func(op, name string) error { return objstore.ErrMemAppliedThenFailed }
		_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("a.parquet")})
		require.NoError(t, err, "the commit is found and recognized as ours")
	})

	t.Run("not applied, then retried", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		failures := 2
		store.BeforeWrite = func(op, name string) error {
			if failures > 0 {
				failures--
				return objstore.Unknown(errors.New("connection reset"))
			}
			return nil
		}
		_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("a.parquet")})
		require.NoError(t, err)
		s, err := l.Load(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a.parquet"}, s.FilePaths())
	})

	t.Run("another writer won while ours was unclear", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		_, err := New(store, "other").Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("theirs.parquet")})
		require.NoError(t, err)
		store.BeforeWrite = func(op, name string) error { return objstore.Unknown(errors.New("timeout")) }
		_, err = l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("ours.parquet")})
		require.ErrorIs(t, err, ErrConflict)
	})

	t.Run("still unknown after every attempt", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		l.MaxAttempts = 3
		store.BeforeWrite = func(op, name string) error { return objstore.Unknown(errors.New("timeout")) }
		_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("a.parquet")})
		require.ErrorIs(t, err, objstore.ErrUnknown, "never reported as failed while it might still land")
		assert.NotErrorIs(t, err, ErrConflict)
	})

	t.Run("rejected outright", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		denied := errors.New("403 forbidden")
		store.BeforeWrite = func(op, name string) error { return denied }
		_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("a.parquet")})
		require.ErrorIs(t, err, denied)
		assert.NotErrorIs(t, err, objstore.ErrUnknown)
	})
}

func TestRefusesCommitsItCannotTrust(t *testing.T) {
	ctx := context.Background()

	t.Run("foreign writer", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		spark := `{"commitInfo":{"timestamp":1,"operation":"WRITE","engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.2.0"}}` + "\n" +
			`{"add":{"path":"spark.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}` + "\n"
		require.NoError(t, store.PutIfAbsent(ctx, CommitName(1), []byte(spark)))
		_, err := l.Load(ctx)
		require.ErrorIs(t, err, ErrForeign, "a commit from another writer stops replay (F11)")
	})

	t.Run("another table's commit", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		_, err := l.Commit(ctx, 1, "some-other-table", "WRITE", testTime, []Action{add("a.parquet")})
		require.NoError(t, err)
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, ErrForeign)
	})

	t.Run("newer GoAlert format", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		data, err := store.Get(ctx, CommitName(0))
		require.NoError(t, err)
		newer := strings.ReplaceAll(string(data), `"goalert.format":"1"`, `"goalert.format":"2"`)
		require.NoError(t, store.Put(ctx, CommitName(0), []byte(newer)))
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, ErrUnsupported, "a newer format is refused, not guessed at (F19, F30)")
	})

	t.Run("newer protocol", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		_, err := l.Commit(ctx, 1, testTable, "UPGRADE", testTime, []Action{{Protocol: &Protocol{MinReaderVersion: 3, MinWriterVersion: 7}}})
		require.NoError(t, err)
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, ErrUnsupported)
	})

	t.Run("gap in the log", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		_, err := l.Commit(ctx, 2, testTable, "WRITE", testTime, []Action{add("a.parquet")})
		require.NoError(t, err)
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, ErrCorrupt)
	})

	t.Run("remove of an unknown file", func(t *testing.T) {
		store := objstore.NewMem()
		l := newTestLog(t, store)
		create(t, l)
		_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{remove("never-added.parquet")})
		require.NoError(t, err)
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, ErrCorrupt)
	})
}

func TestCommitFileFormat(t *testing.T) {
	// Commit files are what other Delta readers see: one JSON action per
	// line, with commitInfo first.
	ctx := context.Background()
	store := objstore.NewMem()
	l := newTestLog(t, store)
	create(t, l)
	_, err := l.Commit(ctx, 1, testTable, "WRITE", testTime, []Action{add("tbl=alerts/bucket=3/part-1.parquet")})
	require.NoError(t, err)

	data, err := store.Get(ctx, CommitName(1))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
	assert.True(t, strings.HasPrefix(lines[0], `{"commitInfo":{"timestamp":1795262400000,"operation":"WRITE"`), lines[0])
	assert.Contains(t, lines[1], `"add":{"path":"tbl=alerts/bucket=3/part-1.parquet"`)
	assert.Equal(t, "_delta_log/00000000000000000001.json", CommitName(1))
}
