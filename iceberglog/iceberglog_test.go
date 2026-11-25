package iceberglog_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/iceberglog"
	"github.com/target/goalert/iceberglog/icebergtest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

const location = "gs://bucket/goalert"

var (
	ident = table.Identifier{"ns", "goalert"}
	start = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
)

type env struct {
	t     *testing.T
	store *objstore.Mem
	cat   *icebergtest.Catalog
	warns []error
	mu    sync.Mutex
}

func newEnv(t *testing.T) *env {
	return &env{t: t, store: objstore.NewMem(), cat: &icebergtest.Catalog{}}
}

func (e *env) config() iceberglog.Config {
	return iceberglog.Config{
		Catalog: e.cat, Ident: ident, Store: e.store, Location: location,
		Warn: func(err error) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.warns = append(e.warns, err)
		},
	}
}

func (e *env) log(writer string) *iceberglog.Log {
	e.t.Helper()
	l, err := iceberglog.New(e.config(), writer)
	require.NoError(e.t, err)
	l.SetRetry(4, time.Millisecond)
	return l
}

func meta() tablelog.MetaData {
	return tablelog.MetaData{ID: "store-id", Name: "goalert", SchemaString: `{"type":"struct","fields":[{"name":"users","type":"string","nullable":true,"metadata":{}}]}`}
}

func (e *env) create() *iceberglog.Log {
	e.t.Helper()
	l := e.log("admin")
	require.NoError(e.t, l.Create(context.Background(), meta(), start))
	return l
}

func add(name string) tablelog.Action {
	return tablelog.Action{Add: &tablelog.Add{Path: name, Size: 7, DataChange: true, Stats: `{"numRecords":3}`, PartitionValues: map[string]string{"tbl": "users", "bucket": "0"}}}
}

func remove(name string) tablelog.Action {
	return tablelog.Action{Remove: &tablelog.Remove{Path: name, DeletionTimestamp: start.UnixMilli(), DataChange: true}}
}

func commit(l tablelog.Log, version int64, actions ...tablelog.Action) (tablelog.CommitInfo, error) {
	return l.Commit(context.Background(), version, "store-id", "WRITE", start.Add(time.Duration(version)*time.Second), actions)
}

// icebergFiles are the data files an Iceberg reader finds.
func (e *env) icebergFiles() []string {
	e.t.Helper()
	m := e.cat.Metadata()
	if m.CurrentSnapshot() == nil {
		return nil
	}
	tasks, err := icebergtest.Table(m, e.store, location).Scan().PlanFiles(context.Background())
	require.NoError(e.t, err)
	var out []string
	for _, task := range tasks {
		out = append(out, strings.TrimPrefix(task.File.FilePath(), location+"/"))
	}
	slices.Sort(out)
	return out
}

// check opens the table afresh and checks that Iceberg and Delta readers agree
// with it.
func (e *env) check() *tablelog.Snapshot {
	e.t.Helper()
	s, err := e.log("checker").Load(context.Background())
	require.NoError(e.t, err)
	assert.ElementsMatch(e.t, s.FilePaths(), e.icebergFiles(), "an Iceberg reader sees the same files")
	delta := deltalog.New(e.store, "reader")
	delta.Manager = iceberglog.Manager
	d, err := delta.Load(context.Background())
	require.NoError(e.t, err)
	assert.Equal(e.t, s.Version, d.Version)
	assert.Equal(e.t, s.FilePaths(), d.FilePaths(), "a Delta reader sees the same files")
	return s
}

func TestConformance(t *testing.T) {
	envs := map[objstore.Store]*env{}
	tablelogtest.Run(t,
		func(t *testing.T) objstore.Store {
			e := newEnv(t)
			envs[e.store] = e
			return e.store
		},
		func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
			return envs[store].log(writerID)
		},
	)
}

// TestBackend runs the whole-backend suite. Whatever it leaves must be the same
// to Iceberg and Delta readers.
func TestBackend(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
		e := newEnv(t)
		t.Cleanup(func() {
			if !t.Failed() {
				e.check()
			}
		})
		return func(writer string) *storagebackend.Backend {
			return &storagebackend.Backend{Store: e.store, Log: e.log(writer), Location: location}
		}
	})
}

func TestNewChecksConfig(t *testing.T) {
	good := newEnv(t).config()
	for name, change := range map[string]func(c *iceberglog.Config){
		"catalog":   func(c *iceberglog.Config) { c.Catalog = nil },
		"store":     func(c *iceberglog.Config) { c.Store = nil },
		"namespace": func(c *iceberglog.Config) { c.Ident = table.Identifier{"x"} },
		"location":  func(c *iceberglog.Config) { c.Location = "" },
		"path":      func(c *iceberglog.Config) { c.Location = "bucket/prefix" },
		"query":     func(c *iceberglog.Config) { c.Location = "gs://bucket/prefix?x=y" },
		"snapshots": func(c *iceberglog.Config) { c.Snapshots = -1 },
	} {
		c := good
		change(&c)
		_, err := iceberglog.New(c, "w")
		assert.Error(t, err, name)
	}
}

type user struct {
	ID    string    `parquet:"id"`
	Name  string    `parquet:"name"`
	Tags  []string  `parquet:"tags,list"`
	At    time.Time `parquet:"at,timestamp(microsecond:utc)"`
	Count int64     `parquet:"count"`
	Boss  *string   `parquet:"boss,optional"`
}

func openDB(t *testing.T, e *env, writer string, users **filestore.Table[user]) *filestore.DeltaDB {
	t.Helper()
	s := filestore.NewSchema()
	tbl := filestore.NewTable(s, "users", func(u user) string { return u.ID })
	_ = filestore.NewTable(s, "notes", func(u user) string { return u.ID })
	if users != nil {
		*users = tbl
	}
	opts := filestore.DeltaOptions{WriterID: writer, PollInterval: -1, CommitBackoff: time.Millisecond, CheckpointEvery: 5, Logf: t.Logf}
	if e.cat.Metadata() == nil {
		require.NoError(t, filestore.CreateBackend(context.Background(), e.store, e.log(writer), s, opts))
	}
	db, err := filestore.OpenBackend(context.Background(), e.store, e.log(writer), s, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// rows reads the users with an Iceberg reader.
func rows(t *testing.T, e *env) map[string]user {
	t.Helper()
	tbl, err := icebergtest.Table(e.cat.Metadata(), e.store, location).Scan().ToArrowTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	out := map[string]user{}
	idx := tbl.Schema().FieldIndices("users")
	require.Len(t, idx, 1, "%s", tbl.Schema())
	for _, chunk := range tbl.Column(idx[0]).Data().Chunks() {
		s := chunk.(*array.Struct)
		field := func(name string) arrow.Array {
			i, ok := s.DataType().(*arrow.StructType).FieldIdx(name)
			require.True(t, ok, name)
			return s.Field(i)
		}
		for i := range s.Len() {
			if s.IsNull(i) {
				continue
			}
			u := user{
				ID:    field("id").(*array.String).Value(i),
				Name:  field("name").(*array.String).Value(i),
				Count: field("count").(*array.Int64).Value(i),
				At:    time.UnixMicro(int64(field("at").(*array.Timestamp).Value(i))).UTC(),
			}
			if b := field("boss").(*array.String); !b.IsNull(i) {
				v := b.Value(i)
				u.Boss = &v
			}
			tags := field("tags").(*array.List)
			from, to := tags.ValueOffsets(i)
			for j := from; j < to; j++ {
				u.Tags = append(u.Tags, tags.ListValues().(*array.String).Value(int(j)))
			}
			_, dup := out[u.ID]
			require.False(t, dup, "row %s twice", u.ID)
			out[u.ID] = u
		}
	}
	return out
}

func dump(tbl *filestore.Table[user], db *filestore.DeltaDB) map[string]user {
	out := map[string]user{}
	tbl.Ascend(db.View(), func(u user) bool { out[u.ID] = u; return true })
	return out
}

// TestIcebergReader checks that an Iceberg reader sees the rows GoAlert has,
// through inserts, updates and deletes (F35, F47).
func TestIcebergReader(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	var users *filestore.Table[user]
	db := openDB(t, e, "a", &users)
	boss := "u0"
	for round := range 12 {
		_, err := db.Update(ctx, func(tx *filestore.Tx) error {
			for i := range 5 {
				u := user{ID: fmt.Sprintf("u%d", i*7+round), Name: fmt.Sprintf("round %d", round), At: start.Add(time.Duration(round) * time.Microsecond), Count: int64(round), Tags: []string{"a", "b"}[:i%3]}
				if i%2 == 0 {
					u.Boss = &boss
				}
				if err := users.Put(tx, u); err != nil {
					return err
				}
			}
			if round%4 == 3 {
				_, err := users.Delete(tx, fmt.Sprintf("u%d", round))
				return err
			}
			return nil
		})
		require.NoError(t, err)
		want := dump(users, db)
		got := rows(t, e)
		require.Equal(t, slices.Sorted(maps.Keys(want)), slices.Sorted(maps.Keys(got)), "round %d", round)
		for id, u := range want {
			if len(u.Tags) == 0 {
				u.Tags = nil
			}
			require.Equal(t, u, got[id])
		}
	}
	require.NoError(t, db.Close())
	e.check()
	assert.Empty(t, e.warns)

	// Every commit has a snapshot, on top of the one before.
	m := e.cat.Metadata()
	assert.Len(t, m.Snapshots(), 12)
	assert.Equal(t, "12", m.CurrentSnapshot().Summary.Properties["goalert.version"])
	assert.Equal(t, "false", m.Properties()[table.GCEnabledKey], "other tools must not delete data files (F6)")
}

// TestConcurrentPods runs writers in several pods at once. No write may be
// lost, every pod must end with the same rows, and Iceberg must have them too
// (F4, F25, F47).
func TestConcurrentPods(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	const pods, workers, writes = 3, 3, 6
	tables := make([]*filestore.Table[user], pods)
	dbs := make([]*filestore.DeltaDB, pods)
	for p := range pods {
		dbs[p] = openDB(t, e, fmt.Sprintf("pod-%d", p), &tables[p])
	}
	var wg sync.WaitGroup
	var latest atomic.Int64
	for p := range pods {
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := range writes {
					v, err := dbs[p].Update(ctx, func(tx *filestore.Tx) error {
						c, _ := tables[p].Get(tx, "counter")
						c.ID, c.Count, c.At = "counter", c.Count+1, start
						if err := tables[p].Put(tx, c); err != nil {
							return err
						}
						return tables[p].Put(tx, user{ID: fmt.Sprintf("%d-%d-%d", p, w, n), At: start})
					})
					if !assert.NoError(t, err) {
						return
					}
					for old := latest.Load(); v > old && !latest.CompareAndSwap(old, v); old = latest.Load() {
					}
				}
			}()
		}
	}
	wg.Wait()
	var conflicts int64
	for p := range pods {
		require.NoError(t, dbs[p].WaitFor(ctx, latest.Load()))
		got := dump(tables[p], dbs[p])
		assert.Len(t, got, pods*workers*writes+1)
		assert.EqualValues(t, pods*workers*writes, got["counter"].Count, "no increment is lost")
		assert.Equal(t, dump(tables[0], dbs[0]), got)
		conflicts += dbs[p].Conflicts()
	}
	assert.Len(t, rows(t, e), pods*workers*writes+1)
	for _, db := range dbs {
		require.NoError(t, db.Close())
	}
	s := e.check()
	t.Logf("%d versions, %d conflicts", s.Version, conflicts)
}

// TestCommitOutcomes checks how Commit reports what the catalog does with a
// request, and what the table holds afterwards (F1, F4, F32).
func TestCommitOutcomes(t *testing.T) {
	lost := errors.New("connection reset")
	denied := objstore.Denied(errors.New("token expired"))
	rejected := fmt.Errorf("%w: bad request", iceberglog.ErrRejected)
	conflict := fmt.Errorf("%w: busy", table.ErrCommitFailed)
	// An attempt is done or not, and answered with an error or not.
	type attempt struct {
		done bool
		err  error
	}
	for name, tc := range map[string]struct {
		attempts []attempt
		// committed is whether the version must hold this commit.
		committed bool
		want      error // nil for success
		tries     int
	}{
		"success":                  {attempts: []attempt{{done: true}}, committed: true, tries: 1},
		"reply lost":               {attempts: []attempt{{done: true, err: lost}}, committed: true, tries: 1},
		"request lost":             {attempts: []attempt{{err: lost}, {done: true}}, committed: true, tries: 2},
		"all lost":                 {attempts: []attempt{{err: lost}, {err: lost}, {err: lost}, {err: lost}}, want: objstore.ErrUnknown, tries: 4},
		"denied":                   {attempts: []attempt{{err: denied}}, want: objstore.ErrDenied, tries: 1},
		"rejected":                 {attempts: []attempt{{err: rejected}}, want: iceberglog.ErrRejected, tries: 1},
		"lost, then denied":        {attempts: []attempt{{err: lost}, {err: denied}, {err: denied}, {err: denied}}, want: objstore.ErrUnknown, tries: 4},
		"spurious conflict":        {attempts: []attempt{{err: conflict}, {err: conflict}, {done: true}}, committed: true, tries: 3},
		"only conflicts":           {attempts: []attempt{{err: conflict}, {err: conflict}, {err: conflict}, {err: conflict}}, want: tablelog.ErrConflict, tries: 4},
		"conflict, then lost":      {attempts: []attempt{{err: conflict}, {done: true, err: lost}}, committed: true, tries: 2},
		"answered as a conflict":   {attempts: []attempt{{done: true, err: conflict}}, committed: true, tries: 1},
		"answered as a bad denial": {attempts: []attempt{{done: true, err: denied}}, committed: true, tries: 1},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.create()
			l := e.log("a")
			tries := 0
			e.cat.Before = func(op string) error {
				if op != "commit" {
					return nil
				}
				tries++
				if a := tc.attempts[min(tries, len(tc.attempts))-1]; !a.done {
					return a.err
				}
				return nil
			}
			e.cat.After = func(op string) error {
				if op != "commit" {
					return nil
				}
				return tc.attempts[min(tries, len(tc.attempts))-1].err
			}
			info, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
			e.cat.Before, e.cat.After = nil, nil
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
				for _, other := range []error{objstore.ErrUnknown, objstore.ErrDenied, tablelog.ErrConflict, iceberglog.ErrRejected} {
					if other != tc.want {
						assert.NotErrorIs(t, err, other)
					}
				}
			}
			assert.Equal(t, tc.tries, tries)
			s := e.check()
			if tc.committed {
				assert.EqualValues(t, 1, s.Version)
				assert.Equal(t, info.GoAlertCommitID, s.CommitID)
			} else {
				assert.EqualValues(t, 0, s.Version)
			}
			// And the table goes on.
			_, err = commit(e.log("b"), s.Version+1, add("tbl=users/bucket=0/b.parquet"))
			require.NoError(t, err)
			e.check()
		})
	}
}

// TestLateCommit checks a request that is done long after it was given up on:
// the catalog refuses it, since the version was taken meanwhile.
func TestLateCommit(t *testing.T) {
	e := newEnv(t)
	e.create()
	a, b := e.log("a"), e.log("b")
	lost := errors.New("timeout")
	e.cat.Before = func(op string) error {
		if op == "commit" {
			return lost
		}
		return nil
	}
	_, err := commit(a, 1, add("tbl=users/bucket=0/a.parquet"))
	require.ErrorIs(t, err, objstore.ErrUnknown)
	e.cat.Before = nil
	_, err = commit(b, 1, add("tbl=users/bucket=0/b.parquet"))
	require.NoError(t, err)
	_, err = commit(a, 1, add("tbl=users/bucket=0/a.parquet"))
	require.ErrorIs(t, err, tablelog.ErrConflict)
	assert.Equal(t, []string{"tbl=users/bucket=0/b.parquet"}, e.check().FilePaths())
}

// TestCrashBeforePublish checks that a commit whose writer crashed right after
// the catalog took it is found, by each way of finding it (F1, F43).
func TestCrashBeforePublish(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*env, tablelog.CommitInfo) {
		e := newEnv(t)
		e.create()
		e.store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(1) {
				return errors.New("crash")
			}
			return nil
		}
		info, err := commit(e.log("a"), 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err, "it has happened")
		require.NotEmpty(t, e.warns)
		e.store.BeforeWrite = nil
		_, err = e.store.Get(ctx, deltalog.CommitName(1))
		require.ErrorIs(t, err, objstore.ErrNotFound)
		return e, info
	}
	t.Run("Read", func(t *testing.T) {
		e, info := setup(t)
		got, actions, err := e.log("b").Read(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, info, got)
		assert.Len(t, actions, 1)
		e.check()
	})
	t.Run("Load", func(t *testing.T) {
		e, info := setup(t)
		assert.Equal(t, info.GoAlertCommitID, e.check().CommitID)
	})
	t.Run("Commit", func(t *testing.T) {
		e, _ := setup(t)
		_, err := commit(e.log("b"), 1)
		require.ErrorIs(t, err, tablelog.ErrConflict)
		_, err = commit(e.log("b"), 2, add("tbl=users/bucket=0/b.parquet"))
		require.NoError(t, err)
		_, err = e.store.Get(ctx, deltalog.CommitName(1))
		require.NoError(t, err, "the Delta log has no gap")
		assert.Len(t, e.check().Files, 2)
	})
	t.Run("Unpublishable", func(t *testing.T) {
		// No commit may be made until the one before it is published.
		e, _ := setup(t)
		e.store.BeforeWrite = func(op, name string) error {
			if strings.HasPrefix(name, "_delta_log/0000") {
				return errors.New("no")
			}
			return nil
		}
		_, err := commit(e.log("b"), 2)
		require.Error(t, err)
		assert.Equal(t, 1, e.cat.Commits)
	})
	t.Run("Cleanup", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.log("b").Cleanup(ctx, start.Add(1000*time.Hour), time.Hour)
		require.NoError(t, err)
		e.check()
	})
}

// TestCrashAtEveryWrite fails each write to the object store and the catalog
// in turn, with and without doing it. Whatever was acknowledged must be there,
// and whatever failed must not be (F1, F6, F32, F43).
func TestCrashAtEveryWrite(t *testing.T) {
	ctx := context.Background()
	const commits = 5
	for _, applied := range []bool{false, true} {
		for step := 1; ; step++ {
			e := newEnv(t)
			e.create()
			var n atomic.Int64
			var dead atomic.Bool
			crash := func() bool {
				// From the crash on, nothing is written.
				if !dead.Load() && n.Add(1) == int64(step) {
					dead.Store(true)
				}
				return dead.Load()
			}
			var first atomic.Bool
			e.store.BeforeWrite = func(op, name string) error {
				if crash() {
					if applied && first.CompareAndSwap(false, true) {
						return objstore.ErrMemAppliedThenFailed
					}
					return objstore.Unknown(errors.New("crashed"))
				}
				return nil
			}
			e.cat.Before = func(op string) error {
				if op == "commit" && crash() && !(applied && !first.Load()) {
					return errors.New("crashed")
				}
				return nil
			}
			e.cat.After = func(op string) error {
				if op == "commit" && dead.Load() && first.CompareAndSwap(false, true) {
					return errors.New("crashed")
				}
				return nil
			}
			l := e.log("a")
			l.SetRetry(2, time.Microsecond)
			acked := int64(0)
			var failed error
			for v := int64(1); v <= commits && failed == nil; v++ {
				actions := []tablelog.Action{add(fmt.Sprintf("tbl=users/bucket=%d/%d.parquet", v%2, v))}
				if v > 2 {
					actions = append(actions, remove(fmt.Sprintf("tbl=users/bucket=%d/%d.parquet", v%2, v-2)))
				}
				if _, err := commit(l, v, actions...); err != nil {
					failed = err
				} else {
					acked = v
				}
			}
			e.store.BeforeWrite, e.cat.Before, e.cat.After = nil, nil, nil
			s := e.check()
			switch {
			case failed == nil:
				require.EqualValues(t, commits, s.Version, "step %d", step)
			case errors.Is(failed, objstore.ErrUnknown):
				require.Contains(t, []int64{acked, acked + 1}, s.Version, "step %d", step)
			default:
				require.Equal(t, acked, s.Version, "step %d: a failed commit did not happen: %v", step, failed)
			}
			_, err := commit(e.log("b"), s.Version+1, add("tbl=users/bucket=9/next.parquet"))
			require.NoError(t, err, "step %d: the table goes on", step)
			e.check()
			_, err = e.log("c").Cleanup(ctx, start.Add(1000*time.Hour), time.Hour)
			require.NoError(t, err)
			e.check()
			if !dead.Load() {
				assert.Greater(t, step, commits*5, "every write was covered")
				break
			}
		}
	}
}

// TestOtherWriters checks that what did not come from a GoAlert commit through
// the catalog is refused (F11, F43).
func TestOtherWriters(t *testing.T) {
	ctx := context.Background()
	load := func(e *env) error { _, err := e.log("b").Load(ctx); return err }

	t.Run("Delta alone", func(t *testing.T) {
		e := newEnv(t)
		e.create()
		d := deltalog.New(e.store, "d")
		_, err := d.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrManaged, "a pod configured without the catalog must not write")
		require.ErrorIs(t, d.Create(ctx, meta(), start), tablelog.ErrTableExists)
		d.Manager = iceberglog.Manager
		_, err = commit(d, 1)
		require.ErrorIs(t, err, tablelog.ErrManaged)
	})
	t.Run("Iceberg alone", func(t *testing.T) {
		// A table made without the catalog can't be opened with it.
		e := newEnv(t)
		require.NoError(t, deltalog.New(e.store, "d").Create(ctx, meta(), start))
		require.ErrorIs(t, load(e), tablelog.ErrNoTable)
		require.ErrorIs(t, e.log("a").Create(ctx, meta(), start), tablelog.ErrTableExists)
		require.Nil(t, e.cat.Metadata())
	})
	t.Run("Delta commit", func(t *testing.T) {
		// Something that ignores the catalog wrote to the Delta log.
		e := newEnv(t)
		e.create()
		data, err := deltalog.Encode(deltalog.NewCommitInfo("store-id", "x", "WRITE", start), nil)
		require.NoError(t, err)
		require.NoError(t, e.store.PutIfAbsent(ctx, deltalog.CommitName(1), data))
		require.ErrorIs(t, load(e), tablelog.ErrForeign)
	})
	t.Run("Delta commit first", func(t *testing.T) {
		// The same, right before a pod publishes a commit the catalog took.
		e := newEnv(t)
		l := e.create()
		data, err := deltalog.Encode(deltalog.NewCommitInfo("store-id", "x", "WRITE", start), nil)
		require.NoError(t, err)
		e.cat.After = func(op string) error {
			if op != "commit" {
				return nil
			}
			return e.store.PutIfAbsent(ctx, deltalog.CommitName(1), data)
		}
		_, err = commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err, "the catalog has it")
		e.cat.After = nil
		require.ErrorIs(t, errors.Join(e.warns...), tablelog.ErrForeign)
		require.ErrorIs(t, load(e), tablelog.ErrForeign)
		_, err = commit(l, 2, add("tbl=users/bucket=0/b.parquet"))
		require.ErrorIs(t, err, tablelog.ErrForeign)
	})
	t.Run("Iceberg snapshot", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		_, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
		m := e.cat.Metadata()
		parent := m.CurrentSnapshot()
		_, _, err = e.cat.CommitTable(ctx, ident, nil, []table.Update{
			table.NewAddSnapshotUpdate(&table.Snapshot{
				SnapshotID: 42, ParentSnapshotID: &parent.SnapshotID, SequenceNumber: m.LastSequenceNumber() + 1,
				TimestampMs: m.LastUpdatedMillis(), ManifestList: parent.ManifestList,
				Summary: &table.Summary{Operation: table.OpOverwrite, Properties: iceberg.Properties{}},
			}),
			table.NewSetSnapshotRefUpdate(table.MainBranch, 42, table.BranchRef, 0, 0, 0),
		})
		require.NoError(t, err)
		require.ErrorIs(t, load(e), tablelog.ErrForeign)
		_, err = commit(l, 2)
		require.ErrorIs(t, err, tablelog.ErrForeign)
		_, _, err = l.Read(ctx, 2)
		require.ErrorIs(t, err, tablelog.ErrForeign)
	})
	t.Run("rollback", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		for v := int64(1); v <= 2; v++ {
			_, err := commit(l, v, add(fmt.Sprintf("tbl=users/bucket=0/%d.parquet", v)))
			require.NoError(t, err)
		}
		m := e.cat.Metadata()
		_, _, err := e.cat.CommitTable(ctx, ident, nil, []table.Update{
			table.NewSetSnapshotRefUpdate(table.MainBranch, *m.CurrentSnapshot().ParentSnapshotID, table.BranchRef, 0, 0, 0),
		})
		require.NoError(t, err)
		_, _, err = l.Read(ctx, 3)
		require.Error(t, err)
		require.NotErrorIs(t, err, objstore.ErrNotFound, "must not look like the end of the log")
		_, err = commit(l, 3)
		require.Error(t, err)
		require.ErrorIs(t, load(e), tablelog.ErrForeign, "the Delta log has a version the catalog does not")
	})
	t.Run("other table", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		_, err := l.Commit(ctx, 1, "other", "WRITE", start, nil)
		require.ErrorIs(t, err, tablelog.ErrForeign)
		_, err = commit(l, 1, tablelog.Action{MetaData: &tablelog.MetaData{ID: "store-id"}})
		require.ErrorIs(t, err, tablelog.ErrUnsupported)
	})
	t.Run("replaced", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		other := newEnv(t)
		other.create()
		require.NoError(t, e.cat.SetMetadata(other.cat.Metadata()))
		_, _, err := l.Read(ctx, 1)
		require.ErrorIs(t, err, tablelog.ErrCorrupt)
	})
	t.Run("properties", func(t *testing.T) {
		for prop, want := range map[string]error{"goalert.store-id": tablelog.ErrForeign, "goalert.create-commit-id": tablelog.ErrForeign, "goalert.format": tablelog.ErrUnsupported} {
			e := newEnv(t)
			e.create()
			_, _, err := e.cat.CommitTable(ctx, ident, nil, []table.Update{table.NewRemovePropertiesUpdate([]string{prop})})
			require.NoError(t, err)
			require.ErrorIs(t, load(e), want, prop)
		}
	})
	t.Run("newer format", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		_, err := commit(l, 1)
		require.NoError(t, err)
		m := e.cat.Metadata()
		s := *m.CurrentSnapshot()
		props := maps.Clone(s.Summary.Properties)
		props["goalert.version"], props["goalert.format"] = "2", "99"
		_, _, err = e.cat.CommitTable(ctx, ident, nil, []table.Update{
			table.NewAddSnapshotUpdate(&table.Snapshot{SnapshotID: 42, ParentSnapshotID: &s.SnapshotID, SequenceNumber: 2, TimestampMs: m.LastUpdatedMillis(), ManifestList: s.ManifestList, Summary: &table.Summary{Operation: table.OpAppend, Properties: props}}),
			table.NewSetSnapshotRefUpdate(table.MainBranch, 42, table.BranchRef, 0, 0, 0),
		})
		require.NoError(t, err)
		require.ErrorIs(t, load(e), tablelog.ErrUnsupported)
	})
	t.Run("other files", func(t *testing.T) {
		// Iceberg's files are not GoAlert's: the manifests were changed.
		e := newEnv(t)
		l := e.create()
		_, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
		other := newEnv(t)
		lo := other.create()
		_, err = commit(lo, 1, add("tbl=users/bucket=0/z.parquet"))
		require.NoError(t, err)
		list := func(e *env) string {
			return strings.TrimPrefix(e.cat.Metadata().CurrentSnapshot().ManifestList, location+"/")
		}
		data, err := other.store.Get(ctx, list(other))
		require.NoError(t, err)
		require.NoError(t, e.store.Put(ctx, list(e), data))
		names, err := other.store.List(ctx, "metadata/", "")
		require.NoError(t, err)
		for _, name := range names {
			data, err := other.store.Get(ctx, name)
			require.NoError(t, err)
			require.NoError(t, e.store.Put(ctx, name, data))
		}
		require.ErrorIs(t, load(e), tablelog.ErrCorrupt)
	})
	t.Run("missing manifest", func(t *testing.T) {
		e := newEnv(t)
		l := e.create()
		_, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
		require.NoError(t, e.store.Delete(ctx, strings.TrimPrefix(e.cat.Metadata().CurrentSnapshot().ManifestList, location+"/")))
		require.ErrorIs(t, load(e), tablelog.ErrCorrupt)
	})
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	lost := errors.New("timeout")
	t.Run("reply lost", func(t *testing.T) {
		e := newEnv(t)
		e.cat.After = func(op string) error {
			if op == "create" {
				return lost
			}
			return nil
		}
		require.NoError(t, e.log("a").Create(ctx, meta(), start))
		e.cat.After = nil
		assert.EqualValues(t, 0, e.check().Version)
	})
	t.Run("request lost", func(t *testing.T) {
		e := newEnv(t)
		e.cat.Before = func(op string) error { return lost }
		require.ErrorIs(t, e.log("a").Create(ctx, meta(), start), objstore.ErrUnknown)
		e.cat.Before = nil
		e.create()
	})
	t.Run("denied", func(t *testing.T) {
		e := newEnv(t)
		e.cat.Before = func(op string) error { return objstore.Denied(errors.New("no")) }
		err := e.log("a").Create(ctx, meta(), start)
		require.ErrorIs(t, err, objstore.ErrDenied)
		require.NotErrorIs(t, err, objstore.ErrUnknown)
	})
	t.Run("race", func(t *testing.T) {
		e := newEnv(t)
		errs := make([]error, 6)
		var wg sync.WaitGroup
		for i := range errs {
			l := e.log(fmt.Sprint(i))
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := meta()
				m.ID = fmt.Sprintf("store-%d", i)
				errs[i] = l.Create(ctx, m, start)
			}()
		}
		wg.Wait()
		won := 0
		for _, err := range errs {
			if err == nil {
				won++
			} else {
				require.ErrorIs(t, err, tablelog.ErrTableExists)
			}
		}
		assert.Equal(t, 1, won)
		e.check()
	})
	t.Run("crash before publishing", func(t *testing.T) {
		e := newEnv(t)
		e.store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(0) {
				return errors.New("crash")
			}
			return nil
		}
		require.NoError(t, e.log("a").Create(ctx, meta(), start))
		e.store.BeforeWrite = nil
		assert.EqualValues(t, 0, e.check().Version)
	})
	t.Run("other location", func(t *testing.T) {
		// The catalog's idea of the location is not where the files are (F49).
		e := newEnv(t)
		e.create()
		c := e.config()
		c.Location = "gs://other/place"
		l, err := iceberglog.New(c, "b")
		require.NoError(t, err)
		_, err = l.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrUnsupported)
	})
	t.Run("no table", func(t *testing.T) {
		// Nothing but Create makes a table (F27).
		e := newEnv(t)
		l := e.log("a")
		_, err := l.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrNoTable)
		_, err = commit(l, 1)
		require.ErrorIs(t, err, tablelog.ErrNoTable)
		require.Nil(t, e.cat.Metadata())
	})
	t.Run("schema", func(t *testing.T) {
		e := newEnv(t)
		m := meta()
		m.SchemaString = `{"type":"struct","fields":[{"name":"x","type":"interval","nullable":true}]}`
		require.ErrorIs(t, e.log("a").Create(ctx, m, start), tablelog.ErrUnsupported)
		require.Nil(t, e.cat.Metadata())
	})
}

// TestBounded checks that nothing the table's readers and writers handle grows
// with its history (F16, F46).
func TestBounded(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	c := e.config()
	c.Snapshots = 10
	l, err := iceberglog.New(c, "a")
	require.NoError(t, err)
	require.NoError(t, l.Create(ctx, meta(), start))
	const buckets, commits = 200, 300
	name := func(v int64) string { return fmt.Sprintf("tbl=users/bucket=%d/%d.parquet", v%buckets, v) }
	var snap *tablelog.Snapshot
	var metadata []int
	puts := 0
	for v := int64(1); v <= commits; v++ {
		actions := []tablelog.Action{add(name(v))}
		if v > buckets {
			actions = append(actions, remove(name(v-buckets)))
		}
		before := e.store.Ops()["putIfAbsent"]
		gets := e.store.Ops()["get"]
		info, err := commit(l, v, actions...)
		require.NoError(t, err)
		puts = max(puts, e.store.Ops()["putIfAbsent"]-before)
		assert.LessOrEqual(t, e.store.Ops()["get"]-gets, 1, "a pod reads nothing again that it wrote")
		if v == 1 {
			snap, err = l.Load(ctx)
			require.NoError(t, err)
		} else {
			require.NoError(t, snap.Apply(v, info, actions))
		}
		if v%20 == 0 {
			require.NoError(t, l.WriteCheckpoint(ctx, snap, start, time.Hour))
			m := e.cat.Metadata()
			require.LessOrEqual(t, len(m.Snapshots()), 10)
			tbl := icebergtest.Table(m, e.store, location)
			fs, err := tbl.FS(ctx)
			require.NoError(t, err)
			ms, err := m.CurrentSnapshot().Manifests(fs)
			require.NoError(t, err)
			require.LessOrEqual(t, len(ms), 32)
			metadata = append(metadata, len(fmt.Sprint(m)))
		}
	}
	assert.LessOrEqual(t, puts, 4, "the commit, a manifest, the list, and its publication")
	s := e.check()
	assert.Len(t, s.Files, buckets)
	assert.Equal(t, fmt.Sprint(buckets), e.cat.Metadata().CurrentSnapshot().Summary.Properties["total-data-files"])
	assert.Equal(t, fmt.Sprint(3*buckets), e.cat.Metadata().CurrentSnapshot().Summary.Properties["total-records"])

	// Cleaning up leaves what is in use, and no more than that.
	staged, err := e.store.List(ctx, "_delta_log/_staged_commits/", "")
	require.NoError(t, err)
	assert.Len(t, staged, commits+1)
	n, err := l.Cleanup(ctx, start.Add(time.Minute), time.Hour)
	require.NoError(t, err)
	assert.Equal(t, commits, n, "only staged commits are not kept for readers")
	e.check()
	n, err = l.Cleanup(ctx, start.Add(1000*time.Hour), time.Hour)
	require.NoError(t, err)
	assert.Greater(t, n, commits)
	e.check()
	names, err := e.store.List(ctx, "metadata/", "")
	require.NoError(t, err)
	assert.Less(t, len(names), 10+10*4+32)
	// Every snapshot that is left can still be read.
	for _, snapshot := range e.cat.Metadata().Snapshots() {
		tasks, err := icebergtest.Table(e.cat.Metadata(), e.store, location).Scan(table.WithSnapshotID(snapshot.SnapshotID)).PlanFiles(ctx)
		require.NoError(t, err)
		assert.Len(t, tasks, buckets)
	}
	// However short the retention, what a commit in progress may have staged is
	// left (F6).
	require.NoError(t, e.store.Put(ctx, "metadata/goalert-1795608000000-00000000-0000-0000-0000-000000000000-m01.avro", []byte("x")))
	staged1 := time.UnixMilli(1795608000000)
	_, err = l.Cleanup(ctx, staged1.Add(59*time.Minute), 0)
	require.NoError(t, err)
	names, err = e.store.List(ctx, "metadata/goalert-1795608000000-0000", "")
	require.NoError(t, err)
	assert.Len(t, names, 1)
	_, err = l.Cleanup(ctx, staged1.Add(61*time.Minute), 0)
	require.NoError(t, err)
	names, err = e.store.List(ctx, "metadata/goalert-1795608000000-0000", "")
	require.NoError(t, err)
	assert.Empty(t, names)

	_, err = commit(l, commits+1, add(name(commits+1)), remove(name(commits+1-buckets)))
	require.NoError(t, err)
	assert.Len(t, e.check().Files, buckets)
	assert.Empty(t, e.warns)
}

// TestActions checks the manifests of unusual commits.
func TestActions(t *testing.T) {
	e := newEnv(t)
	l := e.create()
	const a, b, c = "tbl=users/bucket=0/a.parquet", "tbl=users/bucket=0/b.parquet", "tbl=users/bucket=1/c.parquet"
	files := func(want ...string) {
		t.Helper()
		assert.ElementsMatch(t, want, e.check().FilePaths())
	}
	_, err := commit(l, 1) // nothing
	require.NoError(t, err)
	files()
	_, err = commit(l, 2, tablelog.Action{Txn: &tablelog.Txn{AppID: "x", Version: 1}})
	require.NoError(t, err)
	files()
	_, err = commit(l, 3, add(a), add(b), add(c))
	require.NoError(t, err)
	files(a, b, c)
	_, err = commit(l, 4, add(a)) // again
	require.NoError(t, err)
	files(a, b, c)
	_, err = commit(l, 5, remove(a), add("tbl=users/bucket=0/d.parquet"), remove("tbl=users/bucket=0/d.parquet"))
	require.NoError(t, err)
	files(b, c)
	_, err = commit(l, 6, remove(b), remove(c))
	require.NoError(t, err)
	files()
	_, err = commit(l, 7)
	require.NoError(t, err)
	files()
	assert.Equal(t, "0", e.cat.Metadata().CurrentSnapshot().Summary.Properties["total-data-files"])

	_, err = commit(l, 8, remove(a))
	require.ErrorIs(t, err, tablelog.ErrCorrupt, "Iceberg does not have it")
	for _, bad := range []string{"../x.parquet", "a//b.parquet", "a/../b.parquet", "/a.parquet", "a%20b.parquet", "a?b"} {
		_, err = commit(l, 8, add(bad))
		require.Error(t, err, bad)
	}
	bad := add(a)
	bad.Add.Stats = "{"
	_, err = commit(l, 8, bad)
	require.Error(t, err)
	_, err = commit(l, 10)
	require.Error(t, err, "a version too far")
	_, err = commit(l, 0)
	require.Error(t, err)
	files()
}
