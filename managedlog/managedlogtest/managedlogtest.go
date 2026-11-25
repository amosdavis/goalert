// Package managedlogtest is the conformance suite every managedlog.Arbiter must
// pass (docs/filestore-failure-modes.md, F14, F43).
package managedlogtest

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
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/managedlog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

// Env is where one table of a test is.
type Env struct {
	Store *objstore.Mem

	// Connect returns another pod's connection to the arbiter.
	Connect func(t *testing.T, store objstore.Store) managedlog.Arbiter
}

var start = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// faults fails calls to an arbiter: "create", "ask" and "decision". A hook that
// returns an error before the call keeps it from being done; one after it loses
// the reply.
type faults struct {
	mu            sync.Mutex
	before, after func(op string) error
}

func (f *faults) set(before, after func(op string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.before, f.after = before, after
}

func (f *faults) do(op string, fn func() error) error {
	f.mu.Lock()
	before, after := f.before, f.after
	f.mu.Unlock()
	if before != nil {
		if err := before(op); err != nil {
			return err
		}
	}
	if err := fn(); err != nil {
		return err
	}
	if after != nil {
		return after(op)
	}
	return nil
}

type faulty struct {
	managedlog.Arbiter
	f *faults
}

func (a *faulty) Prepare(ctx context.Context, p managedlog.Proposal) (managedlog.Ask, error) {
	ask, err := a.Arbiter.Prepare(ctx, p)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return a.f.do("ask", func() error { return ask(ctx) })
	}, nil
}

func (a *faulty) Decision(ctx context.Context, storeID string, version int64) (d managedlog.Decision, err error) {
	err = a.f.do("decision", func() (err error) {
		d, err = a.Arbiter.Decision(ctx, storeID, version)
		return err
	})
	if err != nil {
		return managedlog.Decision{}, err
	}
	return d, nil
}

func (a *faulty) Checkpointed(ctx context.Context) error {
	if m, ok := a.Arbiter.(managedlog.Maintainer); ok {
		return m.Checkpointed(ctx)
	}
	return nil
}

func (a *faulty) Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error) {
	if m, ok := a.Arbiter.(managedlog.Maintainer); ok {
		return m.Cleanup(ctx, now, retention)
	}
	return 0, nil
}

type faultyCreator struct{ *faulty }

func (a *faultyCreator) Create(ctx context.Context, meta tablelog.MetaData, info tablelog.CommitInfo) error {
	return a.f.do("create", func() error { return a.Arbiter.(managedlog.Creator).Create(ctx, meta, info) })
}

type env struct {
	Env
	t *testing.T
	f *faults

	mu    sync.Mutex
	warns []error
}

func (e *env) log(writer string) *managedlog.Log {
	e.t.Helper()
	var a managedlog.Arbiter = &faulty{Arbiter: e.Connect(e.t, e.Store), f: e.f}
	if _, ok := a.(*faulty).Arbiter.(managedlog.Creator); ok {
		a = &faultyCreator{a.(*faulty)}
	}
	l, err := managedlog.New(managedlog.Config{Arbiter: a, Store: e.Store, Warn: func(err error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.warns = append(e.warns, err)
	}}, writer)
	require.NoError(e.t, err)
	l.SetRetry(4, time.Millisecond)
	return l
}

func (e *env) manager() string { return e.Connect(e.t, e.Store).Manager() }

func meta(id string) tablelog.MetaData {
	return tablelog.MetaData{ID: id, Name: "goalert", SchemaString: `{"type":"struct","fields":[{"name":"users","type":"string","nullable":true,"metadata":{}}]}`}
}

func (e *env) create() *managedlog.Log {
	e.t.Helper()
	l := e.log("admin")
	require.NoError(e.t, l.Create(context.Background(), meta("store-id"), start))
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

// check opens the table afresh, and checks that a Delta reader agrees with it.
func (e *env) check() *tablelog.Snapshot {
	e.t.Helper()
	s, err := e.log("checker").Load(context.Background())
	require.NoError(e.t, err)
	d := deltalog.New(e.Store, "reader")
	d.Manager = e.manager()
	got, err := d.Load(context.Background())
	require.NoError(e.t, err)
	assert.Equal(e.t, s.Version, got.Version)
	assert.Equal(e.t, s.CommitID, got.CommitID)
	assert.Equal(e.t, s.FilePaths(), got.FilePaths(), "a Delta reader sees the same files")
	return s
}

// Run runs the suite. newEnv returns new, empty storage each time it is called.
func Run(t *testing.T, newEnv func(t *testing.T) Env) {
	ctx := context.Background()
	setup := func(t *testing.T) *env {
		t.Helper()
		return &env{Env: newEnv(t), t: t, f: &faults{}}
	}

	t.Run("Log", func(t *testing.T) {
		var mu sync.Mutex
		envs := map[objstore.Store]*env{}
		tablelogtest.Run(t,
			func(t *testing.T) objstore.Store {
				e := setup(t)
				mu.Lock()
				defer mu.Unlock()
				envs[e.Store] = e
				return e.Store
			},
			func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
				mu.Lock()
				defer mu.Unlock()
				return envs[store].log(writerID)
			},
		)
	})

	t.Run("Backend", func(t *testing.T) {
		backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
			e := setup(t)
			t.Cleanup(func() {
				if !t.Failed() {
					e.check()
				}
			})
			return func(writer string) *storagebackend.Backend {
				return &storagebackend.Backend{Store: e.Store, Log: e.log(writer)}
			}
		})
	})

	t.Run("Outcomes", func(t *testing.T) { outcomes(t, setup) })
	t.Run("CrashBeforePublish", func(t *testing.T) { crashBeforePublish(t, setup) })
	t.Run("CrashAtEveryWrite", func(t *testing.T) { crashAtEveryWrite(t, setup) })
	t.Run("OtherWriters", func(t *testing.T) { otherWriters(t, setup) })
	t.Run("Create", func(t *testing.T) { create(t, setup) })

	t.Run("LateCommit", func(t *testing.T) {
		// A request that is done long after it was given up on is refused, since
		// the version was taken meanwhile. Nobody else's commit is touched.
		e := setup(t)
		e.create()
		arbiter := e.Connect(t, e.Store)
		var late []managedlog.Ask
		for i := range 3 {
			info := deltalog.NewCommitInfo("store-id", "slow", "WRITE", start)
			s, err := e.log("x").Load(ctx)
			require.NoError(t, err)
			ask, err := arbiter.Prepare(ctx, managedlog.Proposal{
				Version: 1, Info: info, Base: s.CommitID, Actions: []tablelog.Action{add(fmt.Sprintf("tbl=users/bucket=0/late-%d.parquet", i))},
				Put: e.Store.PutIfAbsent,
			})
			require.NoError(t, err)
			late = append(late, ask)
		}
		for v := int64(1); v <= 3; v++ {
			_, err := commit(e.log("a"), v, add(fmt.Sprintf("tbl=users/bucket=0/%d.parquet", v)))
			require.NoError(t, err)
			require.ErrorIs(t, late[v-1](ctx), tablelog.ErrConflict)
			require.ErrorIs(t, late[v-1](ctx), tablelog.ErrConflict, "however often")
		}
		assert.Len(t, e.check().Files, 3)
	})

	t.Run("RepeatedAsk", func(t *testing.T) {
		// A request that is repeated after it was done is refused or answered
		// the same, and done once.
		e := setup(t)
		e.create()
		s, err := e.log("x").Load(ctx)
		require.NoError(t, err)
		info := deltalog.NewCommitInfo("store-id", "a", "WRITE", start)
		actions := []tablelog.Action{add("tbl=users/bucket=0/a.parquet")}
		data, err := deltalog.Encode(info, actions)
		require.NoError(t, err)
		require.NoError(t, e.Store.PutIfAbsent(ctx, fmt.Sprintf("_delta_log/_staged_commits/%020d.%s.json", 1, info.GoAlertCommitID), data))
		arbiter := e.Connect(t, e.Store)
		ask, err := arbiter.Prepare(ctx, managedlog.Proposal{Version: 1, Info: info, Base: s.CommitID, Actions: actions, Put: e.Store.PutIfAbsent})
		require.NoError(t, err)
		require.NoError(t, ask(ctx))
		for range 3 {
			if err := ask(ctx); err != nil {
				require.ErrorIs(t, err, tablelog.ErrConflict)
			}
		}
		// Found by another pod, through its own connection.
		d, err := e.Connect(t, e.Store).Decision(ctx, "store-id", 1)
		require.NoError(t, err)
		assert.Equal(t, managedlog.Decision{CommitID: info.GoAlertCommitID}, d)
		got := e.check()
		assert.EqualValues(t, 1, got.Version)
		assert.Equal(t, info.GoAlertCommitID, got.CommitID)
	})

	t.Run("Race", func(t *testing.T) {
		// Of many proposals for a version one is decided, through any number of
		// connections (F4).
		e := setup(t)
		e.create()
		for v := int64(1); v <= 4; v++ {
			s, err := e.log("x").Load(ctx)
			require.NoError(t, err)
			const n = 8
			ids := make([]string, n)
			errs := make([]error, n)
			asks := make([]managedlog.Ask, n)
			for i := range n {
				info := deltalog.NewCommitInfo("store-id", fmt.Sprint(i), "WRITE", start)
				actions := []tablelog.Action{add(fmt.Sprintf("tbl=users/bucket=0/%d-%d.parquet", v, i))}
				data, err := deltalog.Encode(info, actions)
				require.NoError(t, err)
				require.NoError(t, e.Store.PutIfAbsent(ctx, fmt.Sprintf("_delta_log/_staged_commits/%020d.%s.json", v, info.GoAlertCommitID), data))
				asks[i], err = e.Connect(t, e.Store).Prepare(ctx, managedlog.Proposal{Version: v, Info: info, Base: s.CommitID, Actions: actions, Put: e.Store.PutIfAbsent})
				require.NoError(t, err)
				ids[i] = info.GoAlertCommitID
			}
			var wg sync.WaitGroup
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = asks[i](ctx)
				}()
			}
			wg.Wait()
			winner := ""
			for i, err := range errs {
				if err == nil {
					require.Empty(t, winner, "two proposals hold version %d", v)
					winner = ids[i]
					continue
				}
				require.ErrorIs(t, err, tablelog.ErrConflict)
			}
			require.NotEmpty(t, winner)
			got := e.check()
			assert.Equal(t, v, got.Version)
			assert.Equal(t, winner, got.CommitID)
		}
	})

	t.Run("Decisions", func(t *testing.T) {
		e := setup(t)
		_, err := e.Connect(t, e.Store).Decision(ctx, "", 0)
		require.ErrorIs(t, err, tablelog.ErrNoTable)
		l := e.create()
		for v := int64(1); v <= 5; v++ {
			a := e.Connect(t, e.Store)
			d, err := a.Decision(ctx, "store-id", v)
			require.NoError(t, err)
			assert.Equal(t, managedlog.Decision{}, d, "version %d is free", v)
			info, err := commit(l, v, add(fmt.Sprintf("tbl=users/bucket=0/%d.parquet", v)))
			require.NoError(t, err)
			d, err = a.Decision(ctx, "store-id", v)
			require.NoError(t, err)
			assert.Equal(t, managedlog.Decision{CommitID: info.GoAlertCommitID}, d)
			if v > 1 {
				d, err = a.Decision(ctx, "store-id", v-1)
				require.NoError(t, err)
				assert.True(t, d.Later || d.CommitID != "", "an older version is decided, whether or not for whom is known")
			}
			d, err = a.Decision(ctx, "store-id", v+7)
			require.NoError(t, err)
			assert.Equal(t, managedlog.Decision{}, d)
		}
	})

	t.Run("Staged", func(t *testing.T) {
		// Staged commits that nobody needs are deleted, and no others (F16).
		e := setup(t)
		l := e.create()
		for v := int64(1); v <= 4; v++ {
			_, err := commit(l, v, add(fmt.Sprintf("tbl=users/bucket=0/%d.parquet", v)))
			require.NoError(t, err)
		}
		// One that lost, one of a later version that may still be decided, and
		// one that is decided and not published.
		e.Store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(5) {
				return errors.New("crash")
			}
			return nil
		}
		_, err := commit(l, 5, add("tbl=users/bucket=0/5.parquet"))
		require.NoError(t, err)
		e.Store.BeforeWrite = nil
		require.NoError(t, e.Store.Put(ctx, "_delta_log/_staged_commits/00000000000000000002.lost.json", []byte("x")))
		require.NoError(t, e.Store.Put(ctx, "_delta_log/_staged_commits/00000000000000000006.future.json", []byte("x")))
		_, err = e.log("b").Cleanup(ctx, start, time.Hour)
		require.NoError(t, err)
		names, err := e.Store.List(ctx, "_delta_log/_staged_commits/", "")
		require.NoError(t, err)
		var versions []string
		for _, n := range names {
			versions = append(versions, strings.TrimLeft(strings.Split(strings.TrimPrefix(n, "_delta_log/_staged_commits/"), ".")[0], "0"))
		}
		assert.Equal(t, []string{"4", "5", "6"}, versions)
		assert.EqualValues(t, 5, e.check().Version)
	})
}

// outcomes checks how Commit reports what the arbiter does with a request, and
// what the table holds afterwards (F1, F4, F32).
func outcomes(t *testing.T, setup func(t *testing.T) *env) {
	lost := errors.New("connection reset")
	denied := objstore.Denied(errors.New("token expired"))
	rejected := fmt.Errorf("%w: bad request", managedlog.ErrRejected)
	busy := fmt.Errorf("%w: quota", managedlog.ErrBusy)
	conflict := fmt.Errorf("%w: busy", tablelog.ErrConflict)
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
		"rejected":                 {attempts: []attempt{{err: rejected}}, want: managedlog.ErrRejected, tries: 1},
		"lost, then denied":        {attempts: []attempt{{err: lost}, {err: denied}, {err: denied}, {err: denied}}, want: objstore.ErrUnknown, tries: 4},
		"busy":                     {attempts: []attempt{{err: busy}, {err: busy}, {done: true}}, committed: true, tries: 3},
		"only busy":                {attempts: []attempt{{err: busy}, {err: busy}, {err: busy}, {err: busy}}, want: managedlog.ErrBusy, tries: 4},
		"spurious conflict":        {attempts: []attempt{{err: conflict}, {err: conflict}, {done: true}}, committed: true, tries: 3},
		"only conflicts":           {attempts: []attempt{{err: conflict}, {err: conflict}, {err: conflict}, {err: conflict}}, want: tablelog.ErrConflict, tries: 4},
		"conflict, then lost":      {attempts: []attempt{{err: conflict}, {done: true, err: lost}}, committed: true, tries: 2},
		"answered as a conflict":   {attempts: []attempt{{done: true, err: conflict}}, committed: true, tries: 1},
		"answered as a bad denial": {attempts: []attempt{{done: true, err: denied}}, committed: true, tries: 1},
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			e.create()
			l := e.log("a")
			tries := 0
			now := func() attempt { return tc.attempts[min(tries, len(tc.attempts))-1] }
			e.f.set(func(op string) error {
				if op != "ask" {
					return nil
				}
				tries++
				if !now().done {
					return now().err
				}
				return nil
			}, func(op string) error {
				if op != "ask" {
					return nil
				}
				return now().err
			})
			info, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
			e.f.set(nil, nil)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
				for _, other := range []error{objstore.ErrUnknown, objstore.ErrDenied, tablelog.ErrConflict, managedlog.ErrRejected, managedlog.ErrBusy} {
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

	t.Run("no answer at all", func(t *testing.T) {
		// Neither the request nor the question after it is answered.
		e := setup(t)
		e.create()
		l := e.log("a")
		_, err := l.Load(context.Background())
		require.NoError(t, err)
		e.f.set(func(string) error { return lost }, nil)
		_, err = commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.ErrorIs(t, err, objstore.ErrUnknown)
		_, _, err = l.Read(context.Background(), 1)
		require.ErrorIs(t, err, lost)
		require.NotErrorIs(t, err, objstore.ErrNotFound, "a question that was not answered says nothing about the version")
		e.f.set(nil, nil)
		assert.EqualValues(t, 0, e.check().Version)
	})
}

// crashBeforePublish checks that a commit whose writer crashed right after it
// was decided is found, by each way of finding it (F1, F43).
func crashBeforePublish(t *testing.T, newEnv func(t *testing.T) *env) {
	ctx := context.Background()
	setup := func(t *testing.T) (*env, tablelog.CommitInfo) {
		e := newEnv(t)
		e.create()
		e.Store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(1) {
				return errors.New("crash")
			}
			return nil
		}
		info, err := commit(e.log("a"), 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err, "it has happened")
		require.NotEmpty(t, e.warns)
		e.Store.BeforeWrite = nil
		_, err = e.Store.Get(ctx, deltalog.CommitName(1))
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
		_, err := commit(e.log("b"), 1, add("tbl=users/bucket=0/b.parquet"))
		require.ErrorIs(t, err, tablelog.ErrConflict)
		_, err = commit(e.log("b"), 2, add("tbl=users/bucket=0/b.parquet"))
		require.NoError(t, err)
		_, err = e.Store.Get(ctx, deltalog.CommitName(1))
		require.NoError(t, err, "the Delta log has no gap")
		assert.Len(t, e.check().Files, 2)
	})
	t.Run("SamePod", func(t *testing.T) {
		// The writer did not crash, and publishes before its next commit.
		e := newEnv(t)
		l := e.create()
		e.Store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(1) {
				return errors.New("no")
			}
			return nil
		}
		_, err := commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
		_, err = commit(l, 2, add("tbl=users/bucket=0/b.parquet"))
		require.Error(t, err, "no commit is made until the one before it is published")
		e.Store.BeforeWrite = nil
		_, err = commit(l, 2, add("tbl=users/bucket=0/b.parquet"))
		require.NoError(t, err)
		assert.Len(t, e.check().Files, 2)
	})
	t.Run("OtherPod", func(t *testing.T) {
		e, _ := setup(t)
		e.Store.BeforeWrite = func(op, name string) error {
			if strings.HasPrefix(name, "_delta_log/0000") {
				return errors.New("no")
			}
			return nil
		}
		asked := false
		e.f.set(func(op string) error { asked = asked || op == "ask"; return nil }, nil)
		_, err := commit(e.log("b"), 2)
		require.Error(t, err)
		assert.False(t, asked, "no decision is asked for until the one before is published")
		e.f.set(nil, nil)
		e.Store.BeforeWrite = nil
		e.check()
	})
	t.Run("Cleanup", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.log("b").Cleanup(ctx, start.Add(1000*time.Hour), time.Hour)
		require.NoError(t, err)
		assert.EqualValues(t, 1, e.check().Version)
	})
	t.Run("Lost", func(t *testing.T) {
		// What was staged is gone, so the log can't go on, and says so (F6).
		e, info := setup(t)
		require.NoError(t, e.Store.Delete(ctx, fmt.Sprintf("_delta_log/_staged_commits/%020d.%s.json", 1, info.GoAlertCommitID)))
		_, err := e.log("b").Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrCorrupt)
		_, err = commit(e.log("b"), 1)
		require.Error(t, err)
	})
}

// crashAtEveryWrite fails each write to the object store and each request to
// the arbiter in turn, with and without doing it. Whatever was acknowledged must
// be there, and whatever failed must not be (F1, F6, F32, F43).
func crashAtEveryWrite(t *testing.T, newEnv func(t *testing.T) *env) {
	ctx := context.Background()
	const commits = 4
	for _, applied := range []bool{false, true} {
		for step := 1; ; step++ {
			e := newEnv(t)
			e.create()
			var n atomic.Int64
			var dead, first atomic.Bool
			crash := func() bool {
				// From the crash on, nothing is written.
				if !dead.Load() && n.Add(1) == int64(step) {
					dead.Store(true)
				}
				return dead.Load()
			}
			e.Store.BeforeWrite = func(op, name string) error {
				if crash() {
					if applied && first.CompareAndSwap(false, true) {
						return objstore.ErrMemAppliedThenFailed
					}
					return objstore.Unknown(errors.New("crashed"))
				}
				return nil
			}
			e.f.set(func(op string) error {
				if op == "ask" && crash() && (!applied || first.Load()) {
					return errors.New("crashed")
				}
				return nil
			}, func(op string) error {
				if op == "ask" && dead.Load() && first.CompareAndSwap(false, true) {
					return errors.New("crashed")
				}
				return nil
			})
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
			e.Store.BeforeWrite = nil
			e.f.set(nil, nil)
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
				assert.GreaterOrEqual(t, step, commits*3, "every write was covered")
				break
			}
		}
	}
}

// otherWriters checks that what did not come from a commit that was decided is
// refused (F11, F43).
func otherWriters(t *testing.T, setup func(t *testing.T) *env) {
	ctx := context.Background()
	load := func(e *env) error { _, err := e.log("b").Load(ctx); return err }

	t.Run("Delta alone", func(t *testing.T) {
		e := setup(t)
		e.create()
		d := deltalog.New(e.Store, "d")
		_, err := d.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrManaged, "a pod configured without the arbiter must not write")
		require.ErrorIs(t, d.Create(ctx, meta("store-id"), start), tablelog.ErrTableExists)
		d.Manager = e.manager()
		_, err = commit(d, 1)
		require.ErrorIs(t, err, tablelog.ErrManaged)
		d.Manager = e.manager() + "-other"
		_, err = d.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrManaged, "nor one with another arbiter")
	})
	t.Run("Delta table", func(t *testing.T) {
		// A table made without the arbiter can't be opened with it.
		e := setup(t)
		require.NoError(t, deltalog.New(e.Store, "d").Create(ctx, meta("store-id"), start))
		require.ErrorIs(t, load(e), tablelog.ErrManaged)
		require.ErrorIs(t, e.log("a").Create(ctx, meta("other-id"), start), tablelog.ErrTableExists)
		_, err := commit(e.log("a"), 1)
		require.ErrorIs(t, err, tablelog.ErrManaged)
	})
	t.Run("Delta commit", func(t *testing.T) {
		// Something that ignores the arbiter wrote to the Delta log.
		e := setup(t)
		e.create()
		data, err := deltalog.Encode(deltalog.NewCommitInfo("store-id", "x", "WRITE", start), nil)
		require.NoError(t, err)
		require.NoError(t, e.Store.PutIfAbsent(ctx, deltalog.CommitName(1), data))
		require.ErrorIs(t, load(e), tablelog.ErrForeign)
		_, err = commit(e.log("b"), 2)
		require.Error(t, err)
		d, err := e.Connect(t, e.Store).Decision(ctx, "store-id", 2)
		require.NoError(t, err)
		assert.Equal(t, managedlog.Decision{}, d, "nothing was decided on top of it")
	})
	t.Run("Delta commit first", func(t *testing.T) {
		// The same, right before a pod publishes a commit that was decided.
		e := setup(t)
		l := e.create()
		data, err := deltalog.Encode(deltalog.NewCommitInfo("store-id", "x", "WRITE", start), nil)
		require.NoError(t, err)
		e.f.set(nil, func(op string) error {
			if op != "ask" {
				return nil
			}
			return e.Store.PutIfAbsent(ctx, deltalog.CommitName(1), data)
		})
		_, err = commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err, "it was decided")
		e.f.set(nil, nil)
		require.ErrorIs(t, errors.Join(e.warns...), tablelog.ErrForeign)
		require.ErrorIs(t, load(e), tablelog.ErrForeign)
		_, err = commit(l, 2, add("tbl=users/bucket=0/b.parquet"))
		require.ErrorIs(t, err, tablelog.ErrForeign)
		_, err = commit(e.log("b"), 2, add("tbl=users/bucket=0/b.parquet"))
		require.ErrorIs(t, err, tablelog.ErrForeign)
	})
	t.Run("other table", func(t *testing.T) {
		e := setup(t)
		l := e.create()
		_, err := l.Commit(ctx, 1, "other", "WRITE", start, nil)
		require.ErrorIs(t, err, tablelog.ErrForeign)
		_, err = e.log("b").Commit(ctx, 1, "other", "WRITE", start, nil)
		require.ErrorIs(t, err, tablelog.ErrForeign)
	})
	t.Run("versions", func(t *testing.T) {
		e := setup(t)
		l := e.create()
		_, err := commit(l, 0)
		require.Error(t, err)
		_, err = commit(l, 2)
		require.Error(t, err, "a version too far")
		require.NotErrorIs(t, err, tablelog.ErrConflict)
		_, err = commit(l, 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
		_, err = commit(l, 1)
		require.ErrorIs(t, err, tablelog.ErrConflict)
		_, err = commit(e.log("b"), 1)
		require.ErrorIs(t, err, tablelog.ErrConflict)
	})
}

func create(t *testing.T, setup func(t *testing.T) *env) {
	ctx := context.Background()
	t.Run("race", func(t *testing.T) {
		e := setup(t)
		errs := make([]error, 6)
		var wg sync.WaitGroup
		for i := range errs {
			l := e.log(fmt.Sprint(i))
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = l.Create(ctx, meta(fmt.Sprintf("store-%d", i)), start)
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
	t.Run("again", func(t *testing.T) {
		// A table that was deleted can be made again, as another table, whose
		// versions are its own.
		e := setup(t)
		if _, ok := e.Connect(t, e.Store).(managedlog.Creator); ok {
			t.Skip("what decides creation has to be cleared too")
		}
		l := e.create()
		for v := int64(1); v <= 2; v++ {
			_, err := commit(l, v, add("tbl=users/bucket=0/a.parquet"))
			require.NoError(t, err)
		}
		names, err := e.Store.List(ctx, "", "")
		require.NoError(t, err)
		for _, n := range names {
			require.NoError(t, e.Store.Delete(ctx, n))
		}
		l = e.log("b")
		require.NoError(t, l.Create(ctx, meta("second-id"), start))
		_, err = l.Commit(ctx, 1, "second-id", "WRITE", start, []tablelog.Action{add("tbl=users/bucket=0/b.parquet")})
		require.NoError(t, err)
		assert.Equal(t, []string{"tbl=users/bucket=0/b.parquet"}, e.check().FilePaths())
	})
	t.Run("other manager", func(t *testing.T) {
		e := setup(t)
		m := meta("store-id")
		m.Configuration = map[string]string{deltalog.ManagerKey: "someone"}
		require.ErrorIs(t, e.log("a").Create(ctx, m, start), tablelog.ErrManaged)
		names, err := e.Store.List(ctx, "", "")
		require.NoError(t, err)
		assert.Empty(t, names)
	})
	t.Run("no table", func(t *testing.T) {
		// Nothing but Create makes a table (F27).
		e := setup(t)
		l := e.log("a")
		_, err := l.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrNoTable)
		_, err = commit(l, 1)
		require.Error(t, err)
		_, _, err = l.Read(ctx, 0)
		require.Error(t, err)
		_, err = e.log("b").Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrNoTable)
	})
	t.Run("crash before publishing", func(t *testing.T) {
		e := setup(t)
		if _, ok := e.Connect(t, e.Store).(managedlog.Creator); !ok {
			t.Skip("the object store decides creation")
		}
		e.Store.BeforeWrite = func(op, name string) error {
			if name == deltalog.CommitName(0) {
				return errors.New("crash")
			}
			return nil
		}
		require.NoError(t, e.log("a").Create(ctx, meta("store-id"), start))
		e.Store.BeforeWrite = nil
		assert.EqualValues(t, 0, e.check().Version)
		_, err := commit(e.log("b"), 1, add("tbl=users/bucket=0/a.parquet"))
		require.NoError(t, err)
	})
	t.Run("lost", func(t *testing.T) {
		e := setup(t)
		if _, ok := e.Connect(t, e.Store).(managedlog.Creator); !ok {
			t.Skip("the object store decides creation")
		}
		lost := objstore.Unknown(errors.New("timeout"))
		e.f.set(func(op string) error {
			if op == "create" {
				return lost
			}
			return nil
		}, nil)
		require.ErrorIs(t, e.log("a").Create(ctx, meta("store-id"), start), objstore.ErrUnknown)
		e.f.set(nil, nil)
		e.create()
		e.check()
	})
}
