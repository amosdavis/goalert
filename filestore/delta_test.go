package filestore

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
	"github.com/target/goalert/objstore"
)

type dUser struct {
	ID      string     `parquet:"id"`
	Name    string     `parquet:"name"`
	Email   string     `parquet:"email"`
	Count   int64      `parquet:"count"`
	Created *time.Time `parquet:"created,optional,timestamp(microsecond:utc)"`
}

type dSchema struct {
	s     *Schema
	users *Table[dUser]
	email *Unique[dUser]
}

func newDSchema() dSchema {
	s := NewSchema()
	users := NewTable(s, "users", func(u dUser) string { return u.ID }).Buckets(4)
	email := users.Unique("email", func(u dUser) (string, bool) { return u.Email, u.Email != "" })
	return dSchema{s: s, users: users, email: email}
}

var dStart = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func dOpts(writer string) DeltaOptions {
	return DeltaOptions{WriterID: writer, Clock: frozenClock(dStart), PollInterval: -1, Logf: func(string, ...any) {}}
}

func newDeltaStore(t *testing.T) *objstore.Mem {
	t.Helper()
	store := objstore.NewMem()
	require.NoError(t, CreateDelta(context.Background(), store, newDSchema().s, dOpts("admin")))
	return store
}

func openPod(t *testing.T, store objstore.Store, sch dSchema, opts DeltaOptions) *DeltaDB {
	t.Helper()
	db, err := OpenDelta(context.Background(), store, sch.s, opts)
	require.NoError(t, err)
	db.log.Backoff = time.Millisecond
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func putUser(sch dSchema, id string) func(tx *Tx) error {
	return func(tx *Tx) error { return sch.users.Put(tx, dUser{ID: id, Name: "user " + id}) }
}

func dumpUsers(sch dSchema, r Reader) []dUser {
	var rows []dUser
	sch.users.Ascend(r, func(u dUser) bool { rows = append(rows, u); return true })
	return rows
}

func isCommitFile(name string) bool {
	return strings.HasPrefix(name, "_delta_log/") && strings.HasSuffix(name, ".json")
}

func TestDeltaCreateAndReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	sch := newDSchema()
	_, err := OpenDelta(ctx, store, sch.s, dOpts("a"))
	require.ErrorIs(t, err, deltalog.ErrNoTable, "a pod never creates a table on its own (F27)")
	require.NoError(t, CreateDelta(ctx, store, newDSchema().s, dOpts("admin")))
	require.ErrorIs(t, CreateDelta(ctx, store, newDSchema().s, dOpts("admin")), deltalog.ErrTableExists)

	db := openPod(t, store, sch, dOpts("a"))
	created := dStart.Add(123456789 * time.Nanosecond)
	v, err := db.Update(ctx, func(tx *Tx) error {
		for i := range 10 {
			u := dUser{ID: fmt.Sprintf("u%d", i), Email: fmt.Sprintf("u%d@example.com", i), Created: &created}
			if err := sch.users.Put(tx, u); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, v)
	u, ok := sch.users.Get(db.View(), "u3")
	require.True(t, ok)
	assert.Equal(t, created.Truncate(time.Microsecond), *u.Created, "rows are kept as they decode (F25)")

	v, err = db.Update(ctx, func(tx *Tx) error {
		_, err := sch.users.Delete(tx, "u0")
		return err
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, v)
	v, err = db.Update(ctx, func(tx *Tx) error { return nil })
	require.NoError(t, err)
	assert.EqualValues(t, 2, v, "a transaction that changes nothing commits nothing")

	fresh := openPod(t, store, sch, dOpts("b"))
	assert.Equal(t, dumpUsers(sch, db.View()), dumpUsers(sch, fresh.View()), "a new pod builds the same state (F25)")
	assert.Len(t, dumpUsers(sch, fresh.View()), 9)
	byEmail, ok := sch.email.Get(fresh.View(), "u5@example.com")
	assert.True(t, ok)
	assert.Equal(t, "u5", byEmail.ID)
}

func TestDeltaPodsDoNotLoseUpdates(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	pods := []*DeltaDB{openPod(t, store, sch, dOpts("a")), openPod(t, store, sch, dOpts("b")), openPod(t, store, sch, dOpts("c"))}

	const workers, increments = 2, 10
	var latest atomic.Int64
	var wg sync.WaitGroup
	for _, pod := range pods {
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range increments {
					v, err := pod.Update(ctx, func(tx *Tx) error {
						u, _ := sch.users.Get(tx, "counter")
						u.ID = "counter"
						u.Count++
						return sch.users.Put(tx, u)
					})
					if !assert.NoError(t, err) {
						return
					}
					for {
						old := latest.Load()
						if v <= old || latest.CompareAndSwap(old, v) {
							break
						}
					}
				}
			}()
		}
	}
	wg.Wait()

	for _, pod := range pods {
		require.NoError(t, pod.WaitFor(ctx, latest.Load()))
		u, _ := sch.users.Get(pod.View(), "counter")
		assert.EqualValues(t, len(pods)*workers*increments, u.Count, "no increment is lost (F4)")
		assert.Equal(t, dumpUsers(sch, pods[0].View()), dumpUsers(sch, pod.View()), "every pod has the same state (F25)")
	}
	assert.EqualValues(t, len(pods)*workers*increments, latest.Load(), "one version per increment")
}

func TestDeltaUniqueAcrossPods(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	pods := []*DeltaDB{openPod(t, store, sch, dOpts("a")), openPod(t, store, sch, dOpts("b"))}

	errs := make([]error, len(pods))
	var wg sync.WaitGroup
	for i, pod := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = pod.Update(ctx, func(tx *Tx) error {
				return sch.users.Put(tx, dUser{ID: fmt.Sprintf("user-%d", i), Email: "same@example.com"})
			})
		}()
	}
	wg.Wait()
	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
			continue
		}
		var uerr *UniqueError
		assert.ErrorAs(t, err, &uerr)
	}
	assert.Equal(t, 1, wins, "exactly one pod gets the address (F8)")
	for _, pod := range pods {
		require.NoError(t, pod.WaitFor(ctx, 1))
		assert.Len(t, dumpUsers(sch, pod.View()), 1)
	}
}

// faultStore fails reads that its hook picks.
type faultStore struct {
	*objstore.Mem
	mu        sync.Mutex
	beforeGet func(name string) error
}

func (f *faultStore) setBeforeGet(fn func(name string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeGet = fn
}

func (f *faultStore) Get(ctx context.Context, name string) ([]byte, error) {
	f.mu.Lock()
	hook := f.beforeGet
	f.mu.Unlock()
	if hook != nil {
		if err := hook(name); err != nil {
			return nil, err
		}
	}
	return f.Mem.Get(ctx, name)
}

func TestDeltaUnclearCommits(t *testing.T) {
	ctx := context.Background()
	mem := newDeltaStore(t)
	store := &faultStore{Mem: mem}
	sch := newDSchema()
	db := openPod(t, store, sch, dOpts("a"))
	has := func(id string) bool {
		_, ok := sch.users.Get(db.View(), id)
		return ok
	}

	// The commit lands but the reply is lost: the pod finds it is its own.
	lost := true
	mem.BeforeWrite = func(op, name string) error {
		if isCommitFile(name) && lost {
			lost = false
			return objstore.ErrMemAppliedThenFailed
		}
		return nil
	}
	_, err := db.Update(ctx, putUser(sch, "u1"))
	require.NoError(t, err)
	assert.True(t, has("u1"))

	// The commit never lands and storage can't say so: Update reports the
	// outcome as unknown, never as a failure (F32).
	mem.BeforeWrite = func(op, name string) error {
		if isCommitFile(name) {
			return objstore.Unknown(errors.New("timeout"))
		}
		return nil
	}
	_, err = db.Update(ctx, putUser(sch, "u2"))
	require.ErrorIs(t, err, objstore.ErrUnknown)
	assert.False(t, has("u2"))

	// The commit lands, and from then on storage can't confirm it either.
	var landed atomic.Bool
	mem.BeforeWrite = func(op, name string) error {
		if isCommitFile(name) {
			landed.Store(true)
			return objstore.ErrMemAppliedThenFailed
		}
		return nil
	}
	store.setBeforeGet(func(name string) error {
		if isCommitFile(name) && landed.Load() {
			return objstore.Unknown(errors.New("timeout"))
		}
		return nil
	})
	_, err = db.Update(ctx, putUser(sch, "u3"))
	require.ErrorIs(t, err, objstore.ErrUnknown)
	assert.True(t, landed.Load())
	assert.False(t, has("u3"), "not applied while its outcome is unknown")

	// Once storage recovers, the next Update finds out what happened by
	// reading the log.
	mem.BeforeWrite = nil
	store.setBeforeGet(nil)
	_, err = db.Update(ctx, putUser(sch, "u4"))
	require.NoError(t, err)
	assert.False(t, has("u2"), "the commit that never landed is not applied")
	assert.True(t, has("u3"), "the commit that landed is found and applied")
	assert.True(t, has("u4"))

	fresh := openPod(t, store, sch, dOpts("b"))
	assert.Equal(t, dumpUsers(sch, db.View()), dumpUsers(sch, fresh.View()))
}

func TestDeltaUploadFailure(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	db := openPod(t, store, sch, dOpts("a"))
	denied := errors.New("403 forbidden")
	store.BeforeWrite = func(op, name string) error {
		if strings.HasSuffix(name, ".parquet") {
			return denied
		}
		return nil
	}
	_, err := db.Update(ctx, putUser(sch, "u1"))
	require.ErrorIs(t, err, denied)
	assert.NotErrorIs(t, err, objstore.ErrUnknown)
	assert.EqualValues(t, 0, db.View().Index(), "no commit without its data file (F6)")
	_, err = store.Get(ctx, deltalog.CommitName(1))
	require.ErrorIs(t, err, objstore.ErrNotFound)

	store.BeforeWrite = nil
	_, err = db.Update(ctx, putUser(sch, "u1"))
	require.NoError(t, err, "a failed upload does not stop later writes")
}

func TestDeltaFollowAndWaitFor(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	a := openPod(t, store, sch, dOpts("a"))
	b := openPod(t, store, sch, dOpts("b"))

	v, err := a.Update(ctx, putUser(sch, "u1"))
	require.NoError(t, err)
	require.NoError(t, b.WaitFor(ctx, v), "a read can wait for a write made through another pod (F29)")
	_, ok := sch.users.Get(b.View(), "u1")
	assert.True(t, ok)
	assert.Error(t, b.WaitFor(ctx, v+5), "a version not in the log yet")

	opts := dOpts("follower")
	opts.PollInterval = 2 * time.Millisecond
	follower := openPod(t, store, sch, opts)
	v, err = a.Update(ctx, putUser(sch, "u2"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return int64(follower.View().Index()) >= v }, 10*time.Second, 2*time.Millisecond,
		"the follower applies other pods' commits on its own")
	_, ok = sch.users.Get(follower.View(), "u2")
	assert.True(t, ok)
}

func TestDeltaCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	opts := dOpts("a")
	opts.CheckpointEvery = 5
	db, err := OpenDelta(ctx, store, sch.s, opts)
	require.NoError(t, err)
	for i := range 12 {
		_, err := db.Update(ctx, putUser(sch, fmt.Sprintf("u%d", i)))
		require.NoError(t, err)
	}
	want := dumpUsers(sch, db.View())
	require.NoError(t, db.Close(), "Close waits for background checkpoints")
	_, err = db.Update(ctx, putUser(sch, "late"))
	require.ErrorIs(t, err, ErrClosed)

	hint, err := store.Get(ctx, "_delta_log/_last_checkpoint")
	require.NoError(t, err)
	assert.Contains(t, string(hint), `"version":10`)

	// With the commits the checkpoint covers deleted, as log cleanup will
	// do, a new pod starts from the checkpoint (F16, F38).
	for v := int64(0); v <= 10; v++ {
		require.NoError(t, store.Delete(ctx, deltalog.CommitName(v)))
	}
	fresh := openPod(t, store, sch, dOpts("b"))
	assert.Equal(t, want, dumpUsers(sch, fresh.View()))
	assert.EqualValues(t, 12, fresh.View().Index())
}

func TestDeltaRefusesOtherSchemas(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)

	moreBuckets := NewSchema()
	NewTable(moreBuckets, "users", func(u dUser) string { return u.ID }).Buckets(8)
	_, err := OpenDelta(ctx, store, moreBuckets, dOpts("a"))
	require.ErrorIs(t, err, ErrVersion, "rows would be looked for in the wrong bucket")

	type userV2 struct {
		ID      string     `parquet:"id"`
		Name    string     `parquet:"name"`
		Email   string     `parquet:"email"`
		Count   int64      `parquet:"count"`
		Created *time.Time `parquet:"created,optional,timestamp(microsecond:utc)"`
		Phone   string     `parquet:"phone"`
	}
	newColumn := NewSchema()
	NewTable(newColumn, "users", func(u userV2) string { return u.ID }).Buckets(4)
	_, err = OpenDelta(ctx, store, newColumn, dOpts("a"))
	require.ErrorIs(t, err, ErrVersion, "a schema change needs a migration (F19, F30)")

	// A schema serves one kind of store, since each normalizes rows its own
	// way (F25).
	deltaSch := newDSchema()
	openPod(t, store, deltaSch, dOpts("a"))
	_, err = Open(t.TempDir(), deltaSch.s, Options{})
	require.Error(t, err, "a schema set up for Delta can't open a local store")

	localSch := newDSchema()
	local, err := Open(t.TempDir(), localSch.s, Options{})
	require.NoError(t, err)
	closeDB(t, local)
	_, err = OpenDelta(ctx, store, localSch.s, dOpts("a"))
	require.Error(t, err, "a schema used by a local store can't open a Delta table")
}

func TestDeltaCommitTimeNeverGoesBack(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	aheadOpts := dOpts("ahead")
	aheadOpts.Clock = frozenClock(dStart.Add(time.Hour))
	ahead := openPod(t, store, sch, aheadOpts)
	behind := openPod(t, store, sch, dOpts("behind"))

	_, err := ahead.Update(ctx, putUser(sch, "u1"))
	require.NoError(t, err)
	var seen time.Time
	_, err = behind.Update(ctx, func(tx *Tx) error {
		seen = tx.Now()
		return sch.users.Put(tx, dUser{ID: "u2"})
	})
	require.NoError(t, err)
	assert.Equal(t, dStart.Add(time.Hour), seen, "a commit is never earlier than the one before it (F13)")
}

func TestDeltaRefusesForeignCommits(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	db := openPod(t, store, sch, dOpts("a"))
	spark := `{"commitInfo":{"timestamp":1,"operation":"WRITE","engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.2.0"}}` + "\n"
	require.NoError(t, store.PutIfAbsent(ctx, deltalog.CommitName(1), []byte(spark)))

	_, err := db.Update(ctx, putUser(sch, "u1"))
	require.ErrorIs(t, err, deltalog.ErrForeign)
	require.ErrorIs(t, db.Health(), deltalog.ErrForeign, "writes stay refused (F11, F21)")
	_, err = db.Update(ctx, putUser(sch, "u1"))
	require.ErrorIs(t, err, deltalog.ErrForeign)
}
