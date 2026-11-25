package authdelta

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

type cred struct {
	ID string `parquet:"id"`
}

type env struct {
	store  objstore.Store
	schema *filestore.Schema
	creds  *filestore.Table[cred]
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema()}
	e.creds = filestore.NewTable(e.schema, "creds", func(c cred) string { return c.ID })
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

func (e *env) pod(t *testing.T, name string) (deltatxn.DB, *filestore.DeltaDB) {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return deltatxn.DB{DB: db}, db
}

func (e *env) put(t *testing.T, db txn.DB, id string) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), db, nil, func(s deltatxn.Scope) error {
		return e.creds.Put(s.Tx, cred{ID: id})
	}))
}

func (e *env) delete(t *testing.T, db txn.DB, id string) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), db, nil, func(s deltatxn.Scope) error {
		_, err := e.creds.Delete(s.Tx, id)
		return err
	}))
}

// has reads a credential with c, like the stores do.
func (e *env) has(c *Checker, id string) error {
	return c.Read(context.Background(), func(_ txn.Tx, s deltatxn.Scope) error {
		if _, ok := e.creds.Get(s.Reader, id); !ok {
			return sql.ErrNoRows
		}
		return nil
	})
}

func TestCheckerSeesOtherPods(t *testing.T) {
	e := newEnv(t)
	a, _ := e.pod(t, "a")
	b, _ := e.pod(t, "b")

	c := NewChecker(b, time.Hour)
	require.ErrorIs(t, e.has(c, "x"), sql.ErrNoRows)
	e.put(t, a, "x")
	require.NoError(t, e.has(c, "x"), "a credential created on another pod works at once, however old the view may be")

	e.delete(t, a, "x")
	require.NoError(t, e.has(c, "x"), "within maxStale, the pod trusts its view")
	require.ErrorIs(t, e.has(NewChecker(b, 0), "x"), sql.ErrNoRows, "a Checker without maxStale always asks the log")
}

func TestCheckerMaxStale(t *testing.T) {
	e := newEnv(t)
	a, _ := e.pod(t, "a")
	b, _ := e.pod(t, "b")
	e.put(t, a, "x")

	c := NewChecker(b, 100*time.Millisecond)
	require.NoError(t, e.has(c, "x"))
	e.delete(t, a, "x")
	assert.Eventually(t, func() bool { return errors.Is(e.has(c, "x"), sql.ErrNoRows) }, 10*time.Second, 10*time.Millisecond,
		"a revoked credential stops working on a pod that doesn't follow the log")
}

func TestCheckerFailsClosed(t *testing.T) {
	e := newEnv(t)
	b, db := e.pod(t, "b")
	e.put(t, b, "x")
	c := NewChecker(b, 0)
	require.NoError(t, e.has(c, "x"))
	require.NoError(t, db.Close())
	require.ErrorIs(t, e.has(c, "x"), filestore.ErrClosed, "a pod that can't read the log accepts nothing")
}

// slowDB counts the catch-ups and makes them slow.
type slowDB struct {
	txn.DB
	runs atomic.Int32
	gate chan struct{}
}

func (d *slowDB) Run(ctx context.Context, opts *txn.Options, fn func(tx txn.Tx) error) error {
	if opts == nil {
		d.runs.Add(1)
		<-d.gate
	}
	return d.DB.Run(ctx, opts, fn)
}

func TestCheckerSharesCatchUps(t *testing.T) {
	e := newEnv(t)
	b, _ := e.pod(t, "b")
	d := &slowDB{DB: b, gate: make(chan struct{})}
	c := NewChecker(d, 0)

	first := make(chan error, 1)
	go func() { first <- e.has(c, "x") }()
	require.Eventually(t, func() bool { return d.runs.Load() == 1 }, 10*time.Second, time.Millisecond)

	// Everyone who comes while that one runs needs one that starts later,
	// and shares it.
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.ErrorIs(t, e.has(c, "x"), sql.ErrNoRows)
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Read(ctx, func(txn.Tx, deltatxn.Scope) error { return nil })
	require.ErrorIs(t, err, context.Canceled, "a caller can give up waiting")

	time.Sleep(50 * time.Millisecond) // the 50 are waiting by now
	close(d.gate)
	wg.Wait()
	require.ErrorIs(t, <-first, sql.ErrNoRows)
	assert.LessOrEqual(t, d.runs.Load(), int32(4), "51 requests share a few catch-ups")
}

// uses collects what a Recorder writes.
type uses struct {
	mu     sync.Mutex
	writes []map[string]int
	err    error
}

func (u *uses) write(_ context.Context, m map[string]int) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.writes = append(u.writes, m)
	return u.err
}

func TestRecorderBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var u uses
		r := NewRecorder(time.Minute, "test uses", u.write)
		r.Add("a", 1)
		synctest.Wait()
		assert.Equal(t, []map[string]int{{"a": 1}}, u.writes, "the first use is written at once")

		r.Add("a", 2)
		r.Add("b", 3)
		time.Sleep(30 * time.Second)
		r.Add("a", 4)
		synctest.Wait()
		assert.Len(t, u.writes, 1, "then at most one write per interval")
		v, ok := r.Pending("a")
		assert.True(t, ok)
		assert.Equal(t, 4, v)

		time.Sleep(31 * time.Second)
		synctest.Wait()
		assert.Equal(t, []map[string]int{{"a": 1}, {"a": 4, "b": 3}}, u.writes, "with the latest use of each key")
		_, ok = r.Pending("a")
		assert.False(t, ok)

		time.Sleep(time.Hour)
		synctest.Wait()
		assert.Len(t, u.writes, 2, "no uses, no writes")
	})
}

func TestRecorderRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		u := uses{err: errors.New("storage is down")}
		r := NewRecorder(time.Minute, "test uses", u.write)
		r.Add("a", 1)
		synctest.Wait()
		assert.Len(t, u.writes, 1)
		v, ok := r.Pending("a")
		assert.True(t, ok, "a use that wasn't written is kept")
		assert.Equal(t, 1, v)

		u.mu.Lock()
		u.err = nil
		u.mu.Unlock()
		time.Sleep(61 * time.Second)
		synctest.Wait()
		assert.Equal(t, []map[string]int{{"a": 1}, {"a": 1}}, u.writes)

		u.mu.Lock()
		u.err = filestore.ErrClosed
		u.mu.Unlock()
		r.Add("a", 2)
		time.Sleep(time.Hour)
		synctest.Wait()
		assert.Len(t, u.writes, 3, "a closed store is not tried again")
	})
}

func TestRecorderFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var u uses
		r := NewRecorder(time.Minute, "test uses", u.write)
		require.NoError(t, r.Flush(t.Context()), "nothing to write")
		assert.Empty(t, u.writes)
		r.Add("a", 1)
		synctest.Wait()
		r.Add("b", 2)
		require.NoError(t, r.Flush(t.Context()))
		assert.Equal(t, []map[string]int{{"a": 1}, {"b": 2}}, u.writes, "a Flush doesn't wait for the interval")
		time.Sleep(time.Hour)
		synctest.Wait()
		assert.Len(t, u.writes, 2)
	})
}
