package enginetables

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
)

// skewed is a pod's object store, whose server's clock is off from the pod's.
type skewed struct {
	objstore.Store
	objstore.ClockWatch
}

func (s *skewed) setOffset(d time.Duration) {
	now := time.Now()
	s.Observe(now, now, now.Add(d))
}

// TestRunnerClock checks that a pod whose clock is off leaves the engine to one
// whose clock is right, and runs it if no other pod does (F13).
func TestRunnerClock(t *testing.T) {
	ctx := t.Context()
	clock := filestore.NewFakeClock(time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC))
	clock.SetSpeed(0)
	mem := objstore.NewMem()
	opts := filestore.DeltaOptions{Clock: clock, PollInterval: -1, Logf: func(string, ...any) {}}
	s := filestore.NewSchema()
	DefineDeltaTables(s)
	require.NoError(t, filestore.CreateDelta(ctx, mem, s, opts))
	open := func(name string) (pod, *skewed, *Runner) {
		store := &skewed{Store: mem}
		s := filestore.NewSchema()
		tables := DefineDeltaTables(s)
		db, err := filestore.OpenDelta(ctx, store, s, opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		p := pod{tables, db, tables.NewLease(name, 30*time.Second)}
		return p, store, NewRunner(db, p.lease)
	}
	catchUp := func(p, from pod) {
		t.Helper()
		require.NoError(t, p.db.WaitFor(ctx, int64(from.db.View().Index())))
	}
	a, aStore, ra := open("a")
	b, _, rb := open("b")
	_, err := ra.Run(ctx)
	require.NoError(t, err)
	require.True(t, ra.Holds())

	// a's clock goes off: it gives the lease up, and b takes it.
	aStore.setOffset(time.Minute)
	_, err = ra.Run(ctx)
	require.ErrorIs(t, err, ErrNotHolder)
	require.ErrorContains(t, err, "clock")
	assert.False(t, ra.Holds())
	catchUp(b, a)
	_, err = rb.Run(ctx)
	require.NoError(t, err)
	assert.True(t, rb.Holds())
	catchUp(a, b)
	_, err = ra.Run(ctx)
	require.ErrorIs(t, err, ErrNotHolder)

	// b gives it back: a does not take it at once,
	require.NoError(t, rb.Release(ctx))
	catchUp(a, b)
	clock.Advance(30 * time.Second)
	_, err = ra.Run(ctx)
	require.ErrorIs(t, err, ErrNotHolder)
	assert.False(t, ra.Holds())
	// but once no other pod took it for two of its terms.
	clock.Advance(31 * time.Second)
	errs, err := ra.Run(ctx, step("work", func(tx *filestore.Tx) error { return a.SaveState(tx, "work", count{1}) }))
	require.NoError(t, err)
	require.NoError(t, errs[0])
	assert.True(t, ra.Holds())

	// Once its clock is right, it goes on as before, and gives the lease up
	// again if it is off again.
	aStore.setOffset(0)
	_, err = ra.Run(ctx)
	require.NoError(t, err)
	aStore.setOffset(-time.Minute)
	_, err = ra.Run(ctx)
	require.ErrorIs(t, err, ErrNotHolder)
	assert.False(t, ra.Holds())
}
