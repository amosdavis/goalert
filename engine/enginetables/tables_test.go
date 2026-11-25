package enginetables

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
)

type pod struct {
	*DeltaTables
	db    *filestore.DeltaDB
	lease *Lease
}

func (p pod) update(t *testing.T, fn func(tx *filestore.Tx) error) error {
	t.Helper()
	_, err := p.db.Update(t.Context(), fn)
	return err
}

// pods opens a table on several pods that share a clock.
func pods(t *testing.T, n int) ([]pod, *filestore.FakeClock) {
	t.Helper()
	clock := filestore.NewFakeClock(time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC))
	clock.SetSpeed(0)
	store := objstore.NewMem()
	opts := filestore.DeltaOptions{Clock: clock, PollInterval: -1, Logf: func(string, ...any) {}}
	s := filestore.NewSchema()
	DefineDeltaTables(s)
	require.NoError(t, filestore.CreateDelta(t.Context(), store, s, opts))
	var out []pod
	for i := range n {
		s := filestore.NewSchema()
		tables := DefineDeltaTables(s)
		db, err := filestore.OpenDelta(t.Context(), store, s, opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		out = append(out, pod{tables, db, tables.NewLease(fmt.Sprintf("pod-%d", i), 30*time.Second)})
	}
	return out, clock
}

func TestLease(t *testing.T) {
	ps, clock := pods(t, 2)
	a, b := ps[0], ps[1]
	status := func(p pod) LeaseStatus { return p.lease.Status(p.db.View(), clock.Now()) }

	assert.Equal(t, LeaseStatus{Free: true}, status(a))
	require.ErrorIs(t, a.update(t, func(tx *filestore.Tx) error { return a.lease.Check(tx) }), ErrNotHolder)
	require.NoError(t, a.update(t, a.lease.Renew))
	require.NoError(t, a.update(t, func(tx *filestore.Tx) error { return a.lease.Check(tx) }))
	assert.Equal(t, LeaseStatus{Mine: true, HolderName: "pod-0", ExpiresAt: clock.Now().Add(30 * time.Second)}, status(a))

	// Another pod can't have it, and giving up what it doesn't have does
	// nothing.
	require.ErrorIs(t, b.update(t, b.lease.Renew), ErrNotHolder)
	require.ErrorIs(t, b.update(t, func(tx *filestore.Tx) error { return b.lease.Check(tx) }), ErrNotHolder)
	require.NoError(t, b.update(t, b.lease.Release))
	assert.Equal(t, "pod-0", status(b).HolderName)
	assert.False(t, status(b).Mine || status(b).Free)

	// Renewing it early costs no commit.
	before := a.db.View().Index()
	clock.Advance(14 * time.Second)
	require.NoError(t, a.update(t, a.lease.Renew))
	assert.Equal(t, before, a.db.View().Index())
	clock.Advance(2 * time.Second)
	require.NoError(t, a.update(t, a.lease.Renew))
	assert.Equal(t, before+1, a.db.View().Index())
	assert.Equal(t, clock.Now().Add(30*time.Second), status(a).ExpiresAt)

	// Once it has expired another pod takes it, and the first one finds out.
	clock.Advance(29 * time.Second)
	require.ErrorIs(t, b.update(t, b.lease.Renew), ErrNotHolder)
	clock.Advance(time.Second)
	assert.True(t, status(a).Free && status(a).Mine)
	require.NoError(t, a.update(t, func(tx *filestore.Tx) error { return a.lease.Check(tx) }), "nobody has taken it")
	require.NoError(t, b.update(t, b.lease.Renew))
	require.ErrorIs(t, a.update(t, func(tx *filestore.Tx) error { return a.lease.Check(tx) }), ErrNotHolder)
	require.ErrorIs(t, a.update(t, a.lease.Renew), ErrNotHolder)
	assert.Equal(t, "pod-1", status(a).HolderName)

	// Giving it back frees it at once.
	require.NoError(t, b.update(t, b.lease.Release))
	require.NoError(t, a.update(t, a.lease.Renew))

	// A share of the same name is another holder.
	again := a.NewLease("pod-0", time.Minute)
	require.ErrorIs(t, a.update(t, again.Renew), ErrNotHolder)
}

// TestLeaseRace checks that a lease that has expired goes to one pod.
func TestLeaseRace(t *testing.T) {
	ps, clock := pods(t, 4)
	for range 5 {
		clock.Advance(time.Minute)
		held := make([]error, len(ps))
		var wg sync.WaitGroup
		for i, p := range ps {
			wg.Go(func() { _, held[i] = p.db.Update(context.Background(), p.lease.Renew) })
		}
		wg.Wait()
		n := 0
		for _, err := range held {
			if err == nil {
				n++
			} else {
				require.ErrorIs(t, err, ErrNotHolder)
			}
		}
		require.Equal(t, 1, n)
	}
}

func TestState(t *testing.T) {
	ps, _ := pods(t, 2)
	a, b := ps[0], ps[1]
	type v1 struct{ A, B int }
	var got v1
	require.NoError(t, a.LoadState(a.db.View(), "m", &got))
	assert.Zero(t, got)
	require.NoError(t, a.update(t, func(tx *filestore.Tx) error { return a.SaveState(tx, "m", v1{1, 2}) }))
	// Another version knows another field. Neither loses the other's.
	type v2 struct{ B, C int }
	var other v2
	require.NoError(t, b.update(t, func(tx *filestore.Tx) error {
		if err := b.LoadState(tx, "m", &other); err != nil {
			return err
		}
		return b.SaveState(tx, "m", v2{B: other.B + 1, C: 9})
	}))
	require.NoError(t, a.db.WaitFor(t.Context(), int64(b.db.View().Index())))
	require.NoError(t, a.LoadState(a.db.View(), "m", &got))
	assert.Equal(t, v1{1, 3}, got)
	require.NoError(t, a.update(t, func(tx *filestore.Tx) error { return a.SaveState(tx, "m", v1{5, 6}) }))
	require.NoError(t, b.db.WaitFor(t.Context(), int64(a.db.View().Index())))
	require.NoError(t, b.LoadState(b.db.View(), "m", &other))
	assert.Equal(t, v2{6, 9}, other)
	require.NoError(t, a.LoadState(a.db.View(), "other", &other))
}
