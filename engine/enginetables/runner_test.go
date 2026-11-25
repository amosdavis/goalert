package enginetables

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
)

// count is a state of a module.
type count struct{ N int }

func step(name string, fn func(tx *filestore.Tx) error) Step {
	return Step{Name: name, Run: func(_ context.Context, tx *filestore.Tx) error { return fn(tx) }}
}

// TestRunner checks that a cycle is one commit of the pod with the lease, in
// which steps see each other's changes and none is hurt by another's failure.
func TestRunner(t *testing.T) {
	ctx := t.Context()
	ps, clock := pods(t, 2)
	a, b := ps[0], ps[1]
	ra, rb := NewRunner(a.db, a.lease), NewRunner(b.db, b.lease)
	boom := errors.New("boom")

	var order []string
	ok := func(p pod, name string) Step {
		return step(name, func(tx *filestore.Tx) error {
			order = append(order, name)
			return p.SaveState(tx, name, count{1})
		})
	}
	// The first step's failure must not take the lease with it.
	before := a.db.View().Index()
	errs, err := ra.Run(ctx,
		step("bad", func(*filestore.Tx) error { order = append(order, "bad"); return boom }),
		ok(a, "x"),
		step("panic", func(*filestore.Tx) error { panic("oops") }),
		step("sees", func(tx *filestore.Tx) error {
			var got count
			require.NoError(t, a.LoadState(tx, "x", &got))
			assert.Equal(t, 1, got.N)
			return nil
		}),
		ok(a, "y"),
	)
	require.NoError(t, err)
	require.Len(t, errs, 5)
	require.ErrorIs(t, errs[0], boom)
	require.ErrorContains(t, errs[0], "bad: ")
	require.NoError(t, errs[1])
	var pe *filestore.TxPanic
	require.ErrorAs(t, errs[2], &pe)
	require.NoError(t, errs[3])
	require.NoError(t, errs[4])
	assert.Equal(t, []string{"bad", "x", "y"}, order)
	assert.Equal(t, before+1, a.db.View().Index())
	assert.True(t, ra.Holds())

	// Another pod does nothing, whether it knows of the lease yet or not.
	order = nil
	_, err = rb.Run(ctx, ok(b, "z"))
	require.ErrorIs(t, err, ErrNotHolder)
	_, err = rb.Run(ctx, ok(b, "z"))
	require.ErrorIs(t, err, ErrNotHolder)
	require.ErrorIs(t, rb.Do(ctx, ok(b, "z")), ErrNotHolder)
	assert.Empty(t, order)
	assert.False(t, rb.Holds())
	assert.Equal(t, "pod-0", rb.Status().HolderName)
	require.NoError(t, ra.Do(ctx, ok(a, "z")))
	require.ErrorIs(t, ra.Do(ctx, step("bad", func(*filestore.Tx) error { return boom })), boom)

	// A pod that lost the lease finds out in the transaction itself (F28).
	clock.Advance(31 * time.Second)
	errs, err = rb.Run(ctx)
	require.NoError(t, err)
	assert.Empty(t, errs)
	assert.True(t, ra.Holds(), "it can't know yet")
	order = nil
	_, err = ra.Run(ctx, ok(a, "late"))
	require.ErrorIs(t, err, ErrNotHolder)
	require.ErrorIs(t, ra.Do(ctx, ok(a, "late")), ErrNotHolder)
	assert.Empty(t, order)
	assert.False(t, ra.Holds())

	// Giving it back lets another go on at once.
	require.NoError(t, ra.Release(ctx), "it has none to give back")
	require.NoError(t, rb.Release(ctx))
	_, err = a.db.Update(ctx, func(*filestore.Tx) error { return nil })
	require.NoError(t, err)
	_, err = ra.Run(ctx)
	require.NoError(t, err)

	require.NoError(t, b.db.Close())
	_, err = rb.Run(ctx, ok(b, "closed"))
	require.Error(t, err)
}

// TestRunnerRace checks that of several pods one does each cycle's work.
func TestRunnerRace(t *testing.T) {
	ps, clock := pods(t, 4)
	for round := range 5 {
		clock.Advance(time.Minute)
		var ran atomic.Int32
		var wg sync.WaitGroup
		for _, p := range ps {
			wg.Go(func() {
				errs, err := NewRunner(p.db, p.lease).Run(context.Background(), step("work", func(tx *filestore.Tx) error {
					return p.SaveState(tx, fmt.Sprint("round-", round), count{1})
				}))
				if err != nil {
					assert.ErrorIs(t, err, ErrNotHolder)
					return
				}
				assert.NoError(t, errs[0])
				ran.Add(1)
			})
		}
		wg.Wait()
		require.EqualValues(t, 1, ran.Load())
	}
}

func TestEach(t *testing.T) {
	ps, _ := pods(t, 1)
	p := ps[0]
	require.NoError(t, p.update(t, func(tx *filestore.Tx) error {
		err := Each(tx, []string{"a", "bad", "panic", "b"}, func(name string) error {
			if err := p.SaveState(tx, name, count{1}); err != nil {
				return err
			}
			switch name {
			case "bad":
				return errors.New("boom")
			case "panic":
				panic("oops")
			}
			return nil
		})
		require.ErrorContains(t, err, "boom")
		require.ErrorContains(t, err, "oops")
		require.NoError(t, Each(tx, []string(nil), func(string) error { return nil }))
		return nil
	}))
	for name, want := range map[string]int{"a": 1, "b": 1, "bad": 0, "panic": 0} {
		var got count
		require.NoError(t, p.LoadState(p.db.View(), name, &got))
		assert.Equal(t, want, got.N, name)
	}
}
