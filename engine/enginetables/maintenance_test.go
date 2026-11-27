package enginetables

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/config"
)

// TestMaintenance checks that tasks run when they are due, on whichever pod has
// the lease, and that one that fails is tried again and hurts no other.
func TestMaintenance(t *testing.T) {
	ctx := config.Config{}.Context(t.Context())
	ps, clock := pods(t, 2)
	ran := map[string]int{}
	var fail error
	start := func(p pod) (*Runner, *Maintenance) {
		r := NewRunner(p.db, p.lease)
		return r, p.NewMaintenance(p.db, r,
			Task{"daily", 24 * time.Hour, func(context.Context) error { ran["daily"]++; return nil }},
			Task{"fails", time.Hour, func(context.Context) error { ran["fails"]++; return fail }},
			Task{"hourly", time.Hour, func(context.Context) error { ran["hourly"]++; return nil }},
		)
	}
	ra, a := start(ps[0])
	_, err := ra.Run(ctx)
	require.NoError(t, err)
	fail = errors.New("boom")
	require.NoError(t, a.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 1, "fails": 1, "hourly": 1}, ran)
	require.NoError(t, a.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 1, "fails": 1, "hourly": 1}, ran)
	fail = nil
	clock.Advance(retryTask)
	require.NoError(t, a.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 1, "fails": 2, "hourly": 1}, ran)

	// The other pod goes on from there.
	rb, b := start(ps[1])
	_, err = rb.Run(ctx)
	require.NoError(t, err)
	require.NoError(t, b.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 1, "fails": 2, "hourly": 1}, ran)
	clock.Advance(time.Hour)
	_, err = rb.Run(ctx)
	require.NoError(t, err)
	require.NoError(t, b.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 1, "fails": 3, "hourly": 2}, ran)
	clock.Advance(24 * time.Hour)
	_, err = rb.Run(ctx)
	require.NoError(t, err)
	require.NoError(t, b.RunDue(ctx))
	assert.Equal(t, map[string]int{"daily": 2, "fails": 4, "hourly": 3}, ran)

	// Without the lease it stops, and can't say that it was done.
	clock.Advance(24 * time.Hour)
	require.ErrorIs(t, a.RunDue(ctx), ErrNotHolder)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = rb.Run(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, b.RunDue(canceled), context.Canceled)
}

func TestMaintenanceConfigChange(t *testing.T) {
	ctx := config.Config{}.Context(t.Context())
	ps, _ := pods(t, 1)
	p := ps[0]
	r := NewRunner(p.db, p.lease)
	ran := 0
	m := p.NewMaintenance(p.db, r, Task{Name: "daily", Every: 24 * time.Hour, Run: func(context.Context) error { ran++; return nil }})
	_, err := r.Run(ctx)
	require.NoError(t, err)
	require.NoError(t, m.RunDue(ctx))
	assert.Equal(t, 1, ran)
	require.NoError(t, m.RunDue(ctx))
	assert.Equal(t, 1, ran, "not due without a change")

	cfg := config.Config{}
	cfg.Maintenance.AlertCleanupDays = 1
	require.NoError(t, m.RunDue(cfg.Context(ctx)))
	assert.Equal(t, 2, ran, "a config change makes the task due at once")
}
