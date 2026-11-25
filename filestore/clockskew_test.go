package filestore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

// serverClock is a store whose server's clock is off from this pod's.
type serverClock struct {
	objstore.Store
	objstore.ClockWatch
}

// TestDeltaClockSkew checks that a pod whose clock is off from the object
// store's says so in its health (F13).
func TestDeltaClockSkew(t *testing.T) {
	ctx := context.Background()
	store := &serverClock{Store: objstore.NewMem()}
	s := NewSchema()
	NewTable(s, "users", func(u dUser) string { return u.ID })
	require.NoError(t, CreateDelta(ctx, store, s, DeltaOptions{}))
	open := func(opts DeltaOptions) *DeltaDB {
		s := NewSchema()
		NewTable(s, "users", func(u dUser) string { return u.ID })
		db, err := OpenDelta(ctx, store, s, opts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db := open(DeltaOptions{PollInterval: -1})
	require.NoError(t, db.Health(), "the store has not said anything yet")

	now := time.Now()
	store.Observe(now, now, now.Add(-time.Minute))
	require.ErrorContains(t, db.ClockError(), "ahead of the storage's")
	require.ErrorContains(t, db.Health(), "(F13)")
	store.Observe(now, now, now.Add(10*time.Second))
	require.NoError(t, db.Health(), "within the default of 15 seconds")
	assert.Error(t, open(DeltaOptions{PollInterval: -1, MaxClockSkew: 5 * time.Second}).Health())
	store.Observe(now, now, now.Add(time.Hour))
	assert.NoError(t, open(DeltaOptions{PollInterval: -1, MaxClockSkew: -1}).Health(), "the check can be off")
}
