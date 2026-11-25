package rotationmanager_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/engine/rotationmanager"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/schedule/rotation"
)

func update(p *deltastorestest.Pod) {
	p.T.Helper()
	p.Update(func(tx *filestore.Tx) error { return rotationmanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx) })
}

func active(p *deltastorestest.Pod, rotationID string) string {
	p.T.Helper()
	id, _ := p.Tables.Rotation.ActiveUserID(p.View(), rotationID)
	return id
}

func TestDeltaAdvance(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob, carol := p.AddUser("alice"), p.AddUser("bob"), p.AddUser("carol")
	hourly := p.AddRotation("hourly", rotation.TypeHourly, 1, in.Clock.Now().Add(-30*time.Minute), alice, bob, carol)
	daily := p.AddRotation("daily", rotation.TypeDaily, 1, in.Clock.Now(), bob, alice)
	empty := p.AddRotation("empty", rotation.TypeDaily, 1, in.Clock.Now())

	update(p)
	assert.Equal(t, alice, active(p, hourly))
	assert.Equal(t, bob, active(p, daily))
	assert.Empty(t, active(p, empty))
	before := p.Delta.View().Index()
	update(p)
	assert.Equal(t, before, p.Delta.View().Index(), "nothing to do is no commit")

	in.Clock.Advance(30 * time.Minute)
	update(p)
	assert.Equal(t, bob, active(p, hourly), "by the rotation's clock, not since the start")
	st, _ := p.Tables.Rotation.State.Get(p.View(), hourly)
	assert.Equal(t, in.Clock.Now(), st.ShiftStart)
	assert.EqualValues(t, 1, st.Position)
	update(p)
	assert.Equal(t, bob, active(p, hourly))

	// Shifts that nobody saw end are skipped, and the last wraps around.
	in.Clock.Advance(2 * time.Hour)
	update(p)
	assert.Equal(t, alice, active(p, hourly))
	assert.Equal(t, bob, active(p, daily))
	in.Clock.Advance(24 * time.Hour)
	update(p)
	assert.Equal(t, alice, active(p, daily))

	// A rotation nobody looked at for long catches up.
	in.Clock.Advance(3 * 365 * 24 * time.Hour)
	update(p)
	assert.NotEmpty(t, active(p, hourly))
	st, _ = p.Tables.Rotation.State.Get(p.View(), hourly)
	assert.Equal(t, in.Clock.Now(), st.ShiftStart)
}

// TestDeltaRepair checks that a rotation whose state and participants don't go
// together is put right, whatever the reason.
func TestDeltaRepair(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob := p.AddUser("alice"), p.AddUser("bob")
	id := p.AddRotation("rot", rotation.TypeDaily, 1, in.Clock.Now(), alice, bob)
	p.Update(func(tx *filestore.Tx) error {
		_, err := p.Tables.Rotation.State.Delete(tx, id)
		return err
	})
	assert.Empty(t, active(p, id))
	update(p)
	assert.Equal(t, alice, active(p, id))

	// An old state, from a version that counted hours differently.
	p.Update(func(tx *filestore.Tx) error {
		st, _ := p.Tables.Rotation.State.Get(tx, id)
		st.Version = 1
		return p.Tables.Rotation.State.Put(tx, st)
	})
	update(p)
	st, _ := p.Tables.Rotation.State.Get(p.View(), id)
	assert.EqualValues(t, 2, st.Version)
	assert.Equal(t, alice, active(p, id))
}

// TestDeltaBadRotation checks that a rotation that can't be updated does not
// keep the others from being (F53).
func TestDeltaBadRotation(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	alice, bob := p.AddUser("alice"), p.AddUser("bob")
	var ids []string
	for _, name := range []string{"first", "second", "third", "fourth"} {
		ids = append(ids, p.AddRotation(name, rotation.TypeHourly, 1, in.Clock.Now(), alice, bob))
	}
	p.Update(func(tx *filestore.Tx) error {
		zone, _ := p.Tables.Rotation.Rotations.Get(tx, ids[0])
		zone.TimeZone = "Nowhere/Else"
		if err := p.Tables.Rotation.Rotations.Put(tx, zone); err != nil {
			return err
		}
		version, _ := p.Tables.Rotation.State.Get(tx, ids[2])
		version.Version = 99
		return p.Tables.Rotation.State.Put(tx, version)
	})
	in.Clock.Advance(time.Hour)
	update(p)
	var got []string
	for _, id := range ids {
		got = append(got, active(p, id))
	}
	assert.Equal(t, []string{alice, bob, alice, bob}, got)
}

// TestDeltaRace checks that a shift that ends advances a rotation once, however
// many pods look at it.
func TestDeltaRace(t *testing.T) {
	in := deltastorestest.New(t)
	a := in.Pod(t)
	users := []string{a.AddUser("u0"), a.AddUser("u1"), a.AddUser("u2"), a.AddUser("u3")}
	id := a.AddRotation("rot", rotation.TypeHourly, 1, in.Clock.Now(), users...)
	pods := []*deltastorestest.Pod{a, in.Pod(t), in.Pod(t)}
	for i := 1; i < 6; i++ {
		in.Clock.Advance(time.Hour)
		var wg sync.WaitGroup
		for _, p := range pods {
			wg.Go(func() {
				_, err := p.Delta.Update(t.Context(), func(tx *filestore.Tx) error {
					return rotationmanager.NewDeltaDB(p.Tables).UpdateAll(p.Ctx(), tx)
				})
				assert.NoError(t, err)
			})
		}
		wg.Wait()
		require.Equal(t, users[i%4], active(a, id))
	}
}
