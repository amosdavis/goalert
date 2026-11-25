package limit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	clock  *filestore.FakeClock
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	e.tables = DefineDeltaTables(e.schema)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

type pod struct {
	*Store
	fdb *filestore.DeltaDB
	db  txn.DB
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return pod{Store: NewDeltaStore(db, e.tables), fdb: fdb, db: db}
}

func (p pod) insertDefaults(t *testing.T, tables *DeltaTables) {
	t.Helper()
	_, err := p.fdb.Update(context.Background(), tables.InsertDefaults)
	require.NoError(t, err)
}

func adminCtx() context.Context {
	return permission.UserContext(context.Background(), "bcefacc0-4764-012d-7bfb-002500d5decb", permission.RoleAdmin)
}

func TestDeltaLimits(t *testing.T) {
	ctx := adminCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")

	all, err := p.All(ctx)
	require.NoError(t, err)
	assert.NotNil(t, all, "an empty map, not nil, as on Postgres")
	assert.Empty(t, all)
	max, err := p.Max(ctx, ContactMethodsPerUser)
	require.NoError(t, err)
	assert.Equal(t, -1, max, "no row means no limit")

	require.NoError(t, p.SetMax(ctx, ContactMethodsPerUser, 3))
	max, err = p.Max(ctx, ContactMethodsPerUser)
	require.NoError(t, err)
	assert.Equal(t, 3, max)
	require.NoError(t, p.SetMax(ctx, ContactMethodsPerUser, 4), "set again")
	require.NoError(t, p.SetMax(ctx, RulesPerSchedule, -1))
	all, err = p.All(ctx)
	require.NoError(t, err)
	assert.Equal(t, Limits{ContactMethodsPerUser: 4, RulesPerSchedule: -1}, all)

	require.Error(t, p.SetMax(ctx, "nope", 3))
	require.Error(t, p.SetMax(ctx, ContactMethodsPerUser, 9001))
	require.Error(t, p.SetMax(ctx, ContactMethodsPerUser, -2))
	_, err = p.Max(ctx, "nope")
	require.True(t, validation.IsValidationError(err), "%v", err)

	// Another pod reads the same limits.
	all, err = e.pod(t, "b").All(ctx)
	require.NoError(t, err)
	assert.Equal(t, Limits{ContactMethodsPerUser: 4, RulesPerSchedule: -1}, all)

	require.NoError(t, p.ResetAll(ctx))
	all, err = p.All(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)
	all, err = e.pod(t, "c").All(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestDeltaLimitsUpdateTx(t *testing.T) {
	ctx := adminCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	require.NoError(t, p.SetMax(ctx, EPStepsPerPolicy, 10))

	require.NoError(t, p.UpdateLimitsTx(ctx, nil, string(EPStepsPerPolicy), 11))
	require.NoError(t, p.UpdateLimitsTx(ctx, nil, string(EPActionsPerStep), 5), "no row: an UPDATE of nothing")
	all, err := p.All(ctx)
	require.NoError(t, err)
	assert.Equal(t, Limits{EPStepsPerPolicy: 11}, all)

	require.Error(t, p.UpdateLimitsTx(ctx, nil, "nope", 5), "not in enum_limit_type")
	require.Error(t, p.UpdateLimitsTx(ctx, nil, string(EPStepsPerPolicy), 1<<40), "not an integer column's value")

	// In a caller's transaction, which sees its own changes and is undone
	// as a whole.
	errUndo := errors.New("undo")
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.UpdateLimitsTx(ctx, tx, string(EPStepsPerPolicy), 12); err != nil {
			return err
		}
		_, r := deltatxn.Unwrap(tx)
		assert.Equal(t, 12, e.tables.Max(r, EPStepsPerPolicy))
		return errUndo
	})
	require.ErrorIs(t, err, errUndo)
	max, err := p.Max(ctx, EPStepsPerPolicy)
	require.NoError(t, err)
	assert.Equal(t, 11, max)
}

func TestDeltaLimitsPermission(t *testing.T) {
	p := newDeltaEnv(t).pod(t, "a")
	ctx := permission.UserContext(context.Background(), "bcefacc0-4764-012d-7bfb-002500d5decb", permission.RoleUser)
	_, err := p.All(ctx)
	assert.True(t, permission.IsPermissionError(err))
	_, err = p.Max(ctx, EPStepsPerPolicy)
	assert.True(t, permission.IsPermissionError(err))
	assert.True(t, permission.IsPermissionError(p.SetMax(ctx, EPStepsPerPolicy, 1)))
	assert.True(t, permission.IsPermissionError(p.UpdateLimitsTx(ctx, nil, string(EPStepsPerPolicy), 1)))
	assert.True(t, permission.IsPermissionError(p.ResetAll(ctx)))
}

func TestDeltaLimitsMaxFunc(t *testing.T) {
	ctx := adminCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	max := e.tables.MaxFunc(RulesPerSchedule)
	assert.Equal(t, -1, max(p.fdb.View()))
	require.NoError(t, p.SetMax(ctx, RulesPerSchedule, 7))
	require.NoError(t, p.SetMax(ctx, TargetsPerSchedule, 8))
	assert.Equal(t, 7, max(p.fdb.View()))
	assert.Equal(t, 8, e.tables.Max(p.fdb.View(), TargetsPerSchedule))
}

func TestDeltaLimitsDefaults(t *testing.T) {
	ctx := adminCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	require.NoError(t, p.SetMax(ctx, EPStepsPerPolicy, 3))
	p.insertDefaults(t, e.tables)

	all, err := p.All(ctx)
	require.NoError(t, err)
	assert.Equal(t, Limits{
		NotificationRulesPerUser:        15,
		ContactMethodsPerUser:           10,
		EPStepsPerPolicy:                3, // set before, so left alone
		EPActionsPerStep:                20,
		ParticipantsPerRotation:         50,
		RulesPerSchedule:                30,
		IntegrationKeysPerService:       30,
		UnackedAlertsPerService:         200,
		TargetsPerSchedule:              10,
		HeartbeatMonitorsPerService:     30,
		UserOverridesPerSchedule:        35,
		CalendarSubscriptionsPerUser:    15,
		PendingSignalsPerService:        50,
		PendingSignalsPerDestPerService: 5,
	}, all, "the limits a new install gets from the migrations")

	// Every known limit has a default, so UpdateLimitsTx can set it.
	for id := range all {
		require.NoError(t, id.Valid())
	}
	require.NoError(t, p.UpdateLimitsTx(ctx, nil, string(PendingSignalsPerService), 60))
	p.insertDefaults(t, e.tables)
	max, err := p.Max(ctx, PendingSignalsPerService)
	require.NoError(t, err)
	assert.Equal(t, 60, max, "inserting the defaults again changes nothing")
}

// TestDeltaLimitsRace has two pods change limits at once. The second to
// commit runs again on the first's change, so neither is lost (F4).
func TestDeltaLimitsRace(t *testing.T) {
	ctx := adminCtx()
	e := newDeltaEnv(t)
	pods := []pod{e.pod(t, "a"), e.pod(t, "b")}
	pods[0].insertDefaults(t, e.tables)

	ids := []ID{ContactMethodsPerUser, RulesPerSchedule, EPStepsPerPolicy, TargetsPerSchedule}
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, pods[i%2].UpdateLimitsTx(ctx, nil, string(id), 100+i))
		}()
	}
	wg.Wait()

	all, err := e.pod(t, "c").All(ctx)
	require.NoError(t, err)
	assert.Len(t, all, len(deltaDefaults))
	for i, id := range ids {
		assert.Equal(t, 100+i, all[id], "%s", id)
	}
}
