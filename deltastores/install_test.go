package deltastores_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/deltastores/deltastorestest"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/heartbeat"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/label"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/override"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/service"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/util/sqlutil"
)

// TestCreateOpen checks that a table is created once, completely, and never by
// opening it (F8, F27).
func TestCreateOpen(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMem()
	backend := func() *storagebackend.Backend {
		return &storagebackend.Backend{Store: store, Log: deltalog.New(store, "w")}
	}
	opts := filestore.DeltaOptions{PollInterval: -1, Logf: func(string, ...any) {}}
	_, _, err := deltastores.Open(ctx, backend(), opts)
	require.ErrorIs(t, err, tablelog.ErrNoTable)

	// Interrupted right after the table itself.
	schema, _ := deltastores.NewSchema()
	require.NoError(t, backend().Create(ctx, schema, opts))
	_, _, err = deltastores.Open(ctx, backend(), opts)
	require.ErrorIs(t, err, deltastores.ErrNotInstalled)
	require.NoError(t, deltastores.Create(ctx, backend(), opts), "finishes it")
	require.ErrorIs(t, deltastores.Create(ctx, backend(), opts), tablelog.ErrTableExists)

	db, tables, err := deltastores.Open(ctx, backend(), opts)
	require.NoError(t, err)
	assert.Equal(t, 10, tables.Limit.Max(db.View(), limit.ContactMethodsPerUser))
	// Without limits, if somebody wants none, it is still complete.
	_, err = db.Update(ctx, func(tx *filestore.Tx) error {
		_, err := tables.Limit.Limits.Delete(tx, string(limit.ContactMethodsPerUser))
		return err
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.ErrorIs(t, deltastores.Create(ctx, backend(), opts), tablelog.ErrTableExists)
	db, tables, err = deltastores.Open(ctx, backend(), opts)
	require.NoError(t, err)
	assert.Equal(t, -1, tables.Limit.Max(db.View(), limit.ContactMethodsPerUser))
	require.NoError(t, db.Close())
}

// TestStores builds an on-call setup on one pod, uses it on another, and takes
// it apart: what one store changes, the others see, in one commit each.
func TestStores(t *testing.T) {
	in := deltastorestest.New(t)
	a, b := in.Pod(t), in.Pod(t)
	ctx, admin := a.Ctx(), a.Admin()

	usr, _ := a.AddNotifiedUser("alice")
	other := a.AddUser("bob")
	sched := a.AddSchedule("Primary")
	rot := a.AddRotation("Weekly", rotation.TypeWeekly, 1, in.Clock.Now(), usr, other)
	a.AddScheduleRule(sched, assignment.RotationTarget(rot))
	_, err := a.Override.CreateUserOverrideTx(admin, nil, &override.UserOverride{
		AddUserID: other, Start: time.Now().Add(time.Hour), End: time.Now().Add(2 * time.Hour),
		Target: assignment.ScheduleTarget(sched),
	})
	require.NoError(t, err)
	pol := a.AddPolicy("Policy", 1)
	step := a.AddStep(pol, 5, deltastorestest.ScheduleDest(sched), deltastorestest.Dest("channel"))
	svc := a.AddService("Service", pol)
	key := a.AddKey(svc)
	_, err = a.Heartbeat.CreateTx(admin, nil, &heartbeat.Monitor{Name: "hb", ServiceID: svc, Timeout: 10 * time.Minute})
	require.NoError(t, err)
	require.NoError(t, a.Label.SetTx(admin, nil, &label.Label{Target: assignment.ServiceTarget(svc), Key: "example/team", Value: "core"}))
	require.NoError(t, a.Favorite.Set(ctx, nil, usr, assignment.ServiceTarget(svc)))
	b.Sync()

	// The other pod sees it all, joined across packages.
	found, err := b.Service.Search(b.As(usr, permission.RoleUser), &service.SearchOptions{Search: "example/team=core", FavoritesUserID: usr})
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.True(t, found[0].IsUserFavorite())
	byToken, err := b.Service.Search(admin, &service.SearchOptions{Search: "token=" + key})
	require.NoError(t, err)
	require.Len(t, byToken, 1)
	scheds, err := b.Schedule.FindManyByUserID(ctx, nil, uuid.NullUUID{UUID: uuid.MustParse(other), Valid: true})
	require.NoError(t, err)
	assert.Len(t, scheds, 1, "through the rotation")
	shifts, err := b.OnCall.HistoryBySchedule(admin, sched, in.Clock.Now(), in.Clock.Now().Add(48*time.Hour))
	require.NoError(t, err)
	assert.NotEmpty(t, shifts)
	dests, err := b.Escalation.FindAllStepActionsTx(admin, nil, uuid.MustParse(step))
	require.NoError(t, err)
	assert.Len(t, dests, 2)

	// An integration's alert escalates by the policy from the start (F2).
	intCtx := permission.ServiceSourceContext(context.Background(), svc, &permission.SourceInfo{Type: permission.SourceTypeIntegrationKey, ID: key})
	al, isNew, err := b.Alert.CreateOrUpdate(intCtx, &alert.Alert{Summary: "down", ServiceID: svc, Source: alert.SourceGeneric, Status: alert.StatusTriggered})
	require.NoError(t, err)
	assert.True(t, isNew)
	st, ok := b.Tables.Escalation.AlertState(b.View(), int64(al.ID))
	require.True(t, ok)
	assert.Equal(t, pol, st.PolicyID)
	logs, err := b.AlertLog.Search(ctx, &alertlog.SearchOptions{FilterAlertIDs: []int{al.ID}})
	require.NoError(t, err)
	require.NotEmpty(t, logs)
	assert.Contains(t, logs[len(logs)-1].String(ctx), "key of", "the name of the integration key")
	a.Sync()
	// Asking for escalation of one that is acknowledged triggers it.
	require.NoError(t, a.Alert.UpdateStatus(admin, al.ID, alert.StatusActive))
	require.NoError(t, a.Alert.EscalateAsOf(admin, al.ID, in.Clock.Now()))
	got, err := a.Alert.FindOne(admin, al.ID)
	require.NoError(t, err)
	assert.Equal(t, alert.StatusTriggered, got.Status)

	// Limits hold, with Postgres's name for them.
	require.NoError(t, a.Limit.SetMax(admin, limit.HeartbeatMonitorsPerService, 1))
	_, err = a.Heartbeat.CreateTx(admin, nil, &heartbeat.Monitor{Name: "hb2", ServiceID: svc, Timeout: 10 * time.Minute})
	require.Error(t, err)
	assert.Equal(t, "heartbeat_monitors_per_service_limit", sqlutil.MapError(err).ConstraintName)

	// What is in use can't be deleted, and deleting a user leaves nothing.
	require.Error(t, a.Escalation.DeleteManyPoliciesTx(admin, nil, []string{pol}))
	require.NoError(t, a.User.DeleteManyTx(admin, nil, []string{other}))
	parts, err := a.Rotation.FindAllParticipantsTx(admin, nil, rot)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, usr, parts[0].Target.TargetID())
	require.NoError(t, a.Service.DeleteManyTx(admin, nil, []string{svc}))
	r := b.View()
	assert.Zero(t, b.Tables.Alert.Alerts.Len(r))
	assert.Zero(t, b.Tables.Escalation.States.Len(r))
	assert.Zero(t, b.Tables.IntegrationKey.Keys.Len(r))
	assert.Zero(t, b.Tables.Heartbeat.Monitors.Len(r))
	assert.Zero(t, b.Tables.Override.Overrides.Len(r))
	require.NoError(t, b.Escalation.DeleteManyPoliciesTx(admin, nil, []string{pol}))
	assert.Zero(t, b.Tables.Escalation.Actions.Len(b.View()))

	u, err := url.Parse(b.Config.Config().PublicURL())
	require.NoError(t, err)
	assert.Equal(t, "localhost", u.Host)
}

// TestReEncryptAll checks that a new key replaces the old one, all at once.
func TestReEncryptAll(t *testing.T) {
	in := deltastorestest.New(t)
	p := in.Pod(t)
	require.NoError(t, p.Config.SetConfig(p.Ctx(), p.Config.Config()))
	newKeys := keyring.Keys{[]byte("new"), []byte("key")}
	require.True(t, permission.IsPermissionError(p.Tables.ReEncryptAll(context.Background(), p.DB, newKeys)))
	require.Error(t, p.Tables.ReEncryptAll(p.Admin(), p.DB, nil))
	before := p.Delta.View().Index()
	require.Error(t, p.Tables.ReEncryptAll(p.Admin(), p.DB, keyring.Keys{[]byte("wrong")}))
	assert.Equal(t, before, p.Delta.View().Index())
	require.NoError(t, p.Tables.ReEncryptAll(p.Admin(), p.DB, newKeys))
	assert.Equal(t, before+1, p.Delta.View().Index())
	// The old key alone does not do any more, and the new one does.
	require.Error(t, p.Tables.ReEncryptAll(p.Admin(), p.DB, keyring.Keys{[]byte("key")}))
	require.NoError(t, p.Tables.ReEncryptAll(p.Admin(), p.DB, keyring.Keys{[]byte("new")}))
}
