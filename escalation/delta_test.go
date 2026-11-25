package escalation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// Stand-ins for the other packages' tables.
type (
	idRow struct {
		ID string `parquet:"id"`
	}
	channelRow struct {
		ID   string `parquet:"id"`
		Name string `parquet:"name"`
	}
	serviceRow struct {
		ID       string `parquet:"id"`
		PolicyID string `parquet:"escalation_policy_id"`
	}
	alertRow struct {
		ID        int64  `parquet:"id"`
		ServiceID string `parquet:"service_id"`
		Status    string `parquet:"status"`
	}
	logRow struct {
		ID       int64  `parquet:"id"`
		PolicyID string `parquet:"policy_id"`
		Type     string `parquet:"type"`
	}
	limitRow struct {
		ID  string `parquet:"id"`
		Max int32  `parquet:"max"`
	}
	onCallRow struct {
		StepID string `parquet:"ep_step_id"`
		UserID string `parquet:"user_id"`
	}
	favRow struct {
		UserID   string `parquet:"user_id"`
		PolicyID string `parquet:"tgt_escalation_policy_id"`
	}
)

type deltaEnv struct {
	*DeltaTables
	store  objstore.Store
	schema *filestore.Schema
	clock  *filestore.FakeClock
	reg    *nfydest.Registry

	users, schedules, rotations *filestore.Table[idRow]
	channels                    *filestore.Table[channelRow]
	services                    *filestore.Table[serviceRow]
	servicesByPolicy            *filestore.Index[serviceRow]
	alerts                      *filestore.Table[alertRow]
	alertsByService             *filestore.Index[alertRow]
	logs                        *filestore.Table[logRow]
	limits                      *filestore.Table[limitRow]
	onCall                      *filestore.Table[onCallRow]
	favs                        *filestore.Table[favRow]

	lookups DeltaLookups
	logErr  atomic.Pointer[error] // what LogEP returns, if set
}

// provider is a destination type that can be an action, unless it starts with
// "no-".
type provider string

func (p provider) ID() string { return string(p) }
func (p provider) TypeInfo(context.Context) (*nfydest.TypeInfo, error) {
	return &nfydest.TypeInfo{Type: string(p), Enabled: true, SupportsAlertNotifications: !strings.HasPrefix(string(p), "no-")}, nil
}
func (provider) ValidateField(context.Context, string, string) error { return nil }
func (provider) DisplayInfo(context.Context, map[string]string) (*nfydest.DisplayInfo, error) {
	return &nfydest.DisplayInfo{}, nil
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	s := filestore.NewSchema()
	byID := func(r idRow) string { return r.ID }
	e := &deltaEnv{
		DeltaTables: DefineDeltaTables(s),
		store:       objstore.NewMem(), schema: s, clock: clock, reg: nfydest.NewRegistry(),
		users:     filestore.NewTable(s, "users", byID),
		schedules: filestore.NewTable(s, "schedules", byID),
		rotations: filestore.NewTable(s, "rotations", byID),
		channels:  filestore.NewTable(s, "notification_channels", func(r channelRow) string { return r.ID }),
		services:  filestore.NewTable(s, "services", func(r serviceRow) string { return r.ID }),
		alerts:    filestore.NewTable(s, "alerts", func(r alertRow) string { return filestore.IntKey(r.ID) }),
		logs:      filestore.NewTable(s, "alert_logs", func(r logRow) string { return filestore.IntKey(r.ID) }),
		limits:    filestore.NewTable(s, "config_limits", func(r limitRow) string { return r.ID }),
		onCall:    filestore.NewTable(s, "ep_step_on_call_users", func(r onCallRow) string { return r.StepID + "/" + r.UserID }),
		favs:      filestore.NewTable(s, "user_favorites", func(r favRow) string { return r.UserID + "/" + r.PolicyID }),
	}
	e.servicesByPolicy = e.services.Index("policy", func(r serviceRow) []string { return []string{r.PolicyID} })
	e.alertsByService = e.alerts.Index("service", func(r alertRow) []string { return []string{r.ServiceID} })
	DeclareDeltaForeignKeys(e.DeltaTables, e.alerts, e.services, e.users, e.schedules, e.rotations, e.channels)

	// What the other packages declare.
	filestore.References(e.services, "services_escalation_policy_id_fkey", e.Policies,
		func(r serviceRow) (string, bool) { return r.PolicyID, true }, filestore.Restrict, nil)
	filestore.References(e.alerts, "alerts_services_id_fkey", e.services,
		func(r alertRow) (string, bool) { return r.ServiceID, true }, filestore.Cascade, nil)
	filestore.References(e.onCall, "ep_step_on_call_users_ep_step_id_fkey", e.Steps,
		func(r onCallRow) (string, bool) { return r.StepID, true }, filestore.Cascade, nil)
	filestore.References(e.favs, "user_favorites_tgt_escalation_policy_id_fkey", e.Policies,
		func(r favRow) (string, bool) { return r.PolicyID, true }, filestore.Cascade, nil)

	ctx := context.Background()
	for _, id := range []string{user.DestTypeUser, schedule.DestTypeSchedule, rotation.DestTypeRotation, "channel", "no-target"} {
		e.reg.RegisterProvider(ctx, provider(id))
	}

	limit := func(id string) func(r filestore.Reader) int {
		return func(r filestore.Reader) int {
			if row, ok := e.limits.Get(r, id); ok {
				return int(row.Max)
			}
			return -1
		}
	}
	e.lookups = DeltaLookups{
		StepLimit:   limit("ep_steps_per_policy"),
		ActionLimit: limit("ep_actions_per_step"),
		IsFavorite: func(r filestore.Reader, userID, policyID string) bool {
			_, ok := e.favs.Get(r, userID+"/"+policyID)
			return ok
		},
		OnCallSteps: func(r filestore.Reader, userID string) []string {
			var ids []string
			e.onCall.Ascend(r, func(row onCallRow) bool {
				if row.UserID == userID {
					ids = append(ids, row.StepID)
				}
				return true
			})
			return ids
		},
		ChannelDest: func(r filestore.Reader, channelID string) (gadb.DestV1, error) {
			row, ok := e.channels.Get(r, channelID)
			if !ok {
				return gadb.DestV1{}, sql.ErrNoRows
			}
			return channelDest(row.Name), nil
		},
		ServicePolicy: func(r filestore.Reader, serviceID string) (string, bool) {
			row, ok := e.services.Get(r, serviceID)
			return row.PolicyID, ok
		},
		PolicyServices: func(r filestore.Reader, policyID string) (ids []string) {
			e.servicesByPolicy.Ascend(r, policyID, func(row serviceRow) bool { ids = append(ids, row.ID); return true })
			return ids
		},
		AlertService: func(r filestore.Reader, alertID int64) (string, bool) {
			row, ok := e.alerts.Get(r, filestore.IntKey(alertID))
			return row.ServiceID, ok
		},
		OpenAlerts: func(r filestore.Reader, serviceID string) (ids []int64) {
			e.alertsByService.Ascend(r, serviceID, func(row alertRow) bool {
				if row.Status != "closed" {
					ids = append(ids, row.ID)
				}
				return true
			})
			return ids
		},
		TriggerAlert: func(tx *filestore.Tx, alertID int64) error {
			row, ok := e.alerts.Get(tx, filestore.IntKey(alertID))
			if !ok {
				return nil
			}
			if row.Status == "closed" {
				return errors.New("cannot change status of closed alert")
			}
			row.Status = "triggered"
			return e.alerts.Put(tx, row)
		},
		MapDestToID: func(_ context.Context, tx txn.Tx, dest gadb.DestV1) (uuid.UUID, error) {
			ftx, _ := deltatxn.Unwrap(tx)
			id := channelID(dest.Arg("name"))
			return id, e.channels.Put(ftx, channelRow{ID: id.String(), Name: dest.Arg("name")})
		},
		LookupDestID: func(_ context.Context, tx txn.Tx, dest gadb.DestV1) (uuid.UUID, error) {
			_, r := deltatxn.Unwrap(tx)
			id := channelID(dest.Arg("name"))
			if _, ok := e.channels.Get(r, id.String()); !ok {
				return uuid.Nil, sql.ErrNoRows
			}
			return id, nil
		},
		LogEP: func(_ context.Context, tx txn.Tx, policyID string, typ alertlog.Type, _ *alertlog.EscalationMetaData) error {
			if err := e.logErr.Load(); err != nil {
				return *err
			}
			ftx, _ := deltatxn.Unwrap(tx)
			id, err := ftx.NextID("alert_logs_id_seq")
			if err != nil {
				return err
			}
			return e.logs.Put(ftx, logRow{ID: id, PolicyID: policyID, Type: string(typ)})
		},
	}
	require.NoError(t, filestore.CreateDelta(ctx, e.store, s, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

func channelID(name string) uuid.UUID { return uuid.NewSHA1(uuid.Nil, []byte(name)) }

func channelDest(name string) gadb.DestV1 { return gadb.NewDestV1("channel", "name", name) }

// pod is the escalation policy store of one GoAlert pod.
type pod struct {
	*Store
	env *deltaEnv
	fdb *filestore.DeltaDB
	db  txn.DB
}

func (e *deltaEnv) pod(t *testing.T, name string) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &pod{Store: NewDeltaStore(db, e.DeltaTables, e.reg, e.lookups), env: e, fdb: fdb, db: db}
}

// write runs fn in a transaction.
func (p *pod) write(fn func(tx *filestore.Tx) error) error {
	_, err := p.fdb.Update(context.Background(), fn)
	return err
}

// put adds rows of the stand-in tables.
func put[V any](t *testing.T, p *pod, table *filestore.Table[V], rows ...V) {
	t.Helper()
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		for _, row := range rows {
			if err := table.Put(tx, row); err != nil {
				return err
			}
		}
		return nil
	}))
}

func (p *pod) addPolicy(t *testing.T, name string) string {
	t.Helper()
	pol, err := p.CreatePolicyTx(asUser(), nil, &Policy{Name: name, Description: "desc"})
	require.NoError(t, err)
	return pol.ID
}

func (p *pod) addSteps(t *testing.T, policyID string, n int) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	for range n {
		st, err := p.CreateStepTx(asUser(), nil, &Step{PolicyID: policyID, DelayMinutes: 5})
		require.NoError(t, err)
		ids = append(ids, st.ID)
	}
	return ids
}

// addService adds a service, and an open alert for each id.
func (p *pod) addService(t *testing.T, policyID string, alertIDs ...int64) string {
	t.Helper()
	id := uuid.NewString()
	put(t, p, p.env.services, serviceRow{ID: id, PolicyID: policyID})
	for _, a := range alertIDs {
		p.addAlert(t, a, id, "triggered")
	}
	return id
}

// addAlert adds an alert like the alert store: its state is the trigger's.
func (p *pod) addAlert(t *testing.T, id int64, serviceID, status string) {
	t.Helper()
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		if err := p.env.alerts.Put(tx, alertRow{ID: id, ServiceID: serviceID, Status: status}); err != nil {
			return err
		}
		if status == "closed" {
			return nil
		}
		return p.env.InsertAlertState(tx, id, serviceID)
	}))
}

func (p *pod) stepIDs(t *testing.T, policyID string) []uuid.UUID {
	t.Helper()
	steps, err := p.FindAllSteps(asUser(), policyID)
	require.NoError(t, err)
	var ids []uuid.UUID
	for i, st := range steps {
		assert.Equal(t, i, st.StepNumber, "no gap in the numbers")
		ids = append(ids, st.ID)
	}
	return ids
}

func (p *pod) alertStatus(id int64) string {
	row, _ := p.env.alerts.Get(p.fdb.View(), filestore.IntKey(id))
	return row.Status
}

// check opens the table as a new pod, which rebuilds the state from storage,
// and checks what must hold between the rows.
func (e *deltaEnv) check(t *testing.T) *pod {
	t.Helper()
	p := e.pod(t, "check")
	r := p.fdb.View()
	e.Policies.Ascend(r, func(pol PolicyRow) bool {
		steps := e.PolicySteps(r, pol.ID)
		assert.EqualValues(t, len(steps), pol.StepCount, "step_count")
		for i, st := range steps {
			assert.EqualValues(t, i, st.StepNumber, "step_number")
		}
		return true
	})
	e.States.Ascend(r, func(st StateRow) bool {
		svc, _ := e.services.Get(r, st.ServiceID)
		assert.Equal(t, svc.PolicyID, st.PolicyID, "svc_ep_fkey")
		return true
	})
	return p
}

var alice = uuid.NewString()

func asUser() context.Context {
	return permission.UserContext(context.Background(), alice, permission.RoleUser)
}

func fieldErrors(t *testing.T, err error) string {
	t.Helper()
	mapped := errutil.MapDBError(err)
	require.Error(t, mapped)
	require.True(t, validation.IsClientError(mapped), "the user can fix it: %v", mapped)
	return mapped.Error()
}

func TestDeltaPolicies(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")

	none, err := p.FindManyPolicies(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.Nil(t, none)

	a, err := p.CreatePolicyTx(ctx, nil, &Policy{Name: "First", Description: "one", Repeat: 2})
	require.NoError(t, err)
	b, err := p.CreatePolicyTx(ctx, nil, &Policy{Name: "Second", Description: "two"})
	require.NoError(t, err)
	_, err = p.CreatePolicyTx(ctx, nil, &Policy{Name: "First", Description: "x"})
	assert.Contains(t, fieldErrors(t, err), "already in use")
	_, err = p.CreatePolicyTx(ctx, nil, &Policy{Name: "FIRST", Description: "x"})
	assert.Contains(t, fieldErrors(t, err), "already in use", "names are unique without regard to case")
	_, err = p.CreatePolicyTx(ctx, nil, &Policy{Name: "!", Description: "x"})
	assert.True(t, validation.IsClientError(err), "still validated")
	_, err = p.CreatePolicyTx(context.Background(), nil, &Policy{Name: "Third", Description: "x"})
	assert.True(t, permission.IsPermissionError(err), "still checked")

	put(t, p, e.favs, favRow{UserID: alice, PolicyID: b.ID})
	many, err := p.FindManyPolicies(ctx, []string{b.ID, strings.ToUpper(a.ID), uuid.NewString(), b.ID})
	require.NoError(t, err)
	favB := *b
	favB.isUserFavorite = true
	assert.Equal(t, []Policy{favB, *a}, many, "each policy once")
	many, err = p.FindManyPolicies(permission.SystemContext(context.Background(), "test"), []string{b.ID})
	require.NoError(t, err)
	assert.False(t, many[0].IsUserFavorite(), "no user, no favorites")

	one, err := p.FindOnePolicyTx(ctx, nil, b.ID)
	require.NoError(t, err)
	assert.Equal(t, &favB, one)
	_, err = p.FindOnePolicyTx(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindOnePolicyForUpdateTx(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
		pol, err := p.FindOnePolicyForUpdateTx(ctx, tx, strings.ToUpper(a.ID))
		if err != nil {
			return err
		}
		pol.Name, pol.Description, pol.Repeat = "Renamed", "changed", 5
		return p.UpdatePolicyTx(ctx, tx, pol)
	}))
	one, err = p.FindOnePolicyTx(ctx, nil, a.ID)
	require.NoError(t, err)
	assert.Equal(t, &Policy{ID: a.ID, Name: "Renamed", Description: "changed", Repeat: 5}, one)

	err = p.UpdatePolicyTx(ctx, nil, &Policy{ID: a.ID, Name: "second", Description: "x"})
	assert.Contains(t, fieldErrors(t, err), "already in use")
	require.NoError(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: a.ID, Name: "RENAMED", Description: "x"}), "its own name in another case")
	require.NoError(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: uuid.NewString(), Name: "Nobody", Description: "x"}), "an UPDATE of no rows")
	require.NoError(t, p.DeleteManyPoliciesTx(ctx, nil, []string{uuid.NewString()}), "a DELETE of no rows")

	name, ok := e.EscalationPolicyName(p.fdb.View(), b.ID)
	assert.True(t, ok)
	assert.Equal(t, "Second", name)
	_, ok = e.EscalationPolicyName(p.fdb.View(), uuid.NewString())
	assert.False(t, ok)

	many, err = e.check(t).FindManyPolicies(ctx, []string{a.ID, b.ID})
	require.NoError(t, err)
	require.Len(t, many, 2, "a new pod reads the same")
	assert.Equal(t, "RENAMED", many[0].Name)
}

func TestDeltaDeletePolicy(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, other := p.addPolicy(t, "Gone"), p.addPolicy(t, "Stays")
	put(t, p, e.users, idRow{alice})
	var firstSteps []uuid.UUID
	for _, id := range []string{pol, other} {
		steps := p.addSteps(t, id, 3)
		firstSteps = append(firstSteps, steps[0])
		for _, st := range steps {
			require.NoError(t, p.AddStepActionTx(ctx, nil, st, user.DestFromID(alice)))
			put(t, p, e.onCall, onCallRow{StepID: st.String(), UserID: alice})
		}
		put(t, p, e.favs, favRow{UserID: alice, PolicyID: id})
	}
	svc := p.addService(t, pol, 1)
	p.addService(t, other, 2)
	for i, st := range firstSteps { // as the engine does, so that the states refer to steps
		id := st.String()
		require.NoError(t, p.write(func(tx *filestore.Tx) error {
			_, err := e.UpdateState(tx, int64(i+1), func(row *StateRow) { row.StepID = &id })
			return err
		}))
	}

	err := p.DeleteManyPoliciesTx(ctx, nil, []string{pol})
	assert.Contains(t, fieldErrors(t, err), "is currently in use", "a service uses it")
	assert.Len(t, p.stepIDs(t, pol), 3, "and nothing is deleted")

	// A state whose service is gone too is the hard case: steps, states and
	// policy all go in one transaction.
	require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		if _, err := e.services.Delete(ftx, svc); err != nil {
			return err
		}
		return p.DeleteManyPoliciesTx(ctx, tx, []string{pol})
	}))

	check := e.check(t)
	r := check.fdb.View()
	assert.Equal(t, 1, e.Policies.Len(r))
	assert.Equal(t, 3, e.Steps.Len(r), "escalation_policy_steps_escalation_policy_id_fkey cascades")
	assert.Equal(t, 3, e.Actions.Len(r), "escalation_policy_actions_escalation_policy_step_id_fkey cascades")
	assert.Equal(t, 1, e.States.Len(r))
	assert.Equal(t, 3, e.onCall.Len(r))
	assert.Equal(t, 1, e.favs.Len(r))
	st, ok := e.AlertState(r, 2)
	require.True(t, ok)
	assert.Equal(t, firstSteps[1].String(), *st.StepID, "another policy's are as they were")
}

// TestDeltaDeletePolicyStates deletes a policy while there are states, on and
// off its steps, that only its own foreign key removes.
func TestDeltaDeletePolicyStates(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, next := p.addPolicy(t, "Gone"), p.addPolicy(t, "Next")
	steps := p.addSteps(t, pol, 2)
	svc := p.addService(t, pol, 1, 2)
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		id := steps[1].String()
		_, err := e.UpdateState(tx, 1, func(st *StateRow) { st.StepID = &id })
		return err
	}))

	// The service moves away without its states, so that they still refer to
	// the policy when it goes.
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		if err := e.services.Put(tx, serviceRow{ID: svc, PolicyID: next}); err != nil {
			return err
		}
		_, err := e.Policies.Delete(tx, pol)
		return err
	}))
	r := e.check(t).fdb.View()
	assert.Zero(t, e.States.Len(r), "escalation_policy_state_escalation_policy_id_fkey cascades")
	assert.Zero(t, e.Steps.Len(r))
}

func TestDeltaSteps(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, other := p.addPolicy(t, "Policy"), p.addPolicy(t, "Other")
	otherSteps := p.addSteps(t, other, 2)

	steps, err := p.FindAllSteps(ctx, pol)
	require.NoError(t, err)
	assert.Nil(t, steps)

	for i := range 4 {
		st, err := p.CreateStepTx(ctx, nil, &Step{PolicyID: strings.ToUpper(pol), DelayMinutes: i + 1, MultiAck: i == 1, StepNumber: 7})
		require.NoError(t, err)
		assert.Equal(t, i, st.StepNumber, "trg_inc_ep_step_number_on_insert sets the number")
		steps = append(steps, *st)
		steps[i].PolicyID = pol // as the rows have it
	}
	got, err := p.FindAllSteps(ctx, pol)
	require.NoError(t, err)
	assert.Equal(t, steps, got)
	one, err := p.FindOneStepForUpdateTx(ctx, nil, steps[1].ID.String())
	require.NoError(t, err)
	assert.Equal(t, &steps[1], one)
	_, err = p.FindOneStepForUpdateTx(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	_, err = p.CreateStepTx(ctx, nil, &Step{PolicyID: uuid.NewString(), DelayMinutes: 1})
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "escalation_policy_steps_escalation_policy_id_fkey", fkErr.ForeignKey)
	_, err = p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 9001})
	assert.True(t, validation.IsClientError(err), "still validated")
	require.Error(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.InsertStep(tx, StepRow{ID: steps[0].ID.String(), PolicyID: pol, Delay: 1})
		return err
	}), "escalation_policy_steps_pkey")

	require.NoError(t, p.UpdateStepDelayTx(ctx, nil, steps[2].ID, 30))
	require.NoError(t, p.UpdateStepMultiAckTx(ctx, nil, steps[2].ID, true))
	require.NoError(t, p.UpdateStepDelayTx(ctx, nil, uuid.New(), 30), "an UPDATE of no rows")
	require.NoError(t, p.UpdateStepMultiAckTx(ctx, nil, uuid.New(), true))
	steps[2].DelayMinutes, steps[2].MultiAck = 30, true
	got, err = p.FindAllSteps(ctx, pol)
	require.NoError(t, err)
	assert.Equal(t, steps, got)

	// Deleting a step closes the gap, and leaves other policies alone.
	id, err := p.DeleteStepTx(ctx, nil, steps[1].ID)
	require.NoError(t, err)
	assert.Equal(t, pol, id)
	assert.Equal(t, []uuid.UUID{steps[0].ID, steps[2].ID, steps[3].ID}, p.stepIDs(t, pol))
	id, err = p.DeleteStepTx(ctx, nil, steps[1].ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	assert.Empty(t, id)
	_, err = p.DeleteStepTx(ctx, nil, steps[0].ID)
	require.NoError(t, err)
	st, err := p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 1})
	require.NoError(t, err)
	assert.Equal(t, 2, st.StepNumber)

	check := e.check(t)
	assert.Equal(t, []uuid.UUID{steps[2].ID, steps[3].ID, st.ID}, check.stepIDs(t, pol))
	assert.Equal(t, otherSteps, check.stepIDs(t, other))

	r := check.fdb.View()
	info, ok := e.Step(r, steps[3].ID.String())
	assert.True(t, ok)
	assert.Equal(t, StepInfo{PolicyID: pol, PolicyName: "Policy", StepNumber: 1}, info)
	_, ok = e.Step(r, steps[0].ID.String())
	assert.False(t, ok)
	row, ok := e.PolicyStep(r, pol, 2)
	assert.True(t, ok)
	assert.Equal(t, st.ID.String(), row.ID)
	_, ok = e.PolicyStep(r, pol, 3)
	assert.False(t, ok)
	assert.True(t, e.PolicyHasSteps(r, pol))
	assert.False(t, e.PolicyHasSteps(r, uuid.NewString()))
}

func TestDeltaStepLimit(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	put(t, p, e.limits, limitRow{ID: "ep_steps_per_policy", Max: 2})
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, 2)

	_, err := p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 1})
	var limitErr errutil.LimitError
	require.ErrorAs(t, errutil.MapDBError(err), &limitErr)
	assert.Equal(t, 2, limitErr.Max())
	assert.EqualValues(t, "ep_steps_per_policy", limitErr.ID())

	// Lowering the limit leaves what there is.
	put(t, p, e.limits, limitRow{ID: "ep_steps_per_policy", Max: 1})
	require.NoError(t, p.UpdateStepDelayTx(ctx, nil, steps[0], 9))
	_, err = p.DeleteStepTx(ctx, nil, steps[0])
	require.NoError(t, err)
	_, err = p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 1})
	require.Error(t, err)
	put(t, p, e.limits, limitRow{ID: "ep_steps_per_policy", Max: -1})
	assert.Len(t, p.addSteps(t, pol, 2), 2, "no limit")
	e.check(t)
}

var errRollback = errors.New("rolled back")

// TestDeltaReorder sets the numbers of a policy's four steps in every order
// that gives a whole policy again, with each step set at most once, as a
// transaction of the GraphQL API does. The result is what setting them one by
// one, and checking them at the end, gives.
func TestDeltaReorder(t *testing.T) {
	const n = 4
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, n)

	type set struct{ step, number int }
	whole := func(numbers []int) bool {
		s := slices.Clone(numbers)
		slices.Sort(s)
		return slices.Equal(s, []int{0, 1, 2, 3})
	}
	tried := 0
	var try func(sets []set, numbers []int)
	try = func(sets []set, numbers []int) {
		if whole(numbers) {
			tried++
			var got [][]StepRow
			err := p.db.Run(ctx, nil, func(tx txn.Tx) error {
				got = nil
				_, r := deltatxn.Unwrap(tx)
				for _, s := range sets {
					if err := p.UpdateStepNumberTx(ctx, tx, steps[s.step], s.number); err != nil {
						return err
					}
					got = append(got, e.PolicySteps(r, pol))
				}
				return errRollback
			})
			require.ErrorIs(t, err, errRollback, "%v", sets)
			for _, rows := range got {
				for i, row := range rows {
					require.EqualValues(t, i, row.StepNumber, "right after each change: %v", sets)
				}
			}
			if len(got) > 0 {
				for _, row := range got[len(got)-1] {
					i := slices.Index(steps, uuid.MustParse(row.ID))
					require.EqualValues(t, numbers[i], row.StepNumber, "%v", sets)
				}
			}
		}
		for step := range n {
			if slices.ContainsFunc(sets, func(s set) bool { return s.step == step }) {
				continue
			}
			for number := range n {
				next := slices.Clone(numbers)
				next[step] = number
				try(append(slices.Clone(sets), set{step, number}), next)
			}
		}
	}
	try(nil, []int{0, 1, 2, 3})
	assert.Equal(t, 749, tried)
	assert.Equal(t, steps, e.check(t).stepIDs(t, pol), "every transaction was rolled back")
}

func TestDeltaStepNumbers(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, 4)
	other := p.addSteps(t, p.addPolicy(t, "Other"), 2)

	// reorder is what updating a policy's steps does: steps that are not
	// listed go, and the others get their place in the list.
	reorder := func(list ...uuid.UUID) error {
		return p.db.Run(ctx, nil, func(tx txn.Tx) error {
			current, err := p.FindAllStepsTx(ctx, tx, pol)
			if err != nil {
				return err
			}
			for _, st := range current {
				if slices.Contains(list, st.ID) {
					continue
				}
				if _, err := p.DeleteStepTx(ctx, tx, st.ID); err != nil {
					return err
				}
			}
			for i, id := range list {
				if err := p.UpdateStepNumberTx(ctx, tx, id, i); err != nil {
					return err
				}
			}
			return nil
		})
	}
	var checkErr *filestore.CheckError
	for _, list := range [][]uuid.UUID{
		{steps[0], steps[0]},
		{steps[1], steps[0], steps[1]},
		{steps[0], steps[1], steps[2], steps[3], steps[0]},
	} {
		require.ErrorAs(t, reorder(list...), &checkErr, "a step listed twice would leave a gap")
		assert.Equal(t, "must not have gap in step_numbers", checkErr.Message)
		assert.Equal(t, steps, p.stepIDs(t, pol), "and nothing changes")
	}
	require.NoError(t, reorder(steps[3], steps[1], steps[2], steps[0]))
	assert.Equal(t, []uuid.UUID{steps[3], steps[1], steps[2], steps[0]}, p.stepIDs(t, pol))
	require.NoError(t, reorder(steps[2], steps[0]))
	assert.Equal(t, []uuid.UUID{steps[2], steps[0]}, p.stepIDs(t, pol))

	// A transaction of its own is checked when it ends.
	require.NoError(t, p.UpdateStepNumberTx(ctx, nil, steps[2], 0), "the number it has")
	var uniqueErr *filestore.UniqueError
	require.ErrorAs(t, p.UpdateStepNumberTx(ctx, nil, steps[2], 1), &uniqueErr)
	assert.Equal(t, "escalation_policy_steps_escalation_policy_id_step_number_key", uniqueErr.Index)
	require.ErrorAs(t, p.UpdateStepNumberTx(ctx, nil, steps[2], 2), &checkErr)
	require.ErrorAs(t, p.UpdateStepNumberTx(ctx, nil, steps[2], -1), &checkErr)
	require.NoError(t, p.UpdateStepNumberTx(ctx, nil, uuid.New(), 5), "an UPDATE of no rows")

	require.NoError(t, reorder())
	check := e.check(t)
	assert.Empty(t, check.stepIDs(t, pol))
	otherPol, _ := e.Steps.Get(check.fdb.View(), other[0].String())
	assert.Equal(t, other, check.stepIDs(t, otherPol.PolicyID), "another policy's steps are as they were")
}

func TestDeltaActions(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, 2)
	step := steps[0]
	userID, schedID, rotID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	put(t, p, e.users, idRow{userID})
	put(t, p, e.schedules, idRow{schedID})
	put(t, p, e.rotations, idRow{rotID})

	actions, err := p.FindAllStepActionsTx(ctx, nil, step)
	require.NoError(t, err)
	assert.NotNil(t, actions, "an empty list, as on Postgres")
	assert.Empty(t, actions)

	dests := []gadb.DestV1{
		channelDest("z"), rotation.DestFromID(rotID), user.DestFromID(userID),
		channelDest("a"), schedule.DestFromID(schedID),
	}
	require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error { // some in one transaction
		for _, dest := range dests[:3] {
			if err := p.AddStepActionTx(ctx, tx, step, dest); err != nil {
				return err
			}
		}
		return nil
	}))
	e.clock.Advance(time.Second)
	for _, dest := range dests[3:] {
		require.NoError(t, p.AddStepActionTx(ctx, nil, step, dest))
		e.clock.Advance(time.Second)
	}
	actions, err = p.FindAllStepActionsTx(ctx, nil, step)
	require.NoError(t, err)
	assert.Equal(t, dests, actions, "in the order they were added")

	for i, msg := range []string{"same destination", "same rotation", "same user", "same destination", "same schedule"} {
		err := p.AddStepActionTx(ctx, nil, step, dests[i])
		assert.Contains(t, fieldErrors(t, err), msg)
		require.NoError(t, p.AddStepActionTx(ctx, nil, steps[1], dests[i]), "another step can have it")
	}

	for _, test := range []struct {
		dest gadb.DestV1
		fkey string
	}{
		{user.DestFromID(uuid.NewString()), "escalation_policy_actions_user_id_fkey"},
		{schedule.DestFromID(uuid.NewString()), "escalation_policy_actions_schedule_id_fkey1"},
		{rotation.DestFromID(uuid.NewString()), "escalation_policy_actions_rotation_id_fkey"},
	} {
		var fkErr *filestore.ForeignKeyError
		require.ErrorAs(t, p.AddStepActionTx(ctx, nil, step, test.dest), &fkErr)
		assert.Equal(t, test.fkey, fkErr.ForeignKey)
		require.NoError(t, p.DeleteStepActionTx(ctx, nil, step, test.dest), "a DELETE of no rows")
	}
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, p.AddStepActionTx(ctx, nil, uuid.New(), user.DestFromID(userID)), &fkErr)
	assert.Equal(t, "escalation_policy_actions_escalation_policy_step_id_fkey", fkErr.ForeignKey)
	none := uuid.NewString()
	require.ErrorAs(t, p.write(func(tx *filestore.Tx) error {
		return e.InsertAction(tx, ActionRow{StepID: step.String(), ChannelID: &none})
	}), &fkErr)
	assert.Equal(t, "escalation_policy_actions_channel_id_fkey", fkErr.ForeignKey)

	assert.True(t, validation.IsClientError(p.AddStepActionTx(ctx, nil, step, user.DestFromID("bob"))), "still validated")
	assert.True(t, validation.IsClientError(p.DeleteStepActionTx(ctx, nil, step, user.DestFromID("bob"))))
	assert.True(t, validation.IsClientError(p.AddStepActionTx(ctx, nil, step, gadb.NewDestV1("no-target"))))
	require.ErrorIs(t, p.AddStepActionTx(ctx, nil, step, gadb.NewDestV1("unknown")), nfydest.ErrUnknownType)
	require.ErrorIs(t, p.DeleteStepActionTx(ctx, nil, step, channelDest("unknown")), sql.ErrNoRows, "the channel doesn't exist")

	for _, i := range []int{4, 0, 2} {
		require.NoError(t, p.DeleteStepActionTx(ctx, nil, step, dests[i]))
	}
	check := e.check(t)
	actions, err = check.FindAllStepActionsTx(ctx, nil, step)
	require.NoError(t, err)
	assert.Equal(t, []gadb.DestV1{dests[1], dests[3]}, actions)
	actions, err = check.FindAllStepActionsTx(ctx, nil, steps[1])
	require.NoError(t, err)
	assert.Equal(t, dests, actions, "another step's are as they were")

	// Deleting a target removes its actions, and only those.
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.users.Delete(tx, userID)
		return err
	}))
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.schedules.Delete(tx, schedID)
		return err
	}))
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.rotations.Delete(tx, rotID)
		return err
	}))
	check = e.check(t)
	actions, err = check.FindAllStepActionsTx(ctx, nil, steps[1])
	require.NoError(t, err)
	assert.Equal(t, []gadb.DestV1{dests[0], dests[3]}, actions, "a user, a rotation and a schedule are gone")
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.channels.Delete(tx, channelID("z").String())
		return err
	}))
	actions, err = e.check(t).FindAllStepActionsTx(ctx, nil, steps[1])
	require.NoError(t, err)
	assert.Equal(t, []gadb.DestV1{dests[3]}, actions, "and a channel")
}

func TestDeltaActionTargets(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	step := p.addSteps(t, p.addPolicy(t, "Policy"), 1)[0].String()
	userID, schedID := uuid.NewString(), uuid.NewString()
	put(t, p, e.users, idRow{userID})
	put(t, p, e.schedules, idRow{schedID})

	for _, row := range []ActionRow{
		{StepID: step},
		{StepID: step, UserID: &userID, ScheduleID: &schedID},
	} {
		err := p.write(func(tx *filestore.Tx) error { return e.InsertAction(tx, row) })
		var checkErr *filestore.CheckError
		require.ErrorAs(t, err, &checkErr)
		assert.Equal(t, "epa_there_can_only_be_one", checkErr.Constraint)
	}
	id := uuid.NewString()
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		return e.InsertAction(tx, ActionRow{ID: id, StepID: step, UserID: &userID})
	}))
	err := p.write(func(tx *filestore.Tx) error {
		return e.InsertAction(tx, ActionRow{ID: id, StepID: step, ScheduleID: &schedID})
	})
	var uniqueErr *filestore.UniqueError
	require.ErrorAs(t, err, &uniqueErr)
	assert.Equal(t, "escalation_policy_actions_pkey", uniqueErr.Index)
}

func TestDeltaActionLimit(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	put(t, p, e.limits, limitRow{ID: "ep_actions_per_step", Max: 2})
	step := p.addSteps(t, p.addPolicy(t, "Policy"), 1)[0]
	require.NoError(t, p.AddStepActionTx(ctx, nil, step, channelDest("a")))
	require.NoError(t, p.AddStepActionTx(ctx, nil, step, channelDest("b")))

	var limitErr errutil.LimitError
	require.ErrorAs(t, errutil.MapDBError(p.AddStepActionTx(ctx, nil, step, channelDest("c"))), &limitErr)
	assert.Equal(t, 2, limitErr.Max())
	assert.EqualValues(t, "ep_actions_per_step", limitErr.ID())

	// At the limit, one can be replaced in one transaction.
	require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.DeleteStepActionTx(ctx, tx, step, channelDest("a")); err != nil {
			return err
		}
		return p.AddStepActionTx(ctx, tx, step, channelDest("c"))
	}))
	actions, err := e.check(t).FindAllStepActionsTx(ctx, nil, step)
	require.NoError(t, err)
	assert.Equal(t, []gadb.DestV1{channelDest("b"), channelDest("c")}, actions)

	put(t, p, e.limits, limitRow{ID: "ep_actions_per_step", Max: -1})
	require.NoError(t, p.AddStepActionTx(ctx, nil, step, channelDest("d")), "no limit")
}

func TestDeltaPoliciesBySchedule(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	sched, other := uuid.NewString(), uuid.NewString()
	put(t, p, e.schedules, idRow{sched}, idRow{other})
	var pols []string
	for i := range 3 {
		pol := p.addPolicy(t, fmt.Sprintf("Policy %d", i))
		pols = append(pols, pol)
		for _, st := range p.addSteps(t, pol, 2) {
			require.NoError(t, p.AddStepActionTx(ctx, nil, st, schedule.DestFromID(other)))
			if i != 1 {
				require.NoError(t, p.AddStepActionTx(ctx, nil, st, schedule.DestFromID(sched)))
			}
		}
	}
	put(t, p, e.favs, favRow{UserID: alice, PolicyID: pols[0]})

	want := []string{pols[0], pols[2]}
	slices.Sort(want)
	got, err := e.check(t).FindAllPoliciesBySchedule(ctx, strings.ToUpper(sched))
	require.NoError(t, err)
	require.Len(t, got, 2, "each policy once")
	assert.Equal(t, want, []string{got[0].ID, got[1].ID})
	assert.Equal(t, "desc", got[0].Description)
	assert.False(t, got[0].IsUserFavorite() || got[1].IsUserFavorite(), "not filled in, as on Postgres")
	got, err = p.FindAllPoliciesBySchedule(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDeltaOnCallSteps(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pols := []string{p.addPolicy(t, "One"), p.addPolicy(t, "Two")}
	slices.Sort(pols)
	a, b := p.addSteps(t, pols[0], 3), p.addSteps(t, pols[1], 2)
	for _, id := range []uuid.UUID{b[1], a[2], a[0]} {
		put(t, p, e.onCall, onCallRow{StepID: id.String(), UserID: alice})
	}
	put(t, p, e.onCall, onCallRow{StepID: a[1].String(), UserID: uuid.NewString()})

	steps, err := e.check(t).FindAllOnCallStepsForUserTx(ctx, nil, strings.ToUpper(alice))
	require.NoError(t, err)
	var ids []uuid.UUID
	for _, st := range steps {
		ids = append(ids, st.ID)
	}
	assert.Equal(t, []uuid.UUID{a[0], a[2], b[1]}, ids, "by policy and number")
	steps, err = p.FindAllOnCallStepsForUserTx(ctx, nil, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, steps)
}

func TestDeltaSearch(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	ids := make(map[string]string)
	for _, pol := range []Policy{
		{Name: "delta Team", Description: "Pages the 100% team"},
		{Name: "Alpha-Primary", Description: "first_line"},
		{Name: "Charlie's Group", Description: "weekends"},
		{Name: "bravo_on_call", Description: "nights"},
	} {
		got, err := p.CreatePolicyTx(ctx, nil, &pol)
		require.NoError(t, err)
		ids[pol.Name] = got.ID
	}
	put(t, p, e.favs, favRow{UserID: alice, PolicyID: ids["delta Team"]}, favRow{UserID: alice, PolicyID: ids["bravo_on_call"]},
		favRow{UserID: uuid.NewString(), PolicyID: ids["Alpha-Primary"]})

	all := []string{"Alpha-Primary", "bravo_on_call", "Charlie's Group", "delta Team"}
	check := e.check(t)
	for _, test := range []struct {
		name string
		opts SearchOptions
		want []string
	}{
		{"all", SearchOptions{}, all},
		{"limit", SearchOptions{Limit: 2}, all[:2]},
		{"after", SearchOptions{After: SearchCursor{Name: "BRAVO_on_call"}}, all[2:]},
		{"omit", SearchOptions{Omit: []string{strings.ToUpper(ids["Alpha-Primary"])}}, all[1:]},
		{"name contains", SearchOptions{Search: "HA-pri"}, []string{"Alpha-Primary"}},
		{"description contains", SearchOptions{Search: "NIGHT"}, []string{"bravo_on_call"}},
		{"percent is plain", SearchOptions{Search: "100%"}, []string{"delta Team"}},
		{"underscore is plain", SearchOptions{Search: "t_l"}, []string{"Alpha-Primary"}},
		{"words start with, in order", SearchOptions{Search: "br c"}, []string{"bravo_on_call"}},
		{"separators don't matter", SearchOptions{Search: "al/pri"}, []string{"Alpha-Primary"}},
		{"not out of order", SearchOptions{Search: "call on"}, nil},
		{"not inside a word", SearchOptions{Search: "bra vo"}, nil},
		{"not one word twice", SearchOptions{Search: "char char"}, nil},
		{"an apostrophe separates words", SearchOptions{Search: "s gr"}, []string{"Charlie's Group"}},
		{"no words", SearchOptions{Search: "!"}, all},
		{"nothing", SearchOptions{Search: "zulu"}, nil},
		{"favorites only", SearchOptions{FavoritesUserID: alice, FavoritesOnly: true}, []string{"bravo_on_call", "delta Team"}},
		{"favorites first", SearchOptions{FavoritesUserID: alice, FavoritesFirst: true}, []string{"bravo_on_call", "delta Team", "Alpha-Primary", "Charlie's Group"}},
		{"favorites after a favorite", SearchOptions{FavoritesUserID: alice, FavoritesFirst: true, After: SearchCursor{Name: "bravo_on_call", IsFavorite: true}}, []string{"delta Team", "Alpha-Primary", "Charlie's Group"}},
		{"favorites after another", SearchOptions{FavoritesUserID: alice, FavoritesFirst: true, After: SearchCursor{Name: "Alpha-Primary"}}, []string{"Charlie's Group"}},
		{"another user's favorites", SearchOptions{FavoritesUserID: uuid.NewString(), FavoritesOnly: true}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := ctx
			if test.opts.FavoritesUserID != alice && test.opts.FavoritesUserID != "" {
				ctx = permission.UserContext(context.Background(), test.opts.FavoritesUserID, permission.RoleUser)
			}
			pols, err := check.Search(ctx, &test.opts)
			require.NoError(t, err)
			var names []string
			for _, pol := range pols {
				names = append(names, pol.Name)
				fav := test.opts.FavoritesUserID == alice && (pol.Name == "delta Team" || pol.Name == "bravo_on_call")
				assert.Equal(t, fav, pol.IsUserFavorite())
			}
			assert.Equal(t, test.want, names)
			if test.want == nil {
				assert.Nil(t, pols)
			}
		})
	}
	pols, err := check.Search(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, pols, 4)
	_, err = check.Search(ctx, &SearchOptions{FavoritesOnly: true})
	assert.True(t, validation.IsClientError(err), "still validated")
}

func TestDeltaAlertStates(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, empty := p.addPolicy(t, "Policy"), p.addPolicy(t, "Empty")
	p.addSteps(t, pol, 2)
	svc := p.addService(t, pol, 1, 2)
	idle := p.addService(t, empty, 3, 4)
	p.addAlert(t, 5, idle, "closed")
	r := p.fdb.View()
	st, ok := e.AlertState(r, 1)
	require.True(t, ok)
	assert.Equal(t, StateRow{AlertID: 1, ID: 1, ServiceID: svc, PolicyID: pol}, st, "trg_10_insert_ep_state_on_alert_insert")
	_, ok = e.AlertState(r, 3)
	assert.False(t, ok, "a policy without steps has nothing to escalate to")
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.InsertAlertState(tx, 9, uuid.NewString()) }), "no such service")

	// trg_10_insert_ep_state_on_step_insert: open alerts start with the
	// policy's first step.
	first := p.addSteps(t, empty, 1)[0]
	r = p.fdb.View()
	st, ok = e.AlertState(r, 4)
	require.True(t, ok)
	assert.Equal(t, idle, st.ServiceID)
	assert.Equal(t, empty, st.PolicyID)
	assert.Nil(t, st.LastEscalation)
	_, ok = e.AlertState(r, 5)
	assert.False(t, ok, "closed")
	assert.Equal(t, 4, e.States.Len(r))
	p.addSteps(t, empty, 1)
	assert.Equal(t, 4, e.States.Len(p.fdb.View()), "the second step starts nothing")

	// Deleting the step an alert is on leaves it on none
	// (escalation_policy_state_escalation_policy_step_id_fkey).
	at := start.Add(time.Minute)
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		id := first.String()
		_, err := e.UpdateState(tx, 4, func(st *StateRow) {
			st.StepID, st.StepNumber, st.LoopCount, st.LastEscalation, st.NextEscalation = &id, 0, 3, &at, &at
		})
		return err
	}))
	_, err := p.DeleteStepTx(asUser(), nil, first)
	require.NoError(t, err)
	st, _ = e.AlertState(e.check(t).fdb.View(), 4)
	assert.Nil(t, st.StepID)
	assert.EqualValues(t, 3, st.LoopCount, "the rest stays")
	assert.Equal(t, &at, st.LastEscalation)

	// Deleting the last step, and adding one again before the engine has
	// cleaned up, keeps the state there is.
	_, err = p.DeleteStepTx(asUser(), nil, p.stepIDs(t, empty)[0])
	require.NoError(t, err)
	p.addSteps(t, empty, 1)
	st, _ = e.AlertState(p.fdb.View(), 4)
	assert.Equal(t, &at, st.LastEscalation)

	// trg_10_clear_ep_state_on_alert_close.
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.DeleteAlertState(tx, 1) }))
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.DeleteAlertState(tx, 1) }), "a DELETE of no rows")
	_, ok = e.AlertState(p.fdb.View(), 1)
	assert.False(t, ok)

	// trg_20_clear_next_esc_on_alert_ack.
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.ClearNextEscalation(tx, 4) }))
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.ClearNextEscalation(tx, 1) }), "an UPDATE of no rows")
	st, _ = e.AlertState(p.fdb.View(), 4)
	assert.Nil(t, st.NextEscalation)
	assert.Equal(t, &at, st.LastEscalation)

	// escalation_policy_state_alert_id_fkey and _service_id_fkey cascade.
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.alerts.Delete(tx, filestore.IntKey(2))
		return err
	}))
	_, ok = e.AlertState(p.fdb.View(), 2)
	assert.False(t, ok)
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, idle)
		return err
	}))
	assert.Zero(t, e.States.Len(e.check(t).fdb.View()))
}

func TestDeltaInsertState(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, other, gone := p.addPolicy(t, "Policy"), p.addPolicy(t, "Other"), uuid.NewString()
	svc := p.addService(t, pol, 1, 2, 3)

	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		return e.InsertState(tx, StateRow{AlertID: 1, PolicyID: pol})
	}))
	st, _ := e.AlertState(p.fdb.View(), 1)
	assert.Equal(t, svc, st.ServiceID, "trg_10_set_ep_state_svc_id_on_insert")
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		return e.InsertState(tx, StateRow{AlertID: 2, ServiceID: svc, PolicyID: pol, ID: 40})
	}))

	for _, test := range []struct {
		row        StateRow
		constraint string
	}{
		{StateRow{AlertID: 1, ServiceID: svc, PolicyID: pol}, "escalation_policy_state_pkey"},
		{StateRow{AlertID: 3, ServiceID: svc, PolicyID: pol, ID: 40}, "escalation_policy_state_uniq_id"},
		{StateRow{AlertID: 3, ServiceID: svc, PolicyID: other}, "svc_ep_fkey"},
		{StateRow{AlertID: 3, ServiceID: gone, PolicyID: pol}, "svc_ep_fkey"},
		{StateRow{AlertID: 99, ServiceID: svc, PolicyID: pol}, "escalation_policy_state_alert_id_fkey"},
		{StateRow{AlertID: 3, ServiceID: svc, PolicyID: pol, StepID: &gone}, "escalation_policy_state_escalation_policy_step_id_fkey"},
	} {
		err := p.write(func(tx *filestore.Tx) error { return e.InsertState(tx, test.row) })
		var c interface{ ConstraintName() string }
		require.ErrorAs(t, err, &c, test.constraint)
		assert.Equal(t, test.constraint, c.ConstraintName())
	}
	var fkErr *filestore.ForeignKeyError
	err := p.write(func(tx *filestore.Tx) error {
		_, err := e.UpdateState(tx, 1, func(st *StateRow) { st.PolicyID = other })
		return err
	})
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "svc_ep_fkey", fkErr.ForeignKey)
	assert.Equal(t, 2, e.States.Len(e.check(t).fdb.View()))
}

func TestDeltaForceEscalation(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, 1)
	svc := p.addService(t, pol, 1)
	p.addAlert(t, 2, svc, "active")

	force := func(id int64) bool {
		t.Helper()
		var ok bool
		require.NoError(t, p.write(func(tx *filestore.Tx) (err error) {
			ok, err = e.ForceEscalation(tx, id)
			return err
		}))
		return ok
	}
	assert.True(t, force(2))
	assert.Equal(t, "triggered", p.alertStatus(2), "trg_30_trig_alert_on_force_escalation")
	assert.False(t, force(2), "already")
	assert.False(t, force(3), "no state")

	// The trigger runs only when the flag is set anew.
	put(t, p, e.alerts, alertRow{ID: 2, ServiceID: svc, Status: "active"})
	at := start
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.UpdateState(tx, 2, func(st *StateRow) { st.LastEscalation = &at })
		return err
	}))
	assert.Equal(t, "active", p.alertStatus(2))
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.UpdateState(tx, 2, func(st *StateRow) { st.ForceEscalation = false })
		return err
	}))
	assert.Equal(t, "active", p.alertStatus(2))

	byTime := func(id int64, since time.Time) bool {
		t.Helper()
		var ok bool
		require.NoError(t, p.write(func(tx *filestore.Tx) (err error) {
			ok, err = e.ForceEscalationByTime(tx, id, since)
			return err
		}))
		return ok
	}
	assert.False(t, byTime(2, start.Add(-time.Microsecond)), "it has escalated since")
	st, _ := e.AlertState(p.fdb.View(), 2)
	assert.False(t, st.ForceEscalation)
	assert.True(t, byTime(2, start))
	assert.Equal(t, "triggered", p.alertStatus(2))
	assert.True(t, byTime(2, start), "again, as an UPDATE of a row to what it is")
	assert.True(t, byTime(1, time.Unix(0, 0)), "it has never escalated")
	assert.False(t, byTime(3, start), "no state")

	// If the alert can't be triggered, nothing changes (F2).
	p.addAlert(t, 4, svc, "active")
	put(t, p, e.alerts, alertRow{ID: 4, ServiceID: svc, Status: "closed"})
	require.Error(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.ForceEscalation(tx, 4)
		return err
	}))
	st, _ = e.AlertState(e.check(t).fdb.View(), 4)
	assert.False(t, st.ForceEscalation)

	assert.False(t, e.AlertMultiAck(p.fdb.View(), 1), "on no step")
	id := steps[0].String()
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		_, err := e.UpdateState(tx, 1, func(st *StateRow) { st.StepID = &id })
		return err
	}))
	assert.False(t, e.AlertMultiAck(p.fdb.View(), 1))
	require.NoError(t, p.UpdateStepMultiAckTx(asUser(), nil, steps[0], true))
	assert.True(t, e.AlertMultiAck(p.fdb.View(), 1))
	assert.False(t, e.AlertMultiAck(p.fdb.View(), 99), "no state")
}

func TestDeltaServicePolicyChange(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol, next, empty := p.addPolicy(t, "Policy"), p.addPolicy(t, "Next"), p.addPolicy(t, "Empty")
	steps := p.addSteps(t, pol, 2)
	p.addSteps(t, next, 1)
	svc, other := p.addService(t, pol, 1, 2), p.addService(t, pol, 3)
	at := start
	id := steps[1].String()
	require.NoError(t, p.write(func(tx *filestore.Tx) error {
		for _, alert := range []int64{1, 3} {
			_, err := e.UpdateState(tx, alert, func(st *StateRow) {
				st.StepID, st.StepNumber, st.LoopCount, st.LastEscalation, st.NextEscalation, st.ForceEscalation = &id, 1, 2, &at, &at, true
			})
			if err != nil {
				return err
			}
		}
		return nil
	}))
	before, _ := e.AlertState(p.fdb.View(), 1)

	change := func(policyID string) {
		t.Helper()
		require.NoError(t, p.write(func(tx *filestore.Tx) error {
			if err := e.services.Put(tx, serviceRow{ID: svc, PolicyID: policyID}); err != nil {
				return err
			}
			return e.ClearEscalationPolicyState(tx, svc, policyID)
		}))
	}
	change(next)
	r := e.check(t).fdb.View()
	st, _ := e.AlertState(r, 1)
	assert.Equal(t, StateRow{AlertID: 1, ID: before.ID, ServiceID: svc, PolicyID: next}, st, "trg_10_clear_ep_state_on_svc_ep_change")
	st, _ = e.AlertState(r, 3)
	assert.EqualValues(t, 2, st.LoopCount, "another service's alerts go on")
	assert.Equal(t, other, st.ServiceID)

	// A policy without steps: the engine removes states that can't escalate.
	change(empty)
	assert.Equal(t, 3, e.States.Len(p.fdb.View()))
	require.NoError(t, p.write(func(tx *filestore.Tx) error { return e.DeleteAlertState(tx, 1) }))
	p.addAlert(t, 4, svc, "active")
	p.addAlert(t, 5, svc, "closed")

	// With one that has steps, every open alert escalates again (F2).
	change(next)
	r = e.check(t).fdb.View()
	for _, alert := range []int64{1, 2, 4} {
		st, ok := e.AlertState(r, alert)
		assert.True(t, ok, "alert %d", alert)
		assert.Equal(t, next, st.PolicyID)
	}
	_, ok := e.AlertState(r, 5)
	assert.False(t, ok, "closed")
}

func TestDeltaLogChange(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	logs := func() int { return e.logs.Len(e.check(t).fdb.View()) }
	require.Zero(t, logs(), "a new policy has no alerts")

	step := p.addSteps(t, pol, 1)[0]
	require.NoError(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: strings.ToUpper(pol), Name: "Renamed", Description: "x"}))
	_, err := p.DeleteStepTx(ctx, nil, step)
	require.NoError(t, err)
	require.Equal(t, 3, logs())
	e.logs.Ascend(p.fdb.View(), func(row logRow) bool {
		assert.Equal(t, pol, row.PolicyID)
		assert.Equal(t, string(alertlog.TypePolicyUpdated), row.Type)
		return true
	})

	// The entry is made in the transaction, whether it is the caller's or not.
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.UpdatePolicyTx(ctx, tx, &Policy{ID: pol, Name: "Again", Description: "x"}); err != nil {
			return err
		}
		return errRollback
	})
	require.ErrorIs(t, err, errRollback)
	p.addPolicy(t, "Taken")
	require.Error(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: pol, Name: "Taken", Description: "x"}))
	require.Equal(t, 3, logs())

	// Without one, the change is still made.
	boom := errors.New("boom")
	e.logErr.Store(&boom)
	require.NoError(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: pol, Name: "Quiet", Description: "x"}))
	assert.Equal(t, 3, logs())
	name, _ := e.EscalationPolicyName(p.fdb.View(), pol)
	assert.Equal(t, "Quiet", name)
}

func TestDeltaMissingLookups(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	full := e.pod(t, "a")
	pol := full.addPolicy(t, "Policy")
	step := full.addSteps(t, pol, 1)[0]
	svc := full.addService(t, pol, 1)

	e.lookups = DeltaLookups{}
	p := e.pod(t, "b")
	for name, fn := range map[string]func() error{
		"CreateStepTx": func() error {
			_, err := p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 1})
			return err
		},
		"DeleteStepTx":   func() error { _, err := p.DeleteStepTx(ctx, nil, step); return err },
		"UpdatePolicyTx": func() error { return p.UpdatePolicyTx(ctx, nil, &Policy{ID: pol, Name: "Xx", Description: "x"}) },
		"FindAllStepActionsTx": func() error {
			_, err := p.FindAllStepActionsTx(ctx, nil, step)
			return err
		},
		"FindAllOnCallStepsForUserTx": func() error {
			_, err := p.FindAllOnCallStepsForUserTx(ctx, nil, alice)
			return err
		},
		"AddStepActionTx user":    func() error { return p.AddStepActionTx(ctx, nil, step, user.DestFromID(alice)) },
		"AddStepActionTx channel": func() error { return p.AddStepActionTx(ctx, nil, step, channelDest("a")) },
		"DeleteStepActionTx":      func() error { return p.DeleteStepActionTx(ctx, nil, step, channelDest("a")) },
		"InsertState": func() error {
			return p.write(func(tx *filestore.Tx) error { return e.InsertState(tx, StateRow{AlertID: 1, PolicyID: pol}) })
		},
		"InsertState service": func() error {
			return p.write(func(tx *filestore.Tx) error {
				return e.InsertState(tx, StateRow{AlertID: 1, ServiceID: svc, PolicyID: pol})
			})
		},
		"InsertAlertState": func() error {
			return p.write(func(tx *filestore.Tx) error { return e.InsertAlertState(tx, 1, svc) })
		},
		"ForceEscalation": func() error {
			return p.write(func(tx *filestore.Tx) error { _, err := e.ForceEscalation(tx, 1); return err })
		},
		"ClearEscalationPolicyState": func() error {
			return p.write(func(tx *filestore.Tx) error { return e.ClearEscalationPolicyState(tx, svc, pol) })
		},
	} {
		require.ErrorIs(t, fn(), deltatxn.ErrNotPorted, name)
	}
	one, err := p.FindOnePolicyTx(ctx, nil, pol)
	require.NoError(t, err)
	assert.Equal(t, "Policy", one.Name, "and nothing changed")
	assert.False(t, one.IsUserFavorite())
	assert.Equal(t, []uuid.UUID{step}, p.stepIDs(t, pol))
}

func TestDeltaReadOnly(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	step := p.addSteps(t, pol, 1)[0]
	require.NoError(t, p.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, err := p.FindAllStepsTx(ctx, tx, pol)
		assert.NoError(t, err)
		_, err = p.FindOnePolicyTx(ctx, tx, pol)
		assert.NoError(t, err)
		_, err = p.FindAllStepActionsTx(ctx, tx, step)
		assert.NoError(t, err)
		_, err = p.CreateStepTx(ctx, tx, &Step{PolicyID: pol, DelayMinutes: 1})
		assert.ErrorIs(t, err, deltatxn.ErrReadOnly)
		_, err = p.DeleteStepTx(ctx, tx, step)
		assert.ErrorIs(t, err, deltatxn.ErrReadOnly)
		return nil
	}))
}

// race runs fn on two pods at once, n times each, and returns their errors.
func (e *deltaEnv) race(t *testing.T, n int, fn func(p *pod, worker, i int) error) []error {
	t.Helper()
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for w, p := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range n {
				if err := fn(p, w, i); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return errs
}

func TestDeltaRaceSteps(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	put(t, p, e.limits, limitRow{ID: "ep_steps_per_policy", Max: 5})
	pol := p.addPolicy(t, "Policy")
	svc := p.addService(t, pol, 1, 2)

	errs := e.race(t, 4, func(p *pod, _, _ int) error {
		_, err := p.CreateStepTx(asUser(), nil, &Step{PolicyID: pol, DelayMinutes: 1})
		return err
	})
	require.Len(t, errs, 3, "8 steps, and room for 5")
	for _, err := range errs {
		assert.True(t, errutil.IsLimitError(errutil.MapDBError(err)), "%v", err)
	}
	check := e.check(t)
	assert.Len(t, check.stepIDs(t, pol), 5, "each with a number of its own")
	for _, alert := range []int64{1, 2} {
		st, ok := e.AlertState(check.fdb.View(), alert)
		assert.True(t, ok, "the first step starts them, once")
		assert.Equal(t, svc, st.ServiceID)
	}
}

func TestDeltaRaceStepChanges(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	pol := p.addPolicy(t, "Policy")
	steps := p.addSteps(t, pol, 6)

	// One pod deletes steps and adds others, while the other turns the order
	// around.
	errs := e.race(t, 3, func(p *pod, worker, i int) error {
		if worker == 0 {
			if _, err := p.DeleteStepTx(ctx, nil, steps[2*i]); err != nil {
				return err
			}
			_, err := p.CreateStepTx(ctx, nil, &Step{PolicyID: pol, DelayMinutes: 1})
			return err
		}
		return p.db.Run(ctx, nil, func(tx txn.Tx) error {
			all, err := p.FindAllStepsTx(ctx, tx, pol)
			if err != nil {
				return err
			}
			slices.Reverse(all)
			for i, st := range all {
				if err := p.UpdateStepNumberTx(ctx, tx, st.ID, i); err != nil {
					return err
				}
			}
			return nil
		})
	})
	require.Empty(t, errs)
	got := e.check(t).stepIDs(t, pol)
	assert.Len(t, got, 6)
	for _, i := range []int{1, 3, 5} {
		assert.Contains(t, got, steps[i])
	}
}

func TestDeltaRaceUnique(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	put(t, p, e.users, idRow{alice})
	put(t, p, e.limits, limitRow{ID: "ep_actions_per_step", Max: 3})
	step := p.addSteps(t, p.addPolicy(t, "Policy"), 1)[0]

	errs := e.race(t, 1, func(p *pod, worker, _ int) error {
		_, err := p.CreatePolicyTx(ctx, nil, &Policy{Name: []string{"Same", "SAME"}[worker], Description: "x"})
		return err
	})
	require.Len(t, errs, 1, "one of them gets the name")
	assert.Contains(t, fieldErrors(t, errs[0]), "already in use")

	errs = e.race(t, 1, func(p *pod, _, _ int) error {
		return p.AddStepActionTx(ctx, nil, step, user.DestFromID(alice))
	})
	require.Len(t, errs, 1)
	assert.Contains(t, fieldErrors(t, errs[0]), "same user")

	errs = e.race(t, 2, func(p *pod, worker, i int) error {
		return p.AddStepActionTx(ctx, nil, step, channelDest(fmt.Sprintf("%d-%d", worker, i)))
	})
	require.Len(t, errs, 2, "one action, and room for two more")
	for _, err := range errs {
		assert.True(t, errutil.IsLimitError(errutil.MapDBError(err)), "%v", err)
	}
	actions, err := e.check(t).FindAllStepActionsTx(ctx, nil, step)
	require.NoError(t, err)
	assert.Len(t, actions, 3)
}

// TestDeltaRaceFirstStep adds a policy's first step while alerts come in.
// Whichever is first, every alert escalates (F2).
func TestDeltaRaceFirstStep(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	pol := p.addPolicy(t, "Policy")
	svc := p.addService(t, pol, 1)

	errs := e.race(t, 4, func(p *pod, worker, i int) error {
		if worker == 0 {
			if i != 1 {
				return nil
			}
			_, err := p.CreateStepTx(asUser(), nil, &Step{PolicyID: pol, DelayMinutes: 1})
			return err
		}
		return p.write(func(tx *filestore.Tx) error {
			id := int64(i + 10)
			if err := e.alerts.Put(tx, alertRow{ID: id, ServiceID: svc, Status: "triggered"}); err != nil {
				return err
			}
			return e.InsertAlertState(tx, id, svc)
		})
	})
	require.Empty(t, errs)
	r := e.check(t).fdb.View()
	for _, id := range []int64{1, 10, 11, 12, 13} {
		_, ok := e.AlertState(r, id)
		assert.True(t, ok, "alert %d", id)
	}
}

// TestDeltaRaceState forces alerts to escalate on one pod while the other
// closes them. An alert is closed, with no state, in the end.
func TestDeltaRaceState(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	pol := p.addPolicy(t, "Policy")
	p.addSteps(t, pol, 1)
	svc := p.addService(t, pol, 0, 1, 2, 3)

	errs := e.race(t, 4, func(p *pod, worker, i int) error {
		if worker == 0 {
			return p.write(func(tx *filestore.Tx) error {
				_, err := e.ForceEscalation(tx, int64(i))
				return err
			})
		}
		return p.write(func(tx *filestore.Tx) error {
			if err := e.alerts.Put(tx, alertRow{ID: int64(i), ServiceID: svc, Status: "closed"}); err != nil {
				return err
			}
			return e.DeleteAlertState(tx, int64(i))
		})
	})
	require.Empty(t, errs)
	check := e.check(t)
	assert.Zero(t, e.States.Len(check.fdb.View()))
	for i := range int64(4) {
		assert.Equal(t, "closed", check.alertStatus(i))
	}
}

// TestDeltaRaceServicePolicy changes a service's policy on one pod while alerts
// come in on the other. Every alert ends on the service's policy.
func TestDeltaRaceServicePolicy(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	pols := []string{p.addPolicy(t, "One"), p.addPolicy(t, "Two"), p.addPolicy(t, "Empty")}
	p.addSteps(t, pols[0], 1)
	p.addSteps(t, pols[1], 1)
	svc := p.addService(t, pols[0], 100)

	errs := e.race(t, 6, func(p *pod, worker, i int) error {
		return p.write(func(tx *filestore.Tx) error {
			if worker == 0 {
				pol := pols[(i+1)%3] // the last is One
				if err := e.services.Put(tx, serviceRow{ID: svc, PolicyID: pol}); err != nil {
					return err
				}
				return e.ClearEscalationPolicyState(tx, svc, pol)
			}
			if err := e.alerts.Put(tx, alertRow{ID: int64(i), ServiceID: svc, Status: "triggered"}); err != nil {
				return err
			}
			return e.InsertAlertState(tx, int64(i), svc)
		})
	})
	require.Empty(t, errs)
	r := e.check(t).fdb.View()
	assert.Equal(t, 7, e.States.Len(r))
	e.States.Ascend(r, func(st StateRow) bool {
		assert.Equal(t, pols[0], st.PolicyID)
		return true
	})
}

// TestDeltaRaceForce forces one alert on two pods: one of them does it.
func TestDeltaRaceForce(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	pol := p.addPolicy(t, "Policy")
	p.addSteps(t, pol, 1)
	p.addService(t, pol, 1)

	var forced atomic.Int32
	require.Empty(t, e.race(t, 1, func(p *pod, _, _ int) error {
		var ok bool
		err := p.write(func(tx *filestore.Tx) (err error) {
			ok, err = e.ForceEscalation(tx, 1)
			return err
		})
		if ok {
			forced.Add(1)
		}
		return err
	}))
	assert.EqualValues(t, 1, forced.Load())
}

// TestDeltaRaceDelete deletes a policy on one pod while a service starts to
// use it on the other. One of them fails (F7).
func TestDeltaRaceDelete(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "setup")
	for i := range 3 {
		pol := p.addPolicy(t, fmt.Sprintf("Policy %d", i))
		p.addSteps(t, pol, 2)
		svc := uuid.NewString()
		errs := e.race(t, 1, func(p *pod, worker, _ int) error {
			if worker == 0 {
				return p.DeleteManyPoliciesTx(asUser(), nil, []string{pol})
			}
			return p.write(func(tx *filestore.Tx) error { return e.services.Put(tx, serviceRow{ID: svc, PolicyID: pol}) })
		})
		require.Len(t, errs, 1)
		var fkErr *filestore.ForeignKeyError
		require.ErrorAs(t, errs[0], &fkErr)
		assert.Equal(t, "services_escalation_policy_id_fkey", fkErr.ForeignKey)
		r := e.check(t).fdb.View()
		_, hasPolicy := e.Policies.Get(r, pol)
		_, hasService := e.services.Get(r, svc)
		assert.Equal(t, hasPolicy, hasService)
	}
}

func TestPolicyMinutes(t *testing.T) {
	ctx := asUser()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	pol := p.addPolicy(t, "Policy")
	assert.Zero(t, e.PolicyMinutes(p.fdb.View(), pol))
	p.addSteps(t, pol, 3)
	assert.Equal(t, 15, e.PolicyMinutes(p.fdb.View(), pol))
	require.NoError(t, p.UpdatePolicyTx(ctx, nil, &Policy{ID: pol, Name: "Policy", Description: "x", Repeat: 2}))
	assert.Equal(t, 45, e.PolicyMinutes(p.fdb.View(), pol))
	assert.Zero(t, e.PolicyMinutes(p.fdb.View(), uuid.NewString()))
}

func TestActionID(t *testing.T) {
	first := newActionID(start, uuid.Nil)
	assert.EqualValues(t, 7, first.Version())
	assert.Equal(t, uuid.RFC4122, first.Variant())
	assert.NotEqual(t, first, newActionID(start, uuid.Nil))
	second := newActionID(start, first)
	assert.EqualValues(t, 7, second.Version())
	assert.Less(t, first.String(), second.String())
	assert.Less(t, second.String(), newActionID(start.Add(-time.Hour), second).String())
	assert.Less(t, newActionID(start, second).String(), newActionID(start.Add(time.Millisecond), second).String())
	assert.Less(t, newActionID(start, uuid.Max).String(), newActionID(start.Add(time.Millisecond), uuid.Max).String(), "an id of another kind")
}
