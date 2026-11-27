package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/label"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

// Stand-ins for other packages' tables.
type (
	policy struct {
		ID   string `parquet:"id"`
		Name string `parquet:"name"`
	}
	favorite struct {
		UserID    string `parquet:"user_id"`
		ServiceID string `parquet:"service_id"`
	}
	intKey struct {
		ID        string `parquet:"id"`
		ServiceID string `parquet:"service_id"`
	}
	epState struct { // escalation_policy_state
		ServiceID string `parquet:"service_id"`
		PolicyID  string `parquet:"policy_id"`
		Step      int64  `parquet:"step"`
	}
	parent struct {
		ID string `parquet:"id"`
	}
)

type env struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables

	policies  *filestore.Table[policy]
	favorites *filestore.Table[favorite]
	keys      *filestore.Table[intKey]
	states    *filestore.Table[epState]
	labels    *label.DeltaTables

	lookups DeltaLookups
}

func newEnv(t *testing.T) *env {
	t.Helper()
	s := filestore.NewSchema()
	e := &env{store: objstore.NewMem(), schema: s}
	e.policies = filestore.NewTable(s, "escalation_policies", func(p policy) string { return p.ID })
	e.favorites = filestore.NewTable(s, "user_favorites", func(f favorite) string { return f.UserID + "/" + f.ServiceID })
	e.keys = filestore.NewTable(s, "integration_keys", func(k intKey) string { return k.ID })
	e.states = filestore.NewTable(s, "escalation_policy_state", func(x epState) string { return x.ServiceID })
	schedules := filestore.NewTable(s, "schedules", func(p parent) string { return p.ID })
	rotations := filestore.NewTable(s, "rotations", func(p parent) string { return p.ID })

	e.tables = DefineDeltaTables(s)
	DeclareDeltaForeignKeys(e.tables, e.policies)
	e.labels = label.DefineDeltaTables(s)
	label.DeclareDeltaForeignKeys(e.labels, e.tables.Services, schedules, rotations, e.policies)
	filestore.References(e.keys, "integration_keys_services_id_fkey", e.tables.Services,
		func(k intKey) (string, bool) { return k.ServiceID, true }, filestore.Cascade, nil)

	e.lookups = DeltaLookups{
		EscalationPolicyName: func(r filestore.Reader, id string) (string, bool) {
			p, ok := e.policies.Get(r, id)
			return p.Name, ok
		},
		FavoriteServices: func(r filestore.Reader, userID string) []string {
			var ids []string
			e.favorites.Ascend(r, func(f favorite) bool {
				if f.UserID == userID {
					ids = append(ids, f.ServiceID)
				}
				return true
			})
			return ids
		},
		IntegrationKeyService: func(r filestore.Reader, keyID string) (string, bool) {
			k, ok := e.keys.Get(r, keyID)
			return k.ServiceID, ok
		},
		ServicesWithLabel: e.labels.ServicesWithLabel,
		ClearEscalationPolicyState: func(tx *filestore.Tx, serviceID, newPolicyID string) error {
			if _, ok := e.states.Get(tx, serviceID); !ok {
				return nil
			}
			return e.states.Put(tx, epState{ServiceID: serviceID, PolicyID: newPolicyID})
		},
	}
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, s, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

type pod struct {
	*Store
	db     deltatxn.DB
	env    *env
	labels *label.Store
}

// open opens the table as one GoAlert pod.
func (e *env) open(t *testing.T) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &pod{Store: NewDeltaStore(db, e.tables, e.lookups), db: db, env: e, labels: label.NewDeltaStore(db, e.labels)}
}

// update runs fn on the filestore tables.
func (p *pod) update(fn func(tx *filestore.Tx) error) error {
	_, err := p.db.DB.Update(context.Background(), fn)
	return err
}

func (p *pod) addPolicy(t *testing.T, name string) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return p.env.policies.Put(tx, policy{ID: id, Name: name}) }))
	return id
}

var userID = uuid.NewString()

func testCtx() context.Context {
	return permission.UserContext(context.Background(), userID, permission.RoleUser)
}

func (p *pod) create(t *testing.T, epID, name, description string) *Service {
	t.Helper()
	svc, err := p.CreateServiceTx(testCtx(), nil, &Service{Name: name, Description: description, EscalationPolicyID: epID})
	require.NoError(t, err)
	return svc
}

func (p *pod) setFavorite(t *testing.T, serviceID string) {
	t.Helper()
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		return p.env.favorites.Put(tx, favorite{UserID: userID, ServiceID: serviceID})
	}))
}

func requireConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	require.Error(t, err)
	var dbErr interface {
		SQLState() string
		ConstraintName() string
	}
	require.ErrorAs(t, err, &dbErr)
	assert.Equal(t, code, dbErr.SQLState())
	assert.Equal(t, constraint, dbErr.ConstraintName())
}

func TestDeltaServices(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	ep, ep2 := p.addPolicy(t, "Policy"), p.addPolicy(t, "Policy 2")

	a, err := p.CreateServiceTx(ctx, nil, &Service{Name: "Alpha", Description: "first", EscalationPolicyID: strings.ToUpper(ep)})
	require.NoError(t, err)
	want := &Service{ID: a.ID, Name: "Alpha", Description: "first", EscalationPolicyID: ep, epName: "Policy"}
	got, err := p.FindOne(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	got, err = e.open(t).FindOne(ctx, strings.ToUpper(a.ID))
	require.NoError(t, err)
	assert.Equal(t, want, got, "another pod reads the same, and a UUID in another form is the same UUID")

	b := p.create(t, ep, "Beta", "")
	p.setFavorite(t, b.ID)
	got, err = p.FindOneForUser(ctx, userID, b.ID)
	require.NoError(t, err)
	assert.True(t, got.IsUserFavorite())
	got, err = p.FindOneForUser(ctx, strings.ToUpper(userID), b.ID)
	require.NoError(t, err)
	assert.True(t, got.IsUserFavorite())
	got, err = p.FindOne(ctx, b.ID)
	require.NoError(t, err)
	assert.False(t, got.IsUserFavorite(), "of no user")

	_, err = p.FindOne(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindOneForUpdate(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	many, err := p.FindMany(ctx, []string{b.ID, a.ID, b.ID, uuid.NewString()})
	require.NoError(t, err)
	require.Len(t, many, 2, "each once")
	assert.Equal(t, Service{ID: b.ID, Name: "Beta", EscalationPolicyID: ep, epName: "Policy", isUserFavorite: true}, many[0], "with the user's favorites")
	assert.Equal(t, *want, many[1])
	many, err = p.FindMany(permission.SystemContext(context.Background(), "test"), []string{b.ID})
	require.NoError(t, err)
	assert.False(t, many[0].IsUserFavorite(), "of no user")
	many, err = p.FindMany(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.Nil(t, many, "nil without services, as on Postgres")
	many, err = p.FindMany(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, many)

	byEP, err := p.FindAllByEP(ctx, strings.ToUpper(ep))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{a.ID, b.ID}, ids(byEP))
	assert.Equal(t, "Policy", byEP[0].EscalationPolicyName())
	assert.False(t, byEP[0].IsUserFavorite() || byEP[1].IsUserFavorite(), "never favorites, as on Postgres")
	byEP, err = p.FindAllByEP(ctx, ep2)
	require.NoError(t, err)
	assert.Nil(t, byEP)
	byEP, err = p.FindAllByEP(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, byEP)
	_, err = p.FindAllByEP(ctx, "x")
	require.Error(t, err)

	require.NoError(t, p.UpdateTx(ctx, nil, &Service{ID: strings.ToUpper(a.ID), Name: "Alpha 2", Description: "second", EscalationPolicyID: ep2}))
	got, err = p.FindOne(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, &Service{ID: a.ID, Name: "Alpha 2", Description: "second", EscalationPolicyID: ep2, epName: "Policy 2"}, got)
	require.NoError(t, p.UpdateTx(ctx, nil, &Service{ID: uuid.NewString(), Name: "none", EscalationPolicyID: ep}), "an UPDATE of no rows")

	require.NoError(t, p.DeleteManyTx(ctx, nil, []string{a.ID, uuid.NewString(), a.ID}))
	require.NoError(t, p.DeleteManyTx(ctx, nil, nil))
	_, err = p.FindOne(ctx, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindOne(ctx, b.ID)
	require.NoError(t, err)

	// Checks shared with Postgres.
	anon := context.Background()
	for _, err := range []error{
		second(p.FindOne(anon, b.ID)), second(p.FindOneForUpdate(anon, nil, b.ID)), second(p.FindMany(anon, []string{b.ID})),
		second(p.FindAllByEP(anon, ep)), second(p.CreateServiceTx(anon, nil, b)), p.UpdateTx(anon, nil, b),
		p.DeleteManyTx(anon, nil, []string{b.ID}), second(p.Search(anon, nil)), second(p.FindOneForUser(ctx, uuid.NewString(), b.ID)),
	} {
		assert.True(t, permission.IsPermissionError(err), "%v", err)
	}
	for _, err := range []error{
		second(p.FindOne(ctx, "x")), second(p.FindOneForUpdate(ctx, nil, "x")), second(p.FindMany(ctx, []string{"x"})),
		second(p.CreateServiceTx(ctx, nil, &Service{Name: "x"})), p.UpdateTx(ctx, nil, &Service{Name: "ok", EscalationPolicyID: ep}),
		p.DeleteManyTx(ctx, nil, []string{"x"}),
	} {
		assert.True(t, validation.IsValidationError(err), "%v", err)
	}
}

func second[T any](_ T, err error) error { return err }

func ids(services []Service) []string {
	res := []string{}
	for _, s := range services {
		res = append(res, s.ID)
	}
	return res
}

func TestDeltaServicesMaintenance(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc := p.create(t, p.addPolicy(t, "Policy"), "Alpha", "")
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)

	change := func(fn func(*Service)) Service {
		t.Helper()
		require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
			s, err := p.FindOneForUpdate(ctx, tx, svc.ID)
			if err != nil {
				return err
			}
			fn(s)
			return p.UpdateTx(ctx, tx, s)
		}))
		many, err := e.open(t).FindMany(ctx, []string{svc.ID})
		require.NoError(t, err)
		return many[0]
	}
	assert.Equal(t, until, change(func(s *Service) { s.MaintenanceExpiresAt = until }).MaintenanceExpiresAt)
	got := change(func(s *Service) { s.Name = "Beta" })
	assert.Equal(t, "Beta", got.Name)
	assert.Equal(t, until, got.MaintenanceExpiresAt, "another change doesn't end maintenance, as it would on Postgres")
	assert.Zero(t, change(func(s *Service) { s.MaintenanceExpiresAt = time.Now().Add(-time.Hour) }).MaintenanceExpiresAt)
}

func TestDeltaServicesConstraints(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	ep := p.addPolicy(t, "Policy")
	a, b := p.create(t, ep, "Alpha", ""), p.create(t, ep, "Beta", "")

	inUse := validation.NewFieldError("Name", "already in use").Error()
	for name, constraint := range map[string]string{"Alpha": "services_name_key", "ALPHA": "services_name"} {
		_, err := p.CreateServiceTx(ctx, nil, &Service{Name: name, EscalationPolicyID: ep})
		requireConstraint(t, err, "23505", constraint)
		assert.EqualError(t, errutil.MapDBError(err), inUse)
		err = p.UpdateTx(ctx, nil, &Service{ID: b.ID, Name: name, EscalationPolicyID: ep})
		requireConstraint(t, err, "23505", constraint)
	}
	require.NoError(t, p.UpdateTx(ctx, nil, &Service{ID: a.ID, Name: "ALPHA", EscalationPolicyID: ep}), "its own name")
	err := p.update(func(tx *filestore.Tx) error {
		return e.tables.Services.Insert(tx, serviceRow{ID: a.ID, Name: "x", EscalationPolicyID: ep}, "services_pkey")
	})
	requireConstraint(t, err, "23505", "services_pkey")

	// services_escalation_policy_id_fkey
	missing := validation.NewFieldError("EscalationPolicyID", "does not exist").Error()
	_, err = p.CreateServiceTx(ctx, nil, &Service{Name: "Gamma", EscalationPolicyID: uuid.NewString()})
	requireConstraint(t, err, "23503", "services_escalation_policy_id_fkey")
	assert.EqualError(t, errutil.MapDBError(err), missing)
	err = p.UpdateTx(ctx, nil, &Service{ID: a.ID, Name: "Alpha", EscalationPolicyID: uuid.NewString()})
	requireConstraint(t, err, "23503", "services_escalation_policy_id_fkey")
	assert.EqualError(t, errutil.MapDBError(err), missing)
	deletePolicy := func() error {
		return p.update(func(tx *filestore.Tx) error {
			_, err := e.policies.Delete(tx, ep)
			return err
		})
	}
	err = deletePolicy()
	requireConstraint(t, err, "23503", "services_escalation_policy_id_fkey")
	assert.EqualError(t, errutil.MapDBError(err), validation.NewFieldError("EscalationPolicyID", "is currently in use").Error())

	// What refers to a service goes with it.
	keyID := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return e.keys.Put(tx, intKey{ID: keyID, ServiceID: a.ID}) }))
	require.NoError(t, p.labels.SetTx(ctx, nil, &label.Label{Target: assignment.ServiceTarget(a.ID), Key: "foo/bar", Value: "value"}))
	require.NoError(t, p.DeleteManyTx(ctx, nil, []string{a.ID, b.ID}))
	assert.Zero(t, e.keys.Len(p.db.DB.View()))
	assert.Empty(t, e.labels.ServicesWithLabel(p.db.DB.View(), "foo/bar", ""))
	require.NoError(t, deletePolicy(), "a policy that no service uses")
}

// trg_10_clear_ep_state_on_svc_ep_change
func TestDeltaServicesPolicyChange(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	ep, ep2 := p.addPolicy(t, "Policy"), p.addPolicy(t, "Policy 2")
	svc := p.create(t, ep, "Alpha", "")
	state := epState{ServiceID: svc.ID, PolicyID: ep, Step: 3}
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return e.states.Put(tx, state) }))
	getState := func() epState {
		s, _ := e.states.Get(e.open(t).db.DB.View(), svc.ID)
		return s
	}

	require.NoError(t, p.UpdateTx(ctx, nil, &Service{ID: svc.ID, Name: "Beta", EscalationPolicyID: strings.ToUpper(ep)}))
	assert.Equal(t, state, getState(), "the same policy")

	boom := errors.New("boom")
	err := p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.UpdateTx(ctx, tx, &Service{ID: svc.ID, Name: "Beta", EscalationPolicyID: ep2}); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, state, getState(), "a failed transaction changes nothing")

	require.NoError(t, p.UpdateTx(ctx, nil, &Service{ID: svc.ID, Name: "Beta", EscalationPolicyID: ep2}))
	assert.Equal(t, epState{ServiceID: svc.ID, PolicyID: ep2}, getState())

	lookups := e.lookups
	lookups.ClearEscalationPolicyState = func(*filestore.Tx, string, string) error { return boom }
	failing := NewDeltaStore(p.db, e.tables, lookups)
	require.ErrorIs(t, failing.UpdateTx(ctx, nil, &Service{ID: svc.ID, Name: "Gamma", EscalationPolicyID: ep}), boom)
	got, err := p.FindOne(ctx, svc.ID)
	require.NoError(t, err)
	assert.Equal(t, "Beta", got.Name, "both happen, or neither (F2)")

	// Nothing is left out quietly without a lookup.
	bare := NewDeltaStore(p.db, e.tables, DeltaLookups{})
	require.ErrorIs(t, bare.UpdateTx(ctx, nil, &Service{ID: svc.ID, Name: "Beta", EscalationPolicyID: ep}), deltatxn.ErrNotPorted)
	require.NoError(t, bare.UpdateTx(ctx, nil, &Service{ID: svc.ID, Name: "Gamma", EscalationPolicyID: ep2}))
	require.ErrorIs(t, second(bare.FindOne(ctx, svc.ID)), deltatxn.ErrNotPorted)
	require.ErrorIs(t, second(bare.FindMany(ctx, []string{svc.ID})), deltatxn.ErrNotPorted)
	require.ErrorIs(t, second(bare.FindAllByEP(ctx, ep)), deltatxn.ErrNotPorted)
	require.ErrorIs(t, second(bare.Search(ctx, &SearchOptions{Search: "foo=bar"})), deltatxn.ErrNotPorted)
	require.ErrorIs(t, second(bare.Search(ctx, &SearchOptions{Search: "token=" + uuid.NewString()})), deltatxn.ErrNotPorted)
	found, err := bare.Search(ctx, &SearchOptions{FavoritesFirst: true, FavoritesUserID: userID})
	require.NoError(t, err)
	assert.Len(t, found, 1)
}

func TestDeltaServicesSearch(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)

	found, err := p.Search(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, found, "nil without results, as on Postgres")

	ep := p.addPolicy(t, "Policy")
	byName := map[string]string{}
	for _, s := range []struct{ name, description string }{
		{"delta", "handles 100% of_things"},
		{"Alpha Service", "the first"},
		{"beta_test-app", "Uses ALPHA inside"},
		{"Gamma", `back\slash`},
		{"charlie's", ""},
	} {
		byName[s.name] = p.create(t, ep, s.name, s.description).ID
	}
	p.setFavorite(t, byName["Gamma"])
	p.setFavorite(t, byName["beta_test-app"])
	setLabel := func(name, key, value string) {
		t.Helper()
		require.NoError(t, p.labels.SetTx(ctx, nil, &label.Label{Target: assignment.ServiceTarget(byName[name]), Key: key, Value: value}))
	}
	setLabel("delta", "foo/bar", "one")
	setLabel("delta", "foo/bar", "one") // twice, as on Postgres
	setLabel("Gamma", "foo/bar", "two")
	setLabel("Gamma", "foo/baz", "one")
	keyID := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		return e.keys.Put(tx, intKey{ID: keyID, ServiceID: byName["delta"]})
	}))

	const alpha, beta, charlie, delta, gamma = "Alpha Service", "beta_test-app", "charlie's", "delta", "Gamma"
	all := []string{alpha, beta, charlie, delta, gamma}
	only := func(names ...string) (res []string) {
		for _, n := range names {
			res = append(res, strings.ToUpper(byName[n]))
		}
		return res
	}
	tests := []struct {
		name string
		opts SearchOptions
		want []string
	}{
		{"everything, by lower(name)", SearchOptions{}, all},
		{"limit", SearchOptions{Limit: 2}, all[:2]},
		{"after", SearchOptions{After: SearchCursor{Name: "BETA_test-app"}}, all[2:]},
		{"after and limit", SearchOptions{After: SearchCursor{Name: "alpha service"}, Limit: 2}, all[1:3]},

		{"text in name or description", SearchOptions{Search: "alpha"}, []string{alpha, beta}},
		{"in any case", SearchOptions{Search: "ALPHA"}, []string{alpha, beta}},
		{"part of a name", SearchOptions{Search: "lph"}, []string{alpha, beta}},
		{"words in order", SearchOptions{Search: "al ser"}, []string{alpha}},
		{"words out of order", SearchOptions{Search: "ser al"}, nil},
		{"two words of one", SearchOptions{Search: "al al"}, nil},
		{"words between - and _", SearchOptions{Search: "be te ap"}, []string{beta}},
		{"words of a name with '", SearchOptions{Search: "cha s"}, []string{charlie}},
		{"a later word", SearchOptions{Search: "test"}, []string{beta}},
		{"words of a description only as they are", SearchOptions{Search: "uses inside"}, nil},
		{"% is no wildcard", SearchOptions{Search: "0%"}, []string{delta}},
		{`\ is no escape`, SearchOptions{Search: `k\s`}, []string{gamma}},
		{"no words match any name", SearchOptions{Search: "%"}, all},
		{"nothing", SearchOptions{Search: "zzz"}, nil},

		{"omit", SearchOptions{Omit: only(alpha, delta)}, []string{beta, charlie, gamma}},
		{"only", SearchOptions{Only: only(gamma, beta, gamma)}, []string{beta, gamma}},
		{"only and omit", SearchOptions{Only: only(gamma, beta), Omit: only(beta)}, []string{gamma}},
		{"only and text", SearchOptions{Only: only(gamma, beta), Search: "gam"}, []string{gamma}},
		{"only a missing one", SearchOptions{Only: []string{uuid.NewString()}}, nil},

		{"label", SearchOptions{Search: "foo/bar=one"}, []string{delta}},
		{"label of any value", SearchOptions{Search: "foo/bar=*"}, []string{delta, gamma}},
		{"label without value", SearchOptions{Search: "foo/bar="}, []string{delta, gamma}},
		{"label of no service", SearchOptions{Search: "foo/bar=none"}, nil},
		{"not label", SearchOptions{Search: "foo/bar!=one"}, []string{alpha, beta, charlie, gamma}},
		{"not label of any value", SearchOptions{Search: "foo/bar!=*"}, []string{alpha, beta, charlie}},
		{"not label of no service", SearchOptions{Search: "foo/bar!=none"}, all},
		{"label and only", SearchOptions{Search: "foo/bar=*", Only: only(gamma, alpha)}, []string{gamma}},
		{"label and favorites first", SearchOptions{Search: "foo/bar=*", FavoritesFirst: true, FavoritesUserID: userID}, []string{gamma, delta}},
		{"= without a key is text", SearchOptions{Search: "=one"}, nil},

		{"integration key", SearchOptions{Search: "token=" + keyID}, []string{delta}},
		{"integration key in upper case", SearchOptions{Search: "token=" + strings.ToUpper(keyID)}, []string{delta}},
		{"missing integration key", SearchOptions{Search: "token=" + uuid.NewString()}, nil},
		{"integration key and label", SearchOptions{Search: "token=" + keyID + " foo/bar=one"}, []string{delta}},
		{"integration key and other label", SearchOptions{Search: "token=" + keyID + " foo/baz=one"}, nil},

		{"favorites", SearchOptions{FavoritesOnly: true, FavoritesUserID: userID}, []string{beta, gamma}},
		{"favorites and text", SearchOptions{FavoritesOnly: true, FavoritesUserID: userID, Search: "gamma"}, []string{gamma}},
		{"favorites after", SearchOptions{FavoritesOnly: true, FavoritesUserID: userID, After: SearchCursor{Name: beta}}, []string{gamma}},
		{"favorites first", SearchOptions{FavoritesFirst: true, FavoritesUserID: userID}, []string{beta, gamma, alpha, charlie, delta}},
		{"favorites first, after a favorite", SearchOptions{FavoritesFirst: true, FavoritesUserID: userID, After: SearchCursor{Name: beta, IsFavorite: true}},
			[]string{gamma, alpha, charlie, delta}},
		{"favorites first, after another", SearchOptions{FavoritesFirst: true, FavoritesUserID: userID, After: SearchCursor{Name: alpha}},
			[]string{charlie, delta}},
		{"favorites first and limit", SearchOptions{FavoritesFirst: true, FavoritesUserID: userID, Limit: 3}, []string{beta, gamma, alpha}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, err := p.Search(ctx, &tt.opts)
			require.NoError(t, err)
			var names []string
			for _, s := range found {
				names = append(names, s.Name)
				isFav := s.Name == beta || s.Name == gamma
				assert.Equal(t, isFav && tt.opts.FavoritesUserID != "", s.IsUserFavorite(), s.Name)
				assert.Empty(t, s.EscalationPolicyName(), "not selected, as on Postgres")
			}
			assert.Equal(t, tt.want, names)
		})
	}

	_, err = p.Search(ctx, &SearchOptions{Search: "token=" + strings.Repeat("x", 36)})
	require.Error(t, err, "not a UUID")
	_, err = p.Search(ctx, &SearchOptions{FavoritesOnly: true})
	assert.True(t, validation.IsValidationError(err), "%v", err)
}

func TestDeltaServicesRace(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	pods := []*pod{e.open(t), e.open(t)}
	ep := pods[0].addPolicy(t, "Policy")

	race := func(fns ...func(p *pod) error) []error {
		errs := make([]error, len(pods))
		var wg sync.WaitGroup
		for i, p := range pods {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = fns[i%len(fns)](p)
			}()
		}
		wg.Wait()
		return errs
	}

	// The same name, at once.
	errs := race(func(p *pod) error {
		return second(p.CreateServiceTx(ctx, nil, &Service{Name: "Alpha", EscalationPolicyID: ep}))
	})
	if errs[0] == nil {
		errs[0], errs[1] = errs[1], errs[0]
	}
	require.NoError(t, errs[1])
	requireConstraint(t, errs[0], "23505", "services_name_key")
	found, err := e.open(t).Search(ctx, nil)
	require.NoError(t, err)
	require.Len(t, found, 1)
	svc := found[0]

	// Changes of two fields, at once: neither is lost.
	change := func(fn func(*Service)) func(p *pod) error {
		return func(p *pod) error {
			return p.db.Run(ctx, nil, func(tx txn.Tx) error {
				s, err := p.FindOneForUpdate(ctx, tx, svc.ID)
				if err != nil {
					return err
				}
				fn(s)
				return p.UpdateTx(ctx, tx, s)
			})
		}
	}
	errs = race(change(func(s *Service) { s.Name = "Beta" }), change(func(s *Service) { s.Description = "changed" }))
	require.NoError(t, errors.Join(errs...))
	got, err := e.open(t).FindOne(ctx, svc.ID)
	require.NoError(t, err)
	assert.Equal(t, "Beta", got.Name)
	assert.Equal(t, "changed", got.Description)

	// A policy is deleted as a service starts to use it.
	for i := range 5 {
		ep := pods[0].addPolicy(t, "Policy")
		errs = race(
			func(p *pod) error {
				return p.update(func(tx *filestore.Tx) error {
					_, err := e.policies.Delete(tx, ep)
					return err
				})
			},
			func(p *pod) error {
				return second(p.CreateServiceTx(ctx, nil, &Service{Name: fmt.Sprintf("Service %d", i), EscalationPolicyID: ep}))
			},
		)
		if errs[0] == nil {
			errs[0], errs[1] = errs[1], errs[0]
		}
		require.NoError(t, errs[1], "one of them succeeds")
		requireConstraint(t, errs[0], "23503", "services_escalation_policy_id_fkey")
	}
}
