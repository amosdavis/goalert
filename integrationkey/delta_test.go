package integrationkey

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/expflag"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/util/sqlutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// parent stands in for the tables that this package's refer to.
type parent struct {
	ID string `parquet:"id"`
}

// channel stands in for notification_channels.
type channel struct {
	ID   string `parquet:"id"`
	Dest string `parquet:"dest"`
}

type env struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	keyrings *keyring.DeltaTables

	services, messages *filestore.Table[parent]
	channels           *filestore.Table[channel]

	clock *filestore.FakeClock
	reg   *nfydest.Registry

	keyLimit, signalLimit, destLimit int

	newSignal func(tx *filestore.Tx, serviceID string) error
}

func newEnv(t *testing.T) *env {
	t.Helper()
	s := filestore.NewSchema()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &env{store: objstore.NewMem(), schema: s, clock: clock, reg: nfydest.NewRegistry(), keyLimit: -1, signalLimit: -1, destLimit: -1}
	e.reg.RegisterProvider(context.Background(), provider{})
	e.services = filestore.NewTable(s, "services", func(p parent) string { return p.ID })
	e.messages = filestore.NewTable(s, "outgoing_messages", func(p parent) string { return p.ID })
	e.channels = filestore.NewTable(s, "notification_channels", func(c channel) string { return c.ID })
	e.keyrings = keyring.DefineDeltaTables(s)
	e.tables = DefineDeltaTables(s)
	DeclareDeltaForeignKeys(e.tables, e.services, e.channels, e.messages)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, s, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// provider is a notification destination that signals can be sent to.
type provider struct{}

func (provider) ID() string { return "test-dest" }
func (provider) TypeInfo(context.Context) (*nfydest.TypeInfo, error) {
	return &nfydest.TypeInfo{Type: "test-dest", Name: "Test", Enabled: true, SupportsSignals: true}, nil
}
func (provider) ValidateField(context.Context, string, string) error { return nil }
func (provider) DisplayInfo(context.Context, map[string]string) (*nfydest.DisplayInfo, error) {
	return &nfydest.DisplayInfo{Text: "Test"}, nil
}
func (provider) ValidateDest(context.Context, gadb.DestV1) error        { return nil }
func (provider) ValidateAction(context.Context, gadb.UIKActionV1) error { return nil }

type pod struct {
	*Store
	db  deltatxn.DB
	env *env
}

// MapDestToID stands in for the notification channel store's.
func (p *pod) MapDestToID(ctx context.Context, tx txn.Tx, d gadb.DestV1) (uuid.UUID, error) {
	id := uuid.New()
	name := d.Type + "/" + d.Arg("name")
	err := deltatxn.Write(ctx, p.db, tx, func(s deltatxn.Scope) error {
		var found bool
		p.env.channels.Ascend(s.Reader, func(c channel) bool {
			if c.Dest == name {
				id, found = uuid.MustParse(c.ID), true
			}
			return !found
		})
		if found {
			return nil
		}
		return p.env.channels.Put(s.Tx, channel{ID: id.String(), Dest: name})
	})
	return id, err
}

// open opens the table as one GoAlert pod.
func (e *env) open(t *testing.T) *pod {
	t.Helper()
	ctx := context.Background()
	fdb, err := filestore.OpenDelta(ctx, e.store, e.schema, filestore.DeltaOptions{
		Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	keys, err := keyring.NewDeltaDB(ctx, log.NewLogger(), db, e.keyrings, &keyring.Config{Name: "api", Keys: keyring.Keys{[]byte("secret")}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = keys.Shutdown(ctx) })
	p := &pod{db: db, env: e}
	p.Store = NewDeltaStore(db, e.tables, keys, e.reg, nil, DeltaLookups{
		KeysPerService:           func(filestore.Reader) int { return e.keyLimit },
		SignalsPerService:        func(filestore.Reader) int { return e.signalLimit },
		SignalsPerDestPerService: func(filestore.Reader) int { return e.destLimit },
		NewSignal:                e.newSignal,
	})
	p.ncStore = p
	return p
}

// update runs fn on the filestore tables.
func (p *pod) update(fn func(tx *filestore.Tx) error) error {
	_, err := p.db.DB.Update(context.Background(), fn)
	return err
}

func (p *pod) add(t *testing.T, table *filestore.Table[parent]) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return table.Put(tx, parent{ID: id}) }))
	return id
}

func (p *pod) addChannel(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := p.MapDestToID(context.Background(), nil, gadb.NewDestV1("test-dest", "name", uuid.NewString()))
	require.NoError(t, err)
	return id
}

func testCtx() context.Context {
	ctx := permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser)
	return expflag.Context(ctx, expflag.FlagSet{expflag.UnivKeys})
}

func (p *pod) create(t *testing.T, serviceID, name string, typ Type) *IntegrationKey {
	t.Helper()
	k, err := p.Create(testCtx(), nil, &IntegrationKey{Name: name, ServiceID: serviceID, Type: typ})
	require.NoError(t, err)
	return k
}

func second[T any](_ T, err error) error { return err }

func requireConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	require.Error(t, err)
	dbErr := sqlutil.MapError(err)
	require.NotNil(t, dbErr, "%v", err)
	assert.Equal(t, code, dbErr.Code)
	assert.Equal(t, constraint, dbErr.ConstraintName)
}

// oneFailed returns the error of the one of two that failed.
func oneFailed(t *testing.T, errs []error) error {
	t.Helper()
	require.Len(t, errs, 2)
	if errs[0] == nil {
		errs[0], errs[1] = errs[1], errs[0]
	}
	require.NoError(t, errs[1])
	require.Error(t, errs[0])
	return errs[0]
}

// race runs fn on every pod at once.
func race(pods []*pod, fn func(i int, p *pod) error) []error {
	errs := make([]error, len(pods))
	var wg sync.WaitGroup
	for i, p := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(i, p)
		}()
	}
	wg.Wait()
	return errs
}

func TestDeltaKeys(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc, other := p.add(t, e.services), p.add(t, e.services)

	k, err := p.Create(ctx, nil, &IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: strings.ToUpper(svc), ExternalSystemName: "ext"})
	require.NoError(t, err)
	want := &IntegrationKey{ID: k.ID, Name: "Alpha", Type: TypeGeneric, ServiceID: svc, ExternalSystemName: "ext"}
	got, err := p.FindOne(ctx, k.ID)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	got, err = e.open(t).FindOne(ctx, strings.ToUpper(k.ID))
	require.NoError(t, err)
	assert.Equal(t, want, got, "another pod reads the same, and a UUID in another form is the same UUID")
	got, err = p.FindOne(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, got)

	b := p.create(t, svc, "Beta", TypeEmail)
	p.create(t, other, "Beta", TypeEmail)
	all, err := p.FindAllByService(ctx, strings.ToUpper(svc))
	require.NoError(t, err)
	assert.ElementsMatch(t, []IntegrationKey{*want, *b}, all)
	all, err = p.FindAllByService(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Equal(t, []IntegrationKey{}, all, "an empty list, not nil, as on Postgres")

	id, err := p.GetServiceID(ctx, k.ID, TypeGeneric)
	require.NoError(t, err)
	assert.Equal(t, svc, id)
	_, err = p.GetServiceID(ctx, k.ID, TypeEmail)
	require.ErrorIs(t, err, sql.ErrNoRows, "of another type")
	_, err = p.GetServiceID(ctx, uuid.NewString(), TypeEmail)
	require.ErrorIs(t, err, sql.ErrNoRows)

	authCtx, err := p.Authorize(context.Background(), authtoken.Token{ID: uuid.MustParse(k.ID)}, TypeGeneric)
	require.NoError(t, err)
	assert.Equal(t, svc, permission.ServiceID(authCtx))
	_, err = p.Authorize(context.Background(), authtoken.Token{ID: uuid.New()}, TypeGeneric)
	assert.True(t, permission.IsUnauthorized(err), "%v", err)

	// The service store's lookup.
	id, ok := e.tables.ServiceID(p.db.DB.View(), k.ID)
	assert.True(t, ok)
	assert.Equal(t, svc, id)
	_, ok = e.tables.ServiceID(p.db.DB.View(), uuid.NewString())
	assert.False(t, ok)

	require.NoError(t, p.Delete(ctx, nil, k.ID))
	require.NoError(t, p.DeleteMany(ctx, nil, []string{k.ID, b.ID, b.ID, uuid.NewString()}))
	require.NoError(t, p.DeleteMany(ctx, nil, nil))
	all, err = p.FindAllByService(ctx, svc)
	require.NoError(t, err)
	assert.Empty(t, all)
	assert.Equal(t, 1, e.tables.Keys.Len(p.db.DB.View()))

	// Checks shared with Postgres.
	anon := context.Background()
	for _, err := range []error{
		second(p.Create(anon, nil, want)), p.DeleteMany(anon, nil, []string{k.ID}), second(p.FindOne(anon, k.ID)),
		second(p.FindAllByService(anon, svc)), second(p.GetServiceID(anon, k.ID, TypeEmail)), second(p.Search(anon, nil)),
		second(p.Config(anon, nil, uuid.New())), p.SetConfig(anon, nil, uuid.New(), nil), second(p.GenerateToken(anon, nil, uuid.New())),
		p.PromoteSecondaryToken(anon, nil, uuid.New()), p.DeleteSecondaryToken(anon, nil, uuid.New()),
	} {
		assert.True(t, permission.IsPermissionError(err), "%v", err)
	}
	_, _, err = p.TokenHints(anon, nil, uuid.New())
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	for _, err := range []error{
		second(p.Create(ctx, nil, &IntegrationKey{})), p.DeleteMany(ctx, nil, []string{"x"}), second(p.FindOne(ctx, "x")),
		second(p.FindAllByService(ctx, "x")), second(p.GetServiceID(ctx, k.ID, "x")),
	} {
		assert.True(t, validation.IsValidationError(err), "%v", err)
	}
}

func TestDeltaKeysConstraints(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc, other := p.add(t, e.services), p.add(t, e.services)
	k := p.create(t, svc, "Alpha", TypeGeneric)
	univ := p.create(t, svc, "Universal", TypeUniversal)

	// idx_int_key_name_svc_ext
	create := func(k IntegrationKey) error { return second(p.Create(ctx, nil, &k)) }
	err := create(IntegrationKey{Name: "ALPHA", Type: TypeEmail, ServiceID: svc})
	requireConstraint(t, err, "23505", "idx_int_key_name_svc_ext")
	assert.EqualError(t, errutil.MapDBError(err), validation.NewFieldError("Name", "already in use").Error())
	require.NoError(t, create(IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: svc, ExternalSystemName: "ext"}), "of another system")
	require.NoError(t, create(IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: svc, ExternalSystemName: "EXT"}))
	requireConstraint(t, create(IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: svc, ExternalSystemName: "ext"}), "23505", "idx_int_key_name_svc_ext")
	require.NoError(t, create(IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: other}), "of another service")

	// integration_keys_pkey
	err = p.update(func(tx *filestore.Tx) error {
		return e.tables.Keys.Insert(tx, keyRow{ID: k.ID, Name: "x", ServiceID: svc}, "integration_keys_pkey")
	})
	requireConstraint(t, err, "23505", "integration_keys_pkey")

	// integration_keys_services_id_fkey
	requireConstraint(t, create(IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: uuid.NewString()}), "23503", "integration_keys_services_id_fkey")
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, svc)
		return err
	}))
	view := p.db.DB.View()
	assert.Equal(t, 1, e.tables.Keys.Len(view), "keys go with their service")
	assert.Zero(t, e.tables.configs.Len(view), "and configs with their key")

	// uik_config_id_fkey, and the token constraints
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return e.services.Put(tx, parent{ID: svc}) }))
	u1, u2 := p.create(t, svc, "U1", TypeUniversal), p.create(t, svc, "U2", TypeUniversal)
	err = p.SetConfig(ctx, nil, uuid.MustParse(univ.ID), &gadb.UIKConfigV1{})
	require.ErrorIs(t, err, sql.ErrNoRows, "a key that is gone")
	err = p.update(func(tx *filestore.Tx) error {
		return e.tables.configs.Put(tx, uikConfigRow{ID: univ.ID, Config: []byte("{}")})
	})
	requireConstraint(t, err, "23503", "uik_config_id_fkey")
	tok := uuid.NewString()
	setTokens := func(id string, primary, secondary *string) error {
		return p.update(func(tx *filestore.Tx) error {
			return e.tables.configs.Put(tx, uikConfigRow{ID: id, Config: []byte("{}"), PrimaryToken: primary, SecondaryToken: secondary})
		})
	}
	require.NoError(t, setTokens(u1.ID, &tok, &tok))
	requireConstraint(t, setTokens(u2.ID, &tok, nil), "23505", "uik_config_primary_token_key")
	requireConstraint(t, setTokens(u2.ID, nil, &tok), "23505", "uik_config_secondary_token_key")
}

func TestDeltaKeysLimit(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	e.keyLimit = 2
	p := e.open(t)
	svc := p.add(t, e.services)
	p.create(t, svc, "one", TypeGeneric)
	p.create(t, svc, "two", TypeUniversal)
	_, err := p.Create(ctx, nil, &IntegrationKey{Name: "three", Type: TypeUniversal, ServiceID: svc})
	requireConstraint(t, err, "23514", "integration_keys_per_service_limit")
	assert.Equal(t, "max=2", sqlutil.MapError(err).Hint)
	assert.EqualError(t, errutil.MapDBError(err), "too many integration keys on this service")
	assert.Equal(t, 2, e.tables.Keys.Len(p.db.DB.View()), "nothing was added")
	assert.Equal(t, 1, e.tables.configs.Len(p.db.DB.View()))
	p.create(t, p.add(t, e.services), "other service", TypeGeneric)

	// The last place under the limit, at once.
	e.keyLimit = 3
	errs := race([]*pod{e.open(t), e.open(t)}, func(i int, p *pod) error {
		return second(p.Create(ctx, nil, &IntegrationKey{Name: fmt.Sprintf("key %d", i), Type: TypeGeneric, ServiceID: svc}))
	})
	requireConstraint(t, oneFailed(t, errs), "23514", "integration_keys_per_service_limit")

	e.keyLimit = -1
	p.create(t, svc, "four", TypeGeneric)
	for _, missing := range []func(l *DeltaLookups){
		func(l *DeltaLookups) { l.KeysPerService = nil },
		func(l *DeltaLookups) { l.SignalsPerService = nil },
		func(l *DeltaLookups) { l.SignalsPerDestPerService = nil },
	} {
		l := p.delta.DeltaLookups
		missing(&l)
		assert.Panics(t, func() { NewDeltaStore(p.db, e.tables, nil, nil, nil, l) }, "a limit can't be left out by mistake (F8)")
	}
}

func TestDeltaKeysSearch(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc := p.add(t, e.services)
	found, err := p.Search(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, found, "nil without results, as on Postgres")

	const a, b, c = "aaaaaaaa-0000-0000-0000-000000000001", "aaaaaaaa-0000-0000-0000-000000000002", "aabbbbbb-0000-0000-0000-000000000003"
	ext := "ext"
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		for _, r := range []keyRow{
			{ID: a, Name: "Zulu", Type: "generic", ServiceID: svc, ExternalSystemName: &ext},
			{ID: b, Name: "alpha", Type: "email", ServiceID: svc},
			{ID: c, Name: "Beta", Type: "email", ServiceID: svc},
		} {
			if err := e.tables.Keys.Put(tx, r); err != nil {
				return err
			}
		}
		return nil
	}))
	search := func(opts InKeySearchOptions) (ids []string) {
		t.Helper()
		found, err := p.Search(ctx, &opts)
		require.NoError(t, err)
		for _, k := range found {
			ids = append(ids, k.ID)
		}
		return ids
	}
	assert.Equal(t, []string{b, c, a}, search(InKeySearchOptions{}))
	assert.Equal(t, []string{b, c}, search(InKeySearchOptions{Limit: 2}))
	assert.Equal(t, []string{c, a}, search(InKeySearchOptions{After: "Alpha"}))
	assert.Equal(t, []string{b, a}, search(InKeySearchOptions{Search: "AAAA"}), "a prefix of the ID")
	assert.Equal(t, []string{a}, search(InKeySearchOptions{Search: a}))
	assert.Nil(t, search(InKeySearchOptions{Search: "0000"}))
	assert.Nil(t, search(InKeySearchOptions{Search: "%"}), "% is no wildcard")
	assert.Equal(t, []string{c, a}, search(InKeySearchOptions{Omit: []string{strings.ToUpper(b)}}))
	_, err = p.Search(ctx, &InKeySearchOptions{Omit: []string{"x"}})
	require.Error(t, err)

	found, err = p.Search(ctx, &InKeySearchOptions{Search: a})
	require.NoError(t, err)
	assert.Equal(t, []IntegrationKey{{ID: a, Name: "Zulu", Type: TypeGeneric, ServiceID: svc}}, found, "without the external system, as on Postgres")
}

func action(name string) gadb.UIKActionV1 {
	return gadb.UIKActionV1{Dest: gadb.NewDestV1("test-dest", "name", name), Params: map[string]string{"p": "'v'"}}
}

func TestDeltaUIKConfig(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	svc := p.add(t, e.services)

	_, err := p.Create(permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser), nil,
		&IntegrationKey{Name: "Uni", Type: TypeUniversal, ServiceID: svc})
	assert.True(t, validation.IsClientError(err), "without the flag: %v", err)

	version := p.db.DB.View().Index()
	k := p.create(t, svc, "Uni", TypeUniversal)
	assert.Equal(t, version+1, p.db.DB.View().Index(), "the key and its config are one commit (F2)")
	assert.Equal(t, 1, e.tables.configs.Len(p.db.DB.View()))
	id := uuid.MustParse(k.ID)
	cfg, err := p.Config(ctx, nil, id)
	require.NoError(t, err)
	assert.Equal(t, &gadb.UIKConfigV1{}, cfg)
	cfg, err = p.Config(ctx, nil, uuid.New())
	require.NoError(t, err)
	assert.Equal(t, &gadb.UIKConfigV1{}, cfg, "an empty config without one")

	_, err = p.GenerateToken(ctx, nil, id)
	require.NoError(t, err)
	set := &gadb.UIKConfigV1{
		Rules:          []gadb.UIKRuleV1{{Name: "Rule", ConditionExpr: "a < 1", Actions: []gadb.UIKActionV1{action("one"), action("two")}}},
		DefaultActions: []gadb.UIKActionV1{action("one")},
	}
	require.NoError(t, p.db.Run(ctx, nil, func(tx txn.Tx) error { return p.SetConfig(ctx, tx, id, set) }))
	got, err := e.open(t).Config(ctx, nil, id)
	require.NoError(t, err)
	require.Len(t, got.Rules, 1)
	assert.NotZero(t, got.Rules[0].ID, "rules get IDs")
	assert.Equal(t, set, got, "another pod reads the same")
	one, err := p.MapDestToID(ctx, nil, action("one").Dest)
	require.NoError(t, err)
	assert.Equal(t, one, got.Rules[0].Actions[0].ChannelID, "actions get channels")
	assert.Equal(t, one, got.DefaultActions[0].ChannelID)
	assert.NotEqual(t, one, got.Rules[0].Actions[1].ChannelID)
	primary, _, err := p.TokenHints(ctx, nil, id)
	require.NoError(t, err)
	assert.NotEmpty(t, primary, "setting the config keeps the tokens")

	// The config that a pod reads is its own.
	got.Rules[0].Name = "changed"
	got.Rules[0].Actions[0].Params["p"] = "changed"
	again, err := p.Config(ctx, nil, id)
	require.NoError(t, err)
	assert.Equal(t, set, again)

	// A failed transaction changes nothing.
	boom := errors.New("boom")
	channels := e.channels.Len(p.db.DB.View())
	require.ErrorIs(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.SetConfig(ctx, tx, id, &gadb.UIKConfigV1{DefaultActions: []gadb.UIKActionV1{action("new")}}); err != nil {
			return err
		}
		cfg, err := p.Config(ctx, tx, id)
		if err != nil {
			return err
		}
		assert.Len(t, cfg.DefaultActions, 1, "a transaction sees its own changes")
		return boom
	}), boom)
	again, err = p.Config(ctx, nil, id)
	require.NoError(t, err)
	assert.Equal(t, set, again)
	assert.Equal(t, channels, e.channels.Len(p.db.DB.View()))

	// Without a transaction.
	require.NoError(t, p.SetConfig(ctx, nil, id, &gadb.UIKConfigV1{DefaultActions: []gadb.UIKActionV1{action("three")}}))
	again, err = p.Config(ctx, nil, id)
	require.NoError(t, err)
	assert.Empty(t, again.Rules)
	assert.NotZero(t, again.DefaultActions[0].ChannelID)

	generic := uuid.MustParse(p.create(t, svc, "Gen", TypeGeneric).ID)
	assert.True(t, validation.IsClientError(p.SetConfig(ctx, nil, generic, &gadb.UIKConfigV1{})))
	require.ErrorIs(t, p.SetConfig(ctx, nil, uuid.New(), &gadb.UIKConfigV1{}), sql.ErrNoRows)
	require.ErrorIs(t, p.SetConfig(ctx, nil, uuid.New(), nil), sql.ErrNoRows)
	assert.True(t, validation.IsClientError(p.SetConfig(ctx, nil, id, &gadb.UIKConfigV1{DefaultActions: []gadb.UIKActionV1{action("one"), action("one")}})))

	require.NoError(t, p.SetConfig(ctx, nil, id, nil))
	assert.Zero(t, e.tables.configs.Len(p.db.DB.View()), "no config deletes it")
	require.NoError(t, p.SetConfig(ctx, nil, id, nil))

	require.NoError(t, p.SetConfig(ctx, nil, id, &gadb.UIKConfigV1{}))
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		row, _ := e.tables.configs.Get(tx, k.ID)
		row.Config = []byte(`{"Version": 2}`)
		return e.tables.configs.Put(tx, row)
	}))
	_, err = p.Config(ctx, nil, id)
	assert.ErrorContains(t, err, "unsupported config version")

	// Rules added at once by two pods are both kept (F4).
	k2 := uuid.MustParse(p.create(t, svc, "U2", TypeUniversal).ID)
	errs := race([]*pod{e.open(t), e.open(t)}, func(i int, p *pod) error {
		return p.db.Run(ctx, nil, func(tx txn.Tx) error {
			cfg, err := p.Config(ctx, tx, k2)
			if err != nil {
				return err
			}
			cfg.Rules = append(cfg.Rules, gadb.UIKRuleV1{Name: fmt.Sprintf("Rule %d", i), ConditionExpr: "true"})
			return p.SetConfig(ctx, tx, k2, cfg)
		})
	})
	require.NoError(t, errors.Join(errs...))
	cfg, err = e.open(t).Config(ctx, nil, k2)
	require.NoError(t, err)
	assert.Len(t, cfg.Rules, 2)
}

func TestDeltaUIKTokens(t *testing.T) {
	ctx := testCtx()
	flagOnly := expflag.Context(context.Background(), expflag.FlagSet{expflag.UnivKeys})
	e := newEnv(t)
	p := e.open(t)
	svc := p.add(t, e.services)
	id := uuid.MustParse(p.create(t, svc, "Uni", TypeUniversal).ID)

	authorized := func(p *pod, tok string) bool {
		t.Helper()
		authCtx, err := p.AuthorizeUIK(flagOnly, tok)
		if err == nil {
			assert.Equal(t, svc, permission.ServiceID(authCtx))
			assert.Equal(t, &permission.SourceInfo{Type: permission.SourceTypeUIK, ID: id.String()}, permission.Source(authCtx))
			return true
		}
		assert.True(t, permission.IsUnauthorized(err), "%v", err)
		return false
	}
	hints := func() [2]string {
		t.Helper()
		primary, secondary, err := p.TokenHints(ctx, nil, id)
		require.NoError(t, err)
		return [2]string{primary, secondary}
	}
	generate := func() string {
		t.Helper()
		tok, err := p.GenerateToken(ctx, nil, id)
		require.NoError(t, err)
		return tok
	}
	hint := func(tok string) string { return tok[:2] + "..." + tok[len(tok)-4:] }

	assert.Equal(t, [2]string{}, hints())
	err := p.PromoteSecondaryToken(ctx, nil, id)
	assert.True(t, validation.IsClientError(err), "%v", err)

	a := generate()
	assert.Equal(t, [2]string{hint(a), ""}, hints(), "the first is the primary")
	assert.True(t, authorized(p, a))
	assert.True(t, authorized(e.open(t), a), "on another pod")
	_, err = p.AuthorizeUIK(context.Background(), a)
	assert.True(t, permission.IsUnauthorized(err), "without the flag")
	assert.False(t, authorized(p, "x"))

	err = p.PromoteSecondaryToken(ctx, nil, id)
	assert.True(t, validation.IsClientError(err), "%v", err)
	assert.True(t, authorized(p, a), "promoting nothing changes nothing")
	assert.Equal(t, [2]string{hint(a), ""}, hints())

	b := generate()
	assert.Equal(t, [2]string{hint(a), hint(b)}, hints())
	assert.True(t, authorized(p, a))
	assert.True(t, authorized(p, b))
	_, err = p.GenerateToken(ctx, nil, id)
	assert.True(t, validation.IsClientError(err), "both are set: %v", err)

	require.NoError(t, p.PromoteSecondaryToken(ctx, nil, id))
	assert.Equal(t, [2]string{hint(b), ""}, hints())
	assert.False(t, authorized(p, a))
	assert.True(t, authorized(p, b))

	c := generate()
	assert.Equal(t, [2]string{hint(b), hint(c)}, hints())
	require.NoError(t, p.DeleteSecondaryToken(ctx, nil, id))
	require.NoError(t, p.DeleteSecondaryToken(ctx, nil, id), "nothing to delete")
	assert.Equal(t, [2]string{hint(b), ""}, hints())
	assert.False(t, authorized(p, c))
	assert.True(t, authorized(p, b))

	// Other and missing keys.
	none := uuid.New()
	_, err = p.GenerateToken(ctx, nil, none)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.ErrorIs(t, p.PromoteSecondaryToken(ctx, nil, none), sql.ErrNoRows)
	require.NoError(t, p.DeleteSecondaryToken(ctx, nil, none))
	primary, secondary, err := p.TokenHints(ctx, nil, none)
	require.NoError(t, err)
	assert.Empty(t, primary+secondary)
	generic := uuid.MustParse(p.create(t, svc, "Gen", TypeGeneric).ID)
	_, err = p.GenerateToken(ctx, nil, generic)
	assert.True(t, validation.IsValidationError(err), "%v", err)
	require.NoError(t, p.SetConfig(ctx, nil, id, nil))
	assert.False(t, authorized(p, b), "tokens go with the config")
	_, err = p.GenerateToken(ctx, nil, id)
	assert.True(t, validation.IsClientError(err), "no config: %v", err)
	require.NoError(t, p.SetConfig(ctx, nil, id, &gadb.UIKConfigV1{}))
	d := generate()
	assert.True(t, authorized(p, d))
	require.NoError(t, p.Delete(ctx, nil, id.String()))
	assert.False(t, authorized(p, d), "and with the key")

	// Two pods give a key tokens at once: both are kept, and a third is not.
	id = uuid.MustParse(p.create(t, svc, "U2", TypeUniversal).ID)
	pods := []*pod{e.open(t), e.open(t)}
	toks := make([]string, 2)
	errs := race(pods, func(i int, p *pod) (err error) {
		toks[i], err = p.GenerateToken(ctx, nil, id)
		return err
	})
	require.NoError(t, errors.Join(errs...))
	fresh := e.open(t)
	assert.True(t, authorized(fresh, toks[0]))
	assert.True(t, authorized(fresh, toks[1]))
	assert.True(t, validation.IsClientError(second(pods[0].GenerateToken(ctx, nil, id))))

	// One promotes as the other does: it happens once, and neither token is lost.
	errs = race(pods, func(_ int, p *pod) error { return p.PromoteSecondaryToken(ctx, nil, id) })
	assert.True(t, validation.IsClientError(oneFailed(t, errs)))
	fresh = e.open(t)
	assert.NotEqual(t, authorized(fresh, toks[0]), authorized(fresh, toks[1]), "the secondary is the primary now")
}

func TestDeltaSignals(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	var jobs []string
	var jobErr error
	e.newSignal = func(_ *filestore.Tx, serviceID string) error {
		jobs = append(jobs, serviceID)
		return jobErr
	}
	p := e.open(t)
	svc, other := uuid.MustParse(p.add(t, e.services)), uuid.MustParse(p.add(t, e.services))
	dest, dest2 := p.addChannel(t), p.addChannel(t)
	insert := func(svc, dest uuid.UUID) error {
		return p.InsertSignalMessage(ctx, nil, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: svc, Params: json.RawMessage(`{"a":"b"}`)})
	}
	all := func() (rows []signalRow) {
		e.tables.Signals.Ascend(e.open(t).db.DB.View(), func(r signalRow) bool { rows = append(rows, r); return true })
		return rows
	}
	unsent := func(key string) int { return e.tables.Unsent.Count(p.db.DB.View(), key) }

	require.NoError(t, insert(svc, dest))
	assert.Equal(t, []signalRow{{ID: 1, CreatedAt: start, DestID: dest.String(), ServiceID: svc.String(), Params: []byte(`{"a":"b"}`)}}, all())
	assert.Equal(t, []string{svc.String()}, jobs, "a new signal queues a job (trg_pending_signals_after_insert)")
	require.Error(t, p.InsertSignalMessage(ctx, nil, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: svc, Params: json.RawMessage("{")}))
	require.Error(t, p.InsertSignalMessage(ctx, nil, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: svc}))

	jobErr = errors.New("boom")
	require.ErrorIs(t, insert(svc, dest), jobErr)
	assert.Len(t, all(), 1, "both happen, or neither (F2)")
	jobErr = nil

	// Foreign keys.
	requireConstraint(t, insert(uuid.New(), dest), "23503", "pending_signals_service_id_fkey")
	requireConstraint(t, insert(svc, uuid.New()), "23503", "pending_signals_dest_id_fkey")

	// Limits.
	require.NoError(t, insert(svc, dest2))
	require.NoError(t, insert(other, dest))
	e.destLimit = 2
	require.NoError(t, insert(svc, dest))
	err := insert(svc, dest)
	requireConstraint(t, err, "23514", "pending_signals_per_dest_per_service_limit")
	assert.Equal(t, "max=2", sqlutil.MapError(err).Hint)
	assert.EqualError(t, errutil.MapDBError(err), "too many pending signals for this destination on this service")
	e.signalLimit = 3
	err = insert(svc, dest2)
	requireConstraint(t, err, "23514", "pending_signals_per_service_limit")
	assert.EqualError(t, errutil.MapDBError(err), "too many pending signals for this service")
	require.NoError(t, insert(other, dest))
	assert.Len(t, all(), 5)
	assert.Equal(t, 5, unsent(AllUnsent))
	assert.Equal(t, 3, unsent(svc.String()))

	// The engine gives a signal a message: it is not unsent any more, and
	// doesn't count towards the limits.
	msg := p.add(t, e.messages)
	setMessage := func(id int64, msg string) error {
		return p.update(func(tx *filestore.Tx) error {
			row, _ := e.tables.Signals.Get(tx, filestore.IntKey(id))
			row.MessageID = &msg
			return e.tables.Signals.Put(tx, row)
		})
	}
	requireConstraint(t, setMessage(1, uuid.NewString()), "23503", "pending_signals_message_id_fkey")
	require.NoError(t, setMessage(1, msg))
	requireConstraint(t, setMessage(2, msg), "23505", "pending_signals_message_id_key")
	sent, ok := e.tables.SignalByMessage.Get(p.db.DB.View(), msg)
	assert.True(t, ok)
	assert.EqualValues(t, 1, sent.ID)
	assert.Equal(t, 4, unsent(AllUnsent))
	assert.Equal(t, 2, unsent(svc.String()))
	require.NoError(t, insert(svc, dest))

	// Cascades.
	del := func(fn func(tx *filestore.Tx) (bool, error)) {
		t.Helper()
		require.NoError(t, p.update(func(tx *filestore.Tx) error { return second(fn(tx)) }))
	}
	del(func(tx *filestore.Tx) (bool, error) { return e.messages.Delete(tx, msg) })
	_, ok = e.tables.Signals.Get(p.db.DB.View(), filestore.IntKey(1))
	assert.False(t, ok, "a signal goes with its message")
	del(func(tx *filestore.Tx) (bool, error) { return e.channels.Delete(tx, dest2.String()) })
	for _, r := range all() {
		assert.Equal(t, dest.String(), r.DestID, "signals go with their channel")
	}
	del(func(tx *filestore.Tx) (bool, error) { return e.services.Delete(tx, svc.String()) })
	for _, r := range all() {
		assert.Equal(t, other.String(), r.ServiceID, "signals go with their service")
	}
	assert.Len(t, all(), 2)

	row := all()[0]
	err = p.update(func(tx *filestore.Tx) error { return e.tables.Signals.Insert(tx, row, "pending_signals_pkey") })
	requireConstraint(t, err, "23505", "pending_signals_pkey")

	// In the caller's transaction.
	boom := errors.New("boom")
	e.destLimit = -1
	require.ErrorIs(t, p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.InsertSignalMessage(ctx, tx, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: other, Params: json.RawMessage("{}")}); err != nil {
			return err
		}
		return boom
	}), boom)
	assert.Len(t, all(), 2)
	e.destLimit = 2

	// The last place under the limit, at once.
	e.newSignal = nil
	errs := race([]*pod{e.open(t), e.open(t)}, func(_ int, p *pod) error {
		return p.InsertSignalMessage(ctx, nil, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: other, Params: json.RawMessage("{}")})
	})
	for _, err := range errs {
		requireConstraint(t, err, "23514", "pending_signals_per_dest_per_service_limit")
	}
	e.destLimit = 3
	errs = race([]*pod{e.open(t), e.open(t)}, func(_ int, p *pod) error {
		return p.InsertSignalMessage(ctx, nil, gadb.IntKeyInsertSignalMessageParams{DestID: dest, ServiceID: other, Params: json.RawMessage("{}")})
	})
	requireConstraint(t, oneFailed(t, errs), "23514", "pending_signals_per_dest_per_service_limit")
	assert.Len(t, all(), 3)
}

func TestDeltaKeysRace(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	pods := []*pod{e.open(t), e.open(t)}
	svc := pods[0].add(t, e.services)
	errs := race(pods, func(_ int, p *pod) error {
		return second(p.Create(ctx, nil, &IntegrationKey{Name: "Alpha", Type: TypeUniversal, ServiceID: svc}))
	})
	requireConstraint(t, oneFailed(t, errs), "23505", "idx_int_key_name_svc_ext")
	all, err := e.open(t).FindAllByService(ctx, svc)
	require.NoError(t, err)
	require.Len(t, all, 1)
	id := uuid.MustParse(all[0].ID)

	// A pod that doesn't know the key yet can give it a token (F29), and one
	// that doesn't know that it is gone can't.
	behind := pods[1]
	if _, ok := e.tables.Keys.Get(behind.db.DB.View(), id.String()); ok {
		behind = pods[0]
	}
	_, err = behind.GenerateToken(ctx, nil, id)
	require.NoError(t, err)
	behind = e.open(t)
	require.NoError(t, pods[0].Delete(ctx, nil, id.String()))
	_, err = behind.GenerateToken(ctx, nil, id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.ErrorIs(t, behind.SetConfig(ctx, nil, id, &gadb.UIKConfigV1{}), sql.ErrNoRows)
}
