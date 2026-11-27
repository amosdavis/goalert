package integrationkey

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
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
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables

	services *filestore.Table[parent]
	clock    *filestore.FakeClock

	keyLimit int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	s := filestore.NewSchema()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &env{store: objstore.NewMem(), schema: s, clock: clock, keyLimit: -1}
	e.services = filestore.NewTable(s, "services", func(p parent) string { return p.ID })
	e.tables = DefineDeltaTables(s)
	DeclareDeltaForeignKeys(e.tables, e.services)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, s, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

type pod struct {
	*Store
	db  deltatxn.DB
	env *env
}

// open opens the table as one GoAlert pod.
func (e *env) open(t *testing.T) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	p := &pod{db: deltatxn.DB{DB: fdb}, env: e}
	p.Store = NewDeltaStore(p.db, e.tables, DeltaLookups{
		KeysPerService: func(filestore.Reader) int { return e.keyLimit },
	})
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

func testCtx() context.Context {
	return permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser)
}

func (p *pod) create(t *testing.T, serviceID, name string, typ Type) *IntegrationKey {
	t.Helper()
	k, err := p.Create(testCtx(), nil, &IntegrationKey{Name: name, ServiceID: serviceID, Type: typ})
	require.NoError(t, err)
	return k
}

func second[T any](_ T, err error) error { return err }

// constraintError is how a broken constraint describes itself, in Postgres's
// terms, to the code that turns it into a message for the user
// (sqlutil.MapError, through errutil.MapDBError).
type constraintError interface {
	error
	SQLState() string
	ConstraintName() string
	TableName() string
	Detail() string
	Hint() string
}

// requireConstraint returns err as the broken constraint it must be.
func requireConstraint(t *testing.T, err error, code, constraint string) constraintError {
	t.Helper()
	var dbErr constraintError
	require.ErrorAs(t, err, &dbErr)
	assert.Equal(t, code, dbErr.SQLState())
	assert.Equal(t, constraint, dbErr.ConstraintName())
	return dbErr
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

	b := p.create(t, svc, "Beta", TypeGeneric)
	p.create(t, other, "Beta", TypeGeneric)
	all, err := p.FindAllByService(ctx, strings.ToUpper(svc))
	require.NoError(t, err)
	assert.ElementsMatch(t, []IntegrationKey{*want, *b}, all)
	all, err = p.FindAllByService(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Equal(t, []IntegrationKey{}, all, "an empty list, not nil, as on Postgres")

	id, err := p.GetServiceID(ctx, k.ID, TypeGeneric)
	require.NoError(t, err)
	assert.Equal(t, svc, id)
	_, err = p.GetServiceID(ctx, k.ID, Type("other"))
	require.Error(t, err, "of another type")
	_, err = p.GetServiceID(ctx, uuid.NewString(), TypeGeneric)
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
		second(p.FindAllByService(anon, svc)), second(p.GetServiceID(anon, k.ID, TypeGeneric)), second(p.Search(anon, nil)),
	} {
		assert.True(t, permission.IsPermissionError(err), "%v", err)
	}
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

	// idx_int_key_name_svc_ext
	create := func(k IntegrationKey) error { return second(p.Create(ctx, nil, &k)) }
	err := create(IntegrationKey{Name: "ALPHA", Type: TypeGeneric, ServiceID: svc})
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
}

func TestDeltaKeysLimit(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	e.keyLimit = 2
	p := e.open(t)
	svc := p.add(t, e.services)
	p.create(t, svc, "one", TypeGeneric)
	p.create(t, svc, "two", TypeGeneric)
	_, err := p.Create(ctx, nil, &IntegrationKey{Name: "three", Type: TypeGeneric, ServiceID: svc})
	cerr := requireConstraint(t, err, "23514", "integration_keys_per_service_limit")
	assert.Equal(t, "max=2", cerr.Hint())
	assert.EqualError(t, errutil.MapDBError(err), "too many integration keys on this service")
	assert.Equal(t, 2, e.tables.Keys.Len(p.db.DB.View()), "nothing was added")
	p.create(t, p.add(t, e.services), "other service", TypeGeneric)

	// The last place under the limit, at once.
	e.keyLimit = 3
	errs := race([]*pod{e.open(t), e.open(t)}, func(i int, p *pod) error {
		return second(p.Create(ctx, nil, &IntegrationKey{Name: fmt.Sprintf("key %d", i), Type: TypeGeneric, ServiceID: svc}))
	})
	requireConstraint(t, oneFailed(t, errs), "23514", "integration_keys_per_service_limit")

	e.keyLimit = -1
	p.create(t, svc, "four", TypeGeneric)
	l := p.delta.DeltaLookups
	l.KeysPerService = nil
	assert.Panics(t, func() { NewDeltaStore(p.db, e.tables, l) }, "a limit can't be left out by mistake (F8)")
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
			{ID: b, Name: "alpha", Type: "generic", ServiceID: svc},
			{ID: c, Name: "Beta", Type: "generic", ServiceID: svc},
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

func TestDeltaKeysRace(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	pods := []*pod{e.open(t), e.open(t)}
	svc := pods[0].add(t, e.services)
	errs := race(pods, func(_ int, p *pod) error {
		return second(p.Create(ctx, nil, &IntegrationKey{Name: "Alpha", Type: TypeGeneric, ServiceID: svc}))
	})
	requireConstraint(t, oneFailed(t, errs), "23505", "idx_int_key_name_svc_ext")
	all, err := e.open(t).FindAllByService(ctx, svc)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.NoError(t, pods[0].Delete(ctx, nil, all[0].ID))
	got, err := e.open(t).FindOne(ctx, all[0].ID)
	require.NoError(t, err)
	assert.Nil(t, got)
}
