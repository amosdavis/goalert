package apikey

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

// testKeys signs JWTs with a fixed key.
type testKeys struct{ keyring.Keyring }

func (testKeys) SignJWT(c jwt.Claims) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte("test"))
}

func (testKeys) VerifyJWT(token string, c jwt.Claims, iss, aud string) (bool, error) {
	_, err := jwt.ParseWithClaims(token, c, func(*jwt.Token) (any, error) { return []byte("test"), nil },
		jwt.WithIssuer(iss), jwt.WithAudience(aud))
	return false, err
}

// userRow stands in for users.
type userRow struct {
	ID string `parquet:"id"`
}

type env struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	users  *filestore.Table[userRow]
	clock  *filestore.FakeClock
	start  time.Time
	admin  string // a user
}

func newEnv(t *testing.T) *env {
	t.Helper()
	start := time.Now().UTC().Truncate(time.Second)
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(start), start: start, admin: uuid.NewString()}
	e.clock.SetSpeed(0)
	e.clock.Set(start)
	e.tables = DefineDeltaTables(e.schema)
	e.users = filestore.NewTable(e.schema, "users", func(u userRow) string { return u.ID })
	DeclareDeltaForeignKeys(e.tables, e.users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	_, err := e.pod(t, "setup").db.Update(context.Background(), func(tx *filestore.Tx) error {
		return e.users.Put(tx, userRow{ID: e.admin})
	})
	require.NoError(t, err)
	return e
}

type pod struct {
	*Store
	db *filestore.DeltaDB
}

func (e *env) pod(t *testing.T, name string) *pod {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return &pod{Store: NewDeltaStore(deltatxn.DB{DB: db}, e.tables, testKeys{}), db: db}
}

// strict makes the pod check every key at the latest version.
func (p *pod) strict() *pod {
	p.delta.check = authdelta.NewChecker(p.delta.db, 0)
	return p
}

func (e *env) ctx() context.Context {
	return permission.UserContext(context.Background(), e.admin, permission.RoleAdmin)
}

// list lists the keys on a fresh pod.
func (e *env) list(t *testing.T) []APIKeyInfo {
	t.Helper()
	keys, err := e.pod(t, "reader-"+uuid.NewString()).FindAllAdminGraphQLKeys(e.ctx())
	require.NoError(t, err)
	return keys
}

func (e *env) opts(name string) NewAdminGQLKeyOpts {
	return NewAdminGQLKeyOpts{
		Name: name, Desc: "desc", Expires: e.start.Add(time.Hour), Role: permission.RoleUser, Query: "query Q { user { id } }",
	}
}

func (e *env) create(t *testing.T, p *pod, name string) (uuid.UUID, string) {
	t.Helper()
	id, tok, err := p.CreateAdminGraphQLKey(e.ctx(), e.opts(name))
	require.NoError(t, err)
	return id, tok
}

func (p *pod) use(tok string) (context.Context, error) {
	return p.AuthorizeGraphQL(context.Background(), tok, "agent", "10.0.0.1:1234")
}

func requireUnauthorized(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, permission.IsUnauthorized(err), "%v", err)
}

func TestDeltaKeys(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	keys, err := p.FindAllAdminGraphQLKeys(e.ctx())
	require.NoError(t, err)
	assert.NotNil(t, keys, "an empty list, not nil")
	assert.Empty(t, keys)

	id1, _ := e.create(t, p, "first")
	e.clock.Set(e.start.Add(time.Minute))
	id2, _ := e.create(t, p, "second")

	admin := uuid.MustParse(e.admin)
	want := []APIKeyInfo{
		{
			ID: id1, Name: "first", Description: "desc", ExpiresAt: e.start.Add(time.Hour), CreatedAt: e.start, UpdatedAt: e.start,
			CreatedBy: &admin, UpdatedBy: &admin, Query: "query Q { user { id } }", Role: permission.RoleUser,
		},
		{
			ID: id2, Name: "second", Description: "desc", ExpiresAt: e.start.Add(time.Hour), CreatedAt: e.start.Add(time.Minute), UpdatedAt: e.start.Add(time.Minute),
			CreatedBy: &admin, UpdatedBy: &admin, Query: "query Q { user { id } }", Role: permission.RoleUser,
		},
	}
	assert.Equal(t, want, e.list(t))

	name, desc := "renamed", ""
	require.NoError(t, p.UpdateAdminGraphQLKey(e.ctx(), id1, &name, nil))
	require.NoError(t, p.UpdateAdminGraphQLKey(e.ctx(), id2, nil, &desc))
	want[0].Name, want[0].UpdatedAt = name, e.start.Add(time.Minute)
	want[1].Description = ""
	assert.Equal(t, want, e.list(t))

	require.NoError(t, p.DeleteAdminGraphQLKey(e.ctx(), id1))
	assert.Equal(t, want[1:], e.list(t))
	require.ErrorIs(t, p.UpdateAdminGraphQLKey(e.ctx(), id1, &name, nil), sql.ErrNoRows, "a deleted key can't be changed")
	require.ErrorIs(t, p.UpdateAdminGraphQLKey(e.ctx(), uuid.New(), &name, nil), sql.ErrNoRows)
	require.NoError(t, p.DeleteAdminGraphQLKey(e.ctx(), uuid.New()), "deleting a missing key does nothing")

	row, ok := e.tables.keys.Get(p.db.View(), id1.String())
	require.True(t, ok, "a deleted key's row stays")
	assert.Equal(t, e.start.Add(time.Minute), *row.DeletedAt)
	assert.Equal(t, e.admin, *row.DeletedBy)

	_, err = p.FindAllAdminGraphQLKeys(permission.UserContext(context.Background(), e.admin, permission.RoleUser))
	assert.True(t, permission.IsPermissionError(err))
}

func TestDeltaKeyName(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	id1, _ := e.create(t, p, "name")
	id2, _ := e.create(t, p, "other")

	check := func(err error) {
		t.Helper()
		var uerr *filestore.UniqueError
		require.ErrorAs(t, err, &uerr)
		assert.Equal(t, "gql_api_keys_name_key", uerr.ConstraintName())
		err = errutil.MapDBError(err)
		assert.True(t, validation.IsClientError(err))
		assert.ErrorContains(t, err, "already in use")
	}
	_, _, err := p.CreateAdminGraphQLKey(e.ctx(), e.opts("name"))
	check(err)
	name := "name"
	check(p.UpdateAdminGraphQLKey(e.ctx(), id2, &name, nil))

	require.NoError(t, p.DeleteAdminGraphQLKey(e.ctx(), id1))
	require.NoError(t, p.UpdateAdminGraphQLKey(e.ctx(), id2, &name, nil), "a deleted key's name is free (a partial index)")
	require.NoError(t, p.DeleteAdminGraphQLKey(e.ctx(), id2))
	e.create(t, p, "name")
}

func TestDeltaKeyExpires(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	o := e.opts("far")
	o.Expires = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	_, _, err := p.CreateAdminGraphQLKey(e.ctx(), o)
	require.Error(t, err)
	assert.True(t, validation.IsClientError(err), "a time no row can hold is the user's error, not an internal one")
	o.Expires = time.Date(2262, 4, 10, 0, 0, 0, 0, time.UTC)
	_, _, err = p.CreateAdminGraphQLKey(e.ctx(), o)
	require.NoError(t, err)
	assert.Equal(t, o.Expires, e.list(t)[0].ExpiresAt)
	o.Expires = e.start.Add(-time.Hour)
	_, _, err = p.CreateAdminGraphQLKey(e.ctx(), o)
	assert.True(t, validation.IsClientError(err))
}

func TestDeltaAuthorize(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b").strict()
	id, tok := e.create(t, a, "key")

	ctx, err := a.use(tok)
	require.NoError(t, err)
	assert.Equal(t, &GQLPolicy{Version: 1, Query: "query Q { user { id } }", Role: permission.RoleUser}, PolicyFromContext(ctx))
	assert.True(t, permission.User(ctx))
	assert.False(t, permission.Admin(ctx))
	assert.Equal(t, &permission.SourceInfo{ID: id.String(), Type: permission.SourceTypeGQLAPIKey}, permission.Source(ctx))
	_, err = a.use("not a token")
	requireUnauthorized(t, err)

	_, err = b.use(tok)
	require.NoError(t, err, "a pod that hasn't followed the log yet knows a new key, and doesn't reject it for good (F29)")

	require.NoError(t, a.DeleteAdminGraphQLKey(e.ctx(), id))
	_, err = a.use(tok)
	requireUnauthorized(t, err)
	_, err = b.use(tok)
	requireUnauthorized(t, err)
	_, err = e.pod(t, "c").use(tok)
	requireUnauthorized(t, err)
}

func TestDeltaAuthorizeUnknownThenCreated(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	// b trusts its view for a while after this.
	_, other := e.create(t, a, "other")
	_, err := b.use(other)
	require.NoError(t, err)

	_, tok := e.create(t, a, "key")
	for range 3 {
		_, err = b.use(tok)
		require.NoError(t, err, "however old the pod's view is (F29)")
	}
}

func TestDeltaAuthorizeExpired(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	_, tok := e.create(t, p, "key")
	_, err := p.use(tok)
	require.NoError(t, err)
	e.clock.Set(e.start.Add(time.Hour))
	_, err = p.use(tok)
	requireUnauthorized(t, err)
	_, err = e.pod(t, "b").use(tok)
	requireUnauthorized(t, err)
}

func TestDeltaUsage(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	id, tok := e.create(t, a, "key")
	_, err := a.use(tok)
	require.NoError(t, err)
	require.NoError(t, a.Shutdown(context.Background()), "shutting down writes what isn't written yet")
	want := &APIKeyUsage{UserAgent: "agent", IP: "10.0.0.1", Time: e.start}
	assert.Equal(t, want, e.list(t)[0].LastUsed)

	ctx := context.Background()
	at := func(d time.Duration) time.Time { return e.start.Add(d) }
	uses := func(d time.Duration, ua string) map[string]keyUse {
		return map[string]keyUse{id.String(): {at: at(d), ua: ua}, uuid.NewString(): {at: at(d)}}
	}
	require.NoError(t, b.delta.writeUsage(ctx, uses(2*time.Minute, "later")), "a use of a key that doesn't exist is dropped")
	want = &APIKeyUsage{UserAgent: "later", Time: at(2 * time.Minute)}
	assert.Equal(t, want, e.list(t)[0].LastUsed, "on conflict do update, with no address if there is none")
	require.NoError(t, a.delta.writeUsage(ctx, uses(time.Minute, "earlier")))
	assert.Equal(t, want, e.list(t)[0].LastUsed, "a use written late doesn't replace a later one")

	// Addresses of both families, and none.
	for addr, ip := range map[string]string{"[2001:db8::1]:99": "2001:db8::1", "192.0.2.7:1": "192.0.2.7", "": "", "garbage": ""} {
		e.clock.Advance(time.Hour)
		require.NoError(t, a._updateLastUsed(ctx, id, "agent\x00", addr))
		require.NoError(t, a.Shutdown(ctx))
		u := e.list(t)[0].LastUsed
		assert.Equal(t, ip, u.IP, addr)
		assert.Equal(t, "agent", u.UserAgent)
	}

	p := e.pod(t, "c")
	assert.Equal(t, 1, e.tables.usage.Len(p.db.View()), "one row for each key")
	_, err = p.db.Update(ctx, func(tx *filestore.Tx) error {
		_, err := e.tables.keys.Delete(tx, id.String())
		return err
	})
	require.NoError(t, err)
	assert.Zero(t, e.tables.usage.Len(p.db.View()), "ON DELETE CASCADE")
}

func TestDeltaUsageRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	id, _ := e.create(t, pods[0], "key")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, pods[i%2].delta.writeUsage(context.Background(), map[string]keyUse{
				id.String(): {at: e.start.Add(time.Duration(i) * time.Minute), ua: "agent"},
			}))
		}()
	}
	wg.Wait()
	assert.Equal(t, e.start.Add(7*time.Minute), e.list(t)[0].LastUsed.Time, "the latest use wins (F4)")
	assert.Equal(t, 1, e.tables.usage.Len(e.pod(t, "c").db.View()), "gql_api_key_usage_api_key_id_key (F8)")
}

func TestDeltaKeyNameRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := pods[i%2].CreateAdminGraphQLKey(e.ctx(), e.opts("name"))
			if err == nil {
				created.Add(1)
				return
			}
			var uerr *filestore.UniqueError
			assert.ErrorAs(t, err, &uerr)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "one key gets the name (F8)")
	assert.Len(t, e.list(t), 1)
}

func TestDeltaKeyUserDeleted(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	id, tok := e.create(t, p, "key")
	id2, _ := e.create(t, p, "deleted")
	require.NoError(t, p.DeleteAdminGraphQLKey(e.ctx(), id2))

	_, err := p.db.Update(context.Background(), func(tx *filestore.Tx) error {
		_, err := e.users.Delete(tx, e.admin)
		return err
	})
	require.NoError(t, err)
	keys := e.list(t)
	require.Len(t, keys, 1)
	assert.Equal(t, id, keys[0].ID)
	assert.Nil(t, keys[0].CreatedBy, "ON DELETE SET NULL")
	assert.Nil(t, keys[0].UpdatedBy)
	row, _ := e.tables.keys.Get(p.db.View(), id2.String())
	assert.Nil(t, row.DeletedBy)
	assert.NotNil(t, row.DeletedAt)
	_, err = p.use(tok)
	require.NoError(t, err, "the key outlives the user who made it")

	_, _, err = p.CreateAdminGraphQLKey(e.ctx(), e.opts("again"))
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "gql_api_keys_created_by_fkey", fkErr.ConstraintName())

	// Without a user, such as with another API key, there is nobody to record.
	_, _, err = p.CreateAdminGraphQLKey(permission.SystemContext(context.Background(), "Test"), e.opts("system"))
	require.NoError(t, err)
}
