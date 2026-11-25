package favorite_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/favorite"
)

type env struct {
	db     txn.DB
	users  *user.Store
	favs   *favorite.Store
	tables *user.DeltaTables
}

func define() (*filestore.Schema, *user.DeltaTables, *favorite.DeltaTables) {
	s := filestore.NewSchema()
	u := user.DefineDeltaTables(s)
	return s, u, favorite.DefineDeltaTables(s, u)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMem()
	opts := filestore.DeltaOptions{WriterID: "test", PollInterval: -1, Logf: func(string, ...any) {}}
	s0, _, _ := define()
	require.NoError(t, filestore.CreateDelta(ctx, store, s0, opts))
	s, ut, ft := define()
	fdb, err := filestore.OpenDelta(ctx, store, s, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &env{db: db, users: user.NewDeltaStore(db, ut, ft.IsUserFavorite), favs: favorite.NewDeltaStore(db, ft), tables: ut}
}

func (e *env) addUser(t *testing.T, name string) string {
	t.Helper()
	u, err := e.users.Insert(permission.SystemContext(context.Background(), "test"), &user.User{Name: name, Role: permission.RoleUser})
	require.NoError(t, err)
	return u.ID
}

func TestDeltaUsers(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newEnv(t)
	alice := e.addUser(t, "Alice")
	bob := e.addUser(t, "Bob")

	u, err := e.users.FindOne(ctx, alice)
	require.NoError(t, err)
	assert.Equal(t, "Alice", u.Name)
	assert.Equal(t, permission.RoleUser, u.Role)

	_, err = e.users.Insert(ctx, &user.User{ID: alice, Name: "Again", Role: permission.RoleUser})
	var uerr *filestore.UniqueError
	require.ErrorAs(t, err, &uerr, "the primary key holds, as on Postgres")

	// UpdateTx validates the whole user, as on Postgres, but sets only the
	// name and email.
	require.NoError(t, e.users.UpdateTx(ctx, nil, &user.User{ID: alice, Name: "Alice B", Email: "alice@example.com", Role: permission.RoleUser}))
	require.NoError(t, e.users.SetUserRoleTx(ctx, nil, alice, permission.RoleAdmin))
	u, err = e.users.FindOne(ctx, alice)
	require.NoError(t, err)
	assert.Equal(t, "Alice B", u.Name)
	assert.Equal(t, "alice@example.com", u.Email)
	assert.Equal(t, permission.RoleAdmin, u.Role)

	_, err = e.users.FindOne(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows, "callers check for sql.ErrNoRows, as with Postgres")

	many, err := e.users.FindMany(ctx, []string{alice, bob, alice, uuid.NewString()})
	require.NoError(t, err)
	assert.Len(t, many, 2, "each user once, like `where id = any($1)`")

	found, err := e.users.Search(ctx, &user.SearchOptions{})
	require.NoError(t, err)
	assert.Len(t, found, 2, "search is available on the Delta backend")
}

func TestDeltaFavorites(t *testing.T) {
	e := newEnv(t)
	alice, bob, carol := e.addUser(t, "Alice"), e.addUser(t, "Bob"), e.addUser(t, "Carol")
	svc := assignment.ServiceTarget(uuid.NewString())
	asAlice := permission.UserContext(context.Background(), alice, permission.RoleUser)
	asCarol := permission.UserContext(context.Background(), carol, permission.RoleUser)

	require.NoError(t, e.favs.Set(asAlice, nil, alice, assignment.UserTarget(bob)))
	require.NoError(t, e.favs.Set(asAlice, nil, alice, svc))
	require.NoError(t, e.favs.Set(asAlice, nil, alice, svc), "setting a favorite again changes nothing")
	require.NoError(t, e.favs.Set(asCarol, nil, carol, assignment.UserTarget(bob)))

	all, err := e.favs.FindAll(asAlice, nil, alice, nil)
	require.NoError(t, err)
	assert.Equal(t, []assignment.Target{assignment.UserTarget(bob), svc}, all, "oldest first")
	onlyUsers, err := e.favs.FindAll(asAlice, nil, alice, []assignment.TargetType{assignment.TargetTypeUser})
	require.NoError(t, err)
	assert.Equal(t, []assignment.Target{assignment.UserTarget(bob)}, onlyUsers)

	u, err := e.users.FindOne(asAlice, bob)
	require.NoError(t, err)
	assert.True(t, u.IsUserFavorite(), "the user store reads the favorite flag")
	u, err = e.users.FindOneTx(asAlice, nil, bob, true)
	require.NoError(t, err)
	assert.False(t, u.IsUserFavorite(), "not when locking for update, as on Postgres")

	require.NoError(t, e.favs.Unset(asAlice, nil, alice, assignment.UserTarget(bob)))
	all, err = e.favs.FindAll(asAlice, nil, alice, nil)
	require.NoError(t, err)
	assert.Equal(t, []assignment.Target{svc}, all)
	all, err = e.favs.FindAll(asCarol, nil, carol, nil)
	require.NoError(t, err)
	assert.Equal(t, []assignment.Target{assignment.UserTarget(bob)}, all, "unsetting a favorite leaves other users' favorites alone")

	none, err := e.favs.FindAll(asCarol, nil, uuid.NewString(), nil)
	if err == nil {
		assert.NotNil(t, none, "an empty list, not nil, as on Postgres")
	}
}

func TestDeltaFavoritesCascade(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	alice, bob := e.addUser(t, "Alice"), e.addUser(t, "Bob")
	asAlice := permission.UserContext(ctx, alice, permission.RoleUser)
	asBob := permission.UserContext(ctx, bob, permission.RoleUser)
	require.NoError(t, e.favs.Set(asAlice, nil, alice, assignment.UserTarget(bob)))
	require.NoError(t, e.favs.Set(asBob, nil, bob, assignment.UserTarget(alice)))

	// Deleting a user removes the favorites by them and of them
	// (ON DELETE CASCADE on both foreign keys).
	require.NoError(t, e.db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		_, err := e.tables.Users.Delete(ftx, bob)
		return err
	}))
	all, err := e.favs.FindAll(asAlice, nil, alice, nil)
	require.NoError(t, err)
	assert.Empty(t, all)

	err = e.favs.Set(asAlice, nil, alice, assignment.UserTarget(bob))
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr, "a favorite of a deleted user is refused (F7)")
}
