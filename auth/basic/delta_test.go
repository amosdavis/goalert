package basic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
	"golang.org/x/crypto/bcrypt"
)

// userRow and subjectRow stand in for users and auth_subjects.
type userRow struct {
	ID string `parquet:"id"`
}

type subjectRow struct {
	Key    string `parquet:"key"`
	UserID string `parquet:"user_id"`
}

type env struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	users    *filestore.Table[userRow]
	subjects *filestore.Table[subjectRow]
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema()}
	e.tables = DefineDeltaTables(e.schema)
	e.users = filestore.NewTable(e.schema, "users", func(u userRow) string { return u.ID })
	e.subjects = filestore.NewTable(e.schema, "subjects", func(s subjectRow) string { return s.Key })
	DeclareDeltaForeignKeys(e.tables, e.users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

type pod struct {
	*Store
	db  *filestore.DeltaDB
	txn txn.DB
}

func (e *env) pod(t *testing.T, name string) *pod {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	p := &pod{db: db, txn: deltatxn.DB{DB: db}}
	p.Store = NewDeltaStore(p.txn, e.tables, func(tx *filestore.Tx, a user.AuthSubject) error {
		return e.subjects.Put(tx, subjectRow{Key: a.ProviderID + "/" + a.SubjectID, UserID: a.UserID})
	})
	return p
}

func (e *env) addUser(t *testing.T, p *pod) string {
	t.Helper()
	id := uuid.NewString()
	_, err := p.db.Update(context.Background(), func(tx *filestore.Tx) error { return e.users.Put(tx, userRow{ID: id}) })
	require.NoError(t, err)
	return id
}

func (e *env) rows(t *testing.T) []basicRow {
	t.Helper()
	var rows []basicRow
	e.tables.users.Ascend(e.pod(t, "reader-"+uuid.NewString()).db.View(), func(r basicRow) bool {
		rows = append(rows, r)
		return true
	})
	return rows
}

func (e *env) subjectUsers(t *testing.T) map[string]string {
	t.Helper()
	m := make(map[string]string)
	e.subjects.Ascend(e.pod(t, "reader-"+uuid.NewString()).db.View(), func(s subjectRow) bool { m[s.Key] = s.UserID; return true })
	return m
}

// pw is a cheap hash, to keep the tests quick. The last test uses real ones.
func pw(t *testing.T, password string) HashedPassword {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	return hashed(h)
}

var (
	ctx    = config.Config{}.Context(context.Background())
	sysCtx = permission.SystemContext(ctx, "Test")
)

func TestDeltaCreateAndValidate(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := e.addUser(t, p), e.addUser(t, p)

	require.NoError(t, p.CreateTx(sysCtx, nil, alice, "alice", pw(t, "password1")))
	password2 := pw(t, "password2")
	require.NoError(t, p.txn.Run(ctx, nil, func(tx txn.Tx) error {
		return p.CreateTx(sysCtx, tx, bob, "bob", password2)
	}), "in a transaction")
	assert.Equal(t, map[string]string{"basic/alice": alice, "basic/bob": bob}, e.subjectUsers(t), "trg_insert_basic_user")

	id, err := p.Validate(ctx, "alice", "password1")
	require.NoError(t, err)
	assert.Equal(t, alice, id)
	_, err = p.Validate(ctx, "alice", "password2")
	require.ErrorContains(t, err, "invalid password")
	_, err = p.Validate(ctx, "carol", "password1")
	require.EqualError(t, err, "invalid username")

	// A fresh pod reads the same.
	id, err = e.pod(t, "b").Validate(ctx, "bob", "password2")
	require.NoError(t, err)
	assert.Equal(t, bob, id)

	asAlice := permission.UserContext(ctx, alice, permission.RoleUser)
	v, err := p.ValidatePassword(asAlice, "password1")
	require.NoError(t, err)
	assert.Equal(t, alice, v.UserID())
	_, err = p.ValidatePassword(asAlice, "wrong-password")
	assert.True(t, validation.IsClientError(err))
	_, err = p.ValidatePassword(permission.UserContext(ctx, uuid.NewString(), permission.RoleUser), "password1")
	require.EqualError(t, err, "unknown userID")
}

func TestDeltaCreateConstraints(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := e.addUser(t, p), e.addUser(t, p)
	require.NoError(t, p.CreateTx(sysCtx, nil, alice, "alice", pw(t, "password1")))

	check := func(err error, constraint, message string) {
		t.Helper()
		var c interface{ ConstraintName() string }
		require.ErrorAs(t, err, &c)
		assert.Equal(t, constraint, c.ConstraintName())
		assert.ErrorContains(t, errutil.MapDBError(err), message)
		assert.NotContains(t, err.Error(), "$2a$", "a password hash must not be in an error (F20)")
	}
	check(p.CreateTx(sysCtx, nil, bob, "alice", pw(t, "password1")), "auth_basic_users_username_key", "already in use")
	check(p.CreateTx(sysCtx, nil, alice, "alice2", pw(t, "password1")), "auth_basic_users_pkey", "already has a basic auth username")
	check(p.CreateTx(sysCtx, nil, uuid.NewString(), "carol", pw(t, "password1")), "auth_basic_users_user_id_fkey", "user does not exist")

	assert.Len(t, e.rows(t), 1)
	assert.Equal(t, map[string]string{"basic/alice": alice}, e.subjectUsers(t), "a failed insert leaves no subject (F2)")
}

func TestDeltaSubjectMovesToNewUser(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice, bob := e.addUser(t, p), e.addUser(t, p)
	_, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
		return e.subjects.Put(tx, subjectRow{Key: "basic/joe", UserID: alice})
	})
	require.NoError(t, err)
	require.NoError(t, p.CreateTx(sysCtx, nil, bob, "joe", pw(t, "password1")))
	assert.Equal(t, map[string]string{"basic/joe": bob}, e.subjectUsers(t), "ON CONFLICT DO UPDATE SET user_id")
}

func TestDeltaUserDeleted(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p)
	require.NoError(t, p.CreateTx(sysCtx, nil, alice, "alice", pw(t, "password1")))
	_, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
		_, err := e.users.Delete(tx, alice)
		return err
	})
	require.NoError(t, err)
	assert.Empty(t, e.rows(t), "ON DELETE CASCADE")
	_, err = p.Validate(ctx, "alice", "password1")
	require.Error(t, err)
}

func TestDeltaUpdate(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	alice, bob := e.addUser(t, a), e.addUser(t, a)
	require.NoError(t, a.CreateTx(sysCtx, nil, alice, "alice", pw(t, "password1")))
	asAlice := permission.UserContext(ctx, alice, permission.RoleUser)
	asAdmin := permission.UserContext(ctx, bob, permission.RoleAdmin)

	_, err := b.Validate(ctx, "alice", "password1")
	require.NoError(t, err, "a pod that hasn't followed the log yet knows a new user (F29)")

	old, err := a.ValidatePassword(asAlice, "password1")
	require.NoError(t, err)
	password2 := pw(t, "password2")
	require.NoError(t, a.txn.Run(ctx, nil, func(tx txn.Tx) error {
		return a.UpdateTx(asAlice, tx, alice, old, password2)
	}))
	_, err = b.Validate(ctx, "alice", "password1")
	require.Error(t, err, "a changed password counts on every pod at once (F29)")
	_, err = b.Validate(ctx, "alice", "password2")
	require.NoError(t, err)

	require.NoError(t, a.UpdateTx(asAdmin, nil, alice, nil, pw(t, "password3")), "an admin needs no old password")
	_, err = b.Validate(ctx, "alice", "password3")
	require.NoError(t, err)

	assert.ErrorContains(t, a.UpdateTx(asAdmin, nil, uuid.NewString(), nil, pw(t, "password3")), "does not have basic auth configured")
	assert.True(t, validation.IsClientError(a.UpdateTx(asAlice, nil, alice, nil, pw(t, "password4"))))
	assert.True(t, permission.IsPermissionError(a.UpdateTx(asAlice, nil, bob, old, pw(t, "password4"))))
	require.Len(t, e.rows(t), 1)
	assert.EqualValues(t, 1, e.rows(t)[0].ID)
}

func TestDeltaCreateRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	alice := e.addUser(t, pods[0])
	users := make([]string, 6)
	for i := range users {
		users[i] = e.addUser(t, pods[0])
	}
	password := pw(t, "password1")

	// One username, several users.
	var created atomic.Int32
	var wg sync.WaitGroup
	for i, id := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pods[i%2].CreateTx(sysCtx, nil, id, "joe", password)
			if err == nil {
				created.Add(1)
				return
			}
			var uerr *filestore.UniqueError
			if assert.ErrorAs(t, err, &uerr) {
				assert.Equal(t, "auth_basic_users_username_key", uerr.Index)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "one user gets the username (F8)")
	rows := e.rows(t)
	require.Len(t, rows, 1)
	assert.Equal(t, map[string]string{"basic/joe": rows[0].UserID}, e.subjectUsers(t))

	// One user, several usernames.
	created.Store(0)
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pods[i%2].CreateTx(sysCtx, nil, alice, "alice"+string(rune('0'+i)), password)
			if err == nil {
				created.Add(1)
				return
			}
			var uerr *filestore.UniqueError
			if assert.ErrorAs(t, err, &uerr) {
				assert.Equal(t, "auth_basic_users_pkey", uerr.Index)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "a user gets one username (F8)")
	assert.Len(t, e.rows(t), 2)
	assert.Len(t, e.subjectUsers(t), 2)
}

func TestDeltaRealHash(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	alice := e.addUser(t, p)
	h, err := p.NewHashedPassword(sysCtx, "password1")
	require.NoError(t, err)
	require.NoError(t, p.CreateTx(sysCtx, nil, alice, "alice", h))
	id, err := e.pod(t, "b").Validate(ctx, "alice", "password1")
	require.NoError(t, err)
	assert.Equal(t, alice, id)
}
