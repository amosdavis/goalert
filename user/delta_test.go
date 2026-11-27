package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

type deltaTestEnv struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	favorite FavoriteFunc
}

func newDeltaTestEnv(t *testing.T, extra func(*filestore.Schema, *DeltaTables)) *deltaTestEnv {
	t.Helper()
	e := &deltaTestEnv{store: objstore.NewMem(), schema: filestore.NewSchema()}
	e.tables = DefineDeltaTables(e.schema)
	if extra != nil {
		extra(e.schema, e.tables)
	}
	opts := filestore.DeltaOptions{WriterID: "admin", PollInterval: -1, Logf: func(string, ...any) {}}
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, opts))
	return e
}

func (e *deltaTestEnv) pod(t *testing.T, name string, opts ...DeltaOption) (*Store, txn.DB) {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return NewDeltaStore(db, e.tables, e.favorite, opts...), db
}

func addDeltaUser(t *testing.T, s *Store, name, email string) string {
	t.Helper()
	u, err := s.Insert(permission.SystemContext(context.Background(), "test"), &User{Name: name, Email: email, Role: permission.RoleUser})
	require.NoError(t, err)
	return u.ID
}

func userIDs(users []User) []string {
	ids := make([]string, len(users))
	for i, u := range users {
		ids[i] = u.ID
	}
	return ids
}

func subjectIDs(subjects []AuthSubject) []string {
	ids := make([]string, len(subjects))
	for i, a := range subjects {
		ids[i] = a.SubjectID
	}
	return ids
}

// deltaUserExists reports whether s finds the user.
func deltaUserExists(t *testing.T, s *Store, id string) bool {
	t.Helper()
	_, err := s.FindOne(permission.SystemContext(context.Background(), "test"), id)
	if !errors.Is(err, sql.ErrNoRows) {
		require.NoError(t, err)
	}
	return err == nil
}

// constraintName returns the name of the constraint that err says was broken.
func constraintName(err error) string {
	var e interface{ ConstraintName() string }
	if errors.As(err, &e) {
		return e.ConstraintName()
	}
	return ""
}

// inTx runs fn in a write transaction.
func inTx(db txn.DB, fn func(tx *filestore.Tx) error) error {
	return db.Run(context.Background(), nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return fn(ftx)
	})
}

// both runs fn n times at once on each of two pods.
func both(a, b *Store, n int, fn func(s *Store, i int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range []*Store{a, b} {
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				fn(s, i)
			}()
		}
	}
	close(start)
	wg.Wait()
}

// newConstraintEnv has a stand-in for the contact methods, which belong to
// users, while users and auth subjects refer to them.
func newConstraintEnv(t *testing.T) (*deltaTestEnv, *filestore.Table[deltaTestContactMethod]) {
	t.Helper()
	var cms *filestore.Table[deltaTestContactMethod]
	e := newDeltaTestEnv(t, func(schema *filestore.Schema, tables *DeltaTables) {
		cms = filestore.NewTable(schema, "user_contact_methods", func(c deltaTestContactMethod) string { return c.ID })
		filestore.References(cms, "user_contact_methods_user_id_fkey", tables.Users,
			func(c deltaTestContactMethod) (string, bool) { return c.UserID, true }, filestore.Cascade, nil)
		DeclareDeltaForeignKeys(tables, cms, func(r filestore.Reader, id string) (string, bool) {
			c, ok := cms.Get(r, id)
			return c.UserID, ok
		})
	})
	return e, cms
}

func TestDeltaInsertRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	a, _ := e.pod(t, "a")
	b, _ := e.pod(t, "b")
	id := uuid.NewString()

	var inserted, refused atomic.Int32
	both(a, b, 4, func(s *Store, i int) {
		_, err := s.Insert(ctx, &User{ID: id, Name: "Alice", Role: permission.RoleUser})
		if err == nil {
			inserted.Add(1)
		} else if assert.Equal(t, "goalert_user_pkey", constraintName(err)) {
			refused.Add(1)
		}
	})
	assert.EqualValues(t, 1, inserted.Load(), "one insert wins (F8)")
	assert.EqualValues(t, 7, refused.Load())

	fresh, _ := e.pod(t, "fresh")
	assert.True(t, deltaUserExists(t, fresh, id))
}

// TestDeltaAlertStatusTrigger covers trg_enforce_status_update_same_user,
// which runs on every update of a user, whatever it changes.
func TestDeltaAlertStatusTrigger(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e, cms := newConstraintEnv(t)
	s, db := e.pod(t, "a")
	alice, bob := addDeltaUser(t, s, "Alice", ""), addDeltaUser(t, s, "Bob", "")
	cm := uuid.NewString()
	setOwner := func(userID string) {
		require.NoError(t, inTx(db, func(tx *filestore.Tx) error {
			return cms.Put(tx, deltaTestContactMethod{ID: cm, UserID: userID})
		}))
	}
	setOwner(alice)
	require.NoError(t, inTx(db, func(tx *filestore.Tx) error {
		row, _ := e.tables.Users.Get(tx, alice)
		row.AlertStatusCMID = &cm
		return e.tables.PutUser(tx, row)
	}))

	writes := func(name string) map[string]error {
		return map[string]error{
			"update": s.UpdateTx(ctx, nil, &User{ID: alice, Name: name, Role: permission.RoleUser}),
			"role":   s.SetUserRoleTx(ctx, nil, alice, permission.RoleAdmin),
			"profile": inTx(db, func(tx *filestore.Tx) error {
				return e.tables.UpdateProfile(tx, alice, name+" too", "")
			}),
		}
	}
	for what, err := range writes("Alice B") {
		require.NoError(t, err, what)
	}
	require.NoError(t, deltatxn.Read(ctx, db, nil, func(s deltatxn.Scope) error {
		row, _ := e.tables.Users.Get(s.Reader, alice)
		assert.Equal(t, "Alice B too", row.Name)
		require.NotNil(t, row.AlertStatusCMID, "other changes keep the contact method")
		assert.Equal(t, cm, *row.AlertStatusCMID)
		return nil
	}))
	u, err := s.FindOne(ctx, alice)
	require.NoError(t, err)
	assert.Empty(t, u.AlertStatusCMID, "which the Postgres store doesn't read")

	setOwner(bob)
	for what, err := range writes("Alice C") {
		assert.Equal(t, "alert_status_user_id_match", constraintName(err), what)
	}
	require.NoError(t, s.SetUserRoleTx(ctx, nil, bob, permission.RoleAdmin), "bob has no status contact method")

	// A schema without contact methods can't check the rule, so it refuses.
	plain := newDeltaTestEnv(t, nil)
	_, pdb := plain.pod(t, "plain")
	err = inTx(pdb, func(tx *filestore.Tx) error {
		return plain.tables.PutUser(tx, userRow{ID: uuid.NewString(), Name: "X", AlertStatusCMID: &cm})
	})
	require.ErrorContains(t, err, "DeclareDeltaForeignKeys")
}

func TestDeltaPutUser(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	id := uuid.NewString()
	put := func(u userRow) error {
		return inTx(db, func(tx *filestore.Tx) error { return e.tables.PutUser(tx, u) })
	}
	require.NoError(t, put(userRow{ID: strings.ToUpper(id), Name: "Alice", Bio: "about her"}))
	require.Error(t, put(userRow{ID: uuid.NewString(), Name: "X", Role: "boss"}), "not in enum_user_role")
	require.Error(t, put(userRow{ID: "not-a-uuid", Name: "X"}))

	u, err := s.FindOne(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, permission.RoleUnknown, u.Role, "the column's default")
	require.NoError(t, s.UpdateTx(ctx, nil, &User{ID: id, Name: "Alice B", Role: permission.RoleUser}))
	require.NoError(t, deltatxn.Read(ctx, db, nil, func(s deltatxn.Scope) error {
		assert.Equal(t, 1, e.tables.Users.Len(s.Reader))
		row, _ := e.tables.Users.Get(s.Reader, id)
		assert.Equal(t, userRow{ID: id, Name: "Alice B", Bio: "about her", Role: "unknown"}, row, "columns the store doesn't use are kept")
		return nil
	}))
}

func TestDeltaUpdateProfile(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	alice := addDeltaUser(t, s, "Alice", "alice@example.com")
	version := func() uint64 { return db.(deltatxn.DB).DB.View().Index() }
	update := func(id, name, email string) error {
		return inTx(db, func(tx *filestore.Tx) error { return e.tables.UpdateProfile(tx, id, name, email) })
	}

	before := version()
	require.NoError(t, update(alice, "", ""))
	require.NoError(t, update(alice, "Alice", "alice@example.com"))
	require.NoError(t, update(uuid.NewString(), "Nobody", ""), "an UPDATE of no rows")
	assert.Equal(t, before, version(), "nothing changed, so there is no commit (F15)")
	require.Error(t, update("not-a-uuid", "X", ""))

	require.NoError(t, update(alice, "", "new@example.com"))
	u, err := s.FindOne(ctx, alice)
	require.NoError(t, err)
	assert.Equal(t, "Alice", u.Name)
	assert.Equal(t, "new@example.com", u.Email)
	for email, want := range map[string][]string{"alice@example.com": {}, "new@example.com": {alice}} {
		users, err := s.Search(ctx, &SearchOptions{Search: "email=" + email})
		require.NoError(t, err)
		assert.Equal(t, want, userIDs(users), "the email index follows the change")
	}
}

// TestDeltaDeleteUserCascade deletes a user who refers to a contact method
// that refers to them.
func TestDeltaDeleteUserCascade(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e, cms := newConstraintEnv(t)
	s, db := e.pod(t, "a", WithRotations(func(tx *filestore.Tx, userID string) error {
		if _, ok := e.tables.Users.Get(tx, userID); !ok {
			return nil // so if the user were deleted first, the row below would stay
		}
		_, err := cms.Delete(tx, "in-a-rotation/"+userID)
		return err
	}))
	alice, bob := addDeltaUser(t, s, "Alice", ""), addDeltaUser(t, s, "Bob", "")
	cmA, cmB := uuid.NewString(), uuid.NewString()
	require.NoError(t, inTx(db, func(tx *filestore.Tx) error {
		for id, owner := range map[string]string{cmA: alice, cmB: bob} {
			if err := cms.Put(tx, deltaTestContactMethod{ID: id, UserID: owner}); err != nil {
				return err
			}
			row, _ := e.tables.Users.Get(tx, owner)
			row.AlertStatusCMID = &id
			if err := e.tables.PutUser(tx, row); err != nil {
				return err
			}
		}
		// Not alice's, so only leaving rotations removes it.
		if err := cms.Put(tx, deltaTestContactMethod{ID: "in-a-rotation/" + alice, UserID: bob}); err != nil {
			return err
		}
		for _, a := range []AuthSubject{
			{ProviderID: "p", SubjectID: "a", UserID: alice},
			{ProviderID: "p", SubjectID: "b", UserID: bob},
			{ProviderID: "p", SubjectID: "b through alice", UserID: bob},
		} {
			if err := e.tables.InsertAuthSubject(tx, a); err != nil {
				return err
			}
		}
		row, _ := e.tables.AuthSubjects.Get(tx, AuthSubjectKey("p", "b through alice"))
		row.CMID = &cmA
		return e.tables.AuthSubjects.Put(tx, row)
	}))

	require.Error(t, s.DeleteManyTx(ctx, nil, []string{bob, "not-a-uuid"}), "an id Postgres refuses")
	require.True(t, deltaUserExists(t, s, bob))
	require.NoError(t, s.DeleteManyTx(ctx, nil, []string{alice, strings.ToUpper(alice), uuid.NewString()}),
		"each user once, and one that doesn't exist is a DELETE of no rows")

	fresh, fdb := e.pod(t, "fresh")
	assert.False(t, deltaUserExists(t, fresh, alice))
	subjects, err := fresh.FindAllAuthSubjectsForUser(ctx, bob)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, subjectIDs(subjects), "a subject goes with its contact method too")
	subjects, err = fresh.FindAllAuthSubjectsForUser(ctx, alice)
	require.NoError(t, err)
	assert.Nil(t, subjects)
	require.NoError(t, deltatxn.Read(ctx, fdb, nil, func(s deltatxn.Scope) error {
		var left []string
		cms.Ascend(s.Reader, func(c deltaTestContactMethod) bool {
			left = append(left, c.ID)
			return true
		})
		assert.Equal(t, []string{cmB}, left)
		row, ok := e.tables.Users.Get(s.Reader, bob)
		require.True(t, ok)
		require.NotNil(t, row.AlertStatusCMID, "bob's stays")
		assert.Equal(t, cmB, *row.AlertStatusCMID)
		return nil
	}))
}

func TestDeltaTransaction(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a", WithRotations(func(*filestore.Tx, string) error { return nil }))

	// What the first login of a user does, in one transaction.
	create := func(tx txn.Tx, name string) (*User, error) {
		u, err := s.InsertTx(ctx, tx, &User{Name: name, Role: permission.RoleUser})
		if err != nil {
			return nil, err
		}
		err = s.AddAuthSubjectTx(ctx, tx, &AuthSubject{ProviderID: "p", SubjectID: name, UserID: u.ID})
		if err != nil {
			return nil, err
		}
		return s.FindOneTx(ctx, tx, u.ID, true)
	}
	var alice *User
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		var err error
		alice, err = create(tx, "alice")
		return err
	}))
	u, err := s.FindOneBySubject(ctx, "p", "alice")
	require.NoError(t, err)
	assert.Equal(t, alice, u)

	boom := errors.New("boom")
	err = db.Run(ctx, nil, func(tx txn.Tx) error {
		if _, err := create(tx, "bob"); err != nil {
			return err
		}
		if err := s.DeleteAuthSubjectTx(ctx, tx, &AuthSubject{ProviderID: "p", SubjectID: "alice", UserID: alice.ID}); err != nil {
			return err
		}
		if err := s.DeleteManyTx(ctx, tx, []string{alice.ID}); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)

	fresh, _ := e.pod(t, "fresh")
	u, err = fresh.FindOneBySubject(ctx, "p", "alice")
	require.NoError(t, err)
	assert.Equal(t, alice, u, "a failed transaction changes nothing")
	users, err := fresh.Search(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{alice.ID}, userIDs(users))

	require.ErrorIs(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		return s.DeleteManyTx(ctx, tx, []string{alice.ID})
	}), deltatxn.ErrReadOnly)
}

func TestDeltaUserPermissions(t *testing.T) {
	sys := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, _ := e.pod(t, "a", WithRotations(func(*filestore.Tx, string) error { return nil }))
	alice, bob := addDeltaUser(t, s, "Alice", ""), addDeltaUser(t, s, "Bob", "")
	require.NoError(t, s.AddAuthSubjectTx(sys, nil, &AuthSubject{ProviderID: "p", SubjectID: "a", UserID: alice}))
	asAlice := permission.UserContext(context.Background(), alice, permission.RoleUser)

	subjects, err := s.FindAllAuthSubjectsForUser(asAlice, alice)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, subjectIDs(subjects), "a user sees their own subjects")
	_, err = s.Search(asAlice, &SearchOptions{FavoritesUserID: alice, FavoritesFirst: true})
	require.NoError(t, err)

	_, oneErr := s.FindOneBySubject(asAlice, "p", "a")
	_, someErr := s.FindSomeAuthSubjectsForProvider(asAlice, 1, "", "p")
	_, searchErr := s.Search(asAlice, &SearchOptions{FavoritesUserID: bob})
	_, allErr := s.FindAllAuthSubjectsForUser(asAlice, bob)
	_, existErr := s.UserExists(context.Background())
	for name, err := range map[string]error{
		"FindOneBySubject":                oneErr,
		"FindSomeAuthSubjectsForProvider": someErr,
		"Search":                          searchErr,
		"FindAllAuthSubjectsForUser":      allErr,
		"UserExists":                      existErr,
		"SetAuthSubject":                  s.SetAuthSubject(asAlice, "p", "a", bob),
		"DeleteManyTx":                    s.DeleteManyTx(asAlice, nil, []string{bob}),
		"AddAuthSubjectTx":                s.AddAuthSubjectTx(asAlice, nil, &AuthSubject{ProviderID: "p", SubjectID: "b", UserID: bob}),
		"DeleteAuthSubjectTx":             s.DeleteAuthSubjectTx(asAlice, nil, &AuthSubject{ProviderID: "p", SubjectID: "a", UserID: alice}),
		"AuthSubjectsFunc":                s.AuthSubjectsFunc(asAlice, "p", nil, func(AuthSubject) error { return nil }),
		"WithoutAuthProviderFunc":         s.WithoutAuthProviderFunc(asAlice, "p", func(User) error { return nil }),
	} {
		assert.True(t, permission.IsPermissionError(err), "%s: %v", name, err)
	}

	// Validation is shared with Postgres too.
	assert.Error(t, s.SetAuthSubject(sys, "", "a", alice))
	assert.Error(t, s.SetAuthSubject(sys, "p", "zero\x00byte", alice))
	assert.Error(t, s.DeleteManyTx(sys, nil, make([]string, 101)))
	_, err = s.FindSomeAuthSubjectsForProvider(sys, 9001, "", "p")
	assert.Error(t, err)
}

func TestDeltaUserCRUD(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	rawID := strings.ToUpper(uuid.NewString())
	id := strings.ToLower(rawID)

	inserted, err := s.Insert(ctx, &User{
		ID: rawID, Name: "Alice", Email: "Alice@Example.COM",
		Role: permission.RoleUser, AlertStatusCMID: uuid.NewString(),
	})
	require.NoError(t, err)
	assert.Equal(t, rawID, inserted.ID, "Insert returns the normalized input, as on Postgres")

	u, err := s.FindOne(ctx, rawID)
	require.NoError(t, err)
	assert.Equal(t, id, u.ID, "UUID keys are canonical and case-insensitive")
	assert.Equal(t, "alice@example.com", u.Email)
	assert.Empty(t, u.AlertStatusCMID, "the insert query does not write this field")

	require.NoError(t, s.UpdateTx(ctx, nil, &User{ID: rawID, Name: "Alice B", Email: "alice+b@example.com", Role: permission.RoleAdmin}))
	u, err = s.FindOne(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "Alice B", u.Name)
	assert.Equal(t, "alice+b@example.com", u.Email)
	assert.Equal(t, permission.RoleUser, u.Role, "update changes only name and email")
	require.NoError(t, s.SetUserRoleTx(ctx, nil, rawID, permission.RoleAdmin))

	// The profile helper used by auth providers canonicalizes IDs and leaves
	// empty fields alone.
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return e.tables.UpdateProfile(ftx, rawID, "Alice Profile", "")
	}))
	u, err = s.FindOne(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "Alice Profile", u.Name)
	assert.Equal(t, "alice+b@example.com", u.Email)
	assert.Equal(t, permission.RoleAdmin, u.Role)

	many, err := s.FindMany(ctx, []string{rawID, id, uuid.NewString()})
	require.NoError(t, err)
	assert.Equal(t, []string{id}, userIDs(many), "duplicates and missing IDs behave like id = any(...)")
	many, err = s.FindMany(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, many, "the Postgres implementation returns an allocated empty slice")
	assert.Empty(t, many)

	_, err = s.FindOne(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = s.FindOne(ctx, "not-a-uuid")
	require.Error(t, err)
	_, err = s.Insert(ctx, &User{ID: "not-a-uuid", Name: "Bad", Role: permission.RoleUser})
	require.Error(t, err)

	checker, err := s.UserExists(ctx)
	require.NoError(t, err)
	assert.True(t, checker.UserExistsString(rawID))
	assert.True(t, checker.UserExistsUUID(uuid.MustParse(id)))
	assert.False(t, checker.UserExistsString("not-a-uuid"))
	assert.False(t, checker.UserExistsUUID(uuid.New()))

	// A checker keeps one snapshot, like the map returned by the SQL store.
	other := addDeltaUser(t, s, "Other", "")
	assert.False(t, checker.UserExistsString(other))
	checker, err = s.UserExists(ctx)
	require.NoError(t, err)
	assert.True(t, checker.UserExistsString(other))
}

func TestDeltaUserSearch(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	var carol string
	s, _ := e.pod(t, "a", WithContactMethods(func(_ filestore.Reader, destType string, destArgs map[string]string) []string {
		if destType == "sms" && len(destArgs) == 1 && destArgs["phone"] == "+15551234567" {
			return []string{strings.ToUpper(carol)}
		}
		return nil
	}))
	alice := addDeltaUser(t, s, "Alice", "alice@example.com")
	bob := addDeltaUser(t, s, "Bob", "bob@example.com")
	bobB := addDeltaUser(t, s, "Bob B", "bob-b@example.com")
	carol = addDeltaUser(t, s, "Carol", "carol@example.com")
	e.favorite = func(_ filestore.Reader, userID, targetUserID string) bool {
		return userID == alice && (targetUserID == bob || targetUserID == carol)
	}

	// The environment supplies the favorite function when a store is built.
	favoriteStore, _ := e.pod(t, "favorites")

	users, err := s.Search(ctx, &SearchOptions{Limit: 2})
	require.NoError(t, err)
	assert.Equal(t, []string{alice, bob}, userIDs(users))

	users, err = s.Search(ctx, &SearchOptions{After: SearchCursor{Name: "Bob"}})
	require.NoError(t, err)
	assert.Equal(t, []string{bobB, carol}, userIDs(users), "the cursor is exclusive")

	users, err = s.Search(ctx, &SearchOptions{Search: "bo b"})
	require.NoError(t, err)
	assert.Equal(t, []string{bobB}, userIDs(users), "ordered word prefixes match")

	users, err = s.Search(ctx, &SearchOptions{Search: "email=ALICE@example.com"})
	require.NoError(t, err)
	assert.Equal(t, []string{alice}, userIDs(users), "email search is case-insensitive")

	users, err = s.Search(ctx, &SearchOptions{Only: []string{strings.ToUpper(carol), strings.ToUpper(alice)}})
	require.NoError(t, err)
	assert.Equal(t, []string{alice, carol}, userIDs(users))
	users, err = s.Search(ctx, &SearchOptions{Omit: []string{bob, bobB}})
	require.NoError(t, err)
	assert.Equal(t, []string{alice, carol}, userIDs(users))

	users, err = favoriteStore.Search(ctx, &SearchOptions{FavoritesUserID: alice, FavoritesOnly: true})
	require.NoError(t, err)
	assert.Equal(t, []string{bob, carol}, userIDs(users))
	assert.True(t, users[0].IsUserFavorite())
	users, err = favoriteStore.Search(ctx, &SearchOptions{FavoritesUserID: alice, FavoritesFirst: true})
	require.NoError(t, err)
	assert.Equal(t, []string{bob, carol, alice, bobB}, userIDs(users))

	// After a favorite, later favorites and every nonfavorite follow. After a
	// nonfavorite, favorites do not reappear on the next page.
	users, err = favoriteStore.Search(ctx, &SearchOptions{
		FavoritesUserID: alice, FavoritesFirst: true,
		After: SearchCursor{Name: "Bob", IsFavorite: true},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{carol, alice, bobB}, userIDs(users))
	users, err = favoriteStore.Search(ctx, &SearchOptions{
		FavoritesUserID: alice, FavoritesFirst: true,
		After: SearchCursor{Name: "Alice"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{bobB}, userIDs(users))

	users, err = s.Search(ctx, &SearchOptions{DestType: "sms", DestArgs: map[string]string{"phone": "+15551234567"}})
	require.NoError(t, err)
	assert.Equal(t, []string{carol}, userIDs(users))
	users, err = s.Search(ctx, &SearchOptions{Search: "nobody"})
	require.NoError(t, err)
	assert.Nil(t, users, "no rows is a nil slice on both backends")

	plain, _ := e.pod(t, "plain")
	_, err = plain.Search(ctx, &SearchOptions{DestArgs: map[string]string{"phone": "+15551234567"}})
	require.ErrorIs(t, err, deltatxn.ErrNotPorted, "destination search needs the contact-method hook")
	_, err = s.Search(ctx, &SearchOptions{DestType: "sms", DestArgs: map[string]string{}})
	require.Error(t, err, "Postgres rejects a type without the join selected by non-empty args")
}

func TestDeltaAuthSubjects(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, _ := e.pod(t, "a")
	alice := addDeltaUser(t, s, "Alice", "")
	bob := addDeltaUser(t, s, "Bob", "")
	carol := addDeltaUser(t, s, "Carol", "")

	add := func(provider, subject, userID string) {
		require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: provider, SubjectID: subject, UserID: userID}))
	}
	add("p", "c", strings.ToUpper(alice))
	add("other", "x", bob)
	add("p", "a", bob)
	add("p", "b", alice)

	u, err := s.FindOneBySubject(ctx, "p", "c")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Equal(t, alice, u.ID)
	u, err = s.FindOneBySubject(ctx, "p", "missing")
	require.NoError(t, err)
	assert.Nil(t, u)

	subjects, err := s.FindSomeAuthSubjectsForProvider(ctx, 2, "", "p")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, subjectIDs(subjects), "ordered by subject, not insertion order")
	subjects, err = s.FindSomeAuthSubjectsForProvider(ctx, 2, "b", "p")
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, subjectIDs(subjects), "the cursor is exclusive")
	subjects, err = s.FindSomeAuthSubjectsForProvider(ctx, 2, "c", "p")
	require.NoError(t, err)
	assert.Nil(t, subjects)

	// Add is ON CONFLICT DO NOTHING, even when another user owns the pair.
	add("p", "c", bob)
	u, err = s.FindOneBySubject(ctx, "p", "c")
	require.NoError(t, err)
	assert.Equal(t, alice, u.ID)

	// Set is an upsert and moves an existing pair to its new user.
	require.NoError(t, s.SetAuthSubject(ctx, "p", "c", strings.ToUpper(bob)))
	u, err = s.FindOneBySubject(ctx, "p", "c")
	require.NoError(t, err)
	assert.Equal(t, bob, u.ID)

	all, err := s.FindAllAuthSubjectsForUser(ctx, strings.ToUpper(bob))
	require.NoError(t, err)
	assert.Equal(t, []string{"c", "x", "a"}, subjectIDs(all), "the stream is oldest first")

	var streamed []AuthSubject
	require.NoError(t, s.AuthSubjectsFunc(ctx, "p", nil, func(a AuthSubject) error {
		streamed = append(streamed, a)
		return nil
	}))
	assert.Equal(t, []string{"c", "a", "b"}, subjectIDs(streamed))
	streamed = nil
	require.NoError(t, s.AuthSubjectsFunc(ctx, "p", []string{alice, strings.ToUpper(alice)}, func(a AuthSubject) error {
		streamed = append(streamed, a)
		return nil
	}))
	assert.Equal(t, []string{"b"}, subjectIDs(streamed), "duplicate user IDs do not duplicate subjects; c now belongs to Bob")

	stop := errors.New("stop")
	calls := 0
	err = s.AuthSubjectsFunc(ctx, "p", nil, func(AuthSubject) error {
		calls++
		return stop
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, 1, calls, "callback errors stop the stream")

	var missing []string
	require.NoError(t, s.WithoutAuthProviderFunc(ctx, "p", func(u User) error {
		missing = append(missing, u.ID)
		return nil
	}))
	assert.Equal(t, []string{carol}, missing)
	calls = 0
	err = s.WithoutAuthProviderFunc(ctx, "p", func(User) error {
		calls++
		return stop
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, 1, calls)

	// Deleting someone else's subject is a no-op; deleting the owner's works.
	require.NoError(t, s.DeleteAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: "c", UserID: alice}))
	u, err = s.FindOneBySubject(ctx, "p", "c")
	require.NoError(t, err)
	require.NotNil(t, u)
	require.NoError(t, s.DeleteAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: "c", UserID: strings.ToUpper(bob)}))
	u, err = s.FindOneBySubject(ctx, "p", "c")
	require.NoError(t, err)
	assert.Nil(t, u)
	require.NoError(t, s.DeleteAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: "c", UserID: bob}))
}

func TestDeltaDeleteMany(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	plain, _ := e.pod(t, "plain")
	alice := addDeltaUser(t, plain, "Alice", "")
	require.NoError(t, plain.DeleteManyTx(ctx, nil, nil), "an empty delete needs no rotation tables")
	err := plain.DeleteManyTx(ctx, nil, []string{alice})
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)

	boom := errors.New("rotation cleanup failed")
	fail := true
	var bob string
	s, db := e.pod(t, "managed", WithRotations(func(_ *filestore.Tx, userID string) error {
		if fail && userID == bob {
			return boom
		}
		return nil
	}))
	bob = addDeltaUser(t, s, "Bob", "")
	require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: "alice", UserID: alice}))

	err = s.DeleteManyTx(ctx, nil, []string{strings.ToUpper(alice), bob})
	require.ErrorIs(t, err, boom)
	_, err = s.FindOne(ctx, alice)
	require.NoError(t, err, "the first delete rolls back when rotation cleanup fails")
	_, err = s.FindOne(ctx, bob)
	require.NoError(t, err)

	fail = false
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		return s.DeleteManyTx(ctx, tx, []string{strings.ToUpper(alice), bob})
	}), "deletion also works in the caller's transaction")
	_, err = s.FindOne(ctx, alice)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = s.FindOne(ctx, bob)
	require.ErrorIs(t, err, sql.ErrNoRows)
	u, err := s.FindOneBySubject(ctx, "p", "alice")
	require.NoError(t, err)
	assert.Nil(t, u, "deleting a user cascades to auth subjects")
}

type deltaTestContactMethod struct {
	ID     string `parquet:"id"`
	UserID string `parquet:"user_id"`
}

func TestDeltaUserConstraintsAndCascade(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	var contactMethods *filestore.Table[deltaTestContactMethod]
	e := newDeltaTestEnv(t, func(schema *filestore.Schema, tables *DeltaTables) {
		contactMethods = filestore.NewTable(schema, "user_contact_methods", func(c deltaTestContactMethod) string { return c.ID })
		DeclareDeltaForeignKeys(tables, contactMethods, func(r filestore.Reader, id string) (string, bool) {
			c, ok := contactMethods.Get(r, id)
			return c.UserID, ok
		})
	})
	s, db := e.pod(t, "a")
	alice := addDeltaUser(t, s, "Alice", "")
	bob := addDeltaUser(t, s, "Bob", "")
	ownCM, otherCM := uuid.NewString(), uuid.NewString()
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		if err := contactMethods.Put(ftx, deltaTestContactMethod{ID: ownCM, UserID: alice}); err != nil {
			return err
		}
		return contactMethods.Put(ftx, deltaTestContactMethod{ID: otherCM, UserID: bob})
	}))

	setStatus := func(userID, cmID string) error {
		return db.Run(ctx, nil, func(tx txn.Tx) error {
			ftx, _ := deltatxn.Unwrap(tx)
			row, ok := e.tables.Users.Get(ftx, userID)
			if !ok {
				return sql.ErrNoRows
			}
			row.AlertStatusCMID = &cmID
			return e.tables.PutUser(ftx, row)
		})
	}
	err := setStatus(alice, otherCM)
	var check *filestore.CheckError
	require.ErrorAs(t, err, &check)
	assert.Equal(t, "alert_status_user_id_match", check.Constraint)
	assert.Equal(t, "wrong user_id", check.Message)
	assert.Equal(t, "users", check.Table)

	err = setStatus(alice, uuid.NewString())
	var fk *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fk, "a missing contact method reaches the foreign key, as the trigger does on Postgres")
	assert.Equal(t, "users_alert_status_log_contact_method_id_fkey", fk.ForeignKey)
	require.NoError(t, setStatus(alice, ownCM))

	require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: "alice", UserID: alice}))
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		row, ok := e.tables.AuthSubjects.Get(ftx, AuthSubjectKey("p", "alice"))
		if !ok {
			return sql.ErrNoRows
		}
		row.CMID = &ownCM
		return e.tables.AuthSubjects.Put(ftx, row)
	}))
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		_, err := contactMethods.Delete(ftx, ownCM)
		return err
	}))

	require.NoError(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, r := deltatxn.Unwrap(tx)
		row, ok := e.tables.Users.Get(r, alice)
		require.True(t, ok)
		assert.Nil(t, row.AlertStatusCMID, "deleting the contact method sets the user reference to null")
		return nil
	}))
	u, err := s.FindOneBySubject(ctx, "p", "alice")
	require.NoError(t, err)
	assert.Nil(t, u, "deleting the contact method cascades to its auth subject")
}

func TestDeltaOnCallOverview(t *testing.T) {
	ctx := context.Background()
	e := newDeltaTestEnv(t, nil)
	plain, _ := e.pod(t, "plain")
	_, err := plain.OnCallOverview(ctx, uuid.New())
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)

	userID := uuid.New()
	stepID, policyID, serviceID := uuid.NewString(), uuid.New(), uuid.New()
	s, _ := e.pod(t, "oncall", WithOnCall(OnCallFuncs{
		Steps: func(_ filestore.Reader, id string) []string {
			if id == userID.String() {
				return []string{stepID, "missing"}
			}
			return nil
		},
		Step: func(_ filestore.Reader, id string) (OnCallStep, bool) {
			if id != stepID {
				return OnCallStep{}, false
			}
			return OnCallStep{PolicyID: policyID.String(), PolicyName: "Policy", StepNumber: 2}, true
		},
		Services: func(_ filestore.Reader, id string) []OnCallService {
			if id == policyID.String() {
				return []OnCallService{{ID: serviceID.String(), Name: "Service"}}
			}
			return nil
		},
	}))
	rows, err := s.OnCallOverview(ctx, userID)
	require.NoError(t, err)
	require.Len(t, rows, 1, "a missing step is skipped, like an inner join")
	assert.Equal(t, serviceID, rows[0].ServiceID)
	assert.Equal(t, "Service", rows[0].ServiceName)
	assert.Equal(t, policyID, rows[0].PolicyID)
	assert.Equal(t, "Policy", rows[0].PolicyName)
	assert.EqualValues(t, 2, rows[0].StepNumber)
	rows, err = s.OnCallOverview(ctx, uuid.New())
	require.NoError(t, err)
	assert.Nil(t, rows)
}

func TestDeltaAddAuthSubjectRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	a, _ := e.pod(t, "a")
	alice := addDeltaUser(t, a, "Alice", "")
	b, _ := e.pod(t, "b")

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 10 {
		s := a
		if i%2 == 1 {
			s = b
		}
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			<-start
			err := s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "provider/with/slashes", SubjectID: "subject/with/slashes", UserID: alice})
			assert.NoError(t, err)
		}(s)
	}
	close(start)
	wg.Wait()

	fresh, _ := e.pod(t, "fresh")
	subjects, err := fresh.FindSomeAuthSubjectsForProvider(ctx, 10, "", "provider/with/slashes")
	require.NoError(t, err)
	require.Len(t, subjects, 1, "racing ON CONFLICT DO NOTHING inserts leave one row (F4)")
	assert.Equal(t, alice, subjects[0].UserID)
}
