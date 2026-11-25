package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
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

func TestDeltaUserCRUD(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	rawID := strings.ToUpper(uuid.NewString())
	id := strings.ToLower(rawID)

	inserted, err := s.Insert(ctx, &User{
		ID: rawID, Name: "Alice", Email: "Alice@Example.COM", AvatarURL: "https://example.com/alice.png",
		Role: permission.RoleUser, AlertStatusCMID: uuid.NewString(),
	})
	require.NoError(t, err)
	assert.Equal(t, rawID, inserted.ID, "Insert returns the normalized input, as on Postgres")

	u, err := s.FindOne(ctx, rawID)
	require.NoError(t, err)
	assert.Equal(t, id, u.ID, "UUID keys are canonical and case-insensitive")
	assert.Equal(t, "alice@example.com", u.Email)
	assert.Equal(t, "https://example.com/alice.png", u.AvatarURL)
	assert.Empty(t, u.AlertStatusCMID, "the insert query does not write this field")

	require.NoError(t, s.UpdateTx(ctx, nil, &User{ID: rawID, Name: "Alice B", Email: "alice+b@example.com", Role: permission.RoleAdmin}))
	u, err = s.FindOne(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "Alice B", u.Name)
	assert.Equal(t, "alice+b@example.com", u.Email)
	assert.Equal(t, permission.RoleUser, u.Role, "update changes only name and email")
	assert.Equal(t, "https://example.com/alice.png", u.AvatarURL)
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
	assert.Empty(t, users[0].AvatarURL, "search does not select avatar URLs")

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
