package user

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
)

func TestDeltaAuthSubjectKeys(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, _ := e.pod(t, "a")

	// Joined with a plain "/", several of these pairs would be one.
	pairs := [][2]string{
		{"a/b", "c"}, {"a", "b/c"}, {"a%2Fb", "c"}, {"a%252Fb", "c"}, {"a", "b"}, {"a", "/b"}, {"a/", "b"},
		{"slack:T1", "U1"}, {"slack:T10", "U1"}, {"slack:T1", "0/U1"}, {"a", "c"}, {"A", "c"}, {"a", "C"},
	}
	owner := make(map[[2]string]string)
	for _, p := range pairs {
		id := addDeltaUser(t, s, "User", "")
		owner[p] = id
		require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: p[0], SubjectID: p[1], UserID: id}))
	}
	require.Len(t, owner, len(pairs))

	fresh, _ := e.pod(t, "fresh") // which finds the rows by the keys it gives them
	for _, p := range pairs {
		u, err := fresh.FindOneBySubject(ctx, p[0], p[1])
		require.NoError(t, err)
		if assert.NotNil(t, u, "%q", p) {
			assert.Equal(t, owner[p], u.ID, "%q", p)
		}
	}
	for provider, want := range map[string][]string{
		"a":        {"/b", "C", "b", "b/c", "c"},
		"a/":       {"b"},
		"a/b":      {"c"},
		"a%2Fb":    {"c"},
		"slack:T1": {"0/U1", "U1"},
	} {
		subjects, err := fresh.FindSomeAuthSubjectsForProvider(ctx, 100, "", provider)
		require.NoError(t, err)
		assert.Equal(t, want, subjectIDs(subjects), "only those of provider %q, ordered by subject", provider)
	}
	subjects, err := fresh.FindSomeAuthSubjectsForProvider(ctx, 100, "", "slack:T")
	require.NoError(t, err)
	assert.Nil(t, subjects, "no rows is a nil slice, as on Postgres")
}

func TestDeltaAuthSubjectPages(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	s, db := e.pod(t, "a")
	alice := addDeltaUser(t, s, "Alice", "")
	require.NoError(t, inTx(db, func(tx *filestore.Tx) error {
		for i := range 120 {
			a := AuthSubject{ProviderID: "p", SubjectID: fmt.Sprintf("s%03d", i), UserID: alice}
			if err := e.tables.InsertAuthSubject(tx, a); err != nil {
				return err
			}
		}
		return nil
	}))

	var got []string
	for after := ""; ; {
		page, err := s.FindSomeAuthSubjectsForProvider(ctx, 0, after, "p")
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		assert.LessOrEqual(t, len(page), 50, "50 by default")
		got = append(got, subjectIDs(page)...)
		after = got[len(got)-1]
		assert.Equal(t, AuthSubject{ProviderID: "p", SubjectID: after, UserID: alice}, page[len(page)-1])
	}
	assert.Len(t, got, 120, "every subject, once")
	assert.True(t, slices.IsSorted(got))
}

func TestDeltaAuthSubjectConstraints(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e, cms := newConstraintEnv(t)
	s, db := e.pod(t, "a")
	alice, bob := addDeltaUser(t, s, "Alice", ""), addDeltaUser(t, s, "Bob", "")
	sub := AuthSubject{ProviderID: "p", SubjectID: "s", UserID: alice}
	get := func(provider, subject string) authSubjectRow {
		var row authSubjectRow
		require.NoError(t, deltatxn.Read(ctx, db, nil, func(s deltatxn.Scope) error {
			var ok bool
			row, ok = e.tables.AuthSubjects.Get(s.Reader, AuthSubjectKey(provider, subject))
			require.True(t, ok)
			return nil
		}))
		return row
	}

	// auth_subjects_user_id_fkey
	missing := AuthSubject{ProviderID: "p", SubjectID: "s", UserID: uuid.NewString()}
	assert.Equal(t, "auth_subjects_user_id_fkey", constraintName(s.AddAuthSubjectTx(ctx, nil, &missing)))
	assert.Equal(t, "auth_subjects_user_id_fkey", constraintName(s.SetAuthSubject(ctx, "p", "s", missing.UserID)))
	require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &sub))
	assert.Equal(t, "auth_subjects_user_id_fkey", constraintName(s.SetAuthSubject(ctx, "p", "s", missing.UserID)),
		"nor can a subject move to a missing user")
	require.NoError(t, s.AddAuthSubjectTx(ctx, nil, &missing), "ON CONFLICT DO NOTHING never gets to the foreign key")

	// auth_subjects_pkey
	err := inTx(db, func(tx *filestore.Tx) error { return e.tables.InsertAuthSubject(tx, sub) })
	assert.Equal(t, "auth_subjects_pkey", constraintName(err))

	// auth_subjects_uniq_id
	first := get("p", "s")
	err = inTx(db, func(tx *filestore.Tx) error {
		dup := first
		dup.SubjectID = "other"
		return e.tables.AuthSubjects.Put(tx, dup)
	})
	assert.Equal(t, "auth_subjects_uniq_id", constraintName(err))
	require.NoError(t, s.SetAuthSubject(ctx, "p", "s2", bob))
	assert.Greater(t, get("p", "s2").ID, first.ID)

	// auth_subjects_cm_id_fkey
	cm := uuid.NewString()
	setCM := func() error {
		return inTx(db, func(tx *filestore.Tx) error {
			row, ok := e.tables.AuthSubjectsByID.Get(tx, filestore.IntKey(first.ID))
			require.True(t, ok)
			row.CMID = &cm
			return e.tables.AuthSubjects.Put(tx, row)
		})
	}
	assert.Equal(t, "auth_subjects_cm_id_fkey", constraintName(setCM()))
	require.NoError(t, inTx(db, func(tx *filestore.Tx) error {
		return cms.Put(tx, deltaTestContactMethod{ID: cm, UserID: alice})
	}))
	require.NoError(t, setCM())

	require.NoError(t, s.SetAuthSubject(ctx, "p", "s", bob))
	moved := get("p", "s")
	assert.Equal(t, bob, moved.UserID)
	assert.Equal(t, first.ID, moved.ID, "DO UPDATE sets only the user")
	require.NotNil(t, moved.CMID)
	assert.Equal(t, cm, *moved.CMID)
	require.NoError(t, deltatxn.Read(ctx, db, nil, func(s deltatxn.Scope) error {
		assert.Equal(t, 1, e.tables.AuthSubjectsByContactMethod.Count(s.Reader, cm))
		assert.Equal(t, 0, e.tables.AuthSubjectsByUser.Count(s.Reader, alice))
		assert.Equal(t, 2, e.tables.AuthSubjectsByUser.Count(s.Reader, bob))
		return nil
	}))
}

func TestDeltaSetAuthSubjectRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	a, _ := e.pod(t, "a")
	alice, bob := addDeltaUser(t, a, "Alice", ""), addDeltaUser(t, a, "Bob", "")
	b, _ := e.pod(t, "b")

	// One pod gives a subject to alice, the other gives it to bob, and both
	// add others.
	both(a, b, 5, func(s *Store, i int) {
		owner, name := alice, "a"
		if s == b {
			owner, name = bob, "b"
		}
		assert.NoError(t, s.SetAuthSubject(ctx, "p", "shared", owner))
		assert.NoError(t, s.SetAuthSubject(ctx, "p", fmt.Sprintf("own-%s-%d", name, i), owner))
	})

	fresh, fdb := e.pod(t, "fresh")
	u, err := fresh.FindOneBySubject(ctx, "p", "shared")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Contains(t, []string{alice, bob}, u.ID)
	require.NoError(t, deltatxn.Read(ctx, fdb, nil, func(s deltatxn.Scope) error {
		ids := make(map[int64]bool)
		e.tables.AuthSubjects.Ascend(s.Reader, func(row authSubjectRow) bool {
			ids[row.ID] = true
			return true
		})
		assert.Equal(t, 11, e.tables.AuthSubjects.Len(s.Reader), "the shared subject once, and ten others (F8)")
		assert.Len(t, ids, 11, "no id twice")
		return nil
	}))
}

func TestDeltaDeleteUserRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaTestEnv(t, nil)
	noRotations := WithRotations(func(*filestore.Tx, string) error { return nil })
	a, _ := e.pod(t, "a", noRotations)
	alice := addDeltaUser(t, a, "Alice", "")
	b, _ := e.pod(t, "b", noRotations)

	// While one pod deletes the user, both add subjects for them.
	both(a, b, 5, func(s *Store, i int) {
		if s == a && i == 0 {
			assert.NoError(t, s.DeleteManyTx(ctx, nil, []string{alice}))
			return
		}
		err := s.AddAuthSubjectTx(ctx, nil, &AuthSubject{ProviderID: "p", SubjectID: fmt.Sprintf("%p-%d", s, i), UserID: alice})
		if err != nil {
			assert.Equal(t, "auth_subjects_user_id_fkey", constraintName(err), "only because the user is gone")
		}
	})

	fresh, fdb := e.pod(t, "fresh")
	assert.False(t, deltaUserExists(t, fresh, alice))
	require.NoError(t, deltatxn.Read(ctx, fdb, nil, func(s deltatxn.Scope) error {
		assert.Zero(t, e.tables.AuthSubjects.Len(s.Reader), "no subject is left without its user (F7)")
		return nil
	}))
}
