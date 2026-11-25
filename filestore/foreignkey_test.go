package filestore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

type fkUser struct {
	ID string `parquet:"id"`
}

type fkSession struct {
	ID     string `parquet:"id"`
	UserID string `parquet:"user_id"`
}

type fkMethod struct {
	ID     string  `parquet:"id"`
	UserID *string `parquet:"user_id,optional"`
}

type fkPolicy struct {
	ID     string `parquet:"id"`
	UserID string `parquet:"user_id"`
}

type fkNode struct {
	ID     string  `parquet:"id"`
	Parent *string `parquet:"parent,optional"`
}

type fkSchema struct {
	s        *Schema
	users    *Table[fkUser]
	sessions *Table[fkSession]
	methods  *Table[fkMethod]
	policies *Table[fkPolicy]
	nodes    *Table[fkNode]
}

func optRef(p *string) (string, bool) {
	if p == nil {
		return "", false
	}
	return *p, true
}

func newFKSchema() fkSchema {
	s := NewSchema()
	f := fkSchema{s: s}
	f.users = NewTable(s, "users", func(u fkUser) string { return u.ID })
	f.sessions = NewTable(s, "sessions", func(x fkSession) string { return x.ID })
	f.methods = NewTable(s, "methods", func(m fkMethod) string { return m.ID })
	f.policies = NewTable(s, "policies", func(p fkPolicy) string { return p.ID })
	f.nodes = NewTable(s, "nodes", func(n fkNode) string { return n.ID })
	References(f.sessions, "sessions_user_id_fkey", f.users, func(x fkSession) (string, bool) { return x.UserID, true }, Cascade, nil)
	References(f.methods, "methods_user_id_fkey", f.users, func(m fkMethod) (string, bool) { return optRef(m.UserID) }, SetNull,
		func(m fkMethod) fkMethod { m.UserID = nil; return m })
	References(f.policies, "policies_user_id_fkey", f.users, func(p fkPolicy) (string, bool) { return p.UserID, true }, Restrict, nil)
	References(f.nodes, "nodes_parent_fkey", f.nodes, func(n fkNode) (string, bool) { return optRef(n.Parent) }, Cascade, nil)
	return f
}

func openFK(t *testing.T) (fkSchema, objstore.Store, *DeltaDB) {
	t.Helper()
	store := objstore.NewMem()
	require.NoError(t, CreateDelta(context.Background(), store, newFKSchema().s, dOpts("admin")))
	f := newFKSchema()
	return f, store, openPod(t, store, dSchema{s: f.s}, dOpts("a"))
}

func strp(s string) *string { return &s }

func TestForeignKeyMissingParent(t *testing.T) {
	ctx := context.Background()
	f, _, db := openFK(t)
	_, err := db.Update(ctx, func(tx *Tx) error {
		return f.sessions.Put(tx, fkSession{ID: "s1", UserID: "nobody"})
	})
	var fkErr *ForeignKeyError
	require.ErrorAs(t, err, &fkErr, "a row can't refer to a missing one (F7)")
	assert.True(t, fkErr.Missing)
	assert.Equal(t, "sessions_user_id_fkey", fkErr.ForeignKey)
	assert.Zero(t, f.sessions.Len(db.View()))

	_, err = db.Update(ctx, func(tx *Tx) error {
		return f.methods.Put(tx, fkMethod{ID: "m1"})
	})
	require.NoError(t, err, "a null reference refers to nothing")
}

func TestForeignKeyCascadeAndSetNull(t *testing.T) {
	ctx := context.Background()
	f, store, db := openFK(t)
	_, err := db.Update(ctx, func(tx *Tx) error {
		for _, err := range []error{
			f.users.Put(tx, fkUser{ID: "u1"}),
			f.users.Put(tx, fkUser{ID: "u2"}),
			f.sessions.Put(tx, fkSession{ID: "s1", UserID: "u1"}),
			f.sessions.Put(tx, fkSession{ID: "s2", UserID: "u1"}),
			f.sessions.Put(tx, fkSession{ID: "s3", UserID: "u2"}),
			f.methods.Put(tx, fkMethod{ID: "m1", UserID: strp("u1")}),
		} {
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	_, err = db.Update(ctx, func(tx *Tx) error {
		_, err := f.users.Delete(tx, "u1")
		return err
	})
	require.NoError(t, err)
	v := db.View()
	_, ok := f.users.Get(v, "u1")
	assert.False(t, ok)
	_, ok = f.sessions.Get(v, "s1")
	assert.False(t, ok, "ON DELETE CASCADE")
	_, ok = f.sessions.Get(v, "s2")
	assert.False(t, ok)
	_, ok = f.sessions.Get(v, "s3")
	assert.True(t, ok, "another user's rows stay")
	m, ok := f.methods.Get(v, "m1")
	require.True(t, ok)
	assert.Nil(t, m.UserID, "ON DELETE SET NULL")

	fresh := openPod(t, store, dSchema{s: f.s}, dOpts("b"))
	assert.Equal(t, 1, f.sessions.Len(fresh.View()), "another pod replays the same result")
}

func TestForeignKeyRestrict(t *testing.T) {
	ctx := context.Background()
	f, _, db := openFK(t)
	_, err := db.Update(ctx, func(tx *Tx) error {
		if err := f.users.Put(tx, fkUser{ID: "u1"}); err != nil {
			return err
		}
		if err := f.sessions.Put(tx, fkSession{ID: "s1", UserID: "u1"}); err != nil {
			return err
		}
		return f.policies.Put(tx, fkPolicy{ID: "p1", UserID: "u1"})
	})
	require.NoError(t, err)

	_, err = db.Update(ctx, func(tx *Tx) error {
		_, err := f.users.Delete(tx, "u1")
		return err
	})
	var fkErr *ForeignKeyError
	require.ErrorAs(t, err, &fkErr, "NO ACTION: a referenced row can't be deleted")
	assert.False(t, fkErr.Missing)
	v := db.View()
	_, ok := f.users.Get(v, "u1")
	assert.True(t, ok, "nothing changed")
	_, ok = f.sessions.Get(v, "s1")
	assert.True(t, ok, "not even the cascade")
}

func TestForeignKeyChainsAndCycles(t *testing.T) {
	ctx := context.Background()
	f, _, db := openFK(t)
	_, err := db.Update(ctx, func(tx *Tx) error {
		for _, n := range []fkNode{{ID: "a"}, {ID: "b", Parent: strp("a")}, {ID: "c", Parent: strp("b")}, {ID: "x"}} {
			if err := f.nodes.Put(tx, n); err != nil {
				return err
			}
		}
		// a and b now refer to each other.
		return f.nodes.Put(tx, fkNode{ID: "a", Parent: strp("b")})
	})
	require.NoError(t, err)

	_, err = db.Update(ctx, func(tx *Tx) error {
		_, err := f.nodes.Delete(tx, "a")
		return err
	})
	require.NoError(t, err, "a cycle of cascades ends")
	assert.Equal(t, []string{"x"}, func() (ids []string) {
		f.nodes.Ascend(db.View(), func(n fkNode) bool { ids = append(ids, n.ID); return true })
		return ids
	}(), "the whole chain goes, and nothing else")
}

func TestForeignKeyAcrossPods(t *testing.T) {
	ctx := context.Background()
	f, store, a := openFK(t)
	b := openPod(t, store, dSchema{s: f.s}, dOpts("b"))
	_, err := a.Update(ctx, func(tx *Tx) error { return f.users.Put(tx, fkUser{ID: "u1"}) })
	require.NoError(t, err)
	require.NoError(t, b.WaitFor(ctx, 1))

	// One pod deletes the user; the other, not yet knowing, adds a session.
	_, err = a.Update(ctx, func(tx *Tx) error {
		_, err := f.users.Delete(tx, "u1")
		return err
	})
	require.NoError(t, err)
	_, err = b.Update(ctx, func(tx *Tx) error {
		return f.sessions.Put(tx, fkSession{ID: "s1", UserID: "u1"})
	})
	var fkErr *ForeignKeyError
	require.ErrorAs(t, err, &fkErr, "the write is made on top of the deletion, and fails its check (F4, F7)")
}

// TestForeignKeySetNullTwice checks that a row can refer to a deleted row
// more than once, and to others that go with it.
func TestForeignKeySetNullTwice(t *testing.T) {
	type user struct {
		ID string `parquet:"id"`
	}
	type team struct {
		ID    string `parquet:"id"`
		Owner string `parquet:"owner"`
	}
	type key struct {
		ID        string  `parquet:"id"`
		CreatedBy *string `parquet:"created_by,optional"`
		UpdatedBy *string `parquet:"updated_by,optional"`
		Owner     *string `parquet:"owner,optional"`
		Team      *string `parquet:"team,optional"`
	}
	ctx := context.Background()
	s := NewSchema()
	users := NewTable(s, "users", func(u user) string { return u.ID })
	teams := NewTable(s, "teams", func(m team) string { return m.ID })
	keys := NewTable(s, "keys", func(k key) string { return k.ID })
	References(keys, "keys_created_by_fkey", users, func(k key) (string, bool) { return Ref(k.CreatedBy) }, SetNull,
		func(k key) key { k.CreatedBy = nil; return k })
	References(keys, "keys_updated_by_fkey", users, func(k key) (string, bool) { return Ref(k.UpdatedBy) }, SetNull,
		func(k key) key { k.UpdatedBy = nil; return k })
	References(keys, "keys_team_fkey", teams, func(k key) (string, bool) { return Ref(k.Team) }, SetNull,
		func(k key) key { k.Team = nil; return k })
	References(teams, "teams_owner_fkey", users, func(m team) (string, bool) { return m.Owner, true }, Cascade, nil)
	// Declared last, so that rows it will delete are cleared first.
	References(keys, "keys_owner_fkey", users, func(k key) (string, bool) { return Ref(k.Owner) }, Cascade, nil)

	store := objstore.NewMem()
	require.NoError(t, CreateDelta(ctx, store, s, dOpts("create")))
	db, err := OpenDelta(ctx, store, s, dOpts("a"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	u1, u2, t1 := "u1", "u2", "t1"
	_, err = db.Update(ctx, func(tx *Tx) error {
		return errors.Join(
			users.Put(tx, user{u1}), users.Put(tx, user{u2}), teams.Put(tx, team{t1, u1}),
			keys.Put(tx, key{ID: "both", CreatedBy: &u1, UpdatedBy: &u1, Team: &t1}),
			keys.Put(tx, key{ID: "one", CreatedBy: &u2, UpdatedBy: &u1}),
			keys.Put(tx, key{ID: "owned", CreatedBy: &u1, Owner: &u1, Team: &t1}),
		)
	})
	require.NoError(t, err)
	_, err = db.Update(ctx, func(tx *Tx) error { _, err := users.Delete(tx, u1); return err })
	require.NoError(t, err)

	k, ok := keys.Get(db.View(), "both")
	require.True(t, ok)
	assert.Equal(t, key{ID: "both"}, k)
	k, ok = keys.Get(db.View(), "one")
	require.True(t, ok)
	assert.Equal(t, key{ID: "one", CreatedBy: &u2}, k)
	_, ok = keys.Get(db.View(), "owned")
	assert.False(t, ok)
	assert.Zero(t, teams.Len(db.View()))
}
