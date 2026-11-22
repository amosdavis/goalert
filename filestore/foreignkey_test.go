package filestore

import (
	"context"
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
