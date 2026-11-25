package slack

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn/deltatxn"
)

type deltaUser struct {
	ID string `parquet:"id"`
}

func TestDeltaTables(t *testing.T) {
	ctx := context.Background()
	s := filestore.NewSchema()
	users := filestore.NewTable(s, "users", func(u deltaUser) string { return u.ID })
	tables := DefineDeltaTables(s)
	DeclareDeltaForeignKeys(tables, users)

	store := objstore.NewMem()
	opts := filestore.DeltaOptions{WriterID: "test", PollInterval: -1, Logf: func(string, ...any) {}}
	require.NoError(t, filestore.CreateDelta(ctx, store, s, opts))
	fdb, err := filestore.OpenDelta(ctx, store, s, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	insert := func(id string) error {
		return deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error {
			return tables.userData.Insert(s.Tx, userSlackDataRow{ID: id, AccessToken: "token"}, "user_slack_data_pkey")
		})
	}

	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, insert("u1"), &fkErr, "the user must exist (F7)")
	assert.Equal(t, "user_slack_data_id_fkey", fkErr.ConstraintName())

	require.NoError(t, deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error { return users.Put(s.Tx, deltaUser{ID: "u1"}) }))
	require.NoError(t, insert("u1"))
	var uErr *filestore.UniqueError
	require.ErrorAs(t, insert("u1"), &uErr)
	assert.Equal(t, "user_slack_data_pkey", uErr.ConstraintName())

	require.NoError(t, deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error {
		_, err := users.Delete(s.Tx, "u1")
		return err
	}))
	assert.Zero(t, tables.userData.Len(fdb.View()), "deleting the user deletes it (ON DELETE CASCADE)")
}
