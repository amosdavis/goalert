package deltatxn_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

type row struct {
	ID   string `parquet:"id"`
	Name string `parquet:"name"`
}

func open(t *testing.T) (deltatxn.DB, *filestore.Table[row]) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMem()
	s := filestore.NewSchema()
	rows := filestore.NewTable(s, "rows", func(r row) string { return r.ID })
	opts := filestore.DeltaOptions{WriterID: "test", PollInterval: -1, Logf: func(string, ...any) {}}
	require.NoError(t, filestore.CreateDelta(ctx, store, s, opts))
	db, err := filestore.OpenDelta(ctx, store, s, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return deltatxn.DB{DB: db}, rows
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	db, rows := open(t)

	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		require.NotNil(t, ftx)
		if err := rows.Put(ftx, row{ID: "a", Name: "first"}); err != nil {
			return err
		}
		got, ok := rows.Get(r, "a")
		assert.True(t, ok, "a transaction sees its own writes")
		assert.Equal(t, "first", got.Name)
		return nil
	}))

	boom := errors.New("boom")
	err := db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		require.NoError(t, rows.Put(ftx, row{ID: "b"}))
		return boom
	})
	require.ErrorIs(t, err, boom)

	require.NoError(t, db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		ftx, r := deltatxn.Unwrap(tx)
		assert.Nil(t, ftx, "a read-only transaction can't write")
		_, ok := rows.Get(r, "a")
		assert.True(t, ok)
		_, ok = rows.Get(r, "b")
		assert.False(t, ok, "a failed transaction changes nothing")
		return nil
	}))
}

func TestManaged(t *testing.T) {
	ctx := context.Background()
	db, _ := open(t)
	_, err := db.BeginTx(ctx, nil)
	require.ErrorIs(t, err, deltatxn.ErrManaged)
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		assert.ErrorIs(t, tx.Commit(), deltatxn.ErrManaged, "Run commits")
		assert.ErrorIs(t, tx.Rollback(), deltatxn.ErrManaged, "Run rolls back")
		return nil
	}))
}

type foreignTx struct{}

func (foreignTx) Commit() error   { return nil }
func (foreignTx) Rollback() error { return nil }

func TestUnwrap(t *testing.T) {
	ftx, r := deltatxn.Unwrap(nil)
	assert.Nil(t, ftx)
	assert.Nil(t, r, "nil means not in a transaction")

	var typedNil *deltatxn.Tx
	ftx, r = deltatxn.Unwrap(typedNil)
	assert.Nil(t, ftx)
	assert.Nil(t, r)

	assert.Panics(t, func() { deltatxn.Unwrap(foreignTx{}) }, "a running GoAlert never mixes backends (F23)")
}
