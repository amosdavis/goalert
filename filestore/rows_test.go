package filestore

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

func TestUntypedRows(t *testing.T) {
	type thing struct {
		ID string `parquet:"id"`
		N  int64  `parquet:"n"`
	}
	s := NewSchema()
	things := NewTable(s, "things", func(v thing) string { return v.ID })
	ctx := t.Context()
	store := objstore.NewMem()
	require.NoError(t, CreateDelta(ctx, store, s, DeltaOptions{}))
	db, err := OpenDelta(ctx, store, s, DeltaOptions{PollInterval: -1})
	require.NoError(t, err)
	defer db.Close()

	_, ok := db.Schema().Rows("nothing")
	assert.False(t, ok)
	_, ok = db.Schema().Rows("_seq")
	assert.False(t, ok)
	rows, ok := db.Schema().Rows("things")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeFor[thing](), rows.Type())
	key, err := rows.Key(&thing{ID: "a"})
	require.NoError(t, err)
	assert.Equal(t, "a", key)
	_, err = rows.Key(thing{ID: "a"})
	assert.Error(t, err)
	_, err = rows.Key((*thing)(nil))
	assert.Error(t, err)

	_, err = db.Update(ctx, func(tx *Tx) error {
		assert.Error(t, rows.Put(tx, "not a row"))
		assert.Error(t, rows.Insert(tx, thing{}, "things_pkey"))
		require.NoError(t, rows.Insert(tx, &thing{"b", 2}, "things_pkey"))
		require.NoError(t, rows.Put(tx, &thing{"a", 1}))
		var dup *UniqueError
		require.ErrorAs(t, rows.Insert(tx, &thing{"a", 2}, "things_pkey"), &dup)
		assert.Equal(t, "things_pkey", dup.Index)
		return nil
	})
	require.NoError(t, err)
	var got []any
	rows.Ascend(db.View(), func(row any) bool { got = append(got, row); return true })
	assert.Equal(t, []any{&thing{"a", 1}, &thing{"b", 2}}, got)
	v, ok := rows.Get(db.View(), "b")
	require.True(t, ok)
	assert.Equal(t, &thing{"b", 2}, v)

	_, err = db.Update(ctx, func(tx *Tx) error {
		ok, err := rows.Delete(tx, "a")
		assert.True(t, ok)
		return err
	})
	require.NoError(t, err)
	_, ok = things.Get(db.View(), "a")
	assert.False(t, ok)
}
