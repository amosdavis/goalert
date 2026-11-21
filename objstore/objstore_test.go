package objstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/objstoretest"
)

func TestMem(t *testing.T) {
	objstoretest.Run(t, func(t *testing.T) objstore.Store { return objstore.NewMem() })
}

func TestDir(t *testing.T) {
	objstoretest.Run(t, func(t *testing.T) objstore.Store {
		s, err := objstore.NewDir(t.TempDir())
		require.NoError(t, err)
		return s
	})
}

func TestMemFaults(t *testing.T) {
	ctx := context.Background()
	m := objstore.NewMem()
	boom := errors.New("boom")

	m.BeforeWrite = func(op, name string) error { return boom }
	require.ErrorIs(t, m.PutIfAbsent(ctx, "a", []byte("1")), boom)
	_, err := m.Get(ctx, "a")
	require.ErrorIs(t, err, objstore.ErrNotFound, "a failed write is not applied")

	m.BeforeWrite = func(op, name string) error { return objstore.ErrMemAppliedThenFailed }
	err = m.PutIfAbsent(ctx, "a", []byte("1"))
	require.ErrorIs(t, err, objstore.ErrUnknown, "the caller can't tell what happened")
	got, err := m.Get(ctx, "a")
	require.NoError(t, err, "but the write happened")
	assert.Equal(t, "1", string(got))

	m.BeforeWrite = nil
	assert.Equal(t, 2, m.Ops()["putIfAbsent"])
}
