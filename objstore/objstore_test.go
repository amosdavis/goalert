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

// lax ignores create-if-absent, as a misbehaving store or emulator might.
type lax struct{ *objstore.Mem }

func (l lax) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	return l.Put(ctx, name, data)
}

// refusing denies every create.
type refusing struct{ *objstore.Mem }

func (refusing) PutIfAbsent(context.Context, string, []byte) error {
	return objstore.Denied(errors.New("403 forbidden"))
}

func TestSelfTest(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, objstore.SelfTest(ctx, objstore.NewMem()))

	err := objstore.SelfTest(ctx, lax{objstore.NewMem()})
	require.ErrorContains(t, err, "does not enforce create-if-absent", "two pods could win one version (F4)")

	m := objstore.NewMem()
	err = objstore.SelfTest(ctx, refusing{m})
	require.ErrorIs(t, err, objstore.ErrDenied, "a missing permission is reported as one (F33)")
	names, err := m.List(ctx, "", "")
	require.NoError(t, err)
	assert.Empty(t, names)
}
