package deltalog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
)

// scriptedStore answers calls for one commit file from a script.
type scriptedStore struct {
	*objstore.Mem
	puts []error // nil does the write
	gets []error // nil does the read
}

func next(script *[]error) (error, bool) {
	if len(*script) == 0 {
		return nil, false
	}
	err := (*script)[0]
	*script = (*script)[1:]
	return err, true
}

func (s *scriptedStore) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err, ok := next(&s.puts); ok && err != nil {
		return err
	}
	return s.Mem.PutIfAbsent(ctx, name, data)
}

func (s *scriptedStore) Get(ctx context.Context, name string) ([]byte, error) {
	if err, ok := next(&s.gets); ok && err != nil {
		return nil, err
	}
	return s.Mem.Get(ctx, name)
}

// TestCommitUnsettled checks that a commit whose version exists, but could not
// be looked at, is not reported as failed when a later attempt is refused: the
// version may hold this very commit, made by an attempt an SDK hid (F32).
func TestCommitUnsettled(t *testing.T) {
	ctx := context.Background()
	store := &scriptedStore{Mem: objstore.NewMem()}
	l := deltalog.New(store, "a")
	l.SetRetry(3, time.Millisecond)
	require.NoError(t, l.Create(ctx, deltalog.MetaData{ID: "t", SchemaString: "{}"}, time.Now()))

	denied := objstore.Denied(errors.New("token expired"))
	store.puts = []error{objstore.ErrExists, denied}
	store.gets = []error{errors.New("connection reset")}
	_, err := l.Commit(ctx, 1, "t", "WRITE", time.Now(), nil)
	require.ErrorIs(t, err, objstore.ErrUnknown)
	require.NotErrorIs(t, err, objstore.ErrDenied)

	// Without that, a refusal is a failure.
	store.puts = []error{denied}
	_, err = l.Commit(ctx, 1, "t", "WRITE", time.Now(), nil)
	require.ErrorIs(t, err, objstore.ErrDenied)
	require.NotErrorIs(t, err, objstore.ErrUnknown)
}

// TestSetRetry checks that a setting that makes no sense changes nothing.
func TestSetRetry(t *testing.T) {
	l := deltalog.New(objstore.NewMem(), "a")
	l.SetRetry(3, time.Second)
	l.SetRetry(0, 0)
	l.SetRetry(-1, -1)
	assert.Equal(t, 3, l.MaxAttempts)
	assert.Equal(t, time.Second, l.Backoff)
}

// TestManager checks that a log is used only for tables whose commits are its
// own to decide, and that a table can't change that (F43).
func TestManager(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	meta := deltalog.MetaData{ID: "t", SchemaString: "{}"}
	managed := meta
	managed.Configuration = map[string]string{deltalog.ManagerKey: "x"}
	require.ErrorIs(t, deltalog.New(store, "a").Create(ctx, managed, time.Now()), deltalog.ErrManaged)
	m := deltalog.New(store, "a")
	m.Manager = "x"
	require.ErrorIs(t, m.Create(ctx, meta, time.Now()), deltalog.ErrManaged)

	l := deltalog.New(store, "a")
	require.NoError(t, l.Create(ctx, meta, time.Now()))
	assert.Nil(t, meta.Configuration, "the caller's metadata is not changed")
	_, err := m.Load(ctx)
	require.ErrorIs(t, err, deltalog.ErrManaged)
	_, err = m.Commit(ctx, 1, "t", "WRITE", time.Now(), nil)
	require.ErrorIs(t, err, deltalog.ErrManaged)

	s, err := l.Load(ctx)
	require.NoError(t, err)
	checked, err := deltalog.CheckMeta(managed, time.Now())
	require.NoError(t, err)
	info := deltalog.NewCommitInfo("t", "a", "WRITE", time.Now())
	require.ErrorIs(t, s.Apply(1, info, []deltalog.Action{{MetaData: &checked}}), deltalog.ErrCorrupt)
}
