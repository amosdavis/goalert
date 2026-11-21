// Package objstoretest is the conformance suite every objstore.Store
// implementation must pass (docs/filestore-failure-modes.md, F14).
package objstoretest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

// Run runs the suite. newStore must return an empty store each time it is
// called, for example one scoped to a fresh prefix in a shared bucket.
func Run(t *testing.T, newStore func(t *testing.T) objstore.Store) {
	ctx := context.Background()

	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		_, err := s.Get(ctx, "nope")
		require.ErrorIs(t, err, objstore.ErrNotFound)
	})

	t.Run("PutIfAbsent", func(t *testing.T) {
		s := newStore(t)
		require.NoError(t, s.PutIfAbsent(ctx, "a/b", []byte("first")))
		err := s.PutIfAbsent(ctx, "a/b", []byte("second"))
		require.ErrorIs(t, err, objstore.ErrExists)
		got, err := s.Get(ctx, "a/b")
		require.NoError(t, err)
		assert.Equal(t, "first", string(got), "a failed PutIfAbsent changes nothing")
	})

	t.Run("PutIfAbsentRace", func(t *testing.T) {
		// Of many writers racing for one name, exactly one wins, and the
		// object holds the winner's data (F4).
		s := newStore(t)
		const n = 12
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = s.PutIfAbsent(ctx, "_delta_log/00000000000000000001.json", fmt.Appendf(nil, "writer %d", i))
			}()
		}
		wg.Wait()
		winner := -1
		for i, err := range errs {
			if err == nil {
				require.Equal(t, -1, winner, "two writers both created the object")
				winner = i
				continue
			}
			require.ErrorIs(t, err, objstore.ErrExists, "writer %d", i)
		}
		require.NotEqual(t, -1, winner, "nobody created the object")
		got, err := s.Get(ctx, "_delta_log/00000000000000000001.json")
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("writer %d", winner), string(got))
	})

	t.Run("Put", func(t *testing.T) {
		s := newStore(t)
		require.NoError(t, s.Put(ctx, "_last_checkpoint", []byte("1")))
		require.NoError(t, s.Put(ctx, "_last_checkpoint", []byte("2")))
		got, err := s.Get(ctx, "_last_checkpoint")
		require.NoError(t, err)
		assert.Equal(t, "2", string(got))
	})

	t.Run("List", func(t *testing.T) {
		s := newStore(t)
		names := []string{
			"_delta_log/00000000000000000000.json",
			"_delta_log/00000000000000000001.json",
			"_delta_log/00000000000000000002.json",
			"_delta_log/_last_checkpoint",
			"tbl=alerts/bucket=3/part-1.parquet",
			"tbl=alerts/bucket=3/part-2.parquet",
			"tbl=users/bucket=0/part-3.parquet",
		}
		for i := len(names) - 1; i >= 0; i-- { // out of order on purpose
			require.NoError(t, s.PutIfAbsent(ctx, names[i], []byte(names[i])))
		}
		list := func(prefix, after string) []string {
			got, err := s.List(ctx, prefix, after)
			require.NoError(t, err)
			return got
		}
		assert.Equal(t, names, list("", ""), "everything, in byte order")
		assert.Equal(t, names[:4], list("_delta_log/", ""))
		assert.Equal(t, names[2:4], list("_delta_log/", "_delta_log/00000000000000000001.json"), "startAfter is exclusive")
		assert.Equal(t, names[:3], list("_delta_log/0000", ""), "a prefix can end mid-name")
		assert.Equal(t, names[4:6], list("tbl=alerts/", ""))
		assert.Empty(t, list("tbl=nobody/", ""))
		assert.Empty(t, list("_delta_log/", "_delta_log/zzz"))
	})

	t.Run("Delete", func(t *testing.T) {
		s := newStore(t)
		require.NoError(t, s.PutIfAbsent(ctx, "x/y", []byte("1")))
		require.NoError(t, s.Delete(ctx, "x/y"))
		_, err := s.Get(ctx, "x/y")
		require.ErrorIs(t, err, objstore.ErrNotFound)
		require.NoError(t, s.Delete(ctx, "x/y"), "deleting a missing object is not an error")
		require.NoError(t, s.PutIfAbsent(ctx, "x/y", []byte("2")), "the name can be created again")
	})

	t.Run("Large", func(t *testing.T) {
		s := newStore(t)
		data := bytes.Repeat([]byte("0123456789abcdef"), 512<<10) // 8 MiB
		require.NoError(t, s.PutIfAbsent(ctx, "big.parquet", data))
		got, err := s.Get(ctx, "big.parquet")
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data, got), "contents round-trip")
	})

	t.Run("Names", func(t *testing.T) {
		s := newStore(t)
		for _, name := range []string{
			"tbl=alerts/bucket=12/part-00000-6c1e.c000.snappy.parquet",
			"with space/and=equals",
			"unicode/é∂",
			"deep/a/b/c/d/e",
		} {
			require.NoError(t, s.PutIfAbsent(ctx, name, []byte(name)), name)
			got, err := s.Get(ctx, name)
			require.NoError(t, err, name)
			assert.Equal(t, name, string(got))
		}
		for _, bad := range []string{"", "/abs", "trailing/", "back\\slash", "nul\x00"} {
			assert.Error(t, s.PutIfAbsent(ctx, bad, nil), "%q is rejected", bad)
		}
	})

	t.Run("SelfTest", func(t *testing.T) {
		s := newStore(t)
		require.NoError(t, objstore.SelfTest(ctx, s))
		got, err := s.List(ctx, "", "")
		require.NoError(t, err)
		assert.Empty(t, got, "the self-test cleans up after itself")
	})

	t.Run("Isolation", func(t *testing.T) {
		// Two stores from newStore never see each other's objects.
		a, b := newStore(t), newStore(t)
		require.NoError(t, a.PutIfAbsent(ctx, "only-in-a", []byte("1")))
		_, err := b.Get(ctx, "only-in-a")
		require.ErrorIs(t, err, objstore.ErrNotFound)
		got, err := b.List(ctx, "", "")
		require.NoError(t, err)
		assert.False(t, strings.Contains(strings.Join(got, ","), "only-in-a"))
	})
}
