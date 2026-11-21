//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package filestore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesAreOwnerOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	s := newTestSchema()
	db, err := Open(dir, s.Schema, Options{Logf: t.Logf})
	require.NoError(t, err)
	put(t, db, s.rows, testRow{ID: "a"})
	require.NoError(t, db.Snapshot())
	require.NoError(t, db.Close())

	st, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), st.Mode().Perm(), "the data directory is owner-only (F20)")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "%s is owner-only (F20)", e.Name())
	}
}
