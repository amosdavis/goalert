//go:build windows

package filestore

import (
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestDirectoryACLIsRestricted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s := newTestSchema()
	db, err := Open(dir, s.Schema, Options{Logf: t.Logf})
	require.NoError(t, err)
	put(t, db, s.rows, testRow{ID: "a"})
	require.NoError(t, db.Close())

	sd, err := windows.GetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	control, _, err := sd.Control()
	require.NoError(t, err)
	assert.NotZero(t, control&windows.SE_DACL_PROTECTED, "the directory does not inherit its parent's ACL (F20)")

	dacl, _, err := sd.DACL()
	require.NoError(t, err)
	require.NotNil(t, dacl)

	// Each inheritable grant can appear as two entries (one for the
	// directory, one inherited by new files), so check who the entries are
	// for, not how many there are.
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	require.NoError(t, err)
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	require.NoError(t, err)
	admins, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	require.NoError(t, err)
	allowed := []*windows.SID{user.User.Sid, system, admins}
	require.NotZero(t, dacl.AceCount)
	for i := range uint32(dacl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		require.NoError(t, windows.GetAce(dacl, i, &ace))
		assert.EqualValues(t, windows.ACCESS_ALLOWED_ACE_TYPE, ace.Header.AceType, "entry %d only grants access", i)
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		ok := false
		for _, a := range allowed {
			ok = ok || sid.Equals(a)
		}
		assert.True(t, ok, "entry %d is for %s; only the owner, SYSTEM and Administrators may have access (F20)", i, sid)
	}

	// The store still works for its owner after the ACL change.
	db, err = Open(dir, s.Schema, Options{Logf: t.Logf})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ids(db.View(), s.rows))
	require.NoError(t, db.Close())
}

func TestIsUNCPath(t *testing.T) {
	for path, want := range map[string]bool{
		`\\server\share\goalert`:       true,
		`\\?\UNC\server\share\goalert`: true,
		`\\.\UNC\server\share`:         true,
		`\\?\C:\goalert`:               false,
		`C:\Users\me\goalert`:          false,
		`D:\`:                          false,
	} {
		assert.Equal(t, want, isUNCPath(path), path)
	}
}

func TestRefusesUNCPath(t *testing.T) {
	err := checkLocalFS(`\\server\share\goalert`)
	require.ErrorIs(t, err, ErrNetworkFS, "network shares are refused (F11)")
}
