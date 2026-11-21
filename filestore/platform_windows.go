//go:build windows

package filestore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// syncDir is a no-op on Windows. A directory cannot be fsynced there
// (FlushFileBuffers needs write access to the handle, which directories
// opened for reading do not have), and NTFS journals directory changes
// itself. Each file's own data is still flushed with FlushFileBuffers.
func syncDir(string) error { return nil }

// lockFile takes an exclusive lock on path, creating it if needed. The lock is
// held until the returned Closer is closed or the process exits (F11).
func lockFile(path string) (io.Closer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	ol := new(windows.Overlapped)
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, ol)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}

// checkLocalFS refuses network drives and UNC paths (F11).
func checkLocalFS(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if isUNCPath(abs) {
		return fmt.Errorf("%w: %s is a network path", ErrNetworkFS, dir)
	}
	root, err := windows.UTF16PtrFromString(filepath.VolumeName(abs) + `\`)
	if err != nil {
		return err
	}
	if windows.GetDriveType(root) == windows.DRIVE_REMOTE {
		return fmt.Errorf("%w: %s is on a network drive", ErrNetworkFS, dir)
	}
	return nil
}

// isUNCPath reports whether an absolute path refers to a network share:
// \\server\share\... or \\?\UNC\server\share\.... Device paths to local
// drives (\\?\C:\...) are not network paths.
func isUNCPath(abs string) bool {
	upper := strings.ToUpper(abs)
	switch {
	case strings.HasPrefix(upper, `\\?\UNC\`), strings.HasPrefix(upper, `\\.\UNC\`):
		return true
	case strings.HasPrefix(upper, `\\?\`), strings.HasPrefix(upper, `\\.\`):
		return false
	}
	return strings.HasPrefix(abs, `\\`)
}

// restrictDir replaces the directory's ACL so that only the current user,
// SYSTEM and Administrators can access it, and new files inherit that
// (F20). Inheritance from the parent directory is cut off.
func restrictDir(dir string) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("get current user: %w", err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	admins, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return err
	}
	grant := func(sid *windows.SID, typ windows.TRUSTEE_TYPE) windows.EXPLICIT_ACCESS {
		return windows.EXPLICIT_ACCESS{
			AccessPermissions: windows.GENERIC_ALL,
			AccessMode:        windows.SET_ACCESS,
			Inheritance:       windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  typ,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		}
	}
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{
		grant(user.User.Sid, windows.TRUSTEE_IS_USER),
		grant(system, windows.TRUSTEE_IS_WELL_KNOWN_GROUP),
		grant(admins, windows.TRUSTEE_IS_WELL_KNOWN_GROUP),
	}, nil)
	if err != nil {
		return fmt.Errorf("build ACL: %w", err)
	}
	err = windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, acl, nil)
	if err != nil {
		return fmt.Errorf("set ACL on %s: %w", dir, err)
	}
	return nil
}
