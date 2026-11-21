//go:build darwin || freebsd

package filestore

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// networkFS lists filesystem type names whose locking, append or fsync
// behavior cannot be trusted across machines (F11).
var networkFS = map[string]bool{
	"nfs": true, "smbfs": true, "cifs": true, "afpfs": true, "webdav": true,
	"macfuse": true, "osxfuse": true, "fusefs": true, "fuse": true,
}

// checkLocalFS refuses network filesystems (F11).
func checkLocalFS(dir string) error {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return fmt.Errorf("statfs %s: %w", dir, err)
	}
	raw := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		raw = append(raw, byte(c))
	}
	name := strings.ToLower(string(bytes.TrimRight(raw, "\x00")))
	if networkFS[name] || strings.HasPrefix(name, "fuse") {
		return fmt.Errorf("%w: %s is on %s", ErrNetworkFS, dir, name)
	}
	return nil
}
