//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package filestore

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// syncDir fsyncs a directory so that entries created, removed or renamed in
// it survive a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// lockFile takes an exclusive lock on path, creating it if needed. The lock is
// held until the returned Closer is closed or the process exits (F11).
func lockFile(path string) (io.Closer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}

// restrictDir makes dir accessible only to its owner (F20).
func restrictDir(dir string) error { return os.Chmod(dir, 0o700) }
