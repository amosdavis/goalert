package filestore

import (
	"errors"
	"fmt"
)

var (
	// ErrLocked means another process has the data directory open (F11).
	ErrLocked = errors.New("filestore: data directory is in use by another process")

	// ErrNetworkFS means the data directory is on a network filesystem (F11).
	ErrNetworkFS = errors.New("filestore: network filesystems are not supported")

	// ErrCorrupt means stored data failed validation. The store refuses to
	// start rather than guess (F6).
	ErrCorrupt = errors.New("filestore: stored data is corrupt")

	// ErrVersion means stored data was written in a format, or with tables,
	// that this version does not understand (F19, F30).
	ErrVersion = errors.New("filestore: unsupported data format")

	// ErrNotStore means the directory has files but is not a data directory.
	ErrNotStore = errors.New("filestore: directory is not empty and is not a GoAlert data directory")

	// ErrClosed means the store has been closed.
	ErrClosed = errors.New("filestore: store is closed")

	// ErrTxDone means a transaction was used after Update returned.
	ErrTxDone = errors.New("filestore: transaction already finished")

	errShortHeader = errors.New("short header")
)

// UniqueError reports a write that would give two rows the same unique key
// (F8).
type UniqueError struct {
	Table string
	Index string
	Key   string
}

func (e *UniqueError) Error() string {
	return fmt.Sprintf("filestore: %s.%s: duplicate key %q", e.Table, e.Index, e.Key)
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}

func errorsIsCorrupt(err error) bool { return errors.Is(err, ErrCorrupt) }
