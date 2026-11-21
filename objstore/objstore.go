// Package objstore is the small object-store interface GoAlert's Delta Lake
// backend is built on, with in-memory and local-directory implementations.
// GCS and Azure Blob Storage implementations live in subpackages, so their
// SDKs are only linked where they are used.
//
// The one primitive a Delta log needs for safe concurrent commits is
// PutIfAbsent: a write that creates an object only if it does not exist yet
// and is atomic, so two writers can never both create the same commit
// (docs/filestore-failure-modes.md, F4).
package objstore

import (
	"context"
	"errors"
	"fmt"
)

// Store is an object store. Object names use "/" as a separator and have no
// leading slash. All implementations must pass objstoretest.Run.
type Store interface {
	// Get returns an object's contents, or an error wrapping ErrNotFound.
	Get(ctx context.Context, name string) ([]byte, error)

	// PutIfAbsent creates an object with the given contents, or returns an
	// error wrapping ErrExists if it already exists. The write is atomic: the
	// object is either created whole or not at all, and of several
	// concurrent calls for one name, at most one succeeds.
	//
	// If the outcome cannot be known (a timeout, a dropped connection, a
	// server error), the error wraps ErrUnknown, and the caller must read the
	// object back to find out (F32). An implementation that retries may
	// return ErrExists for a write of its own that succeeded on an earlier
	// attempt, so callers must also check whether an existing object is
	// theirs.
	PutIfAbsent(ctx context.Context, name string, data []byte) error

	// Put creates or replaces an object.
	Put(ctx context.Context, name string, data []byte) error

	// List returns the names of the objects that start with prefix and sort
	// after startAfter (all of them if startAfter is empty), in byte order.
	List(ctx context.Context, prefix, startAfter string) ([]string, error)

	// Delete removes an object. Deleting a missing object is not an error.
	Delete(ctx context.Context, name string) error
}

var (
	// ErrNotFound means the object does not exist.
	ErrNotFound = errors.New("objstore: object not found")

	// ErrExists means PutIfAbsent found the object already there.
	ErrExists = errors.New("objstore: object already exists")

	// ErrUnknown means a write may or may not have happened.
	ErrUnknown = errors.New("objstore: write outcome unknown")
)

// unknown wraps err so that it matches ErrUnknown as well as itself.
func unknown(err error) error { return fmt.Errorf("%w: %w", ErrUnknown, err) }

// Unknown wraps err so that it matches ErrUnknown, for implementations in
// other packages.
func Unknown(err error) error { return unknown(err) }

// checkName rejects names that every implementation cannot store the same
// way.
func checkName(name string) error {
	if name == "" || name[0] == '/' || name[len(name)-1] == '/' {
		return fmt.Errorf("objstore: invalid object name %q", name)
	}
	for _, c := range name {
		if c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("objstore: invalid character in object name %q", name)
		}
	}
	return nil
}

// CheckName reports whether name is valid for every implementation.
func CheckName(name string) error { return checkName(name) }
