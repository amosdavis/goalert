package filestore

import (
	"io"
	"os"
)

// fileSystem is the set of filesystem operations the store uses. Production
// code uses osFS. Tests use memFS, which can simulate a crash (dropping
// anything that was not fsynced) and inject I/O errors.
type fileSystem interface {
	MkdirAll(dir string) error

	// Create creates a new file for writing. It fails if the file exists.
	Create(name string) (file, error)

	// OpenAppend opens an existing file for writing at its end.
	OpenAppend(name string) (file, error)

	ReadFile(name string) ([]byte, error)

	// ReadDir returns the names of the entries in dir, sorted.
	ReadDir(dir string) ([]string, error)

	Rename(oldName, newName string) error
	Remove(name string) error

	// TruncateFile truncates name to size and fsyncs it.
	TruncateFile(name string, size int64) error

	// SyncDir makes the creation, removal and renaming of entries in dir
	// durable.
	SyncDir(dir string) error
}

type file interface {
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

type osFS struct{}

func (osFS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o700) }

func (osFS) Create(name string) (file, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

func (osFS) OpenAppend(name string) (file, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
}

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

func (osFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir) // sorted by name
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (osFS) Rename(oldName, newName string) error { return os.Rename(oldName, newName) }
func (osFS) Remove(name string) error             { return os.Remove(name) }

func (osFS) TruncateFile(name string, size int64) error {
	f, err := os.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (osFS) SyncDir(dir string) error { return syncDir(dir) }
