package objstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Dir is a Store in a local directory, for development and tests. It is
// safe for several processes on one machine: PutIfAbsent writes a temporary
// file, fsyncs it, and hard-links it into place, and a link fails if the name
// exists. It needs a filesystem with hard links (not FAT), and is not safe
// on network filesystems (F11).
type Dir struct {
	root string
}

var _ Store = (*Dir)(nil)

const dirTempPrefix = ".tmp-"

// NewDir returns a Store in root, creating root if needed.
func NewDir(root string) (*Dir, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return &Dir{root: abs}, nil
}

func (d *Dir) path(name string) string { return filepath.Join(d.root, filepath.FromSlash(name)) }

func (d *Dir) Get(ctx context.Context, name string) ([]byte, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	data, err := readWholeFile(d.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return data, err
}

// writeTemp writes data to a new, fsynced temporary file in the directory
// that will hold name, and returns its path.
func (d *Dir) writeTemp(name string, data []byte) (string, error) {
	dir := filepath.Dir(d.path(name))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, dirTempPrefix+"*")
	if err != nil {
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func (d *Dir) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err := checkName(name); err != nil {
		return err
	}
	tmp, err := d.writeTemp(name, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()

	err = os.Link(tmp, d.path(name))
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%w: %s", ErrExists, name)
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(d.path(name)))
}

func (d *Dir) Put(ctx context.Context, name string, data []byte) error {
	if err := checkName(name); err != nil {
		return err
	}
	tmp, err := d.writeTemp(name, data)
	if err != nil {
		return err
	}
	if err := replaceFile(tmp, d.path(name)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(d.path(name)))
}

func (d *Dir) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	// Walk only the deepest directory the prefix names.
	base := d.root
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		base = d.path(prefix[:i])
	}
	var names []string
	err := filepath.WalkDir(base, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			if p == base && errors.Is(err, fs.ErrNotExist) {
				return filepath.SkipAll
			}
			return err
		}
		if de.IsDir() || strings.HasPrefix(de.Name(), dirTempPrefix) {
			return nil
		}
		rel, err := filepath.Rel(d.root, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if strings.HasPrefix(name, prefix) && name > startAfter {
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (d *Dir) Delete(ctx context.Context, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	err := os.Remove(d.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
