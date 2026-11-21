package filestore

import (
	"bytes"
	"errors"
	"io/fs"
	"maps"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
)

var errCrashed = errors.New("memfs: the machine crashed")

type memFile struct {
	data   []byte // what a process reading the file sees
	synced []byte // what is on disk as of the last Sync
}

// memFS is an in-memory fileSystem that models what survives a crash, the
// way POSIX allows:
//
//   - A file's contents survive only up to its last Sync. Some part of what
//     was written after that may also survive, possibly followed by zeros.
//   - Creating, renaming or removing a file survives only once the
//     directory has been synced.
//
// Tests can make it crash after a given number of operations, and make a
// write or sync fail.
type memFS struct {
	mu      sync.Mutex
	files   map[string]*memFile // the namespace processes see
	durable map[string]*memFile // the namespace as of the last SyncDir

	opsLeft int // when positive, the machine crashes when this reaches 0
	crashed bool

	failWrite        error // the next Write writes failWritePartial bytes, then fails
	failWritePartial int
	failSync         error // the next Sync fails without syncing
}

func newMemFS() *memFS {
	return &memFS{files: map[string]*memFile{}, durable: map[string]*memFile{}}
}

// crashAfter makes the machine crash at the nth mutating operation from now.
func (m *memFS) crashAfter(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opsLeft = n
}

func (m *memFS) isCrashed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.crashed
}

// step counts a mutating operation. It returns errCrashed if the machine has
// crashed, and now=true if it crashed at this operation.
func (m *memFS) step() (now bool, err error) {
	if m.crashed {
		return false, errCrashed
	}
	if m.opsLeft > 0 {
		m.opsLeft--
		if m.opsLeft == 0 {
			m.crashed = true
			return true, errCrashed
		}
	}
	return false, nil
}

// reboot brings the machine back with only what survived the crash. With
// rng, part of each file's unsynced data survives too.
func (m *memFS) reboot(rng *rand.Rand) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make(map[string]*memFile, len(m.durable))
	for name, f := range m.durable {
		data := bytes.Clone(f.synced)
		if rng != nil && len(f.data) > len(f.synced) && bytes.HasPrefix(f.data, f.synced) {
			extra := f.data[len(f.synced):]
			data = append(data, extra[:rng.Intn(len(extra)+1)]...)
			if rng.Intn(4) == 0 {
				data = append(data, make([]byte, rng.Intn(100))...) // zero-filled blocks
			}
		}
		files[name] = &memFile{data: data, synced: bytes.Clone(data)}
	}
	m.files = files
	m.durable = maps.Clone(files)
	m.crashed = false
	m.opsLeft = 0
	m.failWrite, m.failSync = nil, nil
}

func clean(name string) string { return filepath.Clean(name) }

func (m *memFS) MkdirAll(string) error { return nil }

func (m *memFS) Create(name string) (file, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.step(); err != nil {
		return nil, err
	}
	name = clean(name)
	if _, ok := m.files[name]; ok {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrExist}
	}
	f := &memFile{}
	m.files[name] = f
	return &memHandle{m: m, f: f}, nil
}

func (m *memFS) OpenAppend(name string) (file, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashed {
		return nil, errCrashed
	}
	f, ok := m.files[clean(name)]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &memHandle{m: m, f: f}, nil
}

func (m *memFS) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[clean(name)]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return bytes.Clone(f.data), nil
}

func (m *memFS) ReadDir(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = clean(dir)
	var names []string
	for name := range m.files {
		if filepath.Dir(name) == dir {
			names = append(names, filepath.Base(name))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (m *memFS) Rename(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.step(); err != nil {
		return err
	}
	f, ok := m.files[clean(oldName)]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldName, Err: fs.ErrNotExist}
	}
	delete(m.files, clean(oldName))
	m.files[clean(newName)] = f
	return nil
}

func (m *memFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.step(); err != nil {
		return err
	}
	if _, ok := m.files[clean(name)]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(m.files, clean(name))
	return nil
}

func (m *memFS) TruncateFile(name string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.step(); err != nil {
		return err
	}
	f, ok := m.files[clean(name)]
	if !ok {
		return &fs.PathError{Op: "truncate", Path: name, Err: fs.ErrNotExist}
	}
	if int(size) < len(f.data) {
		f.data = f.data[:size]
	}
	f.synced = bytes.Clone(f.data)
	return nil
}

func (m *memFS) SyncDir(string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.step(); err != nil {
		return err
	}
	m.durable = maps.Clone(m.files)
	return nil
}

// put writes a file directly, bypassing crash modeling. For test setup.
func (m *memFS) put(name string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := &memFile{data: bytes.Clone(data), synced: bytes.Clone(data)}
	m.files[clean(name)] = f
	m.durable[clean(name)] = f
}

type memHandle struct {
	m      *memFS
	f      *memFile
	closed bool
}

func (h *memHandle) Write(p []byte) (int, error) {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	if h.closed {
		return 0, fs.ErrClosed
	}
	if err := h.m.failWrite; err != nil {
		h.m.failWrite = nil
		n := min(h.m.failWritePartial, len(p))
		h.f.data = append(h.f.data, p[:n]...)
		return n, err
	}
	now, err := h.m.step()
	if now {
		// The write reached the page cache, then the power went out.
		h.f.data = append(h.f.data, p...)
	}
	if err != nil {
		return 0, err
	}
	h.f.data = append(h.f.data, p...)
	return len(p), nil
}

func (h *memHandle) Sync() error {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	if h.closed {
		return fs.ErrClosed
	}
	if err := h.m.failSync; err != nil {
		h.m.failSync = nil
		return err
	}
	if _, err := h.m.step(); err != nil {
		return err
	}
	h.f.synced = bytes.Clone(h.f.data)
	return nil
}

func (h *memHandle) Truncate(size int64) error {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	if h.closed {
		return fs.ErrClosed
	}
	if _, err := h.m.step(); err != nil {
		return err
	}
	if int(size) < len(h.f.data) {
		h.f.data = h.f.data[:size]
	}
	return nil
}

func (h *memHandle) Close() error {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	h.closed = true
	return nil
}
