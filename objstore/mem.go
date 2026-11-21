package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Mem is an in-memory Store for tests. Hooks can inject failures: conflicts,
// writes with an unknown outcome (applied or not), and errors.
type Mem struct {
	mu      sync.Mutex
	objects map[string][]byte

	// BeforeWrite, if set, is called before every PutIfAbsent and Put. It
	// can return an error to fail the call. Returning ErrMemAppliedThenFailed makes
	// the write happen and still report an unknown outcome, like a timeout
	// after the server committed.
	BeforeWrite func(op, name string) error

	ops map[string]int
}

// ErrMemAppliedThenFailed, returned by a Mem.BeforeWrite hook, applies the write and
// then reports an unknown outcome.
var ErrMemAppliedThenFailed = errors.New("objstore: injected: write applied, then the connection dropped")

var _ Store = (*Mem)(nil)

// NewMem returns an empty in-memory store.
func NewMem() *Mem {
	return &Mem{objects: map[string][]byte{}, ops: map[string]int{}}
}

// Ops returns how many times each operation ran.
func (m *Mem) Ops() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.ops))
	for k, v := range m.ops {
		out[k] = v
	}
	return out
}

func (m *Mem) hook(op, name string) (apply bool, err error) {
	if m.BeforeWrite == nil {
		return true, nil
	}
	err = m.BeforeWrite(op, name)
	if errors.Is(err, ErrMemAppliedThenFailed) {
		return true, unknown(err)
	}
	return err == nil, err
}

func (m *Mem) Get(ctx context.Context, name string) ([]byte, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops["get"]++
	data, ok := m.objects[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return bytes.Clone(data), nil
}

func (m *Mem) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err := checkName(name); err != nil {
		return err
	}
	apply, err := m.hook("putIfAbsent", name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops["putIfAbsent"]++
	if apply {
		if _, ok := m.objects[name]; ok {
			if err == nil {
				return fmt.Errorf("%w: %s", ErrExists, name)
			}
		} else {
			m.objects[name] = bytes.Clone(data)
		}
	}
	return err
}

func (m *Mem) Put(ctx context.Context, name string, data []byte) error {
	if err := checkName(name); err != nil {
		return err
	}
	apply, err := m.hook("put", name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops["put"]++
	if apply {
		m.objects[name] = bytes.Clone(data)
	}
	return err
}

func (m *Mem) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops["list"]++
	var names []string
	for name := range m.objects {
		if strings.HasPrefix(name, prefix) && name > startAfter {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (m *Mem) Delete(ctx context.Context, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops["delete"]++
	delete(m.objects, name)
	return nil
}
