package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

// memRecords is an in-memory RecordStore with a clock of its own, whose writes
// a test can make fail.
type memRecords struct {
	mu        sync.Mutex
	manifests map[string]Manifest
	chunks    map[ChunkID]memChunk
	clock     time.Time
	ops       map[string]int

	// fail, if set, is called before each operation, and its error is returned
	// instead of doing it.
	fail func(op, name string) error
	// after, if set, is called after each successful operation.
	after func(op, name string)
}

type memChunk struct {
	owner   string
	data    []byte
	created time.Time
}

var _ RecordStore = (*memRecords)(nil)

func newMemRecords() *memRecords {
	return &memRecords{
		manifests: map[string]Manifest{},
		chunks:    map[ChunkID]memChunk{},
		ops:       map[string]int{},
		clock:     time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC),
	}
}

func (r *memRecords) advance(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock = r.clock.Add(d)
}

func (r *memRecords) do(ctx context.Context, op, name string, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.fail != nil {
		if err := r.fail(op, name); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.ops[op]++
	err := fn()
	r.mu.Unlock()
	if err == nil && r.after != nil {
		r.after(op, name)
	}
	return err
}

func cloneManifest(m Manifest) Manifest {
	m.Inline = bytes.Clone(m.Inline)
	return m
}

func (r *memRecords) GetManifest(ctx context.Context, name string) (m Manifest, err error) {
	err = r.do(ctx, "getManifest", name, func() error {
		stored, ok := r.manifests[name]
		if !ok {
			return fmt.Errorf("%w: %s", ErrManifestNotFound, name)
		}
		m = cloneManifest(stored)
		return nil
	})
	return m, err
}

func (r *memRecords) CreateManifest(ctx context.Context, m Manifest) error {
	return r.do(ctx, "createManifest", m.Name, func() error {
		if _, ok := r.manifests[m.Name]; ok {
			return fmt.Errorf("%w: %s", ErrManifestExists, m.Name)
		}
		r.manifests[m.Name] = cloneManifest(m)
		return nil
	})
}

func (r *memRecords) SetManifest(ctx context.Context, m Manifest) error {
	return r.do(ctx, "setManifest", m.Name, func() error {
		r.manifests[m.Name] = cloneManifest(m)
		return nil
	})
}

func (r *memRecords) DeleteManifest(ctx context.Context, name string) error {
	return r.do(ctx, "deleteManifest", name, func() error {
		delete(r.manifests, name)
		return nil
	})
}

func (r *memRecords) ListNames(ctx context.Context, prefix, startAfter string) (names []string, err error) {
	err = r.do(ctx, "listNames", prefix, func() error {
		for name := range r.manifests {
			names = append(names, name) // callers filter and sort
		}
		return nil
	})
	return names, err
}

func (r *memRecords) PutChunk(ctx context.Context, c Chunk) error {
	return r.do(ctx, "putChunk", c.String(), func() error {
		r.chunks[c.ChunkID] = memChunk{owner: c.Owner, data: bytes.Clone(c.Data), created: r.clock}
		return nil
	})
}

func (r *memRecords) GetChunk(ctx context.Context, id ChunkID) (data []byte, err error) {
	err = r.do(ctx, "getChunk", id.String(), func() error {
		c, ok := r.chunks[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrChunkNotFound, id)
		}
		data = bytes.Clone(c.data)
		return nil
	})
	return data, err
}

func (r *memRecords) ListChunks(ctx context.Context) (chunks []ChunkInfo, asOf time.Time, err error) {
	err = r.do(ctx, "listChunks", "", func() error {
		asOf = r.clock
		for id, c := range r.chunks {
			chunks = append(chunks, ChunkInfo{ChunkID: id, Owner: c.owner, Created: c.created})
		}
		return nil
	})
	return chunks, asOf, err
}

func (r *memRecords) DeleteChunk(ctx context.Context, id ChunkID) error {
	return r.do(ctx, "deleteChunk", id.String(), func() error {
		delete(r.chunks, id)
		return nil
	})
}

func (r *memRecords) count() (manifests, chunks int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.manifests), len(r.chunks)
}
