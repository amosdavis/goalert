// Package chunkstore implements objstore.Store on document databases, whose
// records are limited in size, such as Cosmos DB and Firestore.
//
// An object is published by one atomic write of its manifest. A small object,
// such as nearly every commit, is in the manifest itself, so it takes a single
// request to write or read. A larger one is split into chunks, which are
// uploaded, and confirmed, before the manifest that names them, so readers see
// a whole object or none (docs/filestore-failure-modes.md, F6, F42).
package chunkstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/target/goalert/objstore"
)

const (
	// MaxRecord is the most object data in one record: a chunk, or an inline
	// manifest. Adapters check that a record of it fits in a document.
	MaxRecord = 512 << 10

	// DefaultChunkSize and DefaultInlineSize leave room in the smallest
	// document limit (Firestore's 1 MiB), and keep records cheap to read whole.
	DefaultChunkSize  = 256 << 10
	DefaultInlineSize = 128 << 10

	// DefaultConcurrency is how many chunks of one object are written, read or
	// deleted at once.
	DefaultConcurrency = 8

	// MinGracePeriod is the shortest grace period CollectGarbage accepts. An
	// upload that takes more than half as long starts over, so no chunk of an
	// upload that can still be published is ever old enough to be collected
	// (F42).
	MinGracePeriod = time.Hour

	maxUpload = MinGracePeriod / 2
)

// Options configure a Store. The zero value is ready to use.
type Options struct {
	// ChunkSize is the most object data in one chunk.
	ChunkSize int
	// InlineSize is the largest object kept in its manifest. A negative value
	// keeps none there, except empty ones.
	InlineSize int
	// Concurrency is how many chunks are written, read or deleted at once.
	Concurrency int
}

// Store implements objstore.Store on a RecordStore. Objects stay readable when
// the options change.
type Store struct {
	records RecordStore
	opts    Options

	now func() time.Time
}

var _ objstore.Store = (*Store)(nil)

// New returns a Store that keeps objects in records.
func New(records RecordStore, opts Options) (*Store, error) {
	if records == nil {
		return nil, errors.New("chunkstore: nil RecordStore")
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	if opts.InlineSize == 0 {
		opts.InlineSize = DefaultInlineSize
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.ChunkSize < 0 || opts.ChunkSize > MaxRecord {
		return nil, fmt.Errorf("chunkstore: chunk size %d is not between 1 and %d", opts.ChunkSize, MaxRecord)
	}
	if opts.InlineSize > MaxRecord {
		return nil, fmt.Errorf("chunkstore: inline size %d is over %d", opts.InlineSize, MaxRecord)
	}
	if opts.Concurrency < 0 {
		return nil, fmt.Errorf("chunkstore: negative concurrency %d", opts.Concurrency)
	}
	return &Store{records: records, opts: opts, now: time.Now}, nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// each calls fn for 0 to n-1, several at once, and returns the first error.
// After an error it starts no more calls.
func (s *Store) each(ctx context.Context, n int, fn func(ctx context.Context, i int) error) error {
	if n == 1 {
		return fn(ctx, 0)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	sem := make(chan struct{}, s.opts.Concurrency)
	for i := range n {
		sem <- struct{}{}
		if ctx.Err() != nil {
			<-sem
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, i); err != nil {
				once.Do(func() { first = err; cancel(err) })
			}
		}()
	}
	wg.Wait()
	if first == nil {
		// Nothing failed, but the caller may have given up part way.
		return context.Cause(ctx)
	}
	return first
}

// checkManifest refuses a manifest no Store writes, so that corruption is
// reported as such rather than as missing data (F21).
func checkManifest(m Manifest, name string) error {
	bad := func(what string) error { return fmt.Errorf("chunkstore: the manifest of %q %s", name, what) }
	switch {
	case m.Name != name:
		return bad(fmt.Sprintf("is for %q", m.Name))
	case m.Length < 0 || m.Chunks < 0:
		return bad("has a negative length")
	case len(m.SHA256) != hex.EncodedLen(sha256.Size):
		return bad("has no digest")
	case m.Chunks > 0 && (m.Upload == "" || len(m.Inline) > 0):
		return bad("has both inline data and chunks, or no upload")
	case m.Chunks == 0 && int64(len(m.Inline)) != m.Length:
		return bad(fmt.Sprintf("holds %d bytes, not %d", len(m.Inline), m.Length))
	}
	return nil
}

func (s *Store) manifest(ctx context.Context, name string) (Manifest, error) {
	m, err := s.records.GetManifest(ctx, name)
	if errors.Is(err, ErrManifestNotFound) {
		return Manifest{}, fmt.Errorf("%w: %s", objstore.ErrNotFound, name)
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("chunkstore: read the manifest of %q: %w", name, err)
	}
	return m, checkManifest(m, name)
}

// read returns the object a manifest describes, checked against its length and
// digest.
func (s *Store) read(ctx context.Context, m Manifest) ([]byte, error) {
	data := m.Inline
	if m.Chunks > 0 {
		parts := make([][]byte, m.Chunks)
		err := s.each(ctx, m.Chunks, func(ctx context.Context, i int) (err error) {
			parts[i], err = s.records.GetChunk(ctx, ChunkID{Upload: m.Upload, Index: i})
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("chunkstore: read the chunks of %q: %w", m.Name, err)
		}
		data = bytes.Join(parts, nil)
	}
	if int64(len(data)) != m.Length || digest(data) != m.SHA256 {
		return nil, fmt.Errorf("chunkstore: %q is corrupt: %d bytes with digest %s, not %d with %s", m.Name, len(data), digest(data), m.Length, m.SHA256)
	}
	return data, nil
}

// Get returns a whole object, checked against its digest.
func (s *Store) Get(ctx context.Context, name string) ([]byte, error) {
	if err := objstore.CheckName(name); err != nil {
		return nil, err
	}
	m, err := s.manifest(ctx, name)
	if err != nil {
		return nil, err
	}
	// If chunks are missing, the object may have been replaced or deleted since
	// its manifest was read, and the chunks collected: look again.
	for attempt := 0; ; attempt++ {
		data, err := s.read(ctx, m)
		if attempt == 4 || !errors.Is(err, ErrChunkNotFound) {
			return data, err
		}
		current, merr := s.manifest(ctx, name)
		if merr != nil {
			return nil, merr
		}
		if current.Upload == m.Upload {
			return nil, err
		}
		m = current
	}
}

// upload is an object on its way to being published.
type upload struct {
	Manifest
	data    []byte
	started time.Time
}

// prepare makes the object's manifest, and uploads its chunks if it needs
// them.
func (s *Store) prepare(ctx context.Context, name string, data []byte) (*upload, error) {
	u := &upload{
		Manifest: Manifest{Name: name, Length: int64(len(data)), SHA256: digest(data)},
		data:     data,
	}
	if len(data) == 0 || len(data) <= s.opts.InlineSize {
		u.Inline = data
		return u, nil
	}
	u.Chunks = (len(data) + s.opts.ChunkSize - 1) / s.opts.ChunkSize
	for range 3 {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		u.Upload, u.started = hex.EncodeToString(id), s.now()
		if err := s.putChunks(ctx, u); err != nil {
			return nil, err
		}
		if !s.slow(u) {
			return u, nil
		}
		// A chunk may be about to be collected, so start over with new ones.
	}
	return nil, fmt.Errorf("chunkstore: uploading %q took more than %v", name, maxUpload)
}

func (s *Store) putChunks(ctx context.Context, u *upload) error {
	size := s.opts.ChunkSize
	err := s.each(ctx, u.Chunks, func(ctx context.Context, i int) error {
		return s.records.PutChunk(ctx, Chunk{
			ChunkID: ChunkID{Upload: u.Upload, Index: i},
			Owner:   u.Name,
			Data:    u.data[i*size : min((i+1)*size, len(u.data))],
		})
	})
	if err != nil {
		return fmt.Errorf("chunkstore: upload the chunks of %q: %w", u.Name, err)
	}
	return nil
}

// slow reports whether an upload took so long, perhaps because this process
// was paused, that its chunks could have been collected. Both clocks count: a
// paused VM's monotonic clock stops, and a wall clock can jump back (F13).
func (s *Store) slow(u *upload) bool {
	now := s.now()
	return u.Chunks > 0 && (now.Sub(u.started) > maxUpload || now.Round(0).Sub(u.started.Round(0)) > maxUpload)
}

// published repairs an object whose chunks may have been collected while it
// was being published. The manifest keeps them from now on.
func (s *Store) published(ctx context.Context, u *upload) error {
	if !s.slow(u) {
		return nil
	}
	if err := s.putChunks(ctx, u); err != nil {
		// The manifest is there, so nobody may take this for a failed write.
		return objstore.Unknown(err)
	}
	return nil
}

// PutIfAbsent uploads the object's chunks, if any, and then atomically creates
// its manifest. Losing the race, or a crash, can leave chunks nobody uses,
// which CollectGarbage removes, but never part of an object.
func (s *Store) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	u, err := s.prepare(ctx, name, data)
	if err != nil {
		return err
	}
	err = s.records.CreateManifest(ctx, u.Manifest)
	if errors.Is(err, ErrManifestExists) {
		return fmt.Errorf("%w: %s", objstore.ErrExists, name)
	}
	if err != nil {
		return fmt.Errorf("chunkstore: create %q: %w", name, err)
	}
	return s.published(ctx, u)
}

// Put uploads the object's chunks, if any, and then atomically sets its
// manifest. Readers see the old object or the new one, whole.
func (s *Store) Put(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	u, err := s.prepare(ctx, name, data)
	if err != nil {
		return err
	}
	if err := s.records.SetManifest(ctx, u.Manifest); err != nil {
		return fmt.Errorf("chunkstore: replace %q: %w", name, err)
	}
	return s.published(ctx, u)
}

// List returns the names of published objects in byte order.
func (s *Store) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	all, err := s.records.ListNames(ctx, prefix, startAfter)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: list %q: %w", prefix, err)
	}
	var names []string
	for _, name := range all {
		if strings.HasPrefix(name, prefix) && name > startAfter {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Delete unpublishes an object. CollectGarbage removes its chunks later, so
// that a reader can finish with them.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	if err := s.records.DeleteManifest(ctx, name); err != nil && !errors.Is(err, ErrManifestNotFound) {
		return fmt.Errorf("chunkstore: delete %q: %w", name, err)
	}
	return nil
}

// CollectGarbage deletes the chunks that no manifest uses and that were stored
// longer ago than gracePeriod by the database's clock, and returns how many it
// deleted. Those are left by uploads that failed or lost a race, and by
// objects that were replaced or deleted.
//
// It reads the manifest of each old chunk's owner, rather than trusting a
// listing of manifests to be complete. An upload's chunks belong to one object
// and are never reused, so a chunk that is old and unused stays unused, and
// several collectors can run at once (F42).
func (s *Store) CollectGarbage(ctx context.Context, gracePeriod time.Duration) (int, error) {
	if gracePeriod < MinGracePeriod {
		return 0, fmt.Errorf("chunkstore: a grace period of %v is under %v", gracePeriod, MinGracePeriod)
	}
	chunks, asOf, err := s.records.ListChunks(ctx)
	if err != nil {
		return 0, fmt.Errorf("chunkstore: list chunks: %w", err)
	}
	if len(chunks) > 0 && asOf.IsZero() {
		return 0, errors.New("chunkstore: list chunks: the database did not report its time")
	}
	cutoff := asOf.Add(-gracePeriod)

	type key struct{ owner, upload string }
	old := make(map[key][]ChunkID)
	var keys []key
	for _, c := range chunks {
		if !c.Created.Before(cutoff) {
			continue
		}
		k := key{c.Owner, c.Upload}
		if old[k] == nil {
			keys = append(keys, k)
		}
		old[k] = append(old[k], c.ChunkID)
	}

	var garbage []ChunkID
	var mu sync.Mutex
	err = s.each(ctx, len(keys), func(ctx context.Context, i int) error {
		k := keys[i]
		m, err := s.records.GetManifest(ctx, k.owner)
		switch {
		case errors.Is(err, ErrManifestNotFound):
		case err != nil:
			return fmt.Errorf("chunkstore: read the manifest of %q: %w", k.owner, err)
		case m.Name != k.owner:
			return fmt.Errorf("chunkstore: the manifest of %q is for %q", k.owner, m.Name)
		case m.Upload == k.upload:
			return nil // in use
		}
		mu.Lock()
		defer mu.Unlock()
		garbage = append(garbage, old[k]...)
		return nil
	})
	if err != nil {
		return 0, err
	}

	deleted := 0
	err = s.each(ctx, len(garbage), func(ctx context.Context, i int) error {
		if err := s.records.DeleteChunk(ctx, garbage[i]); err != nil && !errors.Is(err, ErrChunkNotFound) {
			return fmt.Errorf("chunkstore: delete chunk %s: %w", garbage[i], err)
		}
		mu.Lock()
		defer mu.Unlock()
		deleted++
		return nil
	})
	return deleted, err
}

// CheckSettings reports database settings that break or weaken the store, if
// the RecordStore can check them.
func (s *Store) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	if c, ok := s.records.(interface {
		CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error)
	}); ok {
		return c.CheckSettings(ctx)
	}
	return nil, nil
}
