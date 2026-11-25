package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

func newStore(t *testing.T, r RecordStore, opts Options) *Store {
	t.Helper()
	s, err := New(r, opts)
	require.NoError(t, err)
	return s
}

// small makes every object but an empty one need chunks.
var small = Options{ChunkSize: 4, InlineSize: -1, Concurrency: 3}

func TestNewChecksOptions(t *testing.T) {
	_, err := New(nil, Options{})
	require.Error(t, err)
	for _, o := range []Options{
		{ChunkSize: -1}, {ChunkSize: MaxRecord + 1}, {InlineSize: MaxRecord + 1}, {Concurrency: -1},
	} {
		_, err := New(newMemRecords(), o)
		assert.Error(t, err, "%+v", o)
	}
}

// TestInline checks that a small object, like nearly every commit, takes one
// request to write and one to read (F15).
func TestInline(t *testing.T) {
	ctx := context.Background()
	r := newMemRecords()
	s := newStore(t, r, Options{})
	data := bytes.Repeat([]byte("x"), DefaultInlineSize)
	require.NoError(t, s.PutIfAbsent(ctx, "a", data))
	got, err := s.Get(ctx, "a")
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
	assert.Equal(t, map[string]int{"createManifest": 1, "getManifest": 1}, r.ops)

	require.NoError(t, s.Put(ctx, "b", append(data, 'y')))
	_, chunks := r.count()
	assert.Equal(t, 1, chunks, "a larger one is chunked")
}

func TestChunking(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 3, 4, 5, 8, 41} {
		r := newMemRecords()
		s := newStore(t, r, small)
		data := bytes.Repeat([]byte("abcdefg"), n)[:n]
		require.NoError(t, s.PutIfAbsent(ctx, "o", data))
		_, chunks := r.count()
		assert.Equal(t, (n+3)/4, chunks, "%d bytes", n)

		// Objects stay readable with other options.
		for _, o := range []Options{small, {}, {ChunkSize: 7, Concurrency: 1}} {
			got, err := newStore(t, r, o).Get(ctx, "o")
			require.NoError(t, err)
			assert.Equal(t, string(data), string(got))
		}
	}
}

// TestNoPartialObject injects an error at every step of a write: afterwards the
// object must be whole, the old one, or absent, and a repeated write must work
// (F6, F32).
func TestNoPartialObject(t *testing.T) {
	ctx := context.Background()
	old, data := []byte("the old object"), bytes.Repeat([]byte("new "), 10)
	boom := errors.New("boom")
	for _, replace := range []bool{false, true} {
		for step := int64(0); ; step++ {
			r := newMemRecords()
			s := newStore(t, r, small)
			write := s.PutIfAbsent
			if replace {
				write = s.Put
				require.NoError(t, s.Put(ctx, "o", old))
			}
			var n atomic.Int64
			r.fail = func(op, name string) error {
				if n.Add(1) == step+1 {
					return boom
				}
				return nil
			}
			err := write(ctx, "o", data)
			r.fail = nil
			if n.Load() <= step {
				require.NoError(t, err)
				assert.EqualValues(t, 11, step, "every chunk and the manifest were covered")
				break
			}
			require.ErrorIs(t, err, boom, "step %d", step)

			got, err := s.Get(ctx, "o")
			if replace {
				require.NoError(t, err)
				assert.Equal(t, string(old), string(got), "step %d: a failed replacement changes nothing", step)
			} else {
				require.ErrorIs(t, err, objstore.ErrNotFound, "step %d: a failed create publishes nothing", step)
			}
			require.NoError(t, write(ctx, "o", data), "a repeated write works")
			got, err = s.Get(ctx, "o")
			require.NoError(t, err)
			assert.Equal(t, string(data), string(got))
		}
	}
}

// TestUnknownManifestWrite checks that a manifest write whose reply was lost
// is reported as unknown, and nothing else (F32).
func TestUnknownManifestWrite(t *testing.T) {
	ctx := context.Background()
	r := newMemRecords()
	s := newStore(t, r, small)
	r.fail = func(op, name string) error {
		if op == "createManifest" || op == "setManifest" {
			return objstore.Unknown(errors.New("timeout"))
		}
		return nil
	}
	for _, write := range []func(context.Context, string, []byte) error{s.PutIfAbsent, s.Put} {
		err := write(ctx, "o", []byte("data"))
		require.ErrorIs(t, err, objstore.ErrUnknown)
		require.NotErrorIs(t, err, objstore.ErrExists)
	}
}

func TestCorruption(t *testing.T) {
	ctx := context.Background()
	for name, corrupt := range map[string]func(r *memRecords, m *Manifest){
		"other name":    func(r *memRecords, m *Manifest) { m.Name = "p" },
		"no digest":     func(r *memRecords, m *Manifest) { m.SHA256 = "" },
		"wrong digest":  func(r *memRecords, m *Manifest) { m.SHA256 = digest([]byte("other")) },
		"wrong length":  func(r *memRecords, m *Manifest) { m.Length++ },
		"negative":      func(r *memRecords, m *Manifest) { m.Length = -1 },
		"fewer chunks":  func(r *memRecords, m *Manifest) { m.Chunks-- },
		"more chunks":   func(r *memRecords, m *Manifest) { m.Chunks++ },
		"no upload":     func(r *memRecords, m *Manifest) { m.Upload = "" },
		"both":          func(r *memRecords, m *Manifest) { m.Inline = []byte("x") },
		"inline length": func(r *memRecords, m *Manifest) { m.Chunks, m.Upload, m.Inline = 0, "", []byte("x") },
		"changed chunk": func(r *memRecords, m *Manifest) {
			id := ChunkID{Upload: m.Upload, Index: 1}
			c := r.chunks[id]
			c.data = []byte("!!!!")
			r.chunks[id] = c
		},
		"missing chunk": func(r *memRecords, m *Manifest) { delete(r.chunks, ChunkID{Upload: m.Upload, Index: 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			r := newMemRecords()
			s := newStore(t, r, small)
			require.NoError(t, s.Put(ctx, "o", []byte("0123456789")))
			m := r.manifests["o"]
			corrupt(r, &m)
			r.manifests["o"] = m
			_, err := s.Get(ctx, "o")
			require.Error(t, err)
			require.NotErrorIs(t, err, objstore.ErrNotFound, "corruption must not look like a missing object")
		})
	}
}

func TestGarbageCollection(t *testing.T) {
	ctx := context.Background()
	const grace = MinGracePeriod
	r := newMemRecords()
	s := newStore(t, r, small)

	require.NoError(t, s.Put(ctx, "kept", []byte("kept object")))         // 3 chunks
	require.NoError(t, s.Put(ctx, "replaced", []byte("the old version"))) // 4 chunks
	require.NoError(t, s.Put(ctx, "replaced", []byte("the new one")))     // 3 chunks
	require.NoError(t, s.Put(ctx, "deleted", []byte("deleted")))          // 2 chunks
	require.NoError(t, s.Delete(ctx, "deleted"))
	r.fail = func(op, name string) error {
		if op == "createManifest" {
			return errors.New("crash")
		}
		return nil
	}
	require.Error(t, s.PutIfAbsent(ctx, "failed", []byte("failed"))) // 2 chunks
	r.fail = nil
	require.NoError(t, s.PutIfAbsent(ctx, "raced", []byte("winner")))                              // 2 chunks
	require.ErrorIs(t, s.PutIfAbsent(ctx, "raced", []byte("the loser")), objstore.ErrExists)       // 3 chunks
	require.NoError(t, s.Put(ctx, "inlined", []byte("was chunked")))                               // 3 chunks
	require.NoError(t, newStore(t, r, Options{}).Put(ctx, "inlined", []byte("now in a manifest"))) // none

	_, before := r.count()
	require.Equal(t, 3+4+3+2+2+2+3+3, before)

	n, err := s.CollectGarbage(ctx, grace)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing is old enough")
	r.advance(grace)
	n, err = s.CollectGarbage(ctx, grace)
	require.NoError(t, err)
	assert.Zero(t, n, "exactly the grace period is not over it")
	r.advance(time.Microsecond)

	// Even a collector whose own clock is far off uses the database's (F13).
	s.now = func() time.Time { return time.Unix(0, 0) }
	n, err = s.CollectGarbage(ctx, grace)
	require.NoError(t, err)
	assert.Equal(t, 4+2+2+3+3, n)
	s.now = time.Now
	n, err = s.CollectGarbage(ctx, grace)
	require.NoError(t, err)
	assert.Zero(t, n)

	for name, want := range map[string]string{"kept": "kept object", "replaced": "the new one", "raced": "winner", "inlined": "now in a manifest"} {
		got, err := s.Get(ctx, name)
		require.NoError(t, err)
		assert.Equal(t, want, string(got))
	}
	_, after := r.count()
	assert.Equal(t, 3+3+2, after)

	_, err = s.CollectGarbage(ctx, grace-1)
	assert.Error(t, err, "a shorter grace period could delete an upload in progress")
}

// TestGarbageCollectionErrors checks that a manifest that could not be read is
// never taken for one that does not use its chunks.
func TestGarbageCollectionErrors(t *testing.T) {
	ctx := context.Background()
	r := newMemRecords()
	s := newStore(t, r, small)
	require.NoError(t, s.Put(ctx, "o", []byte("some object")))
	r.advance(48 * time.Hour)
	for _, op := range []string{"getManifest", "listChunks"} {
		r.fail = func(o, name string) error {
			if o == op {
				return errors.New("unavailable")
			}
			return nil
		}
		_, err := s.CollectGarbage(ctx, MinGracePeriod)
		require.Error(t, err)
	}
	r.fail = nil
	got, err := s.Get(ctx, "o")
	require.NoError(t, err)
	assert.Equal(t, "some object", string(got))

	// A database that does not tell its time gets nothing deleted.
	require.NoError(t, s.Delete(ctx, "o"))
	r.clock = time.Time{}
	_, err = s.CollectGarbage(ctx, MinGracePeriod)
	require.Error(t, err)
}

// TestSlowUpload checks the promise behind the grace period: the chunks of an
// upload that took too long, such as one whose process was paused, are never
// published as they are (F42).
func TestSlowUpload(t *testing.T) {
	ctx := context.Background()
	data := []byte("some object")
	base := time.Now()

	t.Run("before publishing", func(t *testing.T) {
		r := newMemRecords()
		s := newStore(t, r, small)
		var paused atomic.Bool
		s.now = func() time.Time {
			if paused.Load() {
				return base.Add(maxUpload + time.Second)
			}
			return base
		}
		var once sync.Once
		r.after = func(op, name string) {
			if op == "putChunk" {
				once.Do(func() { paused.Store(true) })
			}
		}
		require.NoError(t, s.PutIfAbsent(ctx, "o", data))
		_, chunks := r.count()
		assert.Equal(t, 6, chunks, "uploaded again, under another ID")
		got, err := s.Get(ctx, "o")
		require.NoError(t, err)
		assert.Equal(t, string(data), string(got))
	})

	t.Run("while publishing", func(t *testing.T) {
		// The process is paused between its check and the manifest write, for
		// so long that another pod collects the chunks first.
		r := newMemRecords()
		s := newStore(t, r, small)
		other := newStore(t, r, small)
		var now atomic.Int64
		s.now = func() time.Time { return base.Add(time.Duration(now.Load())) }
		var once sync.Once
		r.fail = func(op, name string) error {
			if op != "createManifest" {
				return nil
			}
			once.Do(func() {
				now.Store(int64(2 * time.Hour))
				r.advance(2 * time.Hour)
				n, err := other.CollectGarbage(ctx, MinGracePeriod)
				assert.NoError(t, err)
				assert.Equal(t, 3, n)
			})
			return nil
		}
		require.NoError(t, s.PutIfAbsent(ctx, "o", data))
		got, err := s.Get(ctx, "o")
		require.NoError(t, err)
		assert.Equal(t, string(data), string(got), "the chunks were stored again")
		n, err := other.CollectGarbage(ctx, MinGracePeriod)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("always", func(t *testing.T) {
		r := newMemRecords()
		s := newStore(t, r, small)
		var now atomic.Int64
		s.now = func() time.Time { return base.Add(time.Duration(now.Add(int64(time.Hour)))) }
		err := s.PutIfAbsent(ctx, "o", data)
		require.Error(t, err)
		require.NotErrorIs(t, err, objstore.ErrUnknown, "nothing was published")
		_, err = s.Get(ctx, "o")
		require.ErrorIs(t, err, objstore.ErrNotFound)
	})
}

// TestReadDuringReplace checks that a reader whose chunks are collected because
// the object was replaced gets the new object.
func TestReadDuringReplace(t *testing.T) {
	ctx := context.Background()
	r := newMemRecords()
	s := newStore(t, r, small)
	require.NoError(t, s.Put(ctx, "o", []byte("version one")))
	var replaced atomic.Bool
	r.after = func(op, name string) {
		if op != "getManifest" || !replaced.CompareAndSwap(false, true) {
			return
		}
		r.advance(48 * time.Hour)
		assert.NoError(t, s.Put(ctx, "o", []byte("version two")))
		n, err := s.CollectGarbage(ctx, MinGracePeriod)
		assert.NoError(t, err)
		assert.Equal(t, 3, n)
	}
	got, err := s.Get(ctx, "o")
	require.NoError(t, err)
	assert.Equal(t, "version two", string(got))
}

// TestConcurrentUse runs writers, readers and collectors on one store at once:
// readers see whole objects, and nothing in use is collected.
func TestConcurrentUse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newMemRecords()
	const names = 4
	var wg sync.WaitGroup
	for w := range 3 {
		s := newStore(t, r, small)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				name := fmt.Sprintf("o%d", rand.N(names))
				data := bytes.Repeat(fmt.Appendf(nil, "%d:%d ", w, i), 1+rand.N(5))
				if !assert.NoError(t, s.Put(ctx, name, data)) {
					return
				}
				r.advance(time.Minute)
			}
		}()
	}
	var bg sync.WaitGroup
	for range 2 {
		s := newStore(t, r, small)
		bg.Add(2)
		go func() {
			defer bg.Done()
			for ctx.Err() == nil {
				_, err := s.Get(ctx, fmt.Sprintf("o%d", rand.N(names)))
				if err != nil && !errors.Is(err, objstore.ErrNotFound) && ctx.Err() == nil {
					assert.NoError(t, err)
					return
				}
			}
		}()
		go func() {
			defer bg.Done()
			for ctx.Err() == nil {
				if _, err := s.CollectGarbage(ctx, MinGracePeriod); err != nil && ctx.Err() == nil {
					assert.NoError(t, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	cancel()
	bg.Wait()
}

func TestCancelUpload(t *testing.T) {
	r := newMemRecords()
	s := newStore(t, r, small)
	ctx, cancel := context.WithCancel(context.Background())
	r.after = func(op, name string) { cancel() }
	err := s.PutIfAbsent(ctx, "o", bytes.Repeat([]byte("x"), 400))
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, objstore.ErrUnknown, "nothing was published")
	_, chunks := r.count()
	assert.Less(t, chunks, 100, "the upload stopped")
}
