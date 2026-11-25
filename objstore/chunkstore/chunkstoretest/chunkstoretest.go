// Package chunkstoretest is the conformance suite every chunkstore.RecordStore
// must pass, besides objstoretest.Run for the object store built on it
// (docs/filestore-failure-modes.md, F14). To add another document database,
// implement the port and run both.
package chunkstoretest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore/chunkstore"
)

func uploadID(t *testing.T) string {
	t.Helper()
	id := make([]byte, 16)
	_, err := rand.Read(id)
	require.NoError(t, err)
	return hex.EncodeToString(id)
}

func manifest(name string, data []byte) chunkstore.Manifest {
	return chunkstore.Manifest{Name: name, Length: int64(len(data)), SHA256: fmt.Sprintf("%064x", len(data)), Inline: data}
}

// sameManifest compares manifests; whether an empty Inline is nil is up to the
// database.
func sameManifest(t *testing.T, want, got chunkstore.Manifest) {
	t.Helper()
	assert.True(t, bytes.Equal(want.Inline, got.Inline), "inline data round-trips")
	want.Inline, got.Inline = nil, nil
	assert.Equal(t, want, got)
}

// Run runs the suite. newRecords must return an empty RecordStore each time it
// is called, for example one scoped to a fresh prefix.
func Run(t *testing.T, newRecords func(t *testing.T) chunkstore.RecordStore) {
	ctx := context.Background()

	t.Run("Manifest", func(t *testing.T) {
		r := newRecords(t)
		_, err := r.GetManifest(ctx, "a/b")
		require.ErrorIs(t, err, chunkstore.ErrManifestNotFound)

		first := manifest("a/b", []byte("first"))
		require.NoError(t, r.CreateManifest(ctx, first))
		got, err := r.GetManifest(ctx, "a/b")
		require.NoError(t, err)
		sameManifest(t, first, got)

		require.ErrorIs(t, r.CreateManifest(ctx, manifest("a/b", []byte("second"))), chunkstore.ErrManifestExists)
		got, err = r.GetManifest(ctx, "a/b")
		require.NoError(t, err)
		sameManifest(t, first, got)

		// A manifest with chunks replaces one with inline data, and back.
		chunked := chunkstore.Manifest{Name: "a/b", Length: 1 << 40, SHA256: fmt.Sprintf("%064x", 7), Upload: uploadID(t), Chunks: 1 << 20}
		require.NoError(t, r.SetManifest(ctx, chunked))
		got, err = r.GetManifest(ctx, "a/b")
		require.NoError(t, err)
		sameManifest(t, chunked, got)
		empty := manifest("a/b", nil)
		require.NoError(t, r.SetManifest(ctx, empty))
		got, err = r.GetManifest(ctx, "a/b")
		require.NoError(t, err)
		sameManifest(t, empty, got)

		require.NoError(t, r.DeleteManifest(ctx, "a/b"))
		_, err = r.GetManifest(ctx, "a/b")
		require.ErrorIs(t, err, chunkstore.ErrManifestNotFound)
		if err := r.DeleteManifest(ctx, "a/b"); err != nil {
			require.ErrorIs(t, err, chunkstore.ErrManifestNotFound, "deleting a missing manifest is not an error")
		}

		require.NoError(t, r.SetManifest(ctx, first), "SetManifest creates too")
		require.NoError(t, r.CreateManifest(ctx, manifest("a/c", nil)))
	})

	t.Run("CreateManifestRace", func(t *testing.T) {
		// This is what makes a commit atomic: one winner, whole (F4).
		r := newRecords(t)
		for round := range 5 {
			name := fmt.Sprintf("_delta_log/%020d.json", round)
			const n = 12
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = r.CreateManifest(ctx, manifest(name, fmt.Appendf(nil, "writer %d", i)))
				}()
			}
			wg.Wait()
			winner := -1
			for i, err := range errs {
				if err == nil {
					require.Equal(t, -1, winner, "two writers created the manifest")
					winner = i
					continue
				}
				require.ErrorIs(t, err, chunkstore.ErrManifestExists)
			}
			require.NotEqual(t, -1, winner, "nobody created the manifest")
			got, err := r.GetManifest(ctx, name)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("writer %d", winner), string(got.Inline))
		}
	})

	t.Run("SetManifestRace", func(t *testing.T) {
		// Concurrent replacements all succeed, and one of them stays.
		r := newRecords(t)
		const n = 8
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = r.SetManifest(ctx, manifest("_last_checkpoint", fmt.Appendf(nil, "writer %d", i)))
			}()
		}
		wg.Wait()
		var want []string
		for i, err := range errs {
			require.NoError(t, err)
			want = append(want, fmt.Sprintf("writer %d", i))
		}
		got, err := r.GetManifest(ctx, "_last_checkpoint")
		require.NoError(t, err)
		assert.Contains(t, want, string(got.Inline))
	})

	t.Run("MaxRecord", func(t *testing.T) {
		// The largest records fit, whatever their bytes.
		r := newRecords(t)
		data := make([]byte, chunkstore.MaxRecord)
		_, err := rand.Read(data)
		require.NoError(t, err)
		m := manifest("large", data)
		require.NoError(t, r.CreateManifest(ctx, m))
		got, err := r.GetManifest(ctx, "large")
		require.NoError(t, err)
		sameManifest(t, m, got)

		c := chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: uploadID(t)}, Owner: "large", Data: data}
		require.NoError(t, r.PutChunk(ctx, c))
		read, err := r.GetChunk(ctx, c.ChunkID)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data, read), "a chunk round-trips")
	})

	t.Run("ListNames", func(t *testing.T) {
		r := newRecords(t)
		names := []string{
			"_delta_log/00000000000000000000.json",
			"_delta_log/00000000000000000001.json",
			"_delta_log/_last_checkpoint",
			"tbl=alerts/bucket=3/a b.parquet",
			"tbl=users/bucket=0/é∂.parquet",
			"tbl=users/bucket=0/\U0001F6A8.parquet",
			"tbl=users/bucket=0/￮.parquet",
		}
		for _, name := range names {
			require.NoError(t, r.CreateManifest(ctx, manifest(name, []byte(name))))
		}
		// A chunk is not listed.
		require.NoError(t, r.PutChunk(ctx, chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: uploadID(t)}, Owner: "x", Data: []byte("x")}))

		list := func(prefix, after string) []string {
			got, err := r.ListNames(ctx, prefix, after)
			require.NoError(t, err)
			slices.Sort(got)
			return got
		}
		sorted := slices.Sorted(slices.Values(names))
		assert.Equal(t, sorted, list("", ""))
		// Names beyond the ones asked for are allowed, but none may be missing.
		assert.Subset(t, list("_delta_log/", "_delta_log/00000000000000000000.json"), names[1:3])
		assert.Subset(t, list("tbl=users/", ""), names[4:])
		for i, after := range sorted {
			for _, name := range sorted[i+1:] {
				assert.Contains(t, list("", after), name, "listed after %q", after)
			}
		}
	})

	t.Run("ListManyNames", func(t *testing.T) {
		// More names than a database returns in one page.
		r := newRecords(t)
		const n = 1200
		errs := make([]error, n)
		sem := make(chan struct{}, 16)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				errs[i] = r.CreateManifest(ctx, manifest(fmt.Sprintf("n/%05d", i), bytes.Repeat([]byte("x"), 200)))
			}()
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
		got, err := r.ListNames(ctx, "n/", "")
		require.NoError(t, err)
		slices.Sort(got)
		require.Len(t, got, n)
		assert.Equal(t, "n/00000", got[0])
		assert.Equal(t, fmt.Sprintf("n/%05d", n-1), got[n-1])
		assert.Len(t, slices.Compact(got), n, "no name is listed twice")
	})

	t.Run("Chunks", func(t *testing.T) {
		r := newRecords(t)
		_, _, err := r.ListChunks(ctx)
		require.NoError(t, err, "there may be no chunks")

		var want []chunkstore.ChunkID
		for range 4 {
			upload := uploadID(t)
			for i := range 3 {
				c := chunkstore.Chunk{
					ChunkID: chunkstore.ChunkID{Upload: upload, Index: i},
					Owner:   "owner/" + upload,
					Data:    fmt.Appendf(nil, "%s %d", upload, i),
				}
				require.NoError(t, r.PutChunk(ctx, c))
				require.NoError(t, r.PutChunk(ctx, c), "a repeated upload is fine")
				want = append(want, c.ChunkID)
			}
		}
		_, err = r.GetChunk(ctx, chunkstore.ChunkID{Upload: want[0].Upload, Index: 99})
		require.ErrorIs(t, err, chunkstore.ErrChunkNotFound)

		chunks, asOf, err := r.ListChunks(ctx)
		require.NoError(t, err)
		require.Len(t, chunks, len(want))
		// The database's clock, not this machine's, decides both times.
		var got []chunkstore.ChunkID
		for _, c := range chunks {
			got = append(got, c.ChunkID)
			assert.Equal(t, "owner/"+c.Upload, c.Owner)
			assert.WithinDuration(t, asOf, c.Created, 5*time.Minute)
			data, err := r.GetChunk(ctx, c.ChunkID)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s %d", c.Upload, c.Index), string(data))
		}
		assert.ElementsMatch(t, want, got)

		require.NoError(t, r.DeleteChunk(ctx, want[0]))
		_, err = r.GetChunk(ctx, want[0])
		require.ErrorIs(t, err, chunkstore.ErrChunkNotFound)
		if err := r.DeleteChunk(ctx, want[0]); err != nil {
			require.ErrorIs(t, err, chunkstore.ErrChunkNotFound, "deleting a missing chunk is not an error")
		}
		chunks, _, err = r.ListChunks(ctx)
		require.NoError(t, err)
		assert.Len(t, chunks, len(want)-1)
	})

	t.Run("Isolation", func(t *testing.T) {
		a, b := newRecords(t), newRecords(t)
		require.NoError(t, a.CreateManifest(ctx, manifest("same", []byte("a"))))
		require.NoError(t, b.CreateManifest(ctx, manifest("same", []byte("b"))))
		id := chunkstore.ChunkID{Upload: uploadID(t)}
		require.NoError(t, a.PutChunk(ctx, chunkstore.Chunk{ChunkID: id, Owner: "same", Data: []byte("a")}))
		_, err := b.GetChunk(ctx, id)
		require.ErrorIs(t, err, chunkstore.ErrChunkNotFound)
		chunks, _, err := b.ListChunks(ctx)
		require.NoError(t, err)
		assert.Empty(t, chunks, "another store's chunks must never be collected")
		got, err := a.GetManifest(ctx, "same")
		require.NoError(t, err)
		assert.Equal(t, "a", string(got.Inline))
	})

	t.Run("CanceledContext", func(t *testing.T) {
		// A call that gave up says it failed, and never that all is well.
		r := newRecords(t)
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		require.Error(t, r.CreateManifest(ctx, manifest("x", nil)))
		_, err := r.GetManifest(ctx, "x")
		require.Error(t, err)
		require.NotErrorIs(t, err, chunkstore.ErrManifestNotFound, "a read that did not happen says nothing about the manifest")
		_, err = r.ListNames(ctx, "", "")
		require.Error(t, err)
		_, _, err = r.ListChunks(ctx)
		require.Error(t, err)
	})
}
