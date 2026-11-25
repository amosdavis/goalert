package cosmosstore

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/chunkstore"
	"github.com/target/goalert/objstore/chunkstore/chunkstoretest"
	"github.com/target/goalert/objstore/objstoretest"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

func runSuites(t *testing.T, newRecords func(t *testing.T) chunkstore.RecordStore) {
	newStore := func(t *testing.T) objstore.Store {
		s, err := chunkstore.New(newRecords(t), chunkstore.Options{})
		require.NoError(t, err)
		return s
	}
	t.Run("Records", func(t *testing.T) { chunkstoretest.Run(t, newRecords) })
	t.Run("Objects", func(t *testing.T) { objstoretest.Run(t, newStore) })
	t.Run("Log", func(t *testing.T) {
		tablelogtest.Run(t, newStore, func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
			return deltalog.New(store, writerID)
		})
	})
	t.Run("Backend", func(t *testing.T) {
		backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
			store := newStore(t)
			return func(writer string) *storagebackend.Backend {
				return &storagebackend.Backend{Store: store, Log: deltalog.New(store, writer)}
			}
		})
	})
}

// TestConformance runs every suite through the real SDK against a fake server.
func TestConformance(t *testing.T) {
	f := newFakeCosmos(t)
	var n atomic.Int64
	runSuites(t, func(t *testing.T) chunkstore.RecordStore {
		return f.store(t, fmt.Sprintf("s%d", n.Add(1)))
	})
}

// TestConformanceLive runs every suite against Cosmos DB: the emulator in CI,
// or a real account. It creates a container for the run.
func TestConformanceLive(t *testing.T) {
	endpoint, key := os.Getenv("GOALERT_TEST_COSMOS_ENDPOINT"), os.Getenv("GOALERT_TEST_COSMOS_KEY")
	if endpoint == "" || key == "" {
		t.Skip("set GOALERT_TEST_COSMOS_ENDPOINT and GOALERT_TEST_COSMOS_KEY")
	}
	ctx := context.Background()
	cred, err := azcosmos.NewKeyCredential(key)
	require.NoError(t, err)
	opts := &azcosmos.ClientOptions{}
	if os.Getenv("GOALERT_TEST_COSMOS_EMULATOR") != "" {
		// The emulator's certificate is its own.
		opts.ClientOptions = azcore.ClientOptions{Transport: &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}}}
	}
	client, err := azcosmos.NewClientWithKey(endpoint, cred, opts)
	require.NoError(t, err)
	const database = "goalert-test"
	_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: database}, nil)
	if statusCode(err) != http.StatusConflict {
		require.NoError(t, err, "create database")
	}
	db, err := client.NewDatabase(database)
	require.NoError(t, err)
	container := "conformance-" + uuid.NewString()
	_, err = db.CreateContainer(ctx, ContainerProperties(container), nil)
	require.NoError(t, err, "create container")
	t.Cleanup(func() {
		c, err := db.NewContainer(container)
		if assert.NoError(t, err) {
			_, err = c.Delete(context.Background(), nil)
			assert.NoError(t, err)
		}
	})
	newRecords := func(t *testing.T) chunkstore.RecordStore {
		s, err := New(client, Config{Database: database, Container: container, Prefix: uuid.NewString()})
		require.NoError(t, err)
		return s
	}
	problems, err := newRecords(t).(*Store).CheckSettings(ctx)
	require.NoError(t, err)
	for _, p := range problems {
		assert.False(t, p.Breaks, "%s", p)
	}
	runSuites(t, newRecords)
}

func TestNewChecksConfig(t *testing.T) {
	c := newFakeCosmos(t).client(t)
	for _, cfg := range []Config{{}, {Database: "d"}, {Container: "c"}, {Database: "d", Container: "c", Prefix: "\xff"}, {Database: "d", Container: "c", Prefix: strings.Repeat("x", 513)}} {
		_, err := New(c, cfg)
		assert.Error(t, err, "%+v", cfg)
	}
	_, err := New(nil, Config{Database: "d", Container: "c"})
	assert.Error(t, err)
}

// TestErrors checks how each kind of failure is reported, since calling an
// unknown outcome a failure, or a failed read a missing object, loses data
// (F14, F32).
func TestErrors(t *testing.T) {
	ctx := context.Background()
	m := chunkstore.Manifest{Name: "o", SHA256: strings.Repeat("0", 64)}
	chunk := chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: strings.Repeat("a", 32)}, Owner: "o", Data: []byte("x")}
	type result struct{ denied, unknown bool }
	for name, tc := range map[string]struct {
		status  int
		sub     string
		abort   bool
		want    result
		minTime time.Duration
	}{
		"unauthorized":         {status: 401, want: result{denied: true}},
		"forbidden":            {status: 403, want: result{denied: true}},
		"partition full":       {status: 403, sub: "1014", want: result{denied: true}},
		"bad request":          {status: 400},
		"too large":            {status: 413},
		"gone":                 {status: 410},
		"throttled":            {status: 429},
		"retry with":           {status: 449},
		"container is missing": {status: 404, sub: "1003"},
		"timeout":              {status: 408, want: result{unknown: true}},
		"server error":         {status: 500, want: result{unknown: true}},
		"unavailable":          {status: 503, want: result{unknown: true}},
		"dropped connection":   {abort: true, want: result{unknown: true}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFakeCosmos(t)
			s := f.store(t, "")
			f.intercept = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
				op := opOf(r)
				if op == "account" {
					return false
				}
				if tc.abort {
					panic(http.ErrAbortHandler)
				}
				f.fail(w, tc.status, tc.sub, "injected")
				return true
			}
			check := func(what string, err error, write bool) {
				t.Helper()
				require.Error(t, err, what)
				assert.Equal(t, tc.want.denied, errors.Is(err, objstore.ErrDenied), "%s: denied: %v", what, err)
				assert.Equal(t, write && tc.want.unknown, errors.Is(err, objstore.ErrUnknown), "%s: unknown: %v", what, err)
				for _, wrong := range []error{chunkstore.ErrManifestNotFound, chunkstore.ErrManifestExists, chunkstore.ErrChunkNotFound, objstore.ErrNotFound, objstore.ErrExists} {
					assert.NotErrorIs(t, err, wrong, what)
				}
			}
			check("create", s.CreateManifest(ctx, m), true)
			check("set", s.SetManifest(ctx, m), true)
			check("delete", s.DeleteManifest(ctx, "o"), true)
			check("put chunk", s.PutChunk(ctx, chunk), true)
			check("delete chunk", s.DeleteChunk(ctx, chunk.ChunkID), true)
			_, err := s.GetManifest(ctx, "o")
			check("get", err, false)
			_, err = s.GetChunk(ctx, chunk.ChunkID)
			check("get chunk", err, false)
			_, err = s.ListNames(ctx, "", "")
			check("list", err, false)
			_, _, err = s.ListChunks(ctx)
			check("list chunks", err, false)
		})
	}
}

// TestRetryWith checks that a write Cosmos DB asks to repeat is repeated, so
// that concurrent writers don't fail each other.
func TestRetryWith(t *testing.T) {
	ctx := context.Background()
	f := newFakeCosmos(t)
	s := f.store(t, "")
	var n atomic.Int64
	f.intercept = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		op := opOf(r)
		if (op == "create" || op == "upsert" || op == "delete") && n.Add(1)%3 != 0 {
			f.fail(w, statusRetryWith, "", "Conflicting request to resource has been attempted. Retry to avoid conflicts.")
			return true
		}
		return false
	}
	m := chunkstore.Manifest{Name: "o", SHA256: strings.Repeat("0", 64)}
	require.NoError(t, s.CreateManifest(ctx, m))
	require.NoError(t, s.SetManifest(ctx, m))
	require.NoError(t, s.PutChunk(ctx, chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: strings.Repeat("a", 32)}, Owner: "o"}))
	require.NoError(t, s.DeleteManifest(ctx, "o"))
	assert.EqualValues(t, 12, n.Load())
}

// TestLostReply checks that a commit whose reply was lost is found to have
// happened (F32).
func TestLostReply(t *testing.T) {
	ctx := context.Background()
	f := newFakeCosmos(t)
	s, err := chunkstore.New(f.store(t, ""), chunkstore.Options{})
	require.NoError(t, err)
	l := deltalog.New(s, "a")
	l.SetRetry(3, time.Millisecond)
	require.NoError(t, l.Create(ctx, deltalog.MetaData{ID: "t", SchemaString: "{}"}, time.Now()))

	var lost atomic.Int64
	f.mu.Lock()
	f.intercept = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "create" && lost.Add(1) == 1 {
			f.serveThenDrop(r, body)
		}
		return false
	}
	f.mu.Unlock()
	info, err := l.Commit(ctx, 1, "t", "WRITE", time.Now(), nil)
	require.NoError(t, err)
	got, _, err := l.Read(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, info.GoAlertCommitID, got.GoAlertCommitID)
	assert.EqualValues(t, 1, lost.Load(), "it was not sent again")

	f.mu.Lock()
	lost.Store(0)
	f.mu.Unlock()
	require.ErrorIs(t, s.PutIfAbsent(ctx, "other", []byte("x")), objstore.ErrUnknown)
	require.ErrorIs(t, s.PutIfAbsent(ctx, "other", []byte("x")), objstore.ErrExists)
}

// TestListChunksTimes checks that no chunk's age is judged without the
// server's clock (F13).
func TestListChunksTimes(t *testing.T) {
	ctx := context.Background()
	f := newFakeCosmos(t)
	s := f.store(t, "")
	chunk := chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: strings.Repeat("a", 32)}, Owner: "o", Data: []byte("x")}
	require.NoError(t, s.PutChunk(ctx, chunk))
	stored := f.now
	f.mu.Lock()
	f.now = f.now.Add(3 * time.Hour)
	f.mu.Unlock()
	chunks, asOf, err := s.ListChunks(ctx)
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	assert.Equal(t, stored, chunks[0].Created)
	assert.Equal(t, f.now, asOf)

	f.mu.Lock()
	f.intercept = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		w.Header()["Date"] = nil // suppresses the header
		return false
	}
	f.mu.Unlock()
	_, _, err = s.ListChunks(ctx)
	require.Error(t, err)
}

// TestPartitions checks that chunks are spread over logical partitions, which
// can't hold more than 20 GB each (F45), and that every one is listed.
func TestPartitions(t *testing.T) {
	ctx := context.Background()
	f := newFakeCosmos(t)
	s := f.store(t, "p")
	const n = 600
	for i := range n {
		id := chunkstore.ChunkID{Upload: strings.ReplaceAll(uuid.NewString(), "-", ""), Index: i}
		require.NoError(t, s.PutChunk(ctx, chunkstore.Chunk{ChunkID: id, Owner: "o", Data: []byte("x")}))
	}
	partitions := map[string]int{}
	for k := range f.items {
		partitions[k[0]]++
	}
	assert.Greater(t, len(partitions), 150)
	chunks, _, err := s.ListChunks(ctx)
	require.NoError(t, err)
	assert.Len(t, chunks, n)
}

func TestCheckSettings(t *testing.T) {
	ctx := context.Background()
	f := newFakeCosmos(t)
	s := f.store(t, "")
	problems, err := s.CheckSettings(ctx)
	require.NoError(t, err)
	assert.Empty(t, problems)

	f.mu.Lock()
	f.consistency = "Session"
	f.mu.Unlock()
	problems, err = s.CheckSettings(ctx)
	require.NoError(t, err)
	require.Len(t, problems, 1)
	assert.True(t, problems[0].Breaks, "%s", problems[0])
	// And if nobody checks, reads fail rather than return stale data (F44).
	_, err = s.GetManifest(ctx, "o")
	require.Error(t, err)
	require.NotErrorIs(t, err, chunkstore.ErrManifestNotFound)
	_, err = s.ListNames(ctx, "", "")
	require.Error(t, err)

	f.mu.Lock()
	f.intercept = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		op := opOf(r)
		if op == "container" {
			f.fail(w, http.StatusForbidden, "", "no permission")
			return true
		}
		return false
	}
	f.mu.Unlock()
	_, err = s.CheckSettings(ctx)
	require.ErrorIs(t, err, objstore.ErrDenied)
}

func TestCheckContainer(t *testing.T) {
	ttl := func(n int32) *int32 { return &n }
	for name, tc := range map[string]struct {
		change func(p *azcosmos.ContainerProperties)
		breaks bool
		warns  bool
	}{
		"recommended":       {change: func(p *azcosmos.ContainerProperties) {}},
		"other key":         {change: func(p *azcosmos.ContainerProperties) { p.PartitionKeyDefinition.Paths = []string{"/id"} }, breaks: true},
		"hierarchical key":  {change: func(p *azcosmos.ContainerProperties) { p.PartitionKeyDefinition.Paths = []string{"/pk", "/id"} }, breaks: true},
		"time to live":      {change: func(p *azcosmos.ContainerProperties) { p.DefaultTimeToLive = ttl(3600) }, breaks: true},
		"ttl without value": {change: func(p *azcosmos.ContainerProperties) { p.DefaultTimeToLive = ttl(-1) }},
		"unique keys": {change: func(p *azcosmos.ContainerProperties) {
			p.UniqueKeyPolicy = &azcosmos.UniqueKeyPolicy{UniqueKeys: []azcosmos.UniqueKey{{Paths: []string{"/name"}}}}
		}, breaks: true},
		"not indexed":  {change: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.IndexingMode = azcosmos.IndexingModeNone }, breaks: true},
		"lazy index":   {change: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.IndexingMode = "lazy" }, breaks: true},
		"manual index": {change: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy.Automatic = false }, breaks: true},
		"no name index": {change: func(p *azcosmos.ContainerProperties) {
			p.IndexingPolicy.IncludedPaths = []azcosmos.IncludedPath{{Path: "/kind/?"}}
		}, breaks: true},
		"name excluded": {change: func(p *azcosmos.ContainerProperties) {
			p.IndexingPolicy.IncludedPaths = []azcosmos.IncludedPath{{Path: "/*"}}
			p.IndexingPolicy.ExcludedPaths = []azcosmos.ExcludedPath{{Path: "/name/?"}}
		}, breaks: true},
		"default policy": {change: func(p *azcosmos.ContainerProperties) {
			p.IndexingPolicy.IncludedPaths = []azcosmos.IncludedPath{{Path: "/*"}}
			p.IndexingPolicy.ExcludedPaths = []azcosmos.ExcludedPath{{Path: `/"_etag"/?`}}
		}, warns: true},
		"no policy": {change: func(p *azcosmos.ContainerProperties) { p.IndexingPolicy = nil }, warns: true},
	} {
		t.Run(name, func(t *testing.T) {
			p := ContainerProperties("c")
			tc.change(&p)
			var breaks, warns bool
			for _, problem := range checkContainer(p) {
				breaks = breaks || problem.Breaks
				warns = warns || !problem.Breaks
			}
			assert.Equal(t, tc.breaks, breaks, "breaks")
			assert.Equal(t, tc.warns, warns, "warns")
		})
	}
}

// TestItemLimit checks that an object too large for an item is refused before
// it is sent (F45).
func TestItemLimit(t *testing.T) {
	ctx := context.Background()
	s := newFakeCosmos(t).store(t, "")
	err := s.CreateManifest(ctx, chunkstore.Manifest{Name: "o", Inline: bytes.Repeat([]byte("x"), 2<<20)})
	require.Error(t, err)
	require.NotErrorIs(t, err, objstore.ErrUnknown)
	for _, name := range []string{"\xff", "a\xc0b"} {
		require.Error(t, s.CreateManifest(ctx, chunkstore.Manifest{Name: name}))
	}
}
