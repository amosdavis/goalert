package firestorestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestConformance runs every suite through the real client library against a
// fake server.
func TestConformance(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	f := newFakeFirestore(t)
	var n atomic.Int64
	runSuites(t, func(t *testing.T) chunkstore.RecordStore {
		return f.store(t, fmt.Sprintf("s%d", n.Add(1)))
	})
}

// TestConformanceLive runs every suite against Firestore: the emulator in CI
// (FIRESTORE_EMULATOR_HOST), or a real database.
func TestConformanceLive(t *testing.T) {
	project := os.Getenv("GOALERT_TEST_FIRESTORE_PROJECT")
	if project == "" {
		t.Skip("set GOALERT_TEST_FIRESTORE_PROJECT")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{Project: project, Database: os.Getenv("GOALERT_TEST_FIRESTORE_DATABASE"), Prefix: "settings"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		problems, err := s.CheckSettings(ctx)
		require.NoError(t, err)
		for _, p := range problems {
			assert.False(t, p.Breaks, "%s", p)
		}
	}
	runSuites(t, func(t *testing.T) chunkstore.RecordStore {
		r, err := New(s.client, Config{Project: project, Prefix: uuid.NewString()})
		require.NoError(t, err)
		return r
	})
}

func TestNewChecksConfig(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	c := newFakeFirestore(t).store(t, "").client
	for _, cfg := range []Config{
		{}, {Project: "p", Prefix: "a/b"}, {Project: "p", Prefix: ".."}, {Project: "p", Prefix: "__x__"},
		{Project: "p", Prefix: strings.Repeat("x", 129)}, {Project: "p", Prefix: "\xff"},
	} {
		_, err := New(c, cfg)
		assert.Error(t, err, "%+v", cfg)
	}
	_, err := New(nil, Config{Project: "p"})
	assert.Error(t, err)
}

// TestErrors checks how each kind of failure is reported, since calling an
// unknown outcome a failure, or a failed read a missing object, loses data
// (F14, F32).
func TestErrors(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	m := chunkstore.Manifest{Name: "o", SHA256: strings.Repeat("0", 64)}
	chunk := chunkstore.Chunk{ChunkID: chunkstore.ChunkID{Upload: strings.Repeat("a", 32)}, Owner: "o", Data: []byte("x")}
	type result struct{ denied, unknown bool }
	for code, want := range map[codes.Code]result{
		codes.PermissionDenied:   {denied: true},
		codes.Unauthenticated:    {denied: true},
		codes.InvalidArgument:    {},
		codes.FailedPrecondition: {},
		codes.NotFound:           {}, // the database does not exist
		codes.Aborted:            {},
		codes.OutOfRange:         {},
		codes.Unknown:            {unknown: true},
		codes.Internal:           {unknown: true},
		codes.DeadlineExceeded:   {unknown: true},
		codes.DataLoss:           {unknown: true},
		codes.Canceled:           {unknown: true},
	} {
		t.Run(code.String(), func(t *testing.T) {
			f := newFakeFirestore(t)
			s := f.store(t, "")
			f.mu.Lock()
			f.intercept = func(method string, done bool) error { return status.Error(code, "injected") }
			f.mu.Unlock()
			check := func(what string, fn func(ctx context.Context) error, write bool) {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				err := fn(ctx)
				require.Error(t, err, what)
				assert.Equal(t, want.denied, errors.Is(err, objstore.ErrDenied), "%s: denied: %v", what, err)
				assert.Equal(t, write && want.unknown, errors.Is(err, objstore.ErrUnknown), "%s: unknown: %v", what, err)
				for _, wrong := range []error{chunkstore.ErrManifestNotFound, chunkstore.ErrManifestExists, chunkstore.ErrChunkNotFound, objstore.ErrNotFound, objstore.ErrExists} {
					assert.NotErrorIs(t, err, wrong, what)
				}
			}
			check("create", func(ctx context.Context) error { return s.CreateManifest(ctx, m) }, true)
			check("set", func(ctx context.Context) error { return s.SetManifest(ctx, m) }, true)
			check("delete", func(ctx context.Context) error { return s.DeleteManifest(ctx, "o") }, true)
			check("put chunk", func(ctx context.Context) error { return s.PutChunk(ctx, chunk) }, true)
			check("delete chunk", func(ctx context.Context) error { return s.DeleteChunk(ctx, chunk.ChunkID) }, true)
			if code == codes.Internal || code == codes.DeadlineExceeded || code == codes.Unknown {
				return // the library retries these reads for longer than a test should take
			}
			check("get", func(ctx context.Context) error { _, err := s.GetManifest(ctx, "o"); return err }, false)
			check("get chunk", func(ctx context.Context) error { _, err := s.GetChunk(ctx, chunk.ChunkID); return err }, false)
			check("list", func(ctx context.Context) error { _, err := s.ListNames(ctx, "", ""); return err }, false)
			check("list chunks", func(ctx context.Context) error { _, _, err := s.ListChunks(ctx); return err }, false)
		})
	}
}

// TestHiddenRetry checks the case a client library's own retries hide: the
// first attempt is applied but its reply is lost, and the next one is refused.
// Reporting that as a failure would lose a commit that happened (F32).
func TestHiddenRetry(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	ctx := context.Background()
	m := chunkstore.Manifest{Name: "o", SHA256: strings.Repeat("0", 64)}
	for _, then := range []codes.Code{codes.PermissionDenied, codes.InvalidArgument, codes.FailedPrecondition} {
		f := newFakeFirestore(t)
		s := f.store(t, "")
		var n atomic.Int64
		f.mu.Lock()
		f.intercept = func(method string, done bool) error {
			switch {
			case done:
				return status.Error(codes.Unavailable, "the reply is lost")
			case n.Add(1) > 1:
				return status.Error(then, "refused")
			}
			return nil
		}
		f.mu.Unlock()
		err := s.CreateManifest(ctx, m)
		require.ErrorIs(t, err, objstore.ErrUnknown, "then %s", then)
		require.NotErrorIs(t, err, objstore.ErrDenied)
		f.mu.Lock()
		f.intercept = nil
		f.mu.Unlock()
		_, err = s.GetManifest(ctx, "o")
		require.NoError(t, err, "it was applied")
	}

	// A create the library repeats finds its own document, which callers must
	// check for.
	f := newFakeFirestore(t)
	s, err := chunkstore.New(f.store(t, ""), chunkstore.Options{})
	require.NoError(t, err)
	l := deltalog.New(s, "a")
	l.SetRetry(3, time.Millisecond)
	require.NoError(t, l.Create(ctx, deltalog.MetaData{ID: "t", SchemaString: "{}"}, time.Now()))
	var lost atomic.Bool
	f.mu.Lock()
	f.intercept = func(method string, done bool) error {
		if method == "Commit" && done && lost.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "the reply is lost")
		}
		return nil
	}
	f.mu.Unlock()
	info, err := l.Commit(ctx, 1, "t", "WRITE", time.Now(), nil)
	require.NoError(t, err)
	got, _, err := l.Read(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, info.GoAlertCommitID, got.GoAlertCommitID)
	assert.True(t, lost.Load())
}

// TestAborted checks that a write that ran into another one is repeated.
func TestAborted(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	ctx := context.Background()
	f := newFakeFirestore(t)
	s := f.store(t, "")
	var n atomic.Int64
	f.mu.Lock()
	f.intercept = func(method string, done bool) error {
		if !done && n.Add(1)%3 != 0 {
			return status.Error(codes.Aborted, "Too much contention on these documents. Please try again.")
		}
		return nil
	}
	f.mu.Unlock()
	m := chunkstore.Manifest{Name: "o", SHA256: strings.Repeat("0", 64)}
	require.NoError(t, s.CreateManifest(ctx, m))
	require.NoError(t, s.SetManifest(ctx, m))
	require.NoError(t, s.DeleteManifest(ctx, "o"))
	assert.EqualValues(t, 9, n.Load())
}

// TestListChunksTimes checks that chunks are aged by the server's clock, and
// that storing one again does not make it look new.
func TestListChunksTimes(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	ctx := context.Background()
	f := newFakeFirestore(t)
	s := f.store(t, "")
	stored := f.now
	const n = 2*chunkPage + 3
	for i := range n {
		id := chunkstore.ChunkID{Upload: strings.ReplaceAll(uuid.NewString(), "-", ""), Index: i}
		require.NoError(t, s.PutChunk(ctx, chunkstore.Chunk{ChunkID: id, Owner: "o", Data: []byte("x")}))
	}
	f.mu.Lock()
	f.now = f.now.Add(3 * time.Hour)
	f.mu.Unlock()
	chunks, asOf, err := s.ListChunks(ctx)
	require.NoError(t, err)
	require.Len(t, chunks, n)
	assert.Equal(t, f.now, asOf)
	for _, c := range chunks {
		assert.Equal(t, stored, c.Created)
	}
}

// TestLimits checks that documents and names Firestore can't hold are refused
// before they are sent (F45).
func TestLimits(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	ctx := context.Background()
	s := newFakeFirestore(t).store(t, "")
	for _, name := range []string{"\xff", strings.Repeat("x", maxName+1)} {
		err := s.CreateManifest(ctx, chunkstore.Manifest{Name: name})
		require.Error(t, err)
		require.NotErrorIs(t, err, objstore.ErrUnknown)
	}
	err := s.CreateManifest(ctx, chunkstore.Manifest{Name: "o", Inline: bytes.Repeat([]byte("x"), 1<<20)})
	require.Error(t, err)
	require.NotErrorIs(t, err, objstore.ErrUnknown, "the server refused it")
}

func TestCheckDatabase(t *testing.T) {
	good := func() (*adminpb.Database, map[string]*adminpb.Field) {
		none := func() *adminpb.Field { return &adminpb.Field{IndexConfig: &adminpb.Field_IndexConfig{}} }
		return &adminpb.Database{
				Type:                          adminpb.Database_FIRESTORE_NATIVE,
				DeleteProtectionState:         adminpb.Database_DELETE_PROTECTION_ENABLED,
				PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_ENABLED,
			}, map[string]*adminpb.Field{
				manifestCollection + "/inline": none(),
				chunkCollection + "/data":      none(),
			}
	}
	for name, tc := range map[string]struct {
		change        func(db *adminpb.Database, fields map[string]*adminpb.Field)
		breaks, warns bool
	}{
		"recommended":   {change: func(*adminpb.Database, map[string]*adminpb.Field) {}},
		"datastore":     {change: func(db *adminpb.Database, _ map[string]*adminpb.Field) { db.Type = adminpb.Database_DATASTORE_MODE }, breaks: true},
		"no protection": {change: func(db *adminpb.Database, _ map[string]*adminpb.Field) { db.DeleteProtectionState = 0 }, warns: true},
		"no recovery":   {change: func(db *adminpb.Database, _ map[string]*adminpb.Field) { db.PointInTimeRecoveryEnablement = 0 }, warns: true},
		"time to live": {change: func(_ *adminpb.Database, f map[string]*adminpb.Field) {
			f[chunkCollection+"/created"] = &adminpb.Field{TtlConfig: &adminpb.Field_TtlConfig{}}
		}, breaks: true},
		"data indexed": {change: func(_ *adminpb.Database, f map[string]*adminpb.Field) { delete(f, chunkCollection+"/data") }, warns: true},
		"nothing indexed": {change: func(_ *adminpb.Database, f map[string]*adminpb.Field) {
			f["__default__/*"] = &adminpb.Field{IndexConfig: &adminpb.Field_IndexConfig{}}
		}, breaks: true},
		"only names indexed": {change: func(_ *adminpb.Database, f map[string]*adminpb.Field) {
			f["__default__/*"] = &adminpb.Field{IndexConfig: &adminpb.Field_IndexConfig{}}
			f[manifestCollection+"/name"] = &adminpb.Field{IndexConfig: &adminpb.Field_IndexConfig{Indexes: []*adminpb.Index{{}}}}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			db, fields := good()
			tc.change(db, fields)
			var breaks, warns bool
			for _, p := range checkDatabase(db, fields) {
				breaks = breaks || p.Breaks
				warns = warns || !p.Breaks
			}
			assert.Equal(t, tc.breaks, breaks, "breaks")
			assert.Equal(t, tc.warns, warns, "warns")
		})
	}
}
