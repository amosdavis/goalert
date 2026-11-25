package bigquerylog

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/managedlog"
	"github.com/target/goalert/managedlog/managedlogtest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	"github.com/target/goalert/tablelog"
	bigquery "google.golang.org/api/bigquery/v2"
)

// The suites' commits are made at fixed times, shortly before this.
var now = time.Date(2026, 11, 25, 13, 0, 0, 0, time.UTC)

func init() { selfTestDelay = time.Millisecond }

// TestConformance runs the suite for what decides commits, with the real client
// and a fake server.
func TestConformance(t *testing.T) {
	managedlogtest.Run(t, func(t *testing.T) managedlogtest.Env {
		f := newFake(t)
		return managedlogtest.Env{Store: objstore.NewMem(), Connect: func(t *testing.T, store objstore.Store) managedlog.Arbiter {
			a, err := newArbiter(Config{Service: f.service(t), Project: "proj", Location: "US", Store: store})
			require.NoError(t, err)
			a.now = func() time.Time { return now }
			return a
		}}
	})
}

// TestLive runs a backend on BigQuery itself. It makes jobs in a project, and
// keeps the files in memory.
func TestLive(t *testing.T) {
	project, location := os.Getenv("GOALERT_TEST_BIGQUERY_PROJECT"), os.Getenv("GOALERT_TEST_BIGQUERY_LOCATION")
	if project == "" || location == "" {
		t.Skip("set GOALERT_TEST_BIGQUERY_PROJECT and GOALERT_TEST_BIGQUERY_LOCATION")
	}
	svc, err := bigquery.NewService(t.Context())
	require.NoError(t, err)
	backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
		store := objstore.NewMem()
		return func(writer string) *storagebackend.Backend {
			l, err := New(Config{Service: svc, Project: project, Location: location, Store: store}, writer)
			require.NoError(t, err)
			return &storagebackend.Backend{Store: store, Log: l}
		}
	})
}

type env struct {
	t     *testing.T
	f     *fakeBigQuery
	store *objstore.Mem
	now   time.Time

	mu    sync.Mutex
	warns []error
}

func newEnv(t *testing.T) *env {
	return &env{t: t, f: newFake(t), store: objstore.NewMem(), now: time.Now()}
}

func (e *env) config() Config {
	return Config{Service: e.f.service(e.t), Project: "proj", Location: "US", Store: e.store, Warn: func(err error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.warns = append(e.warns, err)
	}}
}

func (e *env) open(cfg Config) *Log {
	e.t.Helper()
	l, err := New(cfg, "w")
	require.NoError(e.t, err)
	l.SetRetry(4, time.Millisecond)
	l.a.now = func() time.Time { return e.now }
	return l
}

func (e *env) log() *Log { return e.open(e.config()) }

func meta() tablelog.MetaData { return tablelog.MetaData{ID: uuid.NewString(), SchemaString: "{}"} }

func (e *env) create() (*Log, string) {
	e.t.Helper()
	l, m := e.log(), meta()
	require.NoError(e.t, l.Create(context.Background(), m, e.now))
	return l, m.ID
}

func (e *env) commit(l *Log, id string, version int64) (tablelog.CommitInfo, error) {
	return l.Commit(context.Background(), version, id, "WRITE", e.now, nil)
}

func TestNewChecksConfig(t *testing.T) {
	e := newEnv(t)
	for name, change := range map[string]func(c *Config){
		"service":  func(c *Config) { c.Service = nil },
		"store":    func(c *Config) { c.Store = nil },
		"project":  func(c *Config) { c.Project = "a/b" },
		"location": func(c *Config) { c.Location = "" },
		"dataset":  func(c *Config) { c.Register = &Registration{Table: "t", Connection: "c", URI: "gs://b/p"} },
		"table": func(c *Config) {
			c.Register = &Registration{Dataset: "d", Table: "a-b", Connection: "c", URI: "gs://b/p"}
		},
		"connection": func(c *Config) { c.Register = &Registration{Dataset: "d", Table: "t", URI: "gs://b/p"} },
		"uri": func(c *Config) {
			c.Register = &Registration{Dataset: "d", Table: "t", Connection: "c", URI: "file:///p"}
		},
	} {
		c := e.config()
		change(&c)
		_, err := New(c, "w")
		assert.Error(t, err, name)
	}
}

func TestNames(t *testing.T) {
	id := uuid.NewString()
	assert.Equal(t, "goalert-"+id+"-00000000000000000042", jobID(id, 42))
	// Anything else is hashed, and stays different from anything else.
	seen := map[string]bool{}
	for _, v := range []string{"Upper", "upper", "sp ace", "é", strings.Repeat("x", 64), strings.Repeat("x", 65), ""} {
		l := label(v)
		assert.Regexp(t, valuePattern, l)
		assert.Regexp(t, idPattern, jobID(v, 1<<62))
		assert.False(t, seen[l], v)
		seen[l] = true
	}
}

// TestErrors checks what a commit reports for each answer of BigQuery, and
// whether it happened (F14, F32, F50).
func TestErrors(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		code   int
		reason string
		want   error
	}{
		"unauthorized":   {401, "authError", objstore.ErrDenied},
		"forbidden":      {403, "accessDenied", objstore.ErrDenied},
		"rate":           {403, "rateLimitExceeded", managedlog.ErrBusy},
		"quota":          {403, "quotaExceeded", managedlog.ErrBusy},
		"too many":       {429, "", managedlog.ErrBusy},
		"bad request":    {400, "invalid", managedlog.ErrRejected},
		"not found":      {404, "notFound", managedlog.ErrRejected},
		"timeout":        {408, "", objstore.ErrUnknown},
		"server error":   {500, "internalError", objstore.ErrUnknown},
		"backend error":  {503, "backendError", objstore.ErrUnknown},
		"bad gateway":    {502, "", objstore.ErrUnknown},
		"dropped":        {0, "", objstore.ErrUnknown},
		"always claimed": {409, "duplicate", tablelog.ErrConflict},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			l, id := e.create()
			e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
				if opOf(r) != "insert" {
					return false
				}
				if tc.code == 0 {
					panic(http.ErrAbortHandler)
				}
				fail(w, tc.code, tc.reason, "injected")
				return true
			})
			_, err := e.commit(l, id, 1)
			require.ErrorIs(t, err, tc.want)
			for _, other := range []error{objstore.ErrUnknown, objstore.ErrDenied, tablelog.ErrConflict, managedlog.ErrRejected, managedlog.ErrBusy} {
				if other != tc.want {
					assert.NotErrorIs(t, err, other)
				}
			}
			e.f.onRequest(nil)
			_, _, err = l.Read(ctx, 1)
			require.ErrorIs(t, err, objstore.ErrNotFound)
			_, err = e.commit(l, id, 1)
			require.NoError(t, err)
		})
	}
	t.Run("question", func(t *testing.T) {
		// A question that fails says nothing about the version.
		e := newEnv(t)
		l, id := e.create()
		for code, want := range map[int]error{401: objstore.ErrDenied, 403: objstore.ErrDenied, 500: nil, 429: nil} {
			e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
				fail(w, code, "", "injected")
				return true
			})
			_, _, err := l.Read(ctx, 1)
			require.Error(t, err)
			require.NotErrorIs(t, err, objstore.ErrNotFound)
			if want != nil {
				require.ErrorIs(t, err, want)
			} else {
				require.NotErrorIs(t, err, objstore.ErrDenied)
			}
		}
		e.f.onRequest(nil)
		_, err := e.commit(l, id, 1)
		require.NoError(t, err)
	})
}

// TestLostReply checks that a commit whose reply was lost is found to have
// happened, and not made twice (F32).
func TestLostReply(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	l, id := e.create()
	var lost atomic.Int64
	e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "insert" && lost.Add(1) == 1 {
			e.f.thenDrop(r, body)
		}
		return false
	})
	info, err := e.commit(l, id, 1)
	require.NoError(t, err)
	got, _, err := l.Read(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, info, got)
	assert.EqualValues(t, 1, lost.Load(), "it was not asked for again")
	assert.Equal(t, 1, e.f.decisions())
}

// TestLag checks that nothing goes wrong if a new job can't be found at once:
// what decides is that its ID can't be used again.
func TestLag(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	a, id := e.create()
	b := e.log()
	_, err := b.Load(ctx)
	require.NoError(t, err)

	hidden := func(n int64) {
		var gets atomic.Int64
		e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
			if opOf(r) == "get" && gets.Add(1) <= n {
				fail(w, http.StatusNotFound, "notFound", "Not found: Job")
				return true
			}
			return false
		})
	}
	// Two pods want the same version. The second is refused, and can't find out
	// by whom at first.
	_, err = e.commit(a, id, 1)
	require.NoError(t, err)
	require.NoError(t, e.store.Delete(ctx, deltalog.CommitName(1)), "as if it were not published yet")
	hidden(2)
	_, err = e.commit(b, id, 1)
	require.ErrorIs(t, err, tablelog.ErrConflict)
	e.f.onRequest(nil)
	_, _, err = b.Read(ctx, 1)
	require.NoError(t, err)

	// A pod can't find the decision on what it commits on top of, so it has to
	// try again, and nothing happens meanwhile.
	c := e.log()
	_, _, err = c.Read(ctx, 1)
	require.NoError(t, err)
	hidden(1 << 30)
	_, err = e.commit(c, id, 2)
	require.ErrorIs(t, err, tablelog.ErrConflict)
	e.f.onRequest(nil)
	assert.Equal(t, 1, e.f.decisions())
	_, err = e.commit(c, id, 2)
	require.NoError(t, err)

	// A lost reply, and no answer to the question after it, until later.
	var inserts, gets atomic.Int64
	e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		switch {
		case opOf(r) == "insert" && inserts.Add(1) == 1:
			e.f.thenDrop(r, body)
		case opOf(r) == "get" && gets.Add(1) <= 2:
			fail(w, http.StatusNotFound, "notFound", "Not found: Job")
			return true
		}
		return false
	})
	info, err := e.commit(c, id, 3)
	require.NoError(t, err)
	e.f.onRequest(nil)
	got, _, err := e.log().Read(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, info, got)
	assert.Equal(t, 3, e.f.decisions())
}

// TestPlaces checks that pods that would ask elsewhere are refused (F43).
func TestPlaces(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	_, id := e.create()
	for _, change := range []func(c *Config){
		func(c *Config) { c.Location = "EU" },
		func(c *Config) { c.Project = "other" },
	} {
		c := e.config()
		change(&c)
		// Their own jobs work, so it is the table that refuses them.
		l := e.open(c)
		require.NoError(t, l.a.SelfTest(ctx))
		_, err := l.Load(ctx)
		require.ErrorIs(t, err, tablelog.ErrManaged)
		_, err = e.commit(l, id, 1)
		require.ErrorIs(t, err, tablelog.ErrManaged)
	}
	assert.Zero(t, e.f.decisions())
	c := e.config()
	c.Location = "us"
	s, err := e.open(c).Load(ctx)
	require.NoError(t, err, "locations are the same in any case")
	assert.Equal(t, "bigquery:proj/us", s.MetaData.Configuration[deltalog.ManagerKey])
}

// TestForgotten checks that a table nobody used for so long that BigQuery has
// forgotten its jobs can be opened, and goes on (F50).
func TestForgotten(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	l, id := e.create()
	for v := int64(1); v <= 3; v++ {
		_, err := e.commit(l, id, v)
		require.NoError(t, err)
	}
	clear(e.f.jobs)
	e.now = e.now.Add(2 * time.Minute)
	_, err := e.log().Load(ctx)
	require.ErrorIs(t, err, tablelog.ErrForeign, "not so soon: something else wrote to the log, or a job was deleted")
	_, err = e.commit(e.log(), id, 4)
	require.ErrorIs(t, err, tablelog.ErrForeign)

	e.now = e.now.Add(200 * 24 * time.Hour)
	fresh := e.log()
	s, err := fresh.Load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, s.Version)
	_, err = e.commit(fresh, id, 4)
	require.NoError(t, err)
	s, err = e.log().Load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 4, s.Version)
}

// TestForeignJob checks that a job that has an ID of the table's, and is not
// GoAlert's, stops the pod (F11, F50).
func TestForeignJob(t *testing.T) {
	ctx := context.Background()
	for name, change := range map[string]func(labels map[string]string){
		"no labels":    func(l map[string]string) { clear(l) },
		"other table":  func(l map[string]string) { l[labelStore] = "other" },
		"other number": func(l map[string]string) { l[labelVersion] = "7" },
		"no commit":    func(l map[string]string) { delete(l, labelCommit) },
		"no format":    func(l map[string]string) { delete(l, labelFormat) },
		"new format":   func(l map[string]string) { l[labelFormat] = "99" },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			l, id := e.create()
			labels := map[string]string{labelStore: id, labelVersion: "1", labelCommit: "x", labelFormat: "1"}
			change(labels)
			require.NoError(t, l.a.insert(ctx, jobID(id, 1), labels))
			want := tablelog.ErrForeign
			switch name {
			case "no format":
				want = tablelog.ErrCorrupt
			case "new format":
				want = tablelog.ErrUnsupported
			}
			_, err := e.commit(l, id, 1)
			require.ErrorIs(t, err, want)
			_, err = e.log().Load(ctx)
			require.ErrorIs(t, err, want)
		})
	}
}

// TestSelfTest checks that a pod does not start if BigQuery does not do what
// decides commits (F33).
func TestSelfTest(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	e.create()
	require.NoError(t, e.log().a.SelfTest(ctx))

	load := func(intercept func(w http.ResponseWriter, r *http.Request, body []byte) bool) error {
		e.f.onRequest(intercept)
		defer e.f.onRequest(nil)
		_, err := e.log().Load(ctx)
		return err
	}
	err := load(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "insert" {
			_, _ = w.Write([]byte("{}")) // whatever the ID
			return true
		}
		return false
	})
	require.ErrorContains(t, err, "a second job with the same ID was accepted")
	err = load(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "insert" {
			fail(w, 403, "accessDenied", "no bigquery.jobs.create")
			return true
		}
		return false
	})
	require.ErrorIs(t, err, objstore.ErrDenied)
	err = load(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "get" {
			fail(w, 404, "notFound", "Not found: Job")
			return true
		}
		return false
	})
	require.ErrorContains(t, err, "is missing")
	// A hitch is not a failure.
	var n atomic.Int64
	require.NoError(t, load(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "insert" && n.Add(1) == 1 {
			fail(w, 503, "backendError", "try again")
			return true
		}
		return false
	}))
}

// TestCommitRequests checks what a commit costs: one job, and a question only
// after another pod's commit (F15, F37).
func TestCommitRequests(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	l, id := e.create()
	s, err := l.Load(ctx)
	require.NoError(t, err)
	count := func() string {
		e.f.mu.Lock()
		defer e.f.mu.Unlock()
		out := fmt.Sprintf("%d jobs, %d questions", e.f.requests["insert"], e.f.requests["get"])
		clear(e.f.requests)
		return out
	}
	count()
	for v := int64(1); v <= 5; v++ {
		_, err := e.commit(l, id, v)
		require.NoError(t, err)
	}
	assert.Equal(t, "5 jobs, 0 questions", count())
	_ = s

	b := e.log()
	_, err = b.Load(ctx)
	require.NoError(t, err)
	count()
	_, err = e.commit(b, id, 6)
	require.NoError(t, err)
	assert.Equal(t, "1 jobs, 0 questions", count(), "loading confirmed the version below")
	_, _, err = l.Read(ctx, 6)
	require.NoError(t, err)
	_, _, err = l.Read(ctx, 7)
	require.ErrorIs(t, err, objstore.ErrNotFound)
	assert.Equal(t, "0 jobs, 1 questions", count(), "only whether there is a newer one")
	_, err = e.commit(l, id, 7)
	require.NoError(t, err)
	assert.Equal(t, "1 jobs, 1 questions", count(), "whether the other pod's was decided")
}

func (e *env) registered(table string) Config {
	c := e.config()
	c.Register = &Registration{Dataset: "ds", Table: table, Connection: "proj.us.goalert", URI: "gs://bucket/goalert/"}
	return c
}

// TestRegister checks that creating a table registers it in BigQuery, and that
// a failure to do so is reported and fails nothing else.
func TestRegister(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	l := e.open(e.registered("goalert"))
	problems, err := l.CheckSettings(ctx)
	require.NoError(t, err)
	require.Len(t, problems, 1)
	assert.False(t, problems[0].Breaks)
	require.NoError(t, l.Create(ctx, meta(), e.now))
	assert.Empty(t, e.warns)
	ext := e.f.tables["goalert"].ExternalDataConfiguration
	assert.Equal(t, []string{"gs://bucket/goalert"}, ext.SourceUris)
	assert.Equal(t, "proj.us.goalert", ext.ConnectionId)
	problems, err = l.CheckSettings(ctx)
	require.NoError(t, err)
	assert.Empty(t, problems)
	require.NoError(t, l.Register(ctx), "again does nothing")

	// The table can be opened whatever becomes of its registration.
	for name, change := range map[string]func(b *bigquery.Table){
		"other place": func(b *bigquery.Table) { b.ExternalDataConfiguration.SourceUris = []string{"gs://bucket/other"} },
		"expires": func(b *bigquery.Table) {
			b.ExternalDataConfiguration.SourceUris = []string{"gs://bucket/goalert"}
			b.ExpirationTime = 1
		},
		"other kind": func(b *bigquery.Table) { b.ExternalDataConfiguration.SourceFormat = "CSV" },
		"native":     func(b *bigquery.Table) { b.ExternalDataConfiguration = nil },
	} {
		change(e.f.tables["goalert"])
		problems, err = l.CheckSettings(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, problems, name)
		for _, p := range problems {
			assert.False(t, p.Breaks, "%s: %s", name, p)
		}
		require.Error(t, l.Register(ctx), name)
		if name == "native" || name == "other kind" {
			e.f.tables["goalert"] = &bigquery.Table{ExternalDataConfiguration: &bigquery.ExternalDataConfiguration{SourceFormat: "DELTA_LAKE", SourceUris: []string{"gs://bucket/goalert"}}}
		}
	}
	_, err = l.Load(ctx)
	require.NoError(t, err)
}

func TestRegisterFails(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	e.f.onRequest(func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		if opOf(r) == "createTable" {
			fail(w, 403, "accessDenied", "no bigquery.tables.create")
			return true
		}
		return false
	})
	l := e.open(e.registered("goalert"))
	require.NoError(t, l.Create(ctx, meta(), e.now), "the table is created")
	require.ErrorIs(t, errors.Join(e.warns...), objstore.ErrDenied)
	e.f.onRequest(nil)
	require.NoError(t, l.Register(ctx))
	_, err := e.log().Load(ctx)
	require.NoError(t, err)
	require.NoError(t, e.log().Register(ctx), "without a registration there is nothing to do")
}
