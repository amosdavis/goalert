package bigquerylog

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

// fakeBigQuery is an HTTP server that answers the requests this package makes
// with the real client, the way BigQuery documents it: one job for an ID in a
// project and location, whatever becomes of the job, and one table of a name.
type fakeBigQuery struct {
	*httptest.Server

	mu       sync.Mutex
	jobs     map[string]*bigquery.Job
	tables   map[string]*bigquery.Table
	requests map[string]int
	errs     []string // requests the real service would refuse

	// intercept, if set, may answer a request itself and return true.
	intercept func(w http.ResponseWriter, r *http.Request, body []byte) bool
}

func newFake(t *testing.T) *fakeBigQuery {
	t.Helper()
	f := &fakeBigQuery{jobs: map[string]*bigquery.Job{}, tables: map[string]*bigquery.Table{}, requests: map[string]int{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, e := range f.errs {
			t.Error("fake BigQuery: " + e)
		}
	})
	return f
}

func (f *fakeBigQuery) service(t *testing.T) *bigquery.Service {
	t.Helper()
	s, err := bigquery.NewService(t.Context(), option.WithEndpoint(f.URL+"/bigquery/v2/"), option.WithHTTPClient(f.Client()), option.WithoutAuthentication())
	require.NoError(t, err)
	return s
}

// decisions counts the jobs that decided something.
func (f *fakeBigQuery) decisions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for k := range f.jobs {
		if !strings.Contains(k, "selftest") {
			n++
		}
	}
	return n
}

func (f *fakeBigQuery) onRequest(fn func(w http.ResponseWriter, r *http.Request, body []byte) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intercept = fn
}

func fail(w http.ResponseWriter, code int, reason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "errors": []map[string]string{{"message": message, "domain": "global", "reason": reason}},
	}})
}

// pathOf splits a request's path into the project and what follows it.
func pathOf(r *http.Request) (project, path string) {
	project, path, _ = strings.Cut(strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/"), "/")
	return project, path
}

// opOf names what a request asks for.
func opOf(r *http.Request) string {
	_, path := pathOf(r)
	switch {
	case path == "jobs" && r.Method == http.MethodPost:
		return "insert"
	case strings.HasPrefix(path, "jobs/") && r.Method == http.MethodGet:
		return "get"
	case path == "datasets/ds/tables" && r.Method == http.MethodPost:
		return "createTable"
	case strings.HasPrefix(path, "datasets/ds/tables/") && r.Method == http.MethodGet:
		return "getTable"
	}
	return ""
}

func (f *fakeBigQuery) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests[opOf(r)]++
	intercept := f.intercept
	f.mu.Unlock()
	if intercept != nil && intercept(w, r, body) {
		return
	}
	f.handle(w, r, body)
}

// thenDrop does what a request asks and drops the connection instead of
// replying.
func (f *fakeBigQuery) thenDrop(r *http.Request, body []byte) {
	f.handle(httptest.NewRecorder(), r, body)
	panic(http.ErrAbortHandler)
}

var (
	idPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,1000}$`)
	keyPattern   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	valuePattern = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
)

func jobKey(project, location, id string) string {
	return project + "/" + strings.ToLower(location) + "/" + id
}

func (f *fakeBigQuery) handle(w http.ResponseWriter, r *http.Request, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	refuse := func(format string, args ...any) {
		f.errs = append(f.errs, fmt.Sprintf(format, args...))
		fail(w, http.StatusBadRequest, "invalid", "refused by the fake")
	}
	project, path := pathOf(r)
	last := path[strings.LastIndex(path, "/")+1:]
	switch opOf(r) {
	case "insert":
		var job bigquery.Job
		if err := json.Unmarshal(body, &job); err != nil || job.JobReference == nil || job.Configuration == nil || job.Configuration.Query == nil {
			refuse("bad job %s", body)
			return
		}
		ref := job.JobReference
		if !idPattern.MatchString(ref.JobId) || ref.ProjectId != project || ref.Location == "" {
			refuse("bad job reference %+v", ref)
			return
		}
		if len(job.Configuration.Labels) > 64 {
			refuse("too many labels")
			return
		}
		for k, v := range job.Configuration.Labels {
			if !keyPattern.MatchString(k) || !valuePattern.MatchString(v) {
				refuse("bad label %q: %q", k, v)
				return
			}
		}
		if q := job.Configuration.Query; q.UseLegacySql == nil || *q.UseLegacySql || q.Query == "" {
			refuse("bad query %+v", q)
			return
		}
		k := jobKey(project, ref.Location, ref.JobId)
		if _, ok := f.jobs[k]; ok {
			fail(w, http.StatusConflict, "duplicate", "Already Exists: Job "+project+":"+ref.Location+"."+ref.JobId)
			return
		}
		job.Status = &bigquery.JobStatus{State: "PENDING"}
		f.jobs[k] = &job
		_ = json.NewEncoder(w).Encode(&job)
	case "get":
		job, ok := f.jobs[jobKey(project, r.URL.Query().Get("location"), last)]
		if !ok {
			fail(w, http.StatusNotFound, "notFound", "Not found: Job "+project+":"+last)
			return
		}
		_ = json.NewEncoder(w).Encode(job)
	case "createTable":
		var table bigquery.Table
		if err := json.Unmarshal(body, &table); err != nil || table.TableReference == nil {
			refuse("bad table %s", body)
			return
		}
		e := table.ExternalDataConfiguration
		if e == nil || e.SourceFormat != "DELTA_LAKE" || e.ConnectionId == "" || !e.Autodetect || len(e.SourceUris) != 1 {
			refuse("bad external table %s", body)
			return
		}
		if _, ok := f.tables[table.TableReference.TableId]; ok {
			fail(w, http.StatusConflict, "duplicate", "Already Exists: Table "+project+":ds."+table.TableReference.TableId)
			return
		}
		f.tables[table.TableReference.TableId] = &table
		_ = json.NewEncoder(w).Encode(&table)
	case "getTable":
		table, ok := f.tables[last]
		if !ok {
			fail(w, http.StatusNotFound, "notFound", "Not found: Table "+project+":ds."+last)
			return
		}
		_ = json.NewEncoder(w).Encode(table)
	default:
		refuse("unexpected request %s %s", r.Method, r.URL.Path)
	}
}
