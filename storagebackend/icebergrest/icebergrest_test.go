package icebergrest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/iceberglog/icebergtest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
	"github.com/target/goalert/tablelog/tablelogtest"
)

// restServer is an Iceberg REST catalog, as far as GoAlert uses one, on a
// catalog in memory.
type restServer struct {
	*httptest.Server
	cat *icebergtest.Catalog
	// intercept, if set, may answer a request itself and return true.
	intercept func(w http.ResponseWriter, r *http.Request) bool
	// drop, if set, answers a commit that was done by dropping the connection.
	drop  atomic.Bool
	auth  atomic.Value
	calls atomic.Int64
}

func newServer(t *testing.T) *restServer {
	t.Helper()
	s := &restServer{cat: &icebergtest.Catalog{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

func (s *restServer) fail(w http.ResponseWriter, status int, typ string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "test", "type": typ, "code": status}})
}

func (s *restServer) serve(w http.ResponseWriter, r *http.Request) {
	s.calls.Add(1)
	s.auth.Store(r.Header.Get("Authorization"))
	if s.intercept != nil && s.intercept(w, r) {
		return
	}
	reply := func(meta table.Metadata) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata-location": "gs://bucket/goalert/metadata/1.metadata.json", "metadata": meta, "config": map[string]string{}})
	}
	ident := table.Identifier{"ops", "goalert"}
	switch path := strings.TrimPrefix(r.URL.Path, "/iceberg/v1/"); {
	case path == "config":
		_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
	case path == "namespaces/ops/tables" && r.Method == http.MethodPost:
		var req struct {
			Name       string             `json:"name"`
			Schema     *iceberg.Schema    `json:"schema"`
			Location   string             `json:"location"`
			Properties iceberg.Properties `json:"properties"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name != "goalert" {
			s.fail(w, http.StatusBadRequest, "BadRequestException")
			return
		}
		_, err := s.cat.CreateTable(r.Context(), ident, req.Schema, catalog.WithLocation(req.Location),
			catalog.WithPartitionSpec(iceberg.UnpartitionedSpec), catalog.WithProperties(req.Properties))
		switch {
		case errors.Is(err, catalog.ErrTableAlreadyExists):
			s.fail(w, http.StatusConflict, "AlreadyExistsException")
		case err != nil:
			s.fail(w, http.StatusBadRequest, "BadRequestException")
		default:
			reply(s.cat.Metadata())
		}
	case path == "namespaces/ops/tables/goalert" && r.Method == http.MethodGet:
		meta := s.cat.Metadata()
		if meta == nil {
			s.fail(w, http.StatusNotFound, "NoSuchTableException")
			return
		}
		reply(meta)
	case path == "namespaces/ops/tables/goalert" && r.Method == http.MethodPost:
		var req struct {
			Requirements table.Requirements `json:"requirements"`
			Updates      table.Updates      `json:"updates"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.fail(w, http.StatusBadRequest, "BadRequestException")
			return
		}
		meta, _, err := s.cat.CommitTable(r.Context(), ident, req.Requirements, req.Updates)
		switch {
		case errors.Is(err, table.ErrCommitFailed):
			s.fail(w, http.StatusConflict, "CommitFailedException")
		case errors.Is(err, catalog.ErrNoSuchTable):
			s.fail(w, http.StatusNotFound, "NoSuchTableException")
		case err != nil:
			s.fail(w, http.StatusBadRequest, "BadRequestException")
		case s.drop.CompareAndSwap(true, false):
			panic(http.ErrAbortHandler)
		default:
			reply(meta)
		}
	default:
		s.fail(w, http.StatusNotFound, "NoSuchNamespaceException")
	}
}

func (s *restServer) open(t *testing.T, store objstore.Store, writer, params string) (tablelog.Log, error) {
	t.Helper()
	u, err := url.Parse("iceberg+" + s.URL + "/iceberg?" + params)
	require.NoError(t, err)
	log, closeLog, err := open(context.Background(), storagebackend.LogConfig{
		URL: u, Objects: storagebackend.Objects{Store: store, Location: "gs://bucket/goalert/"}, WriterID: writer, Warn: func(error) {},
	})
	if closeLog != nil {
		t.Cleanup(func() { assert.NoError(t, closeLog()) })
	}
	if err == nil {
		log.SetRetry(4, time.Millisecond)
	}
	return log, err
}

// TestConformance runs the log's suite through the real REST client.
func TestConformance(t *testing.T) {
	servers := map[objstore.Store]*restServer{}
	tablelogtest.Run(t,
		func(t *testing.T) objstore.Store {
			store := objstore.NewMem()
			servers[store] = newServer(t)
			return store
		},
		func(t *testing.T, store objstore.Store, writerID string) tablelog.Log {
			l, err := servers[store].open(t, store, writerID, "namespace=ops")
			require.NoError(t, err)
			return l
		},
	)
}

func meta() tablelog.MetaData {
	return tablelog.MetaData{ID: "store-id", SchemaString: `{"type":"struct","fields":[{"name":"users","type":"string","nullable":true,"metadata":{}}]}`}
}

// TestErrors checks what a commit reports for each answer of the catalog, and
// whether it happened (F14, F32).
func TestErrors(t *testing.T) {
	ctx := context.Background()
	commit := func(l tablelog.Log) (tablelog.CommitInfo, error) {
		return l.Commit(ctx, 1, "store-id", "WRITE", time.Now(), nil)
	}
	for status, want := range map[int]error{
		http.StatusUnauthorized:        objstore.ErrDenied,
		http.StatusForbidden:           objstore.ErrDenied,
		http.StatusConflict:            tablelog.ErrConflict,
		http.StatusInternalServerError: objstore.ErrUnknown,
		http.StatusBadGateway:          objstore.ErrUnknown,
		http.StatusServiceUnavailable:  objstore.ErrUnknown,
		http.StatusGatewayTimeout:      objstore.ErrUnknown,
		http.StatusTooManyRequests:     objstore.ErrUnknown,
	} {
		s := newServer(t)
		l, err := s.open(t, objstore.NewMem(), "a", "namespace=ops")
		require.NoError(t, err)
		require.NoError(t, l.Create(ctx, meta(), time.Now()))
		s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodPost {
				return false
			}
			s.fail(w, status, "TestException")
			return true
		}
		_, err = commit(l)
		require.ErrorIs(t, err, want, "%d", status)
		s.intercept = nil
		_, _, err = l.Read(ctx, 1)
		require.ErrorIs(t, err, objstore.ErrNotFound)
		_, err = commit(l)
		require.NoError(t, err)
	}

	// A commit whose reply is lost has happened.
	s := newServer(t)
	l, err := s.open(t, objstore.NewMem(), "a", "namespace=ops")
	require.NoError(t, err)
	require.NoError(t, l.Create(ctx, meta(), time.Now()))
	s.drop.Store(true)
	info, err := commit(l)
	require.NoError(t, err)
	assert.False(t, s.drop.Load())
	got, _, err := l.Read(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, info, got)

	// A read that fails says nothing about the version.
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		s.fail(w, http.StatusForbidden, "ForbiddenException")
		return true
	}
	_, _, err = l.Read(ctx, 2)
	require.ErrorIs(t, err, objstore.ErrDenied)
	require.NotErrorIs(t, err, objstore.ErrNotFound)
}

func TestConfig(t *testing.T) {
	store := objstore.NewMem()
	s := newServer(t)
	for _, bad := range []string{
		"", "namespace=", "namespace=ops&typo=1", "namespace=ops&snapshots=0", "namespace=ops&snapshots=x",
		"namespace=ops&auth=magic", "namespace=ops&auth=token", "namespace=ops&auth=credential", "namespace=ops&auth=sigv4",
	} {
		_, err := s.open(t, store, "a", bad)
		assert.Error(t, err, bad)
	}
	assert.Zero(t, s.calls.Load(), "nothing is contacted")

	t.Setenv(TokenEnv, "sesame")
	_, err := s.open(t, store, "a", "namespace=ops&auth=token&warehouse=w&table=goalert")
	require.NoError(t, err)
	assert.Equal(t, "Bearer sesame", s.auth.Load())

	u, err := url.Parse("iceberg+" + s.URL + "/iceberg?namespace=ops")
	require.NoError(t, err)
	_, _, err = open(context.Background(), storagebackend.LogConfig{URL: u, Objects: storagebackend.Objects{Store: store}})
	require.ErrorContains(t, err, "can't reach", "a document database has no files for Iceberg readers")
}
