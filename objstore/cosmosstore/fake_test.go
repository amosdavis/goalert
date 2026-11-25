package cosmosstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/require"
)

// fakeCosmos is an HTTP server that answers the requests the real SDK sends
// for this package, the way Cosmos DB documents it: partitions, ID conflicts,
// ETags, the item size limit, pages, and refusing a consistency level stronger
// than the account's. It understands only this package's queries.
type fakeCosmos struct {
	*httptest.Server

	mu          sync.Mutex
	items       map[[2]string]fakeItem // by partition key and ID
	etag        int
	now         time.Time
	consistency string // the account's
	pageSize    int
	container   map[string]any
	requests    map[string]int
	errs        []string // requests the real service would refuse

	// intercept, if set, may answer a request itself and return true.
	intercept func(w http.ResponseWriter, r *http.Request, body []byte) bool
}

type fakeItem struct {
	body map[string]json.RawMessage
	etag string
	ts   int64
}

var consistencyRank = map[string]int{"Eventual": 1, "ConsistentPrefix": 2, "Session": 3, "BoundedStaleness": 4, "Strong": 5}

func newFakeCosmos(t *testing.T) *fakeCosmos {
	t.Helper()
	f := &fakeCosmos{
		items:       map[[2]string]fakeItem{},
		now:         time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC),
		consistency: "Strong",
		pageSize:    100,
		requests:    map[string]int{},
	}
	props, err := json.Marshal(ContainerProperties("c"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(props, &f.container))
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, e := range f.errs {
			t.Error("fake Cosmos DB: " + e)
		}
	})
	return f
}

// client returns an SDK client for the fake server that does not wait long
// before it retries.
func (f *fakeCosmos) client(t *testing.T) *azcosmos.Client {
	t.Helper()
	key := make([]byte, 64)
	_, err := rand.Read(key)
	require.NoError(t, err)
	cred, err := azcosmos.NewKeyCredential(base64.StdEncoding.EncodeToString(key))
	require.NoError(t, err)
	c, err := azcosmos.NewClientWithKey(f.URL, cred, &azcosmos.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: f.Client(),
		Retry:     policy.RetryOptions{RetryDelay: time.Millisecond, MaxRetryDelay: 2 * time.Millisecond},
	}})
	require.NoError(t, err)
	return c
}

func (f *fakeCosmos) store(t *testing.T, prefix string) *Store {
	t.Helper()
	s, err := New(f.client(t), Config{Database: "d", Container: "c", Prefix: prefix})
	require.NoError(t, err)
	return s
}

func (f *fakeCosmos) fail(w http.ResponseWriter, status int, sub, message string) {
	if sub != "" {
		w.Header().Set("x-ms-substatus", sub)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"code":%q,"message":%q}`, http.StatusText(status), message)
}

func (f *fakeCosmos) reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// serveThenDrop does what a request asks and drops the connection instead of
// replying.
func (f *fakeCosmos) serveThenDrop(r *http.Request, body []byte) {
	f.handle(&discard{header: http.Header{}}, r, body)
	panic(http.ErrAbortHandler)
}

type discard struct{ header http.Header }

func (d *discard) Header() http.Header         { return d.header }
func (d *discard) Write(p []byte) (int, error) { return len(p), nil }
func (d *discard) WriteHeader(int)             {}

func (f *fakeCosmos) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	intercept, now := f.intercept, f.now
	f.requests[opOf(r)]++
	f.mu.Unlock()
	w.Header().Set("Date", now.Format(http.TimeFormat))
	if intercept != nil && intercept(w, r, body) {
		return
	}
	f.handle(w, r, body)
}

// opOf names what a request asks for.
func opOf(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	switch {
	case path == "":
		return "account"
	case path == "dbs/d/colls/c" && r.Method == http.MethodGet:
		return "container"
	case path == "dbs/d/colls/c/docs" && r.Method == http.MethodPost && r.Header.Get("x-ms-documentdb-query") == "True":
		return "query"
	case path == "dbs/d/colls/c/docs" && r.Method == http.MethodPost && r.Header.Get("x-ms-documentdb-is-upsert") == "true":
		return "upsert"
	case path == "dbs/d/colls/c/docs" && r.Method == http.MethodPost:
		return "create"
	case strings.HasPrefix(path, "dbs/d/colls/c/docs/") && r.Method == http.MethodGet:
		return "read"
	case strings.HasPrefix(path, "dbs/d/colls/c/docs/") && r.Method == http.MethodDelete:
		return "delete"
	}
	return ""
}

func (f *fakeCosmos) handle(w http.ResponseWriter, r *http.Request, body []byte) {
	op, path := opOf(r), strings.Trim(r.URL.Path, "/")
	f.mu.Lock()
	defer f.mu.Unlock()
	if level := r.Header.Get("x-ms-consistency-level"); level != "" && consistencyRank[level] > consistencyRank[f.consistency] {
		f.fail(w, http.StatusBadRequest, "", "ConsistencyLevel "+level+" specified in the request is invalid when service is configured with consistency level "+f.consistency)
		return
	}
	if (op == "read" || op == "query") && r.Header.Get("x-ms-consistency-level") != "Strong" {
		f.errs = append(f.errs, op+" without Strong consistency (F44)")
	}

	var pk string
	if h := r.Header.Get("x-ms-documentdb-partitionkey"); h != "" {
		var pks []string
		if err := json.Unmarshal([]byte(h), &pks); err != nil || len(pks) != 1 {
			f.errs = append(f.errs, "bad partition key "+h)
			f.fail(w, http.StatusBadRequest, "", "bad partition key")
			return
		}
		pk = pks[0]
	}
	id, _ := url.PathUnescape(strings.TrimPrefix(path, "dbs/d/colls/c/docs/"))

	switch op {
	case "account":
		loc := []map[string]string{{"name": "Fake", "databaseAccountEndpoint": f.URL + "/"}}
		f.reply(w, http.StatusOK, map[string]any{
			"id": "fake", "writableLocations": loc, "readableLocations": loc,
			"userConsistencyPolicy": map[string]string{"defaultConsistencyLevel": f.consistency},
		})
	case "container":
		f.reply(w, http.StatusOK, f.container)
	case "create", "upsert":
		if len(body) > 2<<20 {
			f.fail(w, http.StatusRequestEntityTooLarge, "", "Request size is too large")
			return
		}
		var item map[string]json.RawMessage
		var itemID, itemPK string
		if json.Unmarshal(body, &item) != nil || json.Unmarshal(item["id"], &itemID) != nil || json.Unmarshal(item["pk"], &itemPK) != nil ||
			itemID == "" || len(itemID) > 1023 || strings.ContainsAny(itemID, `/\?#`) {
			f.errs = append(f.errs, "bad item "+itemID)
			f.fail(w, http.StatusBadRequest, "", "bad item")
			return
		}
		if itemPK != pk {
			f.errs = append(f.errs, "item with partition key "+itemPK+" sent to partition "+pk)
			f.fail(w, http.StatusBadRequest, "1001", "PartitionKey extracted from document doesn't match the one specified in the header")
			return
		}
		k := [2]string{pk, itemID}
		_, exists := f.items[k]
		if exists && op == "create" {
			f.fail(w, http.StatusConflict, "", "Entity with the specified id already exists in the system.")
			return
		}
		f.etag++
		it := fakeItem{body: item, etag: strconv.Itoa(f.etag), ts: f.now.Unix()}
		f.items[k] = it
		w.Header().Set("etag", it.etag)
		w.WriteHeader(http.StatusCreated)
	case "read":
		it, ok := f.items[[2]string{pk, id}]
		if !ok {
			f.fail(w, http.StatusNotFound, "0", "Resource Not Found.")
			return
		}
		w.Header().Set("etag", it.etag)
		f.reply(w, http.StatusOK, it.withSystem())
	case "delete":
		k := [2]string{pk, id}
		it, ok := f.items[k]
		if !ok {
			f.fail(w, http.StatusNotFound, "0", "Resource Not Found.")
			return
		}
		if m := r.Header.Get("If-Match"); m != "" && m != it.etag {
			f.fail(w, http.StatusPreconditionFailed, "", "One of the specified pre-condition is not met.")
			return
		}
		delete(f.items, k)
		w.WriteHeader(http.StatusNoContent)
	case "query":
		f.query(w, r, body, pk)
	default:
		f.errs = append(f.errs, "unexpected request "+r.Method+" "+r.URL.Path)
		f.fail(w, http.StatusMethodNotAllowed, "", "unexpected request")
	}
}

func (it fakeItem) withSystem() map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, v := range it.body {
		out[k] = v
	}
	out["_etag"], _ = json.Marshal(it.etag)
	out["_ts"], _ = json.Marshal(it.ts)
	return out
}

func (f *fakeCosmos) query(w http.ResponseWriter, r *http.Request, body []byte, pk string) {
	var q struct {
		Query      string
		Parameters []struct{ Name, Value string }
	}
	if err := json.Unmarshal(body, &q); err != nil {
		if r.Context().Err() != nil {
			return // ListChunks cancels the other shard queries after one fails.
		}
		f.errs = append(f.errs, fmt.Sprintf("bad query (%s %s, %d bytes): %v: %q", r.Method, r.URL, len(body), err, body))
		f.fail(w, http.StatusBadRequest, "", "bad query")
		return
	}
	if pk == "" {
		f.errs = append(f.errs, "a query across partitions")
	}
	params := map[string]string{}
	for _, p := range q.Parameters {
		params[p.Name] = p.Value
	}
	var keys []string
	for k := range f.items {
		if k[0] == pk {
			keys = append(keys, k[1])
		}
	}
	sort.Strings(keys)
	var results []any
	for _, id := range keys {
		it := f.items[[2]string{pk, id}].withSystem()
		var name string
		_ = json.Unmarshal(it["name"], &name)
		switch q.Query {
		case namesQuery:
			if it["name"] != nil && strings.HasPrefix(name, params["@prefix"]) && utf16Less(params["@after"], name) {
				results = append(results, name)
			}
		case chunksQuery:
			results = append(results, map[string]json.RawMessage{"upload": it["upload"], "n": it["n"], "owner": it["owner"], "_ts": it["_ts"]})
		case "SELECT TOP 1 VALUE c.id FROM c":
			if len(results) == 0 {
				results = append(results, id)
			}
		default:
			f.errs = append(f.errs, "unexpected query "+q.Query)
		}
	}
	start, _ := strconv.Atoi(r.Header.Get("x-ms-continuation"))
	end := min(start+f.pageSize, len(results))
	page := results[min(start, end):end]
	if end < len(results) {
		w.Header().Set("x-ms-continuation", strconv.Itoa(end))
	}
	if page == nil {
		page = []any{}
	}
	f.reply(w, http.StatusOK, map[string]any{"_rid": "x", "Documents": page, "_count": len(page)})
}

// utf16Less orders strings by their UTF-16 code units, which differs from
// their bytes' order for some characters, as a database might.
func utf16Less(a, b string) bool {
	x, y := utf16Units(a), utf16Units(b)
	for i := range min(len(x), len(y)) {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return len(x) < len(y)
}

func utf16Units(s string) []rune {
	var out []rune
	for _, r := range s {
		if r >= 0x10000 {
			r -= 0x10000
			out = append(out, 0xD800+(r>>10), 0xDC00+(r&0x3FF))
			continue
		}
		out = append(out, r)
	}
	return out
}
