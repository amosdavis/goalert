// Package cosmosstore keeps objects in Azure Cosmos DB for NoSQL: it implements
// chunkstore.RecordStore on one container.
//
// The account must use Strong consistency. Reads and queries ask for it, so an
// account with a weaker default refuses them instead of returning stale data,
// which would let a pod take an old version for the latest (F44). Azure does
// not allow Strong consistency with several write regions.
//
// Manifests are in one logical partition, so that listing them takes a single
// query. Chunks, which hold nearly all the data, are spread over 256 logical
// partitions, since one can hold no more than 20 GB (F45).
package cosmosstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/target/goalert/objstore/chunkstore"
)

const (
	manifestKind = "manifest"
	chunkKind    = "chunk"

	// shards is how many logical partitions hold chunks: one for each first
	// byte of an upload's ID.
	shards = 256

	// maxItem is the largest item Cosmos DB stores.
	maxItem = 2 << 20

	// listConcurrency is how many partitions are listed at once.
	listConcurrency = 8

	// statusRetryWith is Cosmos DB's answer to a write that ran into another
	// one. It was not applied, and can be repeated.
	statusRetryWith = 449
)

// Config names where the data is.
type Config struct {
	// Database and Container must exist. ContainerProperties describes the
	// container.
	Database, Container string

	// Prefix keeps stores apart that share a container. It may be empty.
	Prefix string
}

// Store is a chunkstore.RecordStore in a Cosmos DB container.
type Store struct {
	container *azcosmos.ContainerClient
	prefix    string
}

var _ chunkstore.RecordStore = (*Store)(nil)

// New returns a RecordStore in a container. It does no I/O; use CheckSettings
// to check the container.
func New(client *azcosmos.Client, cfg Config) (*Store, error) {
	switch {
	case client == nil:
		return nil, errors.New("cosmosstore: nil client")
	case cfg.Database == "" || cfg.Container == "":
		return nil, errors.New("cosmosstore: the database and the container are required")
	case !utf8.ValidString(cfg.Prefix) || len(cfg.Prefix) > 512:
		return nil, errors.New("cosmosstore: the prefix must be UTF-8, and at most 512 bytes")
	}
	database, err := client.NewDatabase(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("cosmosstore: database %q: %w", cfg.Database, err)
	}
	container, err := database.NewContainer(cfg.Container)
	if err != nil {
		return nil, fmt.Errorf("cosmosstore: container %q: %w", cfg.Container, err)
	}
	return &Store{container: container, prefix: cfg.Prefix}, nil
}

// The partition keys. They start with what they hold, so no prefix makes one
// store's keys another's.
func (s *Store) manifestPK() string { return "manifests:" + s.prefix }
func (s *Store) chunkPK(id chunkstore.ChunkID) string {
	return "chunks:" + id.Upload[:2] + ":" + s.prefix
}

// manifestID is an item ID for a name, which may hold characters an ID can't.
func manifestID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:])
}

type manifestItem struct {
	ID     string `json:"id"`
	PK     string `json:"pk"`
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
	Inline []byte `json:"inline,omitempty"`
	Upload string `json:"upload,omitempty"`
	Chunks int    `json:"chunks,omitempty"`
}

type chunkItem struct {
	ID     string `json:"id"`
	PK     string `json:"pk"`
	Kind   string `json:"kind"`
	Upload string `json:"upload"`
	N      int    `json:"n"`
	Owner  string `json:"owner"`
	Data   []byte `json:"data"`
}

type chunkInfoItem struct {
	Upload string `json:"upload"`
	N      int    `json:"n"`
	Owner  string `json:"owner"`
	TS     int64  `json:"_ts"` // when the server stored it, in seconds
}

func checkUpload(id chunkstore.ChunkID) error {
	if raw, err := hex.DecodeString(id.Upload); err != nil || len(raw) != 16 || id.Index < 0 {
		return fmt.Errorf("cosmosstore: invalid chunk ID %q", id)
	}
	return nil
}

func encode(what string, item any) ([]byte, error) {
	body, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("cosmosstore: encode %s: %w", what, err)
	}
	if len(body) > maxItem {
		return nil, fmt.Errorf("cosmosstore: %s takes %d bytes, over the limit of %d (F45)", what, len(body), maxItem)
	}
	return body, nil
}

func readOptions() *azcosmos.ItemOptions {
	return &azcosmos.ItemOptions{ConsistencyLevel: azcosmos.ConsistencyLevelStrong.ToPtr()}
}

// retry repeats a write that ran into another one.
func retry(ctx context.Context, fn func() error) error {
	delay := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := fn()
		if statusCode(err) != statusRetryWith || attempt == 8 {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay/2 + rand.N(delay)):
		}
		delay *= 2
	}
}

// GetManifest reads a manifest item by its ID.
func (s *Store) GetManifest(ctx context.Context, name string) (chunkstore.Manifest, error) {
	resp, err := s.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(s.manifestPK()), manifestID(name), readOptions())
	if notFound(err) {
		return chunkstore.Manifest{}, fmt.Errorf("%w: %s", chunkstore.ErrManifestNotFound, name)
	}
	if err != nil {
		return chunkstore.Manifest{}, opErr("read the manifest of "+name, err)
	}
	var item manifestItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return chunkstore.Manifest{}, fmt.Errorf("cosmosstore: decode the manifest of %s: %w", name, err)
	}
	if item.Kind != manifestKind {
		return chunkstore.Manifest{}, fmt.Errorf("cosmosstore: the manifest of %s is a %q item", name, item.Kind)
	}
	return chunkstore.Manifest{
		Name: item.Name, Length: item.Length, SHA256: item.SHA256,
		Inline: item.Inline, Upload: item.Upload, Chunks: item.Chunks,
	}, nil
}

func (s *Store) encodeManifest(m chunkstore.Manifest) ([]byte, error) {
	if !utf8.ValidString(m.Name) {
		return nil, fmt.Errorf("cosmosstore: the name %q is not UTF-8", m.Name)
	}
	return encode("the manifest of "+m.Name, manifestItem{
		ID: manifestID(m.Name), PK: s.manifestPK(), Kind: manifestKind,
		Name: m.Name, Length: m.Length, SHA256: m.SHA256,
		Inline: m.Inline, Upload: m.Upload, Chunks: m.Chunks,
	})
}

// CreateManifest creates one item. Cosmos DB keeps IDs unique in a logical
// partition, so at most one of several creates succeeds.
func (s *Store) CreateManifest(ctx context.Context, m chunkstore.Manifest) error {
	body, err := s.encodeManifest(m)
	if err != nil {
		return err
	}
	err = retry(ctx, func() error {
		_, err := s.container.CreateItem(ctx, azcosmos.NewPartitionKeyString(s.manifestPK()), body, nil)
		return err
	})
	if statusCode(err) == http.StatusConflict {
		return fmt.Errorf("%w: %s", chunkstore.ErrManifestExists, m.Name)
	}
	return writeErr("create the manifest of "+m.Name, err)
}

// SetManifest creates or replaces one item.
func (s *Store) SetManifest(ctx context.Context, m chunkstore.Manifest) error {
	body, err := s.encodeManifest(m)
	if err != nil {
		return err
	}
	return writeErr("replace the manifest of "+m.Name, retry(ctx, func() error {
		_, err := s.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(s.manifestPK()), body, nil)
		return err
	}))
}

// DeleteManifest deletes one item.
func (s *Store) DeleteManifest(ctx context.Context, name string) error {
	err := retry(ctx, func() error {
		_, err := s.container.DeleteItem(ctx, azcosmos.NewPartitionKeyString(s.manifestPK()), manifestID(name), nil)
		return err
	})
	if notFound(err) {
		return nil
	}
	return writeErr("delete the manifest of "+name, err)
}

const namesQuery = "SELECT VALUE c.name FROM c WHERE STARTSWITH(c.name, @prefix) AND c.name > @after"

// ListNames queries the one partition that holds manifests.
func (s *Store) ListNames(ctx context.Context, prefix, startAfter string) ([]string, error) {
	for i := range len(startAfter) {
		if startAfter[i] >= utf8.RuneSelf {
			// The database may order such names differently than by their
			// bytes, so list them all. The caller leaves out the others.
			startAfter = ""
			break
		}
	}
	names := []string{}
	err := s.query(ctx, s.manifestPK(), namesQuery, []azcosmos.QueryParameter{
		{Name: "@prefix", Value: prefix}, {Name: "@after", Value: startAfter},
	}, func(item []byte, _ time.Time) error {
		var name string
		if err := json.Unmarshal(item, &name); err != nil {
			return fmt.Errorf("cosmosstore: decode a name: %w", err)
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, opErr("list manifests", err)
	}
	return names, nil
}

// query calls fn with each result of a query in one logical partition, and the
// server's time when it gave the result's page. If that is unknown, it is zero.
func (s *Store) query(ctx context.Context, pk, query string, params []azcosmos.QueryParameter, fn func(item []byte, at time.Time) error) error {
	crossPartition := false
	pager := s.container.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString(pk), &azcosmos.QueryOptions{
		ConsistencyLevel:          azcosmos.ConsistencyLevelStrong.ToPtr(),
		QueryParameters:           params,
		EnableCrossPartitionQuery: &crossPartition,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}
		var at time.Time
		if page.RawResponse != nil {
			at, _ = http.ParseTime(page.RawResponse.Header.Get("Date"))
		}
		for _, item := range page.Items {
			if err := fn(item, at); err != nil {
				return err
			}
		}
	}
	return nil
}

// PutChunk stores one item. Repeating it stores the same contents again.
func (s *Store) PutChunk(ctx context.Context, c chunkstore.Chunk) error {
	if err := checkUpload(c.ChunkID); err != nil {
		return err
	}
	if !utf8.ValidString(c.Owner) {
		return fmt.Errorf("cosmosstore: the name %q is not UTF-8", c.Owner)
	}
	body, err := encode("chunk "+c.String(), chunkItem{
		ID: c.String(), PK: s.chunkPK(c.ChunkID), Kind: chunkKind,
		Upload: c.Upload, N: c.Index, Owner: c.Owner, Data: c.Data,
	})
	if err != nil {
		return err
	}
	return writeErr("store chunk "+c.String(), retry(ctx, func() error {
		_, err := s.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(s.chunkPK(c.ChunkID)), body, nil)
		return err
	}))
}

// GetChunk reads one item by its ID.
func (s *Store) GetChunk(ctx context.Context, id chunkstore.ChunkID) ([]byte, error) {
	if err := checkUpload(id); err != nil {
		return nil, err
	}
	resp, err := s.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(s.chunkPK(id)), id.String(), readOptions())
	if notFound(err) {
		return nil, fmt.Errorf("%w: %s", chunkstore.ErrChunkNotFound, id)
	}
	if err != nil {
		return nil, opErr("read chunk "+id.String(), err)
	}
	var item chunkItem
	if err := json.Unmarshal(resp.Value, &item); err != nil {
		return nil, fmt.Errorf("cosmosstore: decode chunk %s: %w", id, err)
	}
	if item.Kind != chunkKind || item.Upload != id.Upload || item.N != id.Index {
		return nil, fmt.Errorf("cosmosstore: chunk %s holds %q chunk %s-%d", id, item.Kind, item.Upload, item.N)
	}
	return item.Data, nil
}

const chunksQuery = "SELECT c.upload, c.n, c.owner, c._ts FROM c"

// ListChunks queries every partition that holds chunks. Both their ages and
// the time they are compared with are the server's.
func (s *Store) ListChunks(ctx context.Context) ([]chunkstore.ChunkInfo, time.Time, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu     sync.Mutex
		chunks []chunkstore.ChunkInfo
		asOf   time.Time
		first  error
		wg     sync.WaitGroup
	)
	sem := make(chan struct{}, listConcurrency)
	for shard := range shards {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			pk := fmt.Sprintf("chunks:%02x:%s", shard, s.prefix)
			err := s.query(ctx, pk, chunksQuery, nil, func(item []byte, at time.Time) error {
				var info chunkInfoItem
				if err := json.Unmarshal(item, &info); err != nil {
					return fmt.Errorf("cosmosstore: decode a chunk: %w", err)
				}
				if at.IsZero() || info.TS <= 0 {
					return errors.New("cosmosstore: the server reported no times, so no chunk's age is known")
				}
				mu.Lock()
				defer mu.Unlock()
				if asOf.IsZero() || at.Before(asOf) {
					asOf = at
				}
				chunks = append(chunks, chunkstore.ChunkInfo{
					ChunkID: chunkstore.ChunkID{Upload: info.Upload, Index: info.N},
					Owner:   info.Owner, Created: time.Unix(info.TS, 0).UTC(),
				})
				return nil
			})
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				if first == nil {
					first = err
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return nil, time.Time{}, opErr("list chunks", first)
	}
	return chunks, asOf, nil
}

// DeleteChunk deletes one item.
func (s *Store) DeleteChunk(ctx context.Context, id chunkstore.ChunkID) error {
	if err := checkUpload(id); err != nil {
		return err
	}
	err := retry(ctx, func() error {
		_, err := s.container.DeleteItem(ctx, azcosmos.NewPartitionKeyString(s.chunkPK(id)), id.String(), nil)
		return err
	})
	if notFound(err) {
		return nil
	}
	return writeErr("delete chunk "+id.String(), err)
}
