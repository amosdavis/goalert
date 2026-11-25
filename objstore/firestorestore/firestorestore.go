// Package firestorestore keeps objects in Cloud Firestore: it implements
// chunkstore.RecordStore in two collections of a database in Native mode.
//
// Firestore's server client reads and queries are strongly consistent, and a
// create is atomic, which is all a commit needs (F4, F44).
package firestorestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/firestore"
	"github.com/target/goalert/objstore/chunkstore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	// The collections. Firestore's time-to-live policies and index settings
	// apply to every collection with one ID, so these are unlikely to be
	// anyone else's.
	manifestCollection = "goalert_manifests"
	chunkCollection    = "goalert_chunks"

	// maxName is the longest name that can be listed: Firestore indexes no
	// more than the first 1500 bytes of a value.
	maxName = 1000

	// namePage and chunkPage are how many documents one query returns.
	namePage  = 300
	chunkPage = 500
)

// Config names where the data is.
type Config struct {
	Project string
	// Database is the database's ID. The default is "(default)".
	Database string

	// Prefix keeps stores apart that share a database. It may be empty.
	Prefix string
}

// Store is a chunkstore.RecordStore in a Firestore database.
type Store struct {
	client    *firestore.Client
	cfg       Config
	manifests *firestore.CollectionRef
	chunks    *firestore.CollectionRef
	owned     bool
	opts      []option.ClientOption
}

var _ chunkstore.RecordStore = (*Store)(nil)

var validPrefix = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// DialOption is what a client that New gets must be made with: it notes every
// attempt of a write whose outcome is unknown, since the client library
// retries on its own, and the last attempt's error says nothing about an
// earlier one (F32).
func DialOption() grpc.DialOption { return grpc.WithChainUnaryInterceptor(trackAttempts) }

// Open connects to a database. Close closes the connection.
func Open(ctx context.Context, cfg Config, opts ...option.ClientOption) (*Store, error) {
	if cfg.Database == "" {
		cfg.Database = firestore.DefaultDatabaseID
	}
	dial := opts
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		// With it, the library makes the connection itself.
		dial = append(opts[:len(opts):len(opts)], option.WithGRPCDialOption(DialOption()))
	}
	client, err := firestore.NewClientWithDatabase(ctx, cfg.Project, cfg.Database, dial...)
	if err != nil {
		return nil, fmt.Errorf("firestorestore: %w", err)
	}
	s, err := New(client, cfg)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	s.owned, s.opts = true, opts
	return s, nil
}

// New returns a Store that uses a client made with DialOption, whose project
// and database are cfg's. It does no I/O.
func New(client *firestore.Client, cfg Config) (*Store, error) {
	switch {
	case client == nil:
		return nil, errors.New("firestorestore: nil client")
	case cfg.Project == "":
		return nil, errors.New("firestorestore: the project is required")
	case cfg.Prefix != "" && !validPrefix.MatchString(cfg.Prefix):
		return nil, fmt.Errorf("firestorestore: invalid prefix %q: use letters, digits, dots, dashes and underscores", cfg.Prefix)
	}
	if cfg.Database == "" {
		cfg.Database = firestore.DefaultDatabaseID
	}
	// Every store has a document of its own above its collections, which need
	// not exist. The "-" keeps the empty prefix apart from all others.
	root := client.Collection("goalert").Doc("-" + cfg.Prefix)
	return &Store{
		client: client, cfg: cfg,
		manifests: root.Collection(manifestCollection),
		chunks:    root.Collection(chunkCollection),
	}, nil
}

// Close closes the connection Open made.
func (s *Store) Close() error {
	if !s.owned {
		return nil
	}
	return s.client.Close()
}

type manifestDoc struct {
	Name   string `firestore:"name"`
	Length int64  `firestore:"length"`
	SHA256 string `firestore:"sha256"`
	Inline []byte `firestore:"inline,omitempty"`
	Upload string `firestore:"upload,omitempty"`
	Chunks int64  `firestore:"chunks,omitempty"`
}

type chunkDoc struct {
	Upload string `firestore:"upload"`
	N      int64  `firestore:"n"`
	Owner  string `firestore:"owner"`
	Data   []byte `firestore:"data"`
}

type chunkInfoDoc struct {
	Upload string `firestore:"upload"`
	N      int64  `firestore:"n"`
	Owner  string `firestore:"owner"`
}

// manifest is the document of a name, which may hold characters a document ID
// can't. Hashed IDs also spread writes evenly.
func (s *Store) manifest(name string) *firestore.DocumentRef {
	sum := sha256.Sum256([]byte(name))
	return s.manifests.Doc(hex.EncodeToString(sum[:]))
}

func (s *Store) chunk(id chunkstore.ChunkID) (*firestore.DocumentRef, error) {
	if raw, err := hex.DecodeString(id.Upload); err != nil || len(raw) != 16 || id.Index < 0 {
		return nil, fmt.Errorf("firestorestore: invalid chunk ID %q", id)
	}
	return s.chunks.Doc(id.String()), nil
}

func checkName(name string) error {
	if !utf8.ValidString(name) || len(name) > maxName {
		return fmt.Errorf("firestorestore: the name %q is not UTF-8, or over %d bytes", name, maxName)
	}
	return nil
}

// get reads a document, and reports whether it exists. Firestore answers "not
// found" for a missing database too, which must not be taken for a missing
// document (F16, F27): for that, the library returns a snapshot.
func get(ctx context.Context, ref *firestore.DocumentRef, into any) (bool, error) {
	snap, err := ref.Get(ctx)
	if snap != nil && !snap.Exists() {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, snap.DataTo(into)
}

// GetManifest reads a document by its ID.
func (s *Store) GetManifest(ctx context.Context, name string) (chunkstore.Manifest, error) {
	var d manifestDoc
	ok, err := get(ctx, s.manifest(name), &d)
	if err != nil {
		return chunkstore.Manifest{}, opErr("read the manifest of "+name, err)
	}
	if !ok {
		return chunkstore.Manifest{}, fmt.Errorf("%w: %s", chunkstore.ErrManifestNotFound, name)
	}
	return chunkstore.Manifest{
		Name: d.Name, Length: d.Length, SHA256: d.SHA256,
		Inline: d.Inline, Upload: d.Upload, Chunks: int(d.Chunks),
	}, nil
}

func newManifestDoc(m chunkstore.Manifest) manifestDoc {
	return manifestDoc{
		Name: m.Name, Length: m.Length, SHA256: m.SHA256,
		Inline: m.Inline, Upload: m.Upload, Chunks: int64(m.Chunks),
	}
}

// CreateManifest creates one document, which at most one of several creates
// can do.
func (s *Store) CreateManifest(ctx context.Context, m chunkstore.Manifest) error {
	if err := checkName(m.Name); err != nil {
		return err
	}
	err := write(ctx, "create the manifest of "+m.Name, func(ctx context.Context) error {
		_, err := s.manifest(m.Name).Create(ctx, newManifestDoc(m))
		return err
	})
	if errors.Is(err, errExists) {
		return fmt.Errorf("%w: %s", chunkstore.ErrManifestExists, m.Name)
	}
	return err
}

// SetManifest creates or replaces one document.
func (s *Store) SetManifest(ctx context.Context, m chunkstore.Manifest) error {
	if err := checkName(m.Name); err != nil {
		return err
	}
	return write(ctx, "replace the manifest of "+m.Name, func(ctx context.Context) error {
		_, err := s.manifest(m.Name).Set(ctx, newManifestDoc(m))
		return err
	})
}

// DeleteManifest deletes one document, if it exists.
func (s *Store) DeleteManifest(ctx context.Context, name string) error {
	return write(ctx, "delete the manifest of "+name, func(ctx context.Context) error {
		_, err := s.manifest(name).Delete(ctx)
		return err
	})
}

// ListNames queries the names in order. Firestore sorts strings by their UTF-8
// bytes.
func (s *Store) ListNames(ctx context.Context, prefix, startAfter string) ([]string, error) {
	if !utf8.ValidString(prefix) || !utf8.ValidString(startAfter) {
		return nil, errors.New("firestorestore: list: the prefix and the name to start after must be UTF-8")
	}
	names := []string{}
	q := s.manifests.Select("name").OrderBy("name", firestore.Asc).Limit(namePage)
	page := q.Where("name", ">=", prefix)
	if startAfter >= prefix {
		page = q.Where("name", ">", startAfter)
	}
	for {
		n, last := 0, ""
		past := false
		err := each(page.Documents(ctx), func(snap *firestore.DocumentSnapshot) error {
			var d struct {
				Name string `firestore:"name"`
			}
			if err := snap.DataTo(&d); err != nil {
				return err
			}
			n, last = n+1, d.Name
			if !strings.HasPrefix(d.Name, prefix) {
				past = true // those that have it come first, and in a row
				return nil
			}
			names = append(names, d.Name)
			return nil
		})
		if err != nil {
			return nil, opErr("list manifests", err)
		}
		if past || n < namePage {
			return names, nil
		}
		page = q.Where("name", ">", last)
	}
}

func each(it *firestore.DocumentIterator, fn func(snap *firestore.DocumentSnapshot) error) error {
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(snap); err != nil {
			return err
		}
	}
}

// PutChunk stores one document. Repeating it stores the same contents again.
func (s *Store) PutChunk(ctx context.Context, c chunkstore.Chunk) error {
	ref, err := s.chunk(c.ChunkID)
	if err != nil {
		return err
	}
	if err := checkName(c.Owner); err != nil {
		return err
	}
	return write(ctx, "store chunk "+c.String(), func(ctx context.Context) error {
		_, err := ref.Set(ctx, chunkDoc{Upload: c.Upload, N: int64(c.Index), Owner: c.Owner, Data: c.Data})
		return err
	})
}

// GetChunk reads a document by its ID.
func (s *Store) GetChunk(ctx context.Context, id chunkstore.ChunkID) ([]byte, error) {
	ref, err := s.chunk(id)
	if err != nil {
		return nil, err
	}
	var d chunkDoc
	ok, err := get(ctx, ref, &d)
	if err != nil {
		return nil, opErr("read chunk "+id.String(), err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", chunkstore.ErrChunkNotFound, id)
	}
	if d.Upload != id.Upload || d.N != int64(id.Index) {
		return nil, fmt.Errorf("firestorestore: chunk %s holds chunk %s-%d", id, d.Upload, d.N)
	}
	return d.Data, nil
}

// ListChunks reads every chunk but its data. Both their ages and the time they
// are compared with are the server's.
func (s *Store) ListChunks(ctx context.Context) ([]chunkstore.ChunkInfo, time.Time, error) {
	var chunks []chunkstore.ChunkInfo
	var asOf time.Time
	q := s.chunks.Select("upload", "n", "owner").OrderBy(firestore.DocumentID, firestore.Asc).Limit(chunkPage)
	page := q
	for {
		n, last := 0, ""
		err := each(page.Documents(ctx), func(snap *firestore.DocumentSnapshot) error {
			var d chunkInfoDoc
			if err := snap.DataTo(&d); err != nil {
				return err
			}
			if snap.CreateTime.IsZero() || snap.ReadTime.IsZero() {
				return errors.New("the server reported no times, so no chunk's age is known")
			}
			if asOf.IsZero() || snap.ReadTime.Before(asOf) {
				asOf = snap.ReadTime
			}
			n, last = n+1, snap.Ref.ID
			chunks = append(chunks, chunkstore.ChunkInfo{
				ChunkID: chunkstore.ChunkID{Upload: d.Upload, Index: int(d.N)},
				Owner:   d.Owner, Created: snap.CreateTime,
			})
			return nil
		})
		if err != nil {
			return nil, time.Time{}, opErr("list chunks", err)
		}
		if n < chunkPage {
			return chunks, asOf, nil
		}
		page = q.StartAfter(last)
	}
}

// DeleteChunk deletes one document, if it exists.
func (s *Store) DeleteChunk(ctx context.Context, id chunkstore.ChunkID) error {
	ref, err := s.chunk(id)
	if err != nil {
		return err
	}
	return write(ctx, "delete chunk "+id.String(), func(ctx context.Context) error {
		_, err := ref.Delete(ctx)
		return err
	})
}
