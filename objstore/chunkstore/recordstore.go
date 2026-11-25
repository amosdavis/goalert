package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A Manifest publishes one object. A small object is in the manifest itself;
// a larger one is in the chunks of one upload.
type Manifest struct {
	Name   string
	Length int64
	// SHA256 is the lowercase hexadecimal digest of the whole object.
	SHA256 string

	// Inline holds the object if Chunks is 0.
	Inline []byte

	// Upload identifies the chunks, numbered from 0, that make up the object
	// if Chunks is not 0. Every upload has chunks of its own.
	Upload string
	Chunks int
}

// A ChunkID names one chunk of one upload.
type ChunkID struct {
	// Upload is 32 lowercase hexadecimal digits, spread evenly, so adapters
	// can partition by them.
	Upload string
	Index  int
}

func (id ChunkID) String() string { return fmt.Sprintf("%s-%06d", id.Upload, id.Index) }

// A Chunk is an immutable part of an object.
type Chunk struct {
	ChunkID
	// Owner is the name of the object the chunk was uploaded for. Garbage
	// collection reads the owner's manifest to find out if it uses the chunk.
	Owner string
	Data  []byte
}

// ChunkInfo describes a stored chunk.
type ChunkInfo struct {
	ChunkID
	Owner string
	// Created is when the database stored the chunk, by the database's clock.
	Created time.Time
}

var (
	// ErrManifestNotFound means the manifest does not exist.
	ErrManifestNotFound = errors.New("chunkstore: manifest not found")

	// ErrManifestExists means CreateManifest found the name taken.
	ErrManifestExists = errors.New("chunkstore: manifest already exists")

	// ErrChunkNotFound means the chunk does not exist.
	ErrChunkNotFound = errors.New("chunkstore: chunk not found")
)

// RecordStore is the port a document database implements to hold objects. It
// is small on purpose: a database needs an atomic create, strongly consistent
// reads by key, and listings. Every implementation must pass
// chunkstoretest.Run.
//
// Reads are strongly consistent: they see every write that succeeded before
// they started, whichever client made it. A database that can't promise that
// must not implement RecordStore (docs/filestore-failure-modes.md, F44).
//
// A write returns an error wrapping objstore.ErrDenied if the database refused
// the credentials, and one wrapping objstore.ErrUnknown if its outcome can't be
// known (a timeout, a dropped connection, a server error). Any other error
// means the write did not happen (F32).
type RecordStore interface {
	// GetManifest returns a manifest, or an error wrapping ErrManifestNotFound.
	GetManifest(ctx context.Context, name string) (Manifest, error)

	// CreateManifest publishes a whole manifest if its name is free, atomically,
	// or returns an error wrapping ErrManifestExists. Of several concurrent
	// calls for one name, at most one succeeds. An implementation that retries
	// may return ErrManifestExists for a manifest of its own that an earlier
	// attempt created.
	CreateManifest(ctx context.Context, m Manifest) error

	// SetManifest creates or replaces a whole manifest, atomically.
	SetManifest(ctx context.Context, m Manifest) error

	// DeleteManifest removes a manifest. A missing one is not an error.
	DeleteManifest(ctx context.Context, name string) error

	// ListNames returns the names of manifests that start with prefix and sort
	// after startAfter (all of them if it is empty), in any order. It may
	// return others too.
	ListNames(ctx context.Context, prefix, startAfter string) ([]string, error)

	// PutChunk stores a chunk. A chunk never changes: a repeated call with the
	// same ID carries the same contents, and must keep the chunk whole.
	PutChunk(ctx context.Context, c Chunk) error

	// GetChunk returns a chunk's contents, or an error wrapping
	// ErrChunkNotFound.
	GetChunk(ctx context.Context, id ChunkID) ([]byte, error)

	// ListChunks returns every chunk, and the database's time, read no later
	// than the chunks were. A chunk stored meanwhile may be missing.
	ListChunks(ctx context.Context) (chunks []ChunkInfo, asOf time.Time, err error)

	// DeleteChunk removes a chunk. A missing one is not an error.
	DeleteChunk(ctx context.Context, id ChunkID) error
}
