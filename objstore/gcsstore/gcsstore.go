// Package gcsstore is an objstore.Store on a Google Cloud Storage bucket.
//
// PutIfAbsent creates objects with the ifGenerationMatch=0 precondition,
// which GCS checks atomically when the object is published, in a single
// request, so a commit object is either created whole or not at all.
package gcsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/target/goalert/objstore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Store keeps objects under a prefix in one bucket.
type Store struct {
	bucket *storage.BucketHandle
	prefix string
}

var _ objstore.Store = (*Store)(nil)

// New returns a Store for the objects under prefix in bucket. An empty prefix
// uses the whole bucket; otherwise it must end with "/".
func New(client *storage.Client, bucket, prefix string) (*Store, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("gcsstore: prefix %q must end with /", prefix)
	}
	return &Store{bucket: client.Bucket(bucket), prefix: prefix}, nil
}

func (s *Store) obj(name string) *storage.ObjectHandle { return s.bucket.Object(s.prefix + name) }

func (s *Store) Get(ctx context.Context, name string) ([]byte, error) {
	if err := objstore.CheckName(name); err != nil {
		return nil, err
	}
	r, err := s.obj(name).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, fmt.Errorf("%w: %s", objstore.ErrNotFound, name)
	}
	if err != nil {
		return nil, opErr("read", name, err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, opErr("read", name, err)
	}
	return data, nil
}

// denied reports whether GCS refused the credentials or a permission (F33).
func denied(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && (gerr.Code == http.StatusUnauthorized || gerr.Code == http.StatusForbidden)
}

// opErr wraps an error from an operation other than a write.
func opErr(op, name string, err error) error {
	err = fmt.Errorf("gcsstore: %s %s: %w", op, name, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	return err
}

// writeErr classifies an upload error. A rejected precondition means the
// object exists. A request the server rejected outright did not happen.
// Anything else (a timeout, a dropped connection, a server error) may have.
func writeErr(name string, err error) error {
	if err == nil {
		return nil
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch {
		case gerr.Code == http.StatusPreconditionFailed:
			return fmt.Errorf("%w: %s", objstore.ErrExists, name)
		case denied(err):
			return objstore.Denied(fmt.Errorf("gcsstore: write %s: %w", name, err))
		case rejected(err):
			return fmt.Errorf("gcsstore: write %s: %w", name, err)
		}
	}
	return objstore.Unknown(fmt.Errorf("gcsstore: write %s: %w", name, err))
}

// rejected reports whether GCS answered that it did not do a write.
func rejected(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code >= 400 && gerr.Code < 500 && gerr.Code != http.StatusRequestTimeout
}

func (s *Store) write(ctx context.Context, obj *storage.ObjectHandle, name string, data []byte) error {
	w := obj.NewWriter(ctx)
	// One request, so the object is created whole or not at all. The library
	// does not retry it, so its error is the write's only outcome (F32).
	w.ChunkSize = 0
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		_ = w.Close()
		return writeErr(name, err)
	}
	return writeErr(name, w.Close())
}

func (s *Store) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	return s.write(ctx, s.obj(name).If(storage.Conditions{DoesNotExist: true}), name, data)
}

func (s *Store) Put(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	return s.write(ctx, s.obj(name), name, data)
}

func (s *Store) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	q := &storage.Query{Prefix: s.prefix + prefix}
	if startAfter != "" {
		q.StartOffset = s.prefix + startAfter // inclusive, so skip it below
	}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	var names []string
	it := s.bucket.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, opErr("list", prefix, err)
		}
		name := strings.TrimPrefix(attrs.Name, s.prefix)
		if name > startAfter {
			names = append(names, name)
		}
	}
	return names, nil
}

func (s *Store) Delete(ctx context.Context, name string) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	err := s.obj(name).Delete(ctx)
	if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return opErr("delete", name, err)
}
