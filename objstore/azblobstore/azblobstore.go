// Package azblobstore is an objstore.Store on an Azure Blob Storage
// container. It also works on ADLS Gen2 accounts, which accept the Blob API.
//
// PutIfAbsent creates blobs with a single Put Blob request carrying
// If-None-Match: *, which Azure checks atomically, so a commit blob is either
// created whole or not at all.
//
// An account with a hierarchical namespace (ADLS Gen2) has real directories:
// creating a/b/c creates a and a/b, which listings return like blobs. List
// leaves them out, so the store holds only the objects GoAlert wrote there.
package azblobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/target/goalert/objstore"
)

// Store keeps blobs under a prefix in one container.
type Store struct {
	c      *container.Client
	prefix string
}

var _ objstore.Store = (*Store)(nil)

// New returns a Store for the blobs under prefix in a container. An empty
// prefix uses the whole container; otherwise it must end with "/".
func New(c *container.Client, prefix string) (*Store, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("azblobstore: prefix %q must end with /", prefix)
	}
	return &Store{c: c, prefix: prefix}, nil
}

func (s *Store) Get(ctx context.Context, name string) ([]byte, error) {
	if err := objstore.CheckName(name); err != nil {
		return nil, err
	}
	resp, err := s.c.NewBlobClient(s.prefix+name).DownloadStream(ctx, nil)
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, fmt.Errorf("%w: %s", objstore.ErrNotFound, name)
	}
	if err != nil {
		return nil, opErr("read", name, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, opErr("read", name, err)
	}
	return data, nil
}

// denied reports whether Azure refused the credentials or a permission
// (F33), such as an expired SAS token or a missing role assignment.
func denied(err error) bool {
	var rerr *azcore.ResponseError
	return errors.As(err, &rerr) && (rerr.StatusCode == http.StatusUnauthorized || rerr.StatusCode == http.StatusForbidden)
}

// opErr wraps an error from an operation other than a write.
func opErr(op, name string, err error) error {
	err = fmt.Errorf("azblobstore: %s %s: %w", op, name, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	return err
}

// writeErr classifies an upload error. A failed condition means the blob
// exists. A request the service rejected outright did not happen. Anything
// else (a timeout, a dropped connection, a server error) may have.
func writeErr(name string, err error) error {
	if err == nil {
		return nil
	}
	if bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.ConditionNotMet) {
		return fmt.Errorf("%w: %s", objstore.ErrExists, name)
	}
	if denied(err) {
		return objstore.Denied(fmt.Errorf("azblobstore: write %s: %w", name, err))
	}
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) && rejected(rerr.StatusCode) {
		return fmt.Errorf("azblobstore: write %s: %w", name, err)
	}
	return objstore.Unknown(fmt.Errorf("azblobstore: write %s: %w", name, err))
}

// rejected reports whether a status means Azure did not do a write.
func rejected(status int) bool {
	return status >= 400 && status < 500 && status != http.StatusRequestTimeout
}

func (s *Store) upload(ctx context.Context, name string, data []byte, cond *blob.AccessConditions) error {
	// The SDK retries on its own. Once an attempt may have been applied, a later
	// one being refused does not settle it (F32).
	var unsure atomic.Bool
	ctx = policy.WithRetryOptions(ctx, policy.RetryOptions{
		ShouldRetry: func(resp *http.Response, err error) bool {
			if err != nil {
				unsure.Store(true)
				return true
			}
			switch resp.StatusCode {
			case http.StatusTooManyRequests: // was not applied
				return true
			case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway,
				http.StatusServiceUnavailable, http.StatusGatewayTimeout:
				unsure.Store(true)
				return true
			}
			return false
		},
	})
	_, err := s.c.NewBlockBlobClient(s.prefix+name).Upload(ctx,
		streaming.NopCloser(bytes.NewReader(data)),
		&blockblob.UploadOptions{AccessConditions: cond},
	)
	err = writeErr(name, err)
	if err != nil && unsure.Load() && !errors.Is(err, objstore.ErrExists) && !errors.Is(err, objstore.ErrUnknown) {
		return objstore.Unknown(fmt.Errorf("azblobstore: write %s: an earlier attempt may have been applied, then: %v", name, err))
	}
	return err
}

func (s *Store) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	return s.upload(ctx, name, data, &blob.AccessConditions{
		ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)},
	})
}

func (s *Store) Put(ctx context.Context, name string, data []byte) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	return s.upload(ctx, name, data, nil)
}

func (s *Store) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	pager := s.c.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  to.Ptr(s.prefix + prefix),
		Include: container.ListBlobsInclude{Metadata: true}, // to tell directories
	})
	var names []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, opErr("list", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || isDirectory(item) {
				continue
			}
			name := strings.TrimPrefix(*item.Name, s.prefix)
			if name > startAfter {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

// isDirectory reports whether a listed blob is a directory of an account with a
// hierarchical namespace. The service says so in its resource type, and in the
// hdi_isfolder metadata that marks a directory for the Blob API.
func isDirectory(item *container.BlobItem) bool {
	if p := item.Properties; p != nil && p.ResourceType != nil && strings.EqualFold(*p.ResourceType, "directory") {
		return true
	}
	for k, v := range item.Metadata {
		if strings.EqualFold(k, "hdi_isfolder") && v != nil && strings.EqualFold(*v, "true") {
			return true
		}
	}
	return false
}

func (s *Store) Delete(ctx context.Context, name string) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	_, err := s.c.NewBlobClient(s.prefix+name).Delete(ctx, nil)
	if err == nil || bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil
	}
	return opErr("delete", name, err)
}