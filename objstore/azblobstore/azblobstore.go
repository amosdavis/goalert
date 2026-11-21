// Package azblobstore is an objstore.Store on an Azure Blob Storage
// container. It also works on ADLS Gen2 accounts, which accept the Blob API.
//
// PutIfAbsent creates blobs with a single Put Blob request carrying
// If-None-Match: *, which Azure checks atomically, so a commit blob is either
// created whole or not at all.
package azblobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
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
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) && rerr.StatusCode >= 400 && rerr.StatusCode < 500 && rerr.StatusCode != http.StatusRequestTimeout {
		return fmt.Errorf("azblobstore: write %s: %w", name, err)
	}
	return objstore.Unknown(fmt.Errorf("azblobstore: write %s: %w", name, err))
}

func (s *Store) upload(ctx context.Context, name string, data []byte, cond *blob.AccessConditions) error {
	_, err := s.c.NewBlockBlobClient(s.prefix+name).Upload(ctx,
		streaming.NopCloser(bytes.NewReader(data)),
		&blockblob.UploadOptions{AccessConditions: cond},
	)
	return writeErr(name, err)
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
	pager := s.c.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: to.Ptr(s.prefix + prefix)})
	var names []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			name := strings.TrimPrefix(*item.Name, s.prefix)
			if name > startAfter {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

func (s *Store) Delete(ctx context.Context, name string) error {
	if err := objstore.CheckName(name); err != nil {
		return err
	}
	_, err := s.c.NewBlobClient(s.prefix+name).Delete(ctx, nil)
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil
	}
	return err
}
