// Package gcs registers the "gs" object store: a Google Cloud Storage bucket,
// reached with the application default credentials.
//
//	gs://bucket/prefix
package gcs

import (
	"context"
	"errors"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/target/goalert/objstore/gcsstore"
	"github.com/target/goalert/storagebackend"
)

func init() { storagebackend.RegisterObjects("gs", open) }

func open(ctx context.Context, u *url.URL) (storagebackend.Objects, error) {
	if err := storagebackend.NewParams(u).Done(); err != nil {
		return storagebackend.Objects{}, err
	}
	prefix, err := storagebackend.Prefix(u.Path)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	if u.Host == "" {
		return storagebackend.Objects{}, errors.New("gcs: the URL must be gs://BUCKET/PREFIX")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	store, err := gcsstore.New(client, u.Host, prefix)
	if err != nil {
		_ = client.Close()
		return storagebackend.Objects{}, err
	}
	return storagebackend.Objects{Store: store, Location: "gs://" + u.Host + "/" + prefix, Close: client.Close}, nil
}
