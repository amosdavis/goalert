// Package firestore registers the "firestore" object store: a Cloud Firestore
// database in Native mode, reached with the application default credentials.
//
//	firestore://project/database/prefix
//
// An empty database, or "(default)", is the project's default database.
package firestore

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/target/goalert/objstore/chunkstore"
	"github.com/target/goalert/objstore/firestorestore"
	"github.com/target/goalert/storagebackend"
)

func init() { storagebackend.RegisterObjects("firestore", open) }

func open(ctx context.Context, u *url.URL) (storagebackend.Objects, error) {
	if err := storagebackend.NewParams(u).Done(); err != nil {
		return storagebackend.Objects{}, err
	}
	database, prefix, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	prefix = strings.TrimSuffix(prefix, "/")
	if u.Host == "" {
		return storagebackend.Objects{}, errors.New("firestore: the URL must be firestore://PROJECT/DATABASE/PREFIX")
	}
	records, err := firestorestore.Open(ctx, firestorestore.Config{Project: u.Host, Database: database, Prefix: prefix})
	if err != nil {
		return storagebackend.Objects{}, err
	}
	store, err := chunkstore.New(records, chunkstore.Options{})
	if err != nil {
		_ = records.Close()
		return storagebackend.Objects{}, err
	}
	return storagebackend.Objects{Store: store, Close: records.Close}, nil
}
