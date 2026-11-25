// Package dir registers the "file" object store: a local directory, for
// development and tests.
//
//	file:///var/lib/goalert
package dir

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
)

func init() { storagebackend.RegisterObjects("file", open) }

func open(ctx context.Context, u *url.URL) (storagebackend.Objects, error) {
	if err := storagebackend.NewParams(u).Done(); err != nil {
		return storagebackend.Objects{}, err
	}
	if u.Host != "" || !strings.HasPrefix(u.Path, "/") || u.Path == "/" {
		return storagebackend.Objects{}, errors.New("dir: the URL must be file:/// and an absolute path")
	}
	path := filepath.FromSlash(u.Path)
	if runtime.GOOS == "windows" {
		path = strings.TrimPrefix(path, `\`) // file:///C:/path
	}
	store, err := objstore.NewDir(path)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	return storagebackend.Objects{Store: store, Location: "file://" + strings.TrimRight(u.Path, "/")}, nil
}
