// Package azure registers the "az" object store: an Azure Blob Storage or ADLS
// Gen2 container.
//
//	az://account/container/prefix
//
// It uses the default Azure credential, such as a managed identity. If
// GOALERT_AZURE_STORAGE_CONNECTION_STRING is set, its key is used instead.
package azure

import (
	"context"
	"errors"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/target/goalert/objstore/azblobstore"
	"github.com/target/goalert/storagebackend"
)

// ConnectionStringEnv names the environment variable that may hold a
// connection string.
const ConnectionStringEnv = "GOALERT_AZURE_STORAGE_CONNECTION_STRING"

func init() { storagebackend.RegisterObjects("az", open) }

var (
	validAccount   = regexp.MustCompile(`^[a-z0-9]{3,24}$`)
	validContainer = regexp.MustCompile(`^[a-z0-9](-?[a-z0-9]){2,62}$`)
)

func open(ctx context.Context, u *url.URL) (storagebackend.Objects, error) {
	if err := storagebackend.NewParams(u).Done(); err != nil {
		return storagebackend.Objects{}, err
	}
	account := u.Host
	name, rest, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	if !validAccount.MatchString(account) || !validContainer.MatchString(name) {
		return storagebackend.Objects{}, errors.New("azure: the URL must be az://ACCOUNT/CONTAINER/PREFIX")
	}
	prefix, err := storagebackend.Prefix(rest)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	client, err := newClient(account, name)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	store, err := azblobstore.New(client, prefix)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	return storagebackend.Objects{Store: store, Location: "abfss://" + name + "@" + account + ".dfs.core.windows.net/" + prefix}, nil
}

func newClient(account, name string) (*container.Client, error) {
	if s := os.Getenv(ConnectionStringEnv); s != "" {
		return container.NewClientFromConnectionString(s, name, nil)
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return container.NewClient("https://"+account+".blob.core.windows.net/"+name, cred, nil)
}
