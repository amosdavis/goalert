// Package cosmosdb registers the "cosmos" object store: an Azure Cosmos DB for
// NoSQL container. The account must use Strong consistency.
//
//	cosmos://account.documents.azure.com/database/container/prefix
//
// It uses the default Azure credential, such as a managed identity. If
// GOALERT_COSMOS_KEY is set, that account key is used instead.
package cosmosdb

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/target/goalert/objstore/chunkstore"
	"github.com/target/goalert/objstore/cosmosstore"
	"github.com/target/goalert/storagebackend"
)

// KeyEnv names the environment variable that may hold an account key.
const KeyEnv = "GOALERT_COSMOS_KEY"

func init() { storagebackend.RegisterObjects("cosmos", open) }

func open(ctx context.Context, u *url.URL) (storagebackend.Objects, error) {
	if err := storagebackend.NewParams(u).Done(); err != nil {
		return storagebackend.Objects{}, err
	}
	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 3)
	if u.Host == "" || len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return storagebackend.Objects{}, errors.New("cosmosdb: the URL must be cosmos://HOST/DATABASE/CONTAINER/PREFIX")
	}
	cfg := cosmosstore.Config{Database: parts[0], Container: parts[1]}
	if len(parts) == 3 {
		cfg.Prefix = parts[2]
	}
	client, err := newClient("https://" + u.Host)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	records, err := cosmosstore.New(client, cfg)
	if err != nil {
		return storagebackend.Objects{}, err
	}
	store, err := chunkstore.New(records, chunkstore.Options{})
	if err != nil {
		return storagebackend.Objects{}, err
	}
	return storagebackend.Objects{Store: store}, nil
}

func newClient(endpoint string) (*azcosmos.Client, error) {
	if key := os.Getenv(KeyEnv); key != "" {
		cred, err := azcosmos.NewKeyCredential(key)
		if err != nil {
			// The error may repeat the key.
			return nil, errors.New("cosmosdb: " + KeyEnv + " is not an account key")
		}
		return azcosmos.NewClientWithKey(endpoint, cred, nil)
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return azcosmos.NewClient(endpoint, cred, nil)
}
