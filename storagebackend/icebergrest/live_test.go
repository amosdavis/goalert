package icebergrest

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/backendtest"
	_ "github.com/target/goalert/storagebackend/dir"
	_ "github.com/target/goalert/storagebackend/gcs"
)

// TestLive runs the whole-backend suite on a real REST catalog. The catalog
// writes its metadata to the object store as well, so it must reach it:
//
//	GOALERT_TEST_ICEBERG_URL      iceberg+http://localhost:8181?namespace=goalert_test
//	GOALERT_TEST_ICEBERG_OBJECTS  file:///shared/warehouse, or gs://bucket/prefix
//
// Every table gets a name and a prefix of its own.
func TestLive(t *testing.T) {
	rawLog, rawObjects := os.Getenv("GOALERT_TEST_ICEBERG_URL"), os.Getenv("GOALERT_TEST_ICEBERG_OBJECTS")
	if rawLog == "" || rawObjects == "" {
		t.Skip("set GOALERT_TEST_ICEBERG_URL and GOALERT_TEST_ICEBERG_OBJECTS")
	}
	ctx := context.Background()
	u, err := url.Parse(rawLog)
	require.NoError(t, err)
	base := url.URL{Scheme: strings.TrimPrefix(u.Scheme, "iceberg+"), Host: u.Host, Path: u.Path}
	opts, transport, err := options(ctx, storagebackend.NewParams(u))
	require.NoError(t, err)
	cat, err := rest.NewCatalog(ctx, "setup", base.String(), append(opts, rest.WithCustomTransport(transport))...)
	require.NoError(t, err)
	namespace := table.Identifier(strings.Split(u.Query().Get("namespace"), "."))
	if err := cat.CreateNamespace(ctx, namespace, nil); !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		require.NoError(t, err)
	}

	backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
		name := "goalert_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		logURL := *u
		q := logURL.Query()
		q.Set("table", name)
		logURL.RawQuery = q.Encode()
		cfg := storagebackend.Config{
			Objects: strings.TrimRight(rawObjects, "/") + "/" + name,
			Log:     logURL.String(),
			Warn:    func(err error) { t.Log(err) },
		}
		t.Cleanup(func() {
			assert.NoError(t, cat.DropTable(context.Background(), append(namespace[:len(namespace):len(namespace)], name)))
		})
		return func(writer string) *storagebackend.Backend {
			b, err := storagebackend.New(ctx, cfg, writer)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, b.Close()) })
			return b
		}
	})
}
