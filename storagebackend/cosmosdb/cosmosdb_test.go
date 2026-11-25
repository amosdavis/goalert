package cosmosdb

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpen checks which URLs are accepted, and that opening contacts nothing.
func TestOpen(t *testing.T) {
	t.Setenv(KeyEnv, "c2VjcmV0")
	for raw, location := range map[string]string{
		"cosmos://account.documents.azure.com/db/c":          "",
		"cosmos://account.documents.azure.com/db/c/":         "",
		"cosmos://account.documents.azure.com/db/c/prefix/x": "",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		o, err := open(context.Background(), u)
		require.NoError(t, err, raw)
		assert.NotNil(t, o.Store)
		assert.Equal(t, location, o.Location, raw)
		if o.Close != nil {
			require.NoError(t, o.Close())
		}
	}
	for _, raw := range []string{
		"cosmos://account.documents.azure.com",
		"cosmos://account.documents.azure.com/db",
		"cosmos:///db/c",
		"cosmos://account.documents.azure.com//c",
		"cosmos://account.documents.azure.com/db/c?x=1",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, err = open(context.Background(), u)
		assert.Error(t, err, raw)
	}
}
