package cosmosdb

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBadKey checks that a key that can't be used is not repeated (F20).
func TestBadKey(t *testing.T) {
	t.Setenv(KeyEnv, "not base64!")
	u, err := url.Parse("cosmos://account.documents.azure.com/db/c")
	require.NoError(t, err)
	_, err = open(context.Background(), u)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "base64!")
}
