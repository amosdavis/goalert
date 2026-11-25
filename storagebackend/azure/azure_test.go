package azure

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpen checks which URLs are accepted, and that opening contacts nothing.
func TestOpen(t *testing.T) {
	for raw, location := range map[string]string{
		"az://account1/container/":    "abfss://container@account1.dfs.core.windows.net/",
		"az://account1/container/a/b": "abfss://container@account1.dfs.core.windows.net/a/b/",
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
		"az://account1",
		"az://account1/",
		"az:///container",
		"az://Account.example.com/container",
		"az://account1/Container",
		"az://account1/c--d",
		"az://account1/container/..",
		"az://account1/container?x=1",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, err = open(context.Background(), u)
		assert.Error(t, err, raw)
	}
}
