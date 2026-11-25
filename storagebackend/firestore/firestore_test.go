package firestore

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpen checks which URLs are accepted, and that opening contacts nothing.
func TestOpen(t *testing.T) {
	t.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:1")
	for raw, location := range map[string]string{
		"firestore://project":                  "",
		"firestore://project/":                 "",
		"firestore://project/(default)/prefix": "",
		"firestore://project/ops":              "",
		"firestore://project/ops/prefix/":      "",
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
		"firestore:///ops",
		"firestore://project/ops/a/b",
		"firestore://project/ops/__x__",
		"firestore://project/ops?x=1",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, err = open(context.Background(), u)
		assert.Error(t, err, raw)
	}
}
