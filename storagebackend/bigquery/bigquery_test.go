package bigquery

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
)

// TestURLs checks which URLs are refused, before anything is contacted.
func TestURLs(t *testing.T) {
	for _, raw := range []string{
		"bigquery://proj",
		"bigquery:///ds?location=US",
		"bigquery://proj?location=",
		"bigquery://proj?location=US&typo=1",
		"bigquery://proj?location=US&table=t",
		"bigquery://proj?location=US&connection=c",
		"bigquery://proj/ds?location=US",
		"bigquery://proj/ds/x?location=US&connection=c",
		"bigquery://proj/d-s?location=US&connection=c",
		"bigquery://PROJ?location=US",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, _, err = open(context.Background(), storagebackend.LogConfig{
			URL: u, Objects: storagebackend.Objects{Store: objstore.NewMem(), Location: "gs://bucket/goalert/"}, WriterID: "w",
		})
		assert.Error(t, err, raw)
	}
	// BigQuery can't read what is not in a bucket.
	u, err := url.Parse("bigquery://proj/ds?location=US&connection=c")
	require.NoError(t, err)
	for _, location := range []string{"", "file:///tmp/x", "abfss://c@a.dfs.core.windows.net/"} {
		_, _, err = open(context.Background(), storagebackend.LogConfig{URL: u, Objects: storagebackend.Objects{Store: objstore.NewMem(), Location: location}})
		assert.Error(t, err, location)
	}
}
