package azblobstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDirectories checks that the directories of an account with a
// hierarchical namespace (ADLS Gen2) are not listed as objects. Azurite has no
// such namespace, so this is a reply as the service gives it.
func TestListDirectories(t *testing.T) {
	const reply = `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="http://127.0.0.1/" ContainerName="c">
  <Prefix>p/tbl=</Prefix>
  <Blobs>
    <Blob>
      <Name>p/tbl=alerts</Name>
      <Properties><Content-Length>0</Content-Length><ResourceType>directory</ResourceType></Properties>
      <Metadata><hdi_isfolder>true</hdi_isfolder></Metadata>
    </Blob>
    <Blob>
      <Name>p/tbl=alerts/bucket=0</Name>
      <Properties><Content-Length>0</Content-Length></Properties>
      <Metadata><hdi_isfolder>true</hdi_isfolder></Metadata>
    </Blob>
    <Blob>
      <Name>p/tbl=alerts/bucket=0/part-1.parquet</Name>
      <Properties><Content-Length>3</Content-Length><ResourceType>file</ResourceType></Properties>
    </Blob>
    <Blob>
      <Name>p/tbl=users/bucket=1/part-2.parquet</Name>
      <Properties><Content-Length>3</Content-Length></Properties>
      <Metadata><other>x</other></Metadata>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>`
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(reply))
	}))
	defer srv.Close()
	c, err := container.NewClientWithNoCredential(srv.URL+"/c", nil)
	require.NoError(t, err)
	s, err := New(c, "p/")
	require.NoError(t, err)

	names, err := s.List(context.Background(), "tbl=", "")
	require.NoError(t, err)
	assert.Equal(t, []string{"tbl=alerts/bucket=0/part-1.parquet", "tbl=users/bucket=1/part-2.parquet"}, names)
	assert.Contains(t, query, "include=metadata", "metadata is what marks a directory for the Blob API")
}
