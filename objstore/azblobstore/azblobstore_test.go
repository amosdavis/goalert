package azblobstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/azblobstore"
	"github.com/target/goalert/objstore/objstoretest"
)

// TestConformance runs the object-store conformance suite against Azure Blob
// Storage: the Azurite emulator in CI, or a real account.
func TestConformance(t *testing.T) {
	connStr := os.Getenv("GOALERT_TEST_AZURE_CONNECTION_STRING")
	if connStr == "" {
		t.Skip("set GOALERT_TEST_AZURE_CONNECTION_STRING")
	}
	ctx := context.Background()
	client, err := azblob.NewClientFromConnectionString(connStr, nil)
	require.NoError(t, err)

	const name = "goalert-test"
	_, err = client.CreateContainer(ctx, name, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		require.NoError(t, err, "create container")
	}
	c := client.ServiceClient().NewContainerClient(name)

	objstoretest.Run(t, func(t *testing.T) objstore.Store {
		s, err := azblobstore.New(c, "conformance/"+uuid.NewString()+"/")
		require.NoError(t, err)
		return s
	})
}
