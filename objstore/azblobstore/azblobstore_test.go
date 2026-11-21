package azblobstore_test

import (
	"context"
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
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

	t.Run("CheckSettings", func(t *testing.T) {
		s, err := azblobstore.New(c, "settings/")
		require.NoError(t, err)
		problems, err := s.CheckSettings(ctx)
		require.NoError(t, err)
		require.Empty(t, problems, "the private test container has no problems")

		const public = "goalert-public"
		_, err = client.CreateContainer(ctx, public, &azblob.CreateContainerOptions{Access: to.Ptr(azblob.PublicAccessTypeContainer)})
		if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			require.NoError(t, err, "create public container")
		}
		s, err = azblobstore.New(client.ServiceClient().NewContainerClient(public), "")
		require.NoError(t, err)
		problems, err = s.CheckSettings(ctx)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.True(t, problems[0].Breaks, "public access exposes alert data (F20): %s", problems[0])
	})
}

// TestWrongKey checks that refused credentials are reported as such (F33).
func TestWrongKey(t *testing.T) {
	connStr := os.Getenv("GOALERT_TEST_AZURE_CONNECTION_STRING")
	if connStr == "" {
		t.Skip("set GOALERT_TEST_AZURE_CONNECTION_STRING")
	}
	parts := strings.Split(connStr, ";")
	for i, p := range parts {
		if strings.HasPrefix(p, "AccountKey=") {
			parts[i] = "AccountKey=" + base64.StdEncoding.EncodeToString(make([]byte, 64))
		}
	}
	client, err := azblob.NewClientFromConnectionString(strings.Join(parts, ";"), nil)
	require.NoError(t, err)
	s, err := azblobstore.New(client.ServiceClient().NewContainerClient("goalert-test"), "wrong-key/")
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Get(ctx, "x")
	require.ErrorIs(t, err, objstore.ErrDenied)
	require.ErrorIs(t, s.PutIfAbsent(ctx, "x", []byte("1")), objstore.ErrDenied)
	require.ErrorIs(t, objstore.SelfTest(ctx, s), objstore.ErrDenied)
}
