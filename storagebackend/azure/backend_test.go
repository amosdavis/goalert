package azure_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore/azblobstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/storagebackend/azure"
	"github.com/target/goalert/storagebackend/backendtest"
)

const testContainer = "goalert-test"

// connString returns the connection string of the account to test with, the
// Azurite emulator in CI or a real account, and the name of that account. The
// test container exists once it returns.
func connString(t *testing.T) (conn, account string) {
	t.Helper()
	conn = os.Getenv("GOALERT_TEST_AZURE_CONNECTION_STRING")
	if conn == "" {
		t.Skip("set GOALERT_TEST_AZURE_CONNECTION_STRING")
	}
	for _, part := range strings.Split(conn, ";") {
		if v, ok := strings.CutPrefix(part, "AccountName="); ok {
			account = v
		}
	}
	require.NotEmpty(t, account, "the connection string names no account")
	client, err := azblob.NewClientFromConnectionString(conn, nil)
	require.NoError(t, err)
	_, err = client.CreateContainer(context.Background(), testContainer, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		require.NoError(t, err, "create container")
	}
	return conn, account
}

// TestBackend runs the whole-backend suite on Azure: several pods use one table
// at once, each opened as a deployment opens it, by URL, with a client and
// connections of its own (F14).
func TestBackend(t *testing.T) {
	conn, account := connString(t)
	t.Setenv(azure.ConnectionStringEnv, conn)
	backendtest.Run(t, func(t *testing.T) func(string) *storagebackend.Backend {
		u := fmt.Sprintf("az://%s/%s/backend/%s/", account, testContainer, uuid.NewString())
		return func(writer string) *storagebackend.Backend {
			b, err := storagebackend.New(context.Background(), storagebackend.Config{Objects: u}, writer)
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			return b
		}
	})
}

// TestBackendUnreliable runs pods at once while Azure, or the network to it,
// fails some of their requests: some are refused as a busy server does, some
// fail before they are sent, and some are carried out with the reply lost
// (F32). The Azure SDK and the store see each failure as they would for real.
func TestBackendUnreliable(t *testing.T) {
	conn, _ := connString(t)
	prefix := "unreliable/" + uuid.NewString() + "/"
	connect := func(transport policy.Transporter, writer string) *storagebackend.Backend {
		var opts *container.ClientOptions
		if transport != nil {
			opts = &container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: transport}}
		}
		c, err := container.NewClientFromConnectionString(conn, testContainer, opts)
		require.NoError(t, err)
		store, err := azblobstore.New(c, prefix)
		require.NoError(t, err)
		return &storagebackend.Backend{Store: store, Log: deltalog.New(store, writer)}
	}

	seed, rate := uint64(time.Now().UnixNano()), 0.1
	if s := os.Getenv("GOALERT_TEST_FAULT_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseUint(s, 10, 64)
		require.NoError(t, err)
	}
	if s := os.Getenv("GOALERT_TEST_FAULT_RATE"); s != "" {
		var err error
		rate, err = strconv.ParseFloat(s, 64)
		require.NoError(t, err)
	}
	t.Logf("seed %d (GOALERT_TEST_FAULT_SEED), rate %v (GOALERT_TEST_FAULT_RATE)", seed, rate)
	var faults []*backendtest.Faults
	backendtest.RunUnreliable(t,
		func(writer string) *storagebackend.Backend { return connect(nil, writer) },
		func(writer string) *storagebackend.Backend {
			f := backendtest.NewFaults(seed+uint64(len(faults)), rate, 5*time.Millisecond)
			faults = append(faults, f)
			return connect(f, writer)
		})

	var refused, failed, lost int64
	for _, f := range faults {
		r, fl, l := f.Counts()
		refused, failed, lost = refused+r, failed+fl, lost+l
	}
	t.Logf("faults: %d requests refused, %d failed before they were sent, %d carried out with the reply lost", refused, failed, lost)
	require.NotZero(t, lost, "replies were lost, the case that matters most")
}
