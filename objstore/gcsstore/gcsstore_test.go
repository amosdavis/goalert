package gcsstore_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/gcsstore"
	"github.com/target/goalert/objstore/objstoretest"
	"google.golang.org/api/googleapi"
)

// TestConformance runs the object-store conformance suite against a GCS
// bucket: the fake-gcs-server emulator in CI (STORAGE_EMULATOR_HOST), or a
// real bucket.
func TestConformance(t *testing.T) {
	bucket := os.Getenv("GOALERT_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Skip("set GOALERT_TEST_GCS_BUCKET, and STORAGE_EMULATOR_HOST to use the emulator")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		// The emulator starts empty.
		err := client.Bucket(bucket).Create(ctx, "goalert-test", nil)
		var gerr *googleapi.Error
		if err != nil && (!errors.As(err, &gerr) || gerr.Code != http.StatusConflict) {
			require.NoError(t, err, "create bucket")
		}
	}

	objstoretest.Run(t, func(t *testing.T) objstore.Store {
		s, err := gcsstore.New(client, bucket, "conformance/"+uuid.NewString()+"/")
		require.NoError(t, err)
		return s
	})

	t.Run("CheckSettings", func(t *testing.T) {
		s, err := gcsstore.New(client, bucket, "settings/")
		require.NoError(t, err)
		problems, err := s.CheckSettings(ctx)
		require.NoError(t, err)
		for _, p := range problems {
			require.False(t, p.Breaks, "the test bucket breaks nothing: %s", p)
		}
	})
}
