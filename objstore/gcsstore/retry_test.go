package gcsstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
	"google.golang.org/api/option"
)

// TestNoHiddenRetry checks that the client library does not retry a write on
// its own. If an attempt may have been applied, and one it hides is refused,
// reporting that as a failure would lose a commit that happened (F32).
func TestNoHiddenRetry(t *testing.T) {
	ctx := context.Background()
	for _, then := range []int{http.StatusForbidden, http.StatusBadRequest, http.StatusPreconditionFailed} {
		var n atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if n.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(then)
		}))
		client, err := storage.NewClient(ctx, option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
		require.NoError(t, err)
		s, err := New(client, "bucket", "")
		require.NoError(t, err)
		err = s.PutIfAbsent(ctx, "o", []byte("x"))
		require.EqualValues(t, 1, n.Load())
		require.ErrorIs(t, err, objstore.ErrUnknown)
		require.NotErrorIs(t, err, objstore.ErrDenied)
		srv.Close()
	}
}
