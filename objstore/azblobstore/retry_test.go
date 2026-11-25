package azblobstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

// TestHiddenRetry checks the case the SDK's own retries hide: an attempt that
// may have been applied, followed by one that is refused. Reporting that as a
// failure would lose a commit that happened (F32).
func TestHiddenRetry(t *testing.T) {
	ctx := context.Background()
	for _, first := range []int{http.StatusInternalServerError, 0} {
		for _, then := range []int{http.StatusForbidden, http.StatusBadRequest, http.StatusConflict} {
			var n atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if n.Add(1) == 1 {
					if first == 0 {
						panic(http.ErrAbortHandler)
					}
					w.WriteHeader(first)
					return
				}
				if then == http.StatusConflict {
					w.Header().Set("x-ms-error-code", "BlobAlreadyExists")
				}
				w.WriteHeader(then)
			}))
			c, err := container.NewClientWithNoCredential(srv.URL+"/c", nil)
			require.NoError(t, err)
			s, err := New(c, "")
			require.NoError(t, err)
			err = s.PutIfAbsent(ctx, "o", []byte("x"))
			require.EqualValues(t, 2, n.Load(), "the SDK retried")
			if then == http.StatusConflict {
				require.ErrorIs(t, err, objstore.ErrExists, "which callers check for being theirs")
			} else {
				require.ErrorIs(t, err, objstore.ErrUnknown, "%d, then %d", first, then)
				require.NotErrorIs(t, err, objstore.ErrDenied)
			}
			srv.Close()
		}
	}
}
