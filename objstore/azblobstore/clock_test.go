package azblobstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

// TestClock checks that the store compares Azure's clock with this pod's, from
// the Date of its replies, errors included (F13).
func TestClock(t *testing.T) {
	ctx := context.Background()
	var offset atomic.Int64 // how far Azure's clock is ahead
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Date", time.Now().Add(time.Duration(offset.Load())).UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := container.NewClientWithNoCredential(srv.URL+"/c", nil)
	require.NoError(t, err)
	s, err := New(c, "")
	require.NoError(t, err)
	const limit = 15 * time.Second
	_, _, ok := s.ClockOffset()
	assert.False(t, ok, "no reply yet")

	offset.Store(int64(-time.Hour))
	require.NoError(t, s.PutIfAbsent(ctx, "a", []byte("x")))
	_, hi, ok := s.ClockOffset()
	require.True(t, ok)
	assert.Less(t, hi, -59*time.Minute)
	require.ErrorContains(t, objstore.ClockError(s, limit), "ahead of the storage's")

	offset.Store(0)
	_, err = s.Get(ctx, "a")
	require.ErrorIs(t, err, objstore.ErrNotFound)
	require.NoError(t, objstore.ClockError(s, limit), "a reply that is an error has a date too")
}
