package cacheclient

import (
	"archive/tar"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// A look-ahead request that the server refuses is counted as failed, with the
// server's reason, and never as an empty window.
func TestLookAheadCountsARefusedRequest(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too many keys", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	b.OnBatchEntries = func([]BatchEntry) {}
	la := b.lookAhead
	require.NotNil(t, la)

	la.expand([]string{"seed-one"})
	require.Equal(t, uint32(1), la.Failed.Load())
	require.Zero(t, la.Empty.Load())
	last := la.lastFailure.Load()
	require.NotNil(t, last)
	require.Contains(t, *last, "HTTP 400")
	require.Contains(t, *last, "too many keys")
}

// A window with no entries is counted as empty, not as failed.
func TestLookAheadCountsAnEmptyWindow(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, tar.NewWriter(w).Close())
	}))
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	b.OnBatchEntries = func([]BatchEntry) {}
	la := b.lookAhead
	require.NotNil(t, la)

	la.expand([]string{"seed-one"})
	require.Equal(t, uint32(1), la.Empty.Load())
	require.Zero(t, la.Failed.Load())
}
