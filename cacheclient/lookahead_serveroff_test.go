package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// A server with prefetch off answers every look-ahead request with nothing. The
// first answer that says so stops the pool, so the process sends no more.
func TestLookAheadStopsWhenTheServerSendsNoWindow(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "1")
	var asked atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		w.Header().Set(HeaderPrefetch, "off")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	b.OnBatchEntries = func([]BatchEntry) {}
	la := b.lookAhead
	require.NotNil(t, la)

	la.expand([]string{"seed-one"})
	require.True(t, la.serverOff.Load(), "the header turns the pool off")
	require.Equal(t, int32(1), asked.Load())

	la.Seed([]string{"seed-two"})
	require.Zero(t, len(la.seeds), "a later seed is dropped unsent")
}
