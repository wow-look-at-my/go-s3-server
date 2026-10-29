package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opening the cache starts the key index load, with no lookup made. The load
// and its TLS handshake then overlap the go command's package loading. The
// build's first lookup finds an index instead of probing the server.
func TestOpenCacheStartsTheIndexLoad(t *testing.T) {
	t.Setenv(brokerOffEnv, "1")
	asked := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bk/_index" {
			select {
			case asked <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	store, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	cache, err := OpenCache(t.TempDir(), store)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })

	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("opening the cache did not request the key index")
	}
}

// OpenCache starts the index load on a goroutine, which can run after Close.
// A load that starts then writes its copy into IndexDir after the caller has
// let the directory go.
func TestCloseStopsAnIndexLoadThatHasNotStarted(t *testing.T) {
	asked := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bk/_index" {
			select {
			case asked <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	store, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	store.ensureIndex()

	select {
	case <-asked:
		t.Fatal("a closed store started its key index load")
	case <-time.After(200 * time.Millisecond):
	}
	require.Nil(t, store.indexLoad.Load(), "a closed store holds an index load")
}
