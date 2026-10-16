package cacheclient

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fetch answers a miss at once while the server answers. While it is down,
// Fetch waits for the server to answer again, then asks again.
func TestFetchWaitsOutADownServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	store, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	tier := layerStore(disk, store)
	tier.store.awaitIndex()

	var absent ActionID
	absent[0] = 0xaa
	_, err = tier.Fetch(absent)
	require.Error(t, err, "a server that answers makes a miss final")

	var needed ActionID
	needed[0] = 0xbb
	// No probe is due while the test waits, so only the server's return wakes Fetch.
	tier.store.downSince.Store(time.Now().UnixNano())
	tier.store.probeAt.Store(time.Now().Add(time.Hour).UnixNano())
	arrived := make(chan error, 1)
	go func() {
		_, _, putErr := tier.DiskCache.Put(needed, bytes.NewReader([]byte("archive")))
		tier.store.noteAnswered()
		arrived <- putErr
	}()
	entry, err := tier.Fetch(needed)
	require.NoError(t, <-arrived)
	require.NoError(t, err, "a down server is waited out")
	require.Equal(t, int64(len("archive")), entry.Size)
}
