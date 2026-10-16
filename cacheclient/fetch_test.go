package cacheclient

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fetch answers a miss at once while the server answers, and asks again while
// it is down, until the object arrives.
func TestFetchWaitsOutADownServer(t *testing.T) {
	t.Serial() // fetchRetryEvery is package state
	old := fetchRetryEvery
	fetchRetryEvery = 10 * time.Millisecond
	defer func() { fetchRetryEvery = old }()

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
	tier.store.downSince.Store(time.Now().UnixNano())
	arrived := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _, putErr := tier.DiskCache.Put(needed, bytes.NewReader([]byte("archive")))
		require.NoError(t, putErr)
		close(arrived)
	}()
	entry, err := tier.Fetch(needed)
	require.NoError(t, err, "a down server is waited out")
	<-arrived
	require.Equal(t, int64(len("archive")), entry.Size)
}
