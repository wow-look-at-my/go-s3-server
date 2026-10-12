package cacheclient

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// seedObject stores body under actionID the way a PUT would.
func seedObject(t *testing.T, store map[string][]byte, meta map[string]map[string]string, actionID string, body []byte) {
	t.Helper()
	compressed, err := Compress(body)
	require.NoError(t, err)
	key := "go-buildcache/v1" + actionID
	store[key] = compressed
	meta[key] = map[string]string{"outputid": StoredDigest(body)}
}

// The manifest names objects, and the client pulls them into the local tier
// before the build asks.
func TestManifestFetchesWhatItNames(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("a1", 32)
	two := strings.Repeat("b2", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, two, []byte("object two"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one+"\n"+two+"\nnot-an-id"))
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	var mu sync.Mutex
	got := set.New[string]()
	b.OnBatchEntries = func(entries []BatchEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			got.Add(e.Key)
		}
	}
	b.SetModule("example.com/m")
	b.manifestWG.Wait()

	mu.Lock()
	require.True(t, got.Contains("go-buildcache/v1"+one))
	require.True(t, got.Contains("go-buildcache/v1"+two))
	mu.Unlock()
	require.Equal(t, uint32(2), b.manifest.Named.Load())
	require.Equal(t, uint32(2), b.manifest.Fetched.Load())
	require.NoError(t, b.Close())
}

// Close stores the action IDs the build used under the module's manifest key.
func TestManifestStoresWhatTheBuildUsed(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	var mu sync.Mutex
	var uploads [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method == "PUT" || strings.HasSuffix(r.URL.Path, "/_batch/put") {
			mu.Lock()
			uploads = append(uploads, body)
			mu.Unlock()
			w.WriteHeader(200)
			_, _ = w.Write([]byte("{}"))
			return
		}
		w.WriteHeader(404)
	}))
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	b.SetModule("example.com/m")
	used := strings.Repeat("c3", 32)
	require.NoError(t, b.Put(used, StoredDigest([]byte("body")), []byte("body")))
	require.NoError(t, b.Close())

	id := manifestID("example.com/m", "", "")
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, u := range uploads {
		if bytes.Contains(u, []byte(id)) {
			found = true
		}
	}
	require.True(t, found, "Close must upload the manifest")
}

// A lookup for an object the manifest fetch has on the wire waits for it, and
// a lookup for any other object does not.
func TestAwaitPrefetchWaitsForTheFetch(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	b.manifest.started.Store(true)
	close(b.manifest.ready)
	onWire := strings.Repeat("e5", 32)
	clear := b.manifest.markPending([]string{onWire})
	require.False(t, b.AwaitPrefetch(strings.Repeat("f6", 32)))

	waited := make(chan bool, 1)
	go func() { waited <- b.AwaitPrefetch(onWire) }()
	select {
	case <-waited:
		t.Fatal("the lookup returned while the fetch was on the wire")
	case <-time.After(50 * time.Millisecond):
	}
	clear()
	require.True(t, <-waited)
	require.False(t, b.AwaitPrefetch(onWire), "a landed fetch holds nothing up")
	require.Equal(t, uint32(1), b.manifest.Waited.Load())
}

// A build with no module stores no manifest and fetches nothing.
func TestManifestNeedsAModule(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	b.manifest.note(strings.Repeat("d4", 32))
	b.startManifest("")
	require.Nil(t, b.manifest.id.Load())
	b.storeManifest()
	b.reportManifest()
}
