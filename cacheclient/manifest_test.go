package cacheclient

import (
	"bytes"
	"encoding/hex"
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

// A found manifest stops the server look-ahead, which would only fetch bodies the build does not use.
func TestManifestCoversTheLookAhead(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("a7", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one))
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	require.False(t, b.lookAhead.covered.Load())
	b.SetModule("example.com/m")
	b.manifestWG.Wait()

	require.True(t, b.lookAhead.covered.Load())
	b.lookAhead.Seed([]string{"go-buildcache/v1" + one})
	require.NoError(t, b.Close())
	require.Equal(t, uint32(0), b.lookAhead.Requests.Load(), "a covered pool sends nothing")
}

// With no manifest on the server, the look-ahead stays on.
func TestNoManifestLeavesTheLookAhead(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	srv := fakeBatchServer(t, map[string][]byte{}, map[string]map[string]string{})
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	b.SetModule("example.com/m")
	b.manifestWG.Wait()
	require.False(t, b.lookAhead.covered.Load())
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
	_, _, _, miss := b.Get(used)
	require.True(t, miss)
	require.NoError(t, b.Put(used, StoredDigest([]byte("body")), []byte("body")))
	storedOnly := strings.Repeat("c4", 32)
	require.NoError(t, b.Put(storedOnly, StoredDigest([]byte("other")), []byte("other")))
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
	require.True(t, b.manifest.used.Contains(used), "a looked-up key is named")
	require.True(t, b.manifest.used.Contains(storedOnly), "a key stored without a lookup is named: the next build looks it up")
}

// A lookup the disk tier answers is named too, or a warm build stores a manifest that names almost nothing.
func TestManifestNamesDiskHits(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	tier := layerStore(disk, b)
	var id ActionID
	id[0] = 7
	_, _, err = disk.Put(id, bytes.NewReader([]byte("body")))
	require.NoError(t, err)
	_, tierName, err := tier.GetTiered(id)
	require.NoError(t, err)
	require.Equal(t, TierDisk, tierName)
	require.True(t, b.manifest.used.Contains(hex.EncodeToString(id[:])))
}

// A lookup for an object the manifest fetch has on the wire waits for it, and
// a lookup for any other object does not.
func TestAwaitPrefetchWaitsForTheFetch(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
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

// A lookup stops waiting for a slow manifest fetch after manifestWaitCap.
func TestAwaitPrefetchGivesUpOnASlowFetch(t *testing.T) {
	t.Serial() // manifestWaitCap is package state
	old := manifestWaitCap
	manifestWaitCap = 50 * time.Millisecond
	defer func() { manifestWaitCap = old }()

	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	onWire := strings.Repeat("e7", 32)
	clear := b.manifest.markPending([]string{onWire})
	defer clear()
	start := time.Now()
	require.False(t, b.AwaitPrefetch(onWire))
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint32(1), b.manifest.GaveUp.Load())
	require.Zero(t, b.manifest.Waited.Load())
}

// The manifest names keys in the order the build first looked them up, so the next build fetches the first ones first.
func TestManifestKeepsLookupOrder(t *testing.T) {
	m := newBuildManifest()
	ids := []string{strings.Repeat("f1", 32), strings.Repeat("a2", 32), strings.Repeat("c3", 32)}
	for _, id := range ids {
		m.note(id)
	}
	m.note(ids[1])
	require.Equal(t, ids, m.order)
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
