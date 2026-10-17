package cacheclient

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// Each command of a module gets its own manifest. No command keeps the
// module-wide key that older clients read and write.
func TestManifestScopeSeparatesCommands(t *testing.T) {
	module := "example.com/m"
	require.Equal(t, manifestID(module, "v", "t"), manifestID(manifestScope(module, ""), "v", "t"))
	test := manifestID(manifestScope(module, "test runtime"), "v", "t")
	build := manifestID(manifestScope(module, "build std"), "v", "t")
	require.NotEqual(t, test, build)
	require.NotEqual(t, manifestID(module, "v", "t"), test)
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

// The manifest does not fetch an object the local tier already holds.
func TestManifestSkipsWhatIsLocal(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("a1", 32)
	two := strings.Repeat("b2", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, two, []byte("object two"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one+"\n"+two))
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	local, _ := parseActionHash(one)
	b.HasLocal = func(h actionHash) bool { return h == local }
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
	require.False(t, got.Contains("go-buildcache/v1"+one), "a local object is not fetched")
	require.True(t, got.Contains("go-buildcache/v1"+two))
	mu.Unlock()
	require.Equal(t, uint32(1), b.manifest.Fetched.Load())
	require.NoError(t, b.Close())
}

// A chunk does not fetch an object that came into the local tier after the
// walk named it. On a slow server a lookup fetches such an object itself while
// the chunk waits in the queue.
func TestManifestChunkSkipsWhatArrivedSinceTheWalk(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("c3", 32)
	two := strings.Repeat("d4", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, two, []byte("object two"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one+"\n"+two))
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	arrived, _ := parseActionHash(one)
	var asked atomic.Int32
	// The walk asks first and finds nothing. The chunk asks after, and the lookup's object is there.
	b.HasLocal = func(h actionHash) bool { return h == arrived && asked.Add(1) > 1 }
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
	require.False(t, got.Contains("go-buildcache/v1"+one), "an object that arrived since the walk is fetched again")
	require.True(t, got.Contains("go-buildcache/v1"+two))
	mu.Unlock()
	require.Equal(t, uint32(1), b.manifest.Fetched.Load())
	require.Equal(t, uint32(1), b.manifest.Skipped.Load())
	require.NoError(t, b.Close())
}

// The manifest does not fetch an object this build already looked up while the walk ran.
func TestManifestSkipsWhatTheBuildLookedUp(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("a3", 32)
	two := strings.Repeat("b4", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, two, []byte("object two"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one+"\n"+two))
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
	b.manifest.note(one, "get")
	b.SetModule("example.com/m")
	b.manifestWG.Wait()

	mu.Lock()
	require.False(t, got.Contains("go-buildcache/v1"+one), "a looked-up object has its own request")
	require.True(t, got.Contains("go-buildcache/v1"+two))
	mu.Unlock()
	require.Equal(t, uint32(1), b.manifest.Fetched.Load())
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

// A manifest request the server never answers must not keep Close from
// returning. Close cancels the fetch after manifestCloseWait.
func TestCloseCancelsAStalledManifestFetch(t *testing.T) {
	t.Serial()
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if strings.HasSuffix(r.URL.Path, "/_batch/get") {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	saved := manifestCloseWait
	manifestCloseWait = 200 * time.Millisecond
	t.Cleanup(func() { manifestCloseWait = saved })

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	var logged []string
	SetLogger(recordingLogger{&logged})
	t.Cleanup(func() { SetLogger(nil) })
	b.SetModule("example.com/m")

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited on a manifest fetch the server never answered")
	}
	for _, msg := range logged {
		require.NotContains(t, msg, "manifest: read", "a read that Close cancels is not a failure")
	}
}

// A fetch of what the manifest names that Close cancels logs no warning.
func TestCloseCancelsAStalledManifestFetchQuietly(t *testing.T) {
	t.Serial() // the logger is package state
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	one := strings.Repeat("a1", 32)
	seedObject(t, store, meta, one, []byte("object one"))
	seedObject(t, store, meta, manifestID("example.com/m", "", ""), []byte(one))
	inner := fakeBatchServer(t, store, meta)
	t.Cleanup(inner.Close)
	stalled := make(chan struct{})
	var stallOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "go-buildcache/v1"+one) {
			stallOnce.Do(func() { close(stalled) })
			<-r.Context().Done()
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		inner.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	saved := manifestCloseWait
	manifestCloseWait = 200 * time.Millisecond
	t.Cleanup(func() { manifestCloseWait = saved })

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	var logged []string
	SetLogger(recordingLogger{&logged})
	t.Cleanup(func() { SetLogger(nil) })
	b.SetModule("example.com/m")
	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the manifest fetch never reached the server")
	}

	require.NoError(t, b.Close())
	for _, msg := range logged {
		require.NotContains(t, msg, "manifest: fetch:", "a fetch that Close cancels is not a failure")
	}
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
	chunk := b.manifest.markPending([]string{onWire})
	chunk.send()
	require.False(t, b.AwaitPrefetch(strings.Repeat("f6", 32)))

	waited := make(chan bool, 1)
	go func() { waited <- b.AwaitPrefetch(onWire) }()
	select {
	case <-waited:
		t.Fatal("the lookup returned while the fetch was on the wire")
	case <-time.After(50 * time.Millisecond):
	}
	chunk.finish()
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
	chunk := b.manifest.markPending([]string{onWire})
	chunk.send()
	defer chunk.finish()
	start := time.Now()
	require.False(t, b.AwaitPrefetch(onWire))
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint32(1), b.manifest.GaveUp.Load())
	require.Zero(t, b.manifest.Waited.Load())
}

// A lookup goes on as soon as its own entry lands, while the rest of its chunk is still on the wire.
func TestAwaitPrefetchGoesOnWhenItsEntryLands(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	first := strings.Repeat("e8", 32)
	second := strings.Repeat("e9", 32)
	chunk := b.manifest.markPending([]string{first, second})
	require.Equal(t, []string{first, second}, chunk.send())

	waited := make(chan bool, 1)
	go func() { waited <- b.AwaitPrefetch(first) }()
	select {
	case <-waited:
		t.Fatal("the lookup returned before its entry landed")
	case <-time.After(50 * time.Millisecond):
	}
	chunk.landed([]string{first})
	require.True(t, <-waited, "a landed entry releases its lookup before the chunk ends")

	b.manifest.mu.Lock()
	_, stillPending := b.manifest.pending[second]
	b.manifest.mu.Unlock()
	require.True(t, stillPending, "the rest of the chunk is still on the wire")
	chunk.finish()
	require.False(t, b.AwaitPrefetch(second))
}

// A lookup for an ID whose chunk has not gone out asks on its own at once, and the chunk leaves that ID out.
func TestAwaitPrefetchDoesNotWaitForAQueuedChunk(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	queued := strings.Repeat("ea", 32)
	other := strings.Repeat("eb", 32)
	chunk := b.manifest.markPending([]string{queued, other})

	start := time.Now()
	require.False(t, b.AwaitPrefetch(queued))
	require.Less(t, time.Since(start), manifestWaitCap/2, "a queued chunk holds no lookup up")
	require.Zero(t, b.manifest.GaveUp.Load())
	require.Zero(t, b.manifest.Waited.Load())
	require.Equal(t, []string{other}, chunk.send(), "the chunk leaves out the ID the lookup took")
	chunk.finish()
}

// fetchNamed hands each group's action IDs to landed after the local tier has the group, before the batch ends.
func TestFetchNamedReportsEachGroupAsItLands(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	var keys []string
	for idx := range lookAheadChunk + 1 {
		var raw [32]byte
		raw[0] = byte(idx + 1)
		actionID := hex.EncodeToString(raw[:])
		seedObject(t, store, meta, actionID, []byte("object "+actionID))
		keys = append(keys, "go-buildcache/v1"+actionID)
	}
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	stored := set.New[string]()
	b.OnBatchEntries = func(entries []BatchEntry) {
		for _, e := range entries {
			stored.Add(e.Key)
		}
	}
	var groups [][]string
	count, _ := b.fetchNamed(keys, func(ids []string) {
		for _, actionID := range ids {
			require.True(t, stored.Contains(b.key(actionID)), "an ID is reported only after its entry is stored")
		}
		groups = append(groups, append([]string(nil), ids...))
	})
	require.Equal(t, lookAheadChunk+1, count)
	require.Len(t, groups, 2)
	require.Len(t, groups[0], lookAheadChunk, "the first group lands before the batch ends")
	require.Len(t, groups[1], 1)
	require.NoError(t, b.Close())
}

// The manifest names keys in the order the build first looked them up, so the next build fetches the first ones first.
func TestManifestKeepsLookupOrder(t *testing.T) {
	m := newBuildManifest()
	ids := []string{strings.Repeat("f1", 32), strings.Repeat("a2", 32), strings.Repeat("c3", 32)}
	for _, id := range ids {
		m.note(id, "get")
	}
	m.note(ids[1], "get")
	require.Equal(t, ids, m.order)
}

// A build with no module stores no manifest and fetches nothing.
func TestManifestNeedsAModule(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	b.manifest.note(strings.Repeat("d4", 32), "get")
	b.startManifest("")
	require.Nil(t, b.manifest.id.Load())
	b.storeManifest()
	b.reportManifest()
}

// The manifest starts when both the module and the local tier are known, in either order.
func TestManifestStartsInEitherOpenOrder(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	one := strings.Repeat("e5", 32)
	for _, moduleFirst := range []bool{true, false} {
		store := map[string][]byte{}
		meta := map[string]map[string]string{}
		seedObject(t, store, meta, one, []byte("object one"))
		seedObject(t, store, meta, manifestID(manifestScope("example.com/m", "build"), "", ""), []byte(one))
		srv := fakeBatchServer(t, store, meta)
		t.Cleanup(srv.Close)

		b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", Command: "build", IndexDir: t.TempDir()})
		require.NoError(t, err)
		b.shortenIndexWait(10 * time.Millisecond)
		disk, err := Open(t.TempDir())
		require.NoError(t, err)
		if moduleFirst {
			b.SetModule("example.com/m")
			require.Nil(t, b.manifest.id.Load(), "no manifest starts before the tier exists")
			layerStore(disk, b)
		} else {
			layerStore(disk, b)
			b.SetModule("example.com/m")
		}
		b.manifestWG.Wait()
		require.Equal(t, "example.com/m", b.Module())
		require.Equal(t, uint32(1), b.manifest.Named.Load(), "module first: %v", moduleFirst)
		require.Equal(t, uint32(1), b.manifest.Fetched.Load(), "module first: %v", moduleFirst)
		require.NoError(t, b.Close())
		require.NoError(t, disk.Close())
	}
}

// The walk reads generations in order and uses the newest one, and a missing generation ends it.
func TestManifestWalkPicksTheNewestGeneration(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	base := manifestID("example.com/m", "", "")
	keys := []string{strings.Repeat("a1", 32), strings.Repeat("b2", 32), strings.Repeat("c3", 32), strings.Repeat("d4", 32)}
	for _, k := range keys {
		seedObject(t, store, meta, k, []byte("object "+k))
	}
	seedObject(t, store, meta, manifestGenID(base, 0), []byte(keys[0]))
	seedObject(t, store, meta, manifestGenID(base, 1), []byte(strings.Join(keys[:2], "\n")))
	seedObject(t, store, meta, manifestGenID(base, 2), []byte(strings.Join(keys[:3], "\n")))
	// A later generation is past a gap, so the walk never reaches it.
	seedObject(t, store, meta, manifestGenID(base, 4), []byte(strings.Join(keys, "\n")))
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	b.SetModule("example.com/m")
	b.manifestWG.Wait()

	require.Equal(t, 2, b.manifest.newest)
	require.Equal(t, keys[:3], b.manifest.newestNamed)
	require.Equal(t, uint32(3), b.manifest.Named.Load())
	require.NoError(t, b.Close())
}

// usedKey is the action ID storeManifestAfter notes as the build's key idx.
func usedKey(idx int) string {
	return hex.EncodeToString([]byte(strings.Repeat(string(rune('a'+idx)), 32)))
}

// usedKeys is usedKey for each index from first up to, but not including, last.
func usedKeys(first, last int) []string {
	var keys []string
	for idx := first; idx < last; idx++ {
		keys = append(keys, usedKey(idx))
	}
	return keys
}

// storeManifestAfter runs a build that notes count keys against a walk that found
// generation newest naming named, and returns the bodies it uploaded.
func storeManifestAfter(t *testing.T, newest int, named []string, count int) [][]byte {
	t.Helper()
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
	b.manifestWG.Wait()
	b.manifest.mu.Lock()
	b.manifest.newest, b.manifest.newestNamed = newest, named
	b.manifest.mu.Unlock()
	for idx := range count {
		b.manifest.note(usedKey(idx), "get")
	}
	require.NoError(t, b.Close())
	mu.Lock()
	defer mu.Unlock()
	return uploads
}

func uploadedAny(uploads [][]byte, id string) bool {
	for _, u := range uploads {
		if bytes.Contains(u, []byte(id)) {
			return true
		}
	}
	return false
}

// A newest generation that names too few keys gets a successor with this build's list.
func TestManifestShortGenerationGetsASuccessor(t *testing.T) {
	base := manifestID("example.com/m", "", "")
	uploads := storeManifestAfter(t, 1, usedKeys(0, 5), 10)
	require.True(t, uploadedAny(uploads, manifestGenID(base, 2)), "a short generation 1 gets generation 2")
	require.False(t, uploadedAny(uploads, base))
	require.False(t, uploadedAny(uploads, manifestGenID(base, 1)))
}

func TestManifestNearCompleteGenerationStays(t *testing.T) {
	base := manifestID("example.com/m", "", "")
	uploads := storeManifestAfter(t, 0, usedKeys(0, 9), 10)
	for gen := range manifestGenerations {
		require.False(t, uploadedAny(uploads, manifestGenID(base, gen)), "generation %d", gen)
	}
}

// A generation that names every key this build used and many keys it did not
// is stale. Each extra key is a download the next build never reads.
func TestManifestSupersetGenerationGetsASuccessor(t *testing.T) {
	base := manifestID("example.com/m", "", "")
	uploads := storeManifestAfter(t, 0, usedKeys(0, 16), 10)
	require.True(t, uploadedAny(uploads, manifestGenID(base, 1)), "a generation that names 6 unused keys of 10 gets a successor")
	var successor []byte
	for _, upload := range uploads {
		if bytes.Contains(upload, []byte(manifestGenID(base, 1))) {
			successor = upload
		}
	}
	for _, key := range usedKeys(10, 16) {
		require.False(t, bytes.Contains(successor, []byte(key)), "the successor does not name the unused key %s", key)
	}
}

// A generation with the same count of keys but other keys is stale. A count alone does not show that.
func TestManifestSameSizeOtherKeysGetsASuccessor(t *testing.T) {
	base := manifestID("example.com/m", "", "")
	uploads := storeManifestAfter(t, 0, usedKeys(5, 15), 10)
	require.True(t, uploadedAny(uploads, manifestGenID(base, 1)))
}

func TestManifestDrifted(t *testing.T) {
	used := usedKeys(0, 20)
	require.False(t, manifestDrifted(used, used))
	require.False(t, manifestDrifted(usedKeys(0, 18), used), "2 missing keys of 20 are 10%")
	require.False(t, manifestDrifted(usedKeys(0, 22), used), "2 extra keys of 20 are 10%")
	require.True(t, manifestDrifted(usedKeys(0, 23), used), "3 extra keys of 20 are 15%")
	require.True(t, manifestDrifted(usedKeys(1, 22), used), "1 missing and 2 extra keys of 20 are 15%")
	require.False(t, manifestDrifted(append(usedKeys(0, 20), usedKey(0)), used), "a repeated key counts one time")
}

// With no generation on the server, the build writes one generation.
func TestManifestFirstGenerationIsTheBaseID(t *testing.T) {
	uploads := storeManifestAfter(t, -1, nil, 10)
	require.True(t, uploadedAny(uploads, manifestID("example.com/m", "", "")))
}

// An authoritative index names the newest generation with no request, and a gap ends the walk.
func TestNewestKnownGenReadsTheIndex(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	base := manifestID("example.com/m", "", "")
	for _, gen := range []int{0, 1, 2, 4} {
		b.MarkPresent(manifestGenID(base, gen))
	}
	newest, known := b.newestKnownGen(base)
	require.False(t, known, "an index that is not authoritative cannot name the newest generation")
	require.Equal(t, -1, newest)
	b.keysMu.Lock()
	b.indexAuthoritative = true
	b.keysMu.Unlock()
	newest, known = b.newestKnownGen(base)
	require.True(t, known)
	require.Equal(t, 2, newest)
}

// A seed that arrives while the manifest walk runs waits for it. When the walk
// finds a manifest, the seed never goes out.
func TestLookAheadWaitsForTheManifestWalk(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	srv := fakeBatchServer(t, map[string][]byte{}, map[string]map[string]string{})
	t.Cleanup(srv.Close)
	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	la := b.lookAhead
	la.hold()
	la.Seed([]string{"go-buildcache/v1" + strings.Repeat("a8", 32)})
	require.Eventually(t, func() bool { return len(la.seeds) == 0 }, 5*time.Second, time.Millisecond, "a worker takes the seed")
	la.cover()
	la.release()
	require.NoError(t, b.Close())
	require.Zero(t, la.Requests.Load(), "a seed held through the walk is not sent once a manifest covers the build")
}

// A walk that finds no manifest lets the held seed go out.
func TestLookAheadGoesOnWithoutAManifest(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	srv := fakeBatchServer(t, map[string][]byte{}, map[string]map[string]string{})
	t.Cleanup(srv.Close)
	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.OnBatchEntries = func([]BatchEntry) {}
	b.SetModule("example.com/m")
	b.manifestWG.Wait()
	b.lookAhead.Seed([]string{"go-buildcache/v1" + strings.Repeat("a9", 32)})
	require.Eventually(t, func() bool { return b.lookAhead.Requests.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, b.Close())
}
