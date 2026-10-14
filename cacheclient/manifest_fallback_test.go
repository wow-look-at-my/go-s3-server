package cacheclient

import (
	"fmt"
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

const fallbackModule = "example.com/m"

// fallbackKey is the action ID of fallback test object idx.
func fallbackKey(idx int) string {
	return fmt.Sprintf("%064x", 1<<44+idx)
}

// fallbackKeys is fallbackKey for each index below count.
func fallbackKeys(count int) []string {
	keys := make([]string, count)
	for idx := range keys {
		keys[idx] = fallbackKey(idx)
	}
	return keys
}

// fallbackServer serves store like fakeBatchServer and records the body of each write.
type fallbackServer struct {
	srv     *httptest.Server
	mu      sync.Mutex
	uploads [][]byte
}

func newFallbackServer(t *testing.T, store map[string][]byte, meta map[string]map[string]string) *fallbackServer {
	t.Helper()
	fake := fakeBatchServer(t, store, meta)
	t.Cleanup(fake.Close)
	fs := &fallbackServer{}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" || strings.HasSuffix(r.URL.Path, "/_batch/put") {
			body, _ := io.ReadAll(r.Body)
			fs.mu.Lock()
			fs.uploads = append(fs.uploads, body)
			fs.mu.Unlock()
			w.WriteHeader(200)
			_, _ = w.Write([]byte("{}"))
			return
		}
		fake.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fallbackServer) uploaded(id string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return uploadedAny(fs.uploads, id)
}

// fallbackBackend opens a backend for command over fs. Its index is
// authoritative and holds every key in store, as a loaded index does.
func fallbackBackend(t *testing.T, fs *fallbackServer, store map[string][]byte, command string) (*WebBackend, func() set.Set[string]) {
	t.Helper()
	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: fs.srv.URL, AccessKey: "k", SecretKey: "s", Command: command, IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	b.ensureIndex()
	if load := b.indexLoad.Load(); load != nil {
		<-load.done
	}
	b.keysMu.Lock()
	b.indexAuthoritative = true
	b.keysMu.Unlock()
	for key := range store {
		b.MarkPresent(strings.TrimPrefix(key, "go-buildcache/v1"))
	}
	var mu sync.Mutex
	got := set.New[string]()
	b.OnBatchEntries = func(entries []BatchEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			got.Add(strings.TrimPrefix(e.Key, "go-buildcache/v1"))
		}
	}
	return b, func() set.Set[string] {
		mu.Lock()
		defer mu.Unlock()
		copied := set.New[string]()
		for key := range got.All() {
			copied.Add(key)
		}
		return copied
	}
}

// seedFallback stores keys as objects and as generation gen of the module fallback.
func seedFallback(t *testing.T, store map[string][]byte, meta map[string]map[string]string, gen int, keys []string) {
	t.Helper()
	for _, key := range keys {
		seedObject(t, store, meta, key, []byte("object "+key))
	}
	seedObject(t, store, meta, manifestGenID(manifestFallbackID(fallbackModule, "", ""), gen), manifestBody(keys))
}

// A command with no manifest of its own fetches what the newest module fallback names.
func TestFallbackUsedWhenTheCommandHasNone(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	seedFallback(t, store, meta, 0, fallbackKeys(2))
	keys := fallbackKeys(3)
	seedFallback(t, store, meta, 1, keys)
	fs := newFallbackServer(t, store, meta)
	b, got := fallbackBackend(t, fs, store, "build new")
	b.SetModule(fallbackModule)
	b.manifestWG.Wait()

	f := b.manifest.fallback.Load()
	require.NotNil(t, f, "a command with no manifest reads the fallback")
	require.Equal(t, 1, f.gen, "the newest generation")
	for _, key := range keys {
		require.True(t, got().Contains(key))
	}
	var id ActionID
	copy(id[:], mustHex(t, keys[0]))
	b.NoteUsed(id)
	require.Contains(t, b.manifest.reportFallback(), "used the module fallback generation 1: named 3 keys, fetched 3, the build used 1 of the fetched keys")
	require.NoError(t, b.Close())
}

// A command with a manifest of its own does not read the fallback.
func TestFallbackNotUsedWhenTheCommandHasOne(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	seedFallback(t, store, meta, 0, fallbackKeys(3))
	own := strings.Repeat("c9", 32)
	seedObject(t, store, meta, own, []byte("own"))
	seedObject(t, store, meta, manifestID(manifestScope(fallbackModule, "build old"), "", ""), []byte(own))
	fs := newFallbackServer(t, store, meta)
	b, got := fallbackBackend(t, fs, store, "build old")
	b.SetModule(fallbackModule)
	b.manifestWG.Wait()

	require.Nil(t, b.manifest.fallback.Load())
	require.Equal(t, uint32(1), b.manifest.Named.Load())
	require.True(t, got().Contains(own))
	require.False(t, got().Contains(fallbackKey(0)), "the fallback is not fetched")
	require.Empty(t, b.manifest.reportFallback())
	require.NoError(t, b.Close())
}

// sampleFallback returns a backend with a fallback of count keys, judged after sample keys.
func sampleFallback(t *testing.T, count int, sample int64) (*WebBackend, func() set.Set[string]) {
	t.Helper()
	t.Serial() // lookAheadJudgeAfter is package state
	old := lookAheadJudgeAfter
	lookAheadJudgeAfter = sample
	t.Cleanup(func() { lookAheadJudgeAfter = old })
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	seedFallback(t, store, meta, 0, fallbackKeys(count))
	fs := newFallbackServer(t, store, meta)
	b, got := fallbackBackend(t, fs, store, "build sample")
	b.SetModule(fallbackModule)
	return b, got
}

// A build that does not use the sample stops the fetch, and the look-ahead goes on.
func TestFallbackStopsOnLowUse(t *testing.T) {
	b, got := sampleFallback(t, 3*batchMaxKeys, batchMaxKeys)
	var f *manifestFallback
	require.Eventually(t, func() bool {
		f = b.manifest.fallback.Load()
		if f == nil {
			return false
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.paused
	}, 5*time.Second, time.Millisecond, "the fetch pauses at the sample")
	require.True(t, b.lookAhead.covered.Load(), "the fallback covers the look-ahead while it runs")

	// The build looks up other keys. It uses none of the sample.
	for idx := range batchMaxKeys / 2 {
		var id ActionID
		copy(id[:], mustHex(t, fmt.Sprintf("%064x", 1<<50+idx)))
		b.NoteUsed(id)
	}
	b.manifestWG.Wait()
	require.True(t, f.isStopped())
	require.Equal(t, batchMaxKeys, got().Len(), "no request goes out after the sample")
	require.False(t, b.lookAhead.covered.Load(), "a stopped fallback lets the look-ahead go on")
	require.Contains(t, b.manifest.reportFallback(), "stopped at a low use")
	require.NoError(t, b.Close())
}

// A build that uses the sample lets the fetch go on to the end.
func TestFallbackGoesOnWithHighUse(t *testing.T) {
	b, got := sampleFallback(t, 3*batchMaxKeys, batchMaxKeys)
	require.Eventually(t, func() bool { return got().Len() >= batchMaxKeys }, 5*time.Second, time.Millisecond)
	for _, key := range fallbackKeys(batchMaxKeys / 2) {
		var id ActionID
		copy(id[:], mustHex(t, key))
		b.NoteUsed(id)
	}
	b.manifestWG.Wait()
	require.False(t, b.manifest.fallback.Load().isStopped())
	require.Equal(t, 3*batchMaxKeys, got().Len())
	require.NoError(t, b.Close())
}

// After a run on the fallback, the command stores its own first generation and the next fallback generation.
func TestFallbackRunStoresTheCommandManifest(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	seedFallback(t, store, meta, 0, fallbackKeys(3))
	fs := newFallbackServer(t, store, meta)
	b, _ := fallbackBackend(t, fs, store, "build first")
	b.SetModule(fallbackModule)
	b.manifestWG.Wait()
	require.NotNil(t, b.manifest.fallback.Load())
	b.manifest.note(fallbackKey(0), "get")
	b.manifest.note(fallbackKey(1), "get")
	require.NoError(t, b.Close())

	require.True(t, fs.uploaded(manifestID(manifestScope(fallbackModule, "build first"), "", "")), "the command stores generation 0 of its own manifest")
	require.True(t, fs.uploaded(manifestGenID(manifestFallbackID(fallbackModule, "", ""), 1)), "the command stores the next fallback generation")
}

// A command whose own manifest needs no new generation still writes the first fallback of a module that has none.
func TestFallbackFirstGenerationFromAStableCommand(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	own := fallbackKeys(2)
	for _, key := range own {
		seedObject(t, store, meta, key, []byte("object "+key))
	}
	ownID := manifestID(manifestScope(fallbackModule, "build stable"), "", "")
	seedObject(t, store, meta, ownID, manifestBody(own))
	fs := newFallbackServer(t, store, meta)
	b, _ := fallbackBackend(t, fs, store, "build stable")
	b.SetModule(fallbackModule)
	b.manifestWG.Wait()
	for _, key := range own {
		b.manifest.note(key, "get")
	}
	require.NoError(t, b.Close())

	require.False(t, fs.uploaded(manifestGenID(ownID, 1)), "the own manifest has not drifted")
	require.True(t, fs.uploaded(manifestFallbackID(fallbackModule, "", "")), "the module gets its first fallback")
}

// The search finds the newest of contiguous generations in the index.
func TestNewestFallbackGenSearchesTheIndex(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	base := manifestFallbackID(fallbackModule, "", "")
	_, known := b.newestFallbackGen(base)
	require.False(t, known, "an index that is not authoritative cannot name the newest generation")
	b.keysMu.Lock()
	b.indexAuthoritative = true
	b.keysMu.Unlock()
	newest, known := b.newestFallbackGen(base)
	require.True(t, known)
	require.Equal(t, -1, newest)
	for gen := range 37 {
		b.MarkPresent(manifestGenID(base, gen))
		newest, _ = b.newestFallbackGen(base)
		require.Equal(t, gen, newest)
	}
}

func mustHex(t *testing.T, id string) []byte {
	t.Helper()
	h, ok := parseActionHash(id)
	require.True(t, ok)
	return h[:]
}
