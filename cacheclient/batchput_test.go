package cacheclient

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// parsePutTar reads a /_batch/put request body (a tar) and returns the manifest
// plus the per-key compressed data members. It is the server-side counterpart
// of buildPutTar — a test asserts what the client actually shipped.
func parsePutTar(t *testing.T, r io.Reader) (batchPutManifest, map[string][]byte) {
	t.Helper()
	tr := tar.NewReader(r)
	var manifest batchPutManifest
	data := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		raw, err := io.ReadAll(tr)
		require.NoError(t, err)
		if hdr.Name == "manifest.json" {
			require.NoError(t, json.Unmarshal(raw, &manifest))
			continue
		}
		if strings.HasPrefix(hdr.Name, "data/") {
			data[strings.TrimPrefix(hdr.Name, "data/")] = raw
		}
	}
	return manifest, data
}

// parsePutTarSafe is the require-free variant of parsePutTar, safe to call from
// an HTTP handler goroutine (a require failure there would runtime.Goexit a
// non-test goroutine and can deadlock the test). Parse errors are swallowed;
// the caller asserts on the returned manifest/data from the test goroutine.
func parsePutTarSafe(r io.Reader) (batchPutManifest, map[string][]byte) {
	tr := tar.NewReader(r)
	var manifest batchPutManifest
	data := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		raw, err := io.ReadAll(tr)
		if err != nil {
			break
		}
		if hdr.Name == "manifest.json" {
			_ = json.Unmarshal(raw, &manifest)
			continue
		}
		if strings.HasPrefix(hdr.Name, "data/") {
			data[strings.TrimPrefix(hdr.Name, "data/")] = raw
		}
	}
	return manifest, data
}

// writePutResults writes a /_batch/put JSON response with the given per-key
// statuses (key -> status).
func writePutResults(w http.ResponseWriter, manifest batchPutManifest, status func(key string) string) {
	resp := batchPutResponse{}
	for _, e := range manifest.Entries {
		resp.Results = append(resp.Results, batchPutResult{Key: e.Key, Status: status(e.Key)})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func newTestBackend(t *testing.T, url string) *WebBackend {
	t.Helper()
	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: url,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	return b
}

// claimed reports whether the optimistic index claim for actionID is still held.
func claimed(b *WebBackend, actionID string) bool {
	b.keysMu.RLock()
	defer b.keysMu.RUnlock()
	return b.keys.Contains(b.key(actionID))
}

// TestBatchPut_CoalescesAllObjects is the core regression: many Put calls must
// be shipped as a SINGLE /_batch/put tar containing every object, each stored,
// and every optimistic claim kept. This is what turns a PUT storm into a single
// admission slot.
func TestBatchPut_CoalescesAllObjects(t *testing.T) {
	hermeticOTel(t)
	// Long window so every Put lands in a single batch deterministically; Close drains before it would elapse.
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "5000")

	var batchReqs atomic.Int64
	var singleReqs atomic.Int64
	var contentType atomic.Value
	gotKeys := map[string][]byte{}
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			batchReqs.Add(1)
			contentType.Store(r.Header.Get("Content-Type"))
			manifest, data := parsePutTarSafe(r.Body)
			mu.Lock()
			for k, v := range data {
				gotKeys[k] = v
			}
			mu.Unlock()
			writePutResults(w, manifest, func(string) string { return "stored" })
			return
		}
		if r.Method == "PUT" {
			singleReqs.Add(1)
		}
		w.WriteHeader(http.StatusNotFound) // index fetch etc.
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)

	const n = 5
	var actions []string
	for i := 0; i < n; i++ {
		a := "aabbccdd1122330" + strconv.Itoa(i)
		actions = append(actions, a)
		payload := largePayload(256 + i)
		require.NoError(t, b.Put(a, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))
	}

	// Close drains the coalescer synchronously: ships the buffer as a single batch and waits for the round-trip.
	require.NoError(t, b.Close())

	require.Equal(t, int64(1), batchReqs.Load(), "all PUTs must coalesce into exactly one /_batch/put request")
	require.Equal(t, int64(0), singleReqs.Load(), "no single-object PUT should be issued when batch is supported")
	require.Equal(t, "application/x-tar", contentType.Load())

	mu.Lock()
	require.Len(t, gotKeys, n, "the one batch must carry all %d objects", n)
	mu.Unlock()

	for _, a := range actions {
		require.True(t, claimed(b, a), "a stored object keeps its optimistic claim")
	}
	require.Equal(t, uint32(n), b.Stats.Puts.Load(), "every stored object is counted")
}

// TestBatchPut_PerObjectErrorRollsBackOnlyThatClaim proves a per-object "error"
// result rolls back ONLY that key's optimistic claim, leaving the others held.
func TestBatchPut_PerObjectErrorRollsBackOnlyThatClaim(t *testing.T) {
	hermeticOTel(t)

	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "5000") // a single batch; flushed on Close
	const badAction = "aabbccdd11223300"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			manifest, _ := parsePutTarSafe(r.Body)
			badKey := "go-buildcache/v1" + badAction
			writePutResults(w, manifest, func(key string) string {
				if key == badKey {
					return "error"
				}
				return "stored"
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)

	actions := []string{"aabbccdd11223300", "aabbccdd11223301", "aabbccdd11223302"}
	for _, a := range actions {
		payload := largePayload(128)
		require.NoError(t, b.Put(a, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))
	}

	require.NoError(t, b.Close()) // drains the batch and applies per-object results

	require.False(t, claimed(b, badAction), "the errored object's claim must be rolled back so a later run re-uploads it")
	require.True(t, claimed(b, "aabbccdd11223301"), "a sibling stored object keeps its claim")
	require.True(t, claimed(b, "aabbccdd11223302"), "a sibling stored object keeps its claim")
	require.Equal(t, uint32(2), b.Stats.Puts.Load(), "only the two stored objects are counted")
}

// TestBatchPut_FallsBackToSinglePUTsOn405 proves the un-upgraded-server path: a
// server that 405s /_batch/put makes the client fall back to per-object PUTs,
// set the sticky batchPutUnsupported flag, and never attempt /_batch/put again.
func TestBatchPut_FallsBackToSinglePUTsOn405(t *testing.T) {
	hermeticOTel(t)

	var batchAttempts atomic.Int64
	gotSingle := set.New[string]()
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			batchAttempts.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed) // no batch endpoint
			return
		}
		if r.Method == "PUT" {
			mu.Lock()
			gotSingle.Add(r.URL.Path)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)
	defer b.Close()

	// The opening wave: hits the batch endpoint, is refused, then re-issues these as singles.
	first := []string{"aabbccdd11223300", "aabbccdd11223301"}
	for _, a := range first {
		payload := largePayload(128)
		require.NoError(t, b.Put(a, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotSingle.Len() == len(first)
	}, 2*time.Second, 10*time.Millisecond, "both first-wave objects must arrive as single PUTs after the 405")

	require.True(t, b.batchPutUnsupported.Load(), "the 405 must set the sticky unsupported flag")
	attemptsAfterFirstWave := batchAttempts.Load()
	require.GreaterOrEqual(t, attemptsAfterFirstWave, int64(1), "the first wave must probe /_batch/put at least once")

	// The wave after it: flag is set, so this goes straight to a single PUT.
	second := "aabbccdd11223302"
	payload := largePayload(128)
	require.NoError(t, b.Put(second, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotSingle.Contains("/testbucket/go-buildcache/v1" + second)
	}, 2*time.Second, 10*time.Millisecond, "post-fallback PUT must go straight to the single-PUT path")

	require.Equal(t, attemptsAfterFirstWave, batchAttempts.Load(), "once the flag sticks, no further /_batch/put is attempted")
	require.Eventually(t, func() bool { return b.Stats.Puts.Load() == 3 }, 2*time.Second, 10*time.Millisecond,
		"all three objects upload via the single-PUT fallback")
	for _, a := range append(first, second) {
		require.True(t, claimed(b, a))
	}
}

// TestBatchPut_DrainOnCloseFlushesPartialBuffer proves Close flushes a partial
// (< batchMaxKeys) buffer: a single buffered Put that has NOT yet hit the time
// window must still be shipped before Close returns. Without the drain, a build
// that ends would lose buffered uploads.
func TestBatchPut_DrainOnCloseFlushesPartialBuffer(t *testing.T) {
	hermeticOTel(t)
	// Long window so the buffer would not auto-flush; Close's drain must ship it.
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")
	var batchReqs atomic.Int64
	gotKeys := map[string][]byte{}
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			batchReqs.Add(1)
			manifest, data := parsePutTarSafe(r.Body)
			mu.Lock()
			for k, v := range data {
				gotKeys[k] = v
			}
			mu.Unlock()
			writePutResults(w, manifest, func(string) string { return "stored" })
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)

	const a = "aabbccdd11223399"
	payload := largePayload(64)
	require.NoError(t, b.Put(a, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))

	// Close must flush the single buffered object before returning.
	require.NoError(t, b.Close())

	require.Equal(t, int64(1), batchReqs.Load(), "Close must flush the partial buffer as one batch PUT")
	mu.Lock()
	require.Len(t, gotKeys, 1)
	mu.Unlock()
	require.True(t, claimed(b, a))
	require.Equal(t, uint32(1), b.Stats.Puts.Load())
}

// TestBatchPut_WholeBatch503ThenSucceeds proves an admission shed on the
// WHOLE tar is retried (honoring Retry-After) and the batch ultimately stores.
// An immediate Retry-After keeps the test fast.
func TestBatchPut_WholeBatch503ThenSucceeds(t *testing.T) {
	hermeticOTel(t)
	t.Setenv("GO_TOOLCHAIN_CACHE_MAX_RETRIES", "3")
	// Long window so the Puts don't auto-flush (which under -race could split them apart); Close ships them together.
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")

	var attempts atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_batch/put" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Shed the opening whole-tar attempts the way admission control does.
		if attempts.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		manifest, _ := parsePutTarSafe(r.Body)
		writePutResults(w, manifest, func(string) string { return "stored" })
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)

	actions := []string{"aabbccdd11223300", "aabbccdd11223301"}
	for _, a := range actions {
		payload := largePayload(128)
		require.NoError(t, b.Put(a, testOutputID(payload), strings.NewReader(payload), int64(len(payload))))
	}

	// Close drains synchronously and waits for the retried round-trip, so the assertions below are deterministic.
	require.NoError(t, b.Close())

	require.Equal(t, int64(3), attempts.Load(), "the whole tar should be retried twice before the 3rd attempt is admitted")
	require.Equal(t, uint32(2), b.Stats.Puts.Load(), "both objects must be stored once the batch is admitted")
	for _, a := range actions {
		require.True(t, claimed(b, a))
	}
}

// TestBatchPut_ManifestMetadataMatchesHeaders proves the manifest metadata
// carries the same lowercased meta names (sans X-Cache-Meta-) and values a
// single PUT sends as headers — the wire contract's metadata mapping.
func TestBatchPut_ManifestMetadataMatchesHeaders(t *testing.T) {
	hermeticOTel(t)

	var gotMeta map[string]string
	done := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			manifest, _ := parsePutTarSafe(r.Body)
			if len(manifest.Entries) > 0 {
				gotMeta = manifest.Entries[0].Metadata
			}
			writePutResults(w, manifest, func(string) string { return "stored" })
			select {
			case done <- struct{}{}:
			default:
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newTestBackend(t, srv.URL)
	defer b.Close()

	const a = "aabbccdd11223300"
	payload := largePayload(512)
	out := testOutputID(payload)
	require.NoError(t, b.Put(a, out, strings.NewReader(payload), int64(len(payload))))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch PUT")
	}

	require.Equal(t, out, gotMeta["outputid"])
	require.Equal(t, "lz4", gotMeta["compression"])
	require.Equal(t, strconv.Itoa(len(payload)), gotMeta["body-size"])
	require.NotEmpty(t, gotMeta["object-type"])
	require.NotEmpty(t, gotMeta["created"])
}

// TestBuildPutTar round-trips a tar through buildPutTar/parsePutTar.
func TestBuildPutTar(t *testing.T) {
	reqs := []putReq{
		{key: "go-buildcache/v1aaa", compressed: []byte("comp-a"), metadata: map[string]string{"outputid": "o-a"}},
		{key: "go-buildcache/v1bbb", compressed: []byte("comp-b"), metadata: map[string]string{"outputid": "o-b"}},
	}
	tarBytes, err := buildPutTar(reqs)
	require.NoError(t, err)

	manifest, data := parsePutTar(t, bytes.NewReader(tarBytes))
	require.Len(t, manifest.Entries, 2)
	require.Equal(t, "go-buildcache/v1aaa", manifest.Entries[0].Key)
	require.Equal(t, "o-a", manifest.Entries[0].Metadata["outputid"])
	require.Equal(t, []byte("comp-a"), data["go-buildcache/v1aaa"])
	require.Equal(t, []byte("comp-b"), data["go-buildcache/v1bbb"])
}
