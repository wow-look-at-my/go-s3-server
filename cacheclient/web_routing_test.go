package cacheclient

// Tests index-driven fetch routing: keys the AUTHORITATIVE index lists route
// through batch; keys it omits miss with no round-trip. Only an
// in-protocol not-found marks knownMiss and reclaims a stale index claim.

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// TestWebBackend_EmptyIndexSkipsBatch is the regression for the cold-build
// waste: when the remote's AUTHORITATIVE key index is empty (a real served blob
// holding no keys), a /_batch/get can only ever come back empty, so the
// backend must skip the network entirely and miss cleanly. Before the fix,
// every cold key paid a round-trip per key (thousands of empty batches, ~27s),
// since a served-but-empty batch is a healthy response the per-op retry path never
// backs off from.
func TestWebBackend_EmptyIndexSkipsBatch(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var batchGets, puts atomic.Int64
	// A real empty blob: no keys, reported authoritatively. A missing index is a fetch failure instead (see the sibling test).
	srv := emptyIndexServer(t, &batchGets, &puts, marshalIndex(set.New[string]()))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()
	require.True(t, b.indexEmpty, "a zero-key blob must mark the remote index empty")
	require.True(t, b.indexAuthoritative, "a parsed 200 blob is authoritative")

	// Several distinct cold keys must all miss without any batch round-trip.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%016x", 0xc01d0000+i)
		_, _, _, _, miss, _, err := b.Get(id)
		require.NoError(t, err)
		require.True(t, miss, "a cold key against an empty remote must be a clean miss")
	}

	require.Equal(t, uint32(5), b.SkippedEmptyIndex.Load(),
		"every cold Get against an empty index must record an empty-index skip")
	require.Equal(t, int64(0), batchGets.Load(),
		"an empty remote index must issue ZERO /_batch/get round-trips")
}

// TestWebBackend_AuthoritativeIndexSkipsAbsentKeys pins the batch-probe
// policy: when the index fetch SUCCEEDED this run, a key the index does not
// list is authoritatively absent — probing it 404s/returns-empty by
// construction. The old policy probed exactly these keys, every batch came
// back empty, and the empty-batch backoff then disabled batching (and
// prefetch) for the whole run on every cold CI build.
func TestWebBackend_AuthoritativeIndexSkipsAbsentKeys(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var batchGets, puts atomic.Int64

	indexed := set.New[string]()
	indexed.Add("go-buildcache/v1" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	blob := marshalIndex(indexed)

	srv := emptyIndexServer(t, &batchGets, &puts, blob)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()
	require.False(t, b.indexEmpty)
	require.True(t, b.indexAuthoritative)

	// A cold key NOT in the authoritative index misses cleanly, no round-trip.
	_, _, _, _, miss, _, err := b.Get("bbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	require.True(t, miss)
	require.Equal(t, int64(0), batchGets.Load(),
		"an authoritatively-absent key must not be probed")
	require.Equal(t, uint32(1), b.SkippedNotInIndex.Load())
	require.Equal(t, uint32(1), b.MissNotInIndex.Load())
	require.Equal(t, uint32(0), b.SkippedEmptyIndex.Load())
}

// TestWebBackend_IndexFetchFailureStillProbes is the recovery path: when
// the index fetch FAILS (here: the server has no /_index endpoint), the client
// does not know what the server holds, so cold keys must still be batch-probed
// — bounded by the empty-batch backoff — instead of being fast-missed forever.
func TestWebBackend_IndexFetchFailureStillProbes(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var batchGets, puts atomic.Int64
	srv := emptyIndexServer(t, &batchGets, &puts, nil) // nil => missing index => fetch failure
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()
	require.False(t, b.indexAuthoritative, "a 404 /_index is a fetch failure, not an empty index")

	_, _, _, _, miss, _, err := b.Get("bbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	require.True(t, miss)
	require.Equal(t, int64(1), batchGets.Load(),
		"without an authoritative index, a cold key must still be probed")
	require.Equal(t, uint32(0), b.SkippedNotInIndex.Load())
	require.Equal(t, uint32(0), b.SkippedEmptyIndex.Load())
}

// TestWebBackend_EmptyIndexStillPuts verifies the skip is GET-only: a Put on a
// cold/empty-index run must still upload, so the remote gets populated for the
// next build (which then sees a non-empty index and uses the normal path).
func TestWebBackend_EmptyIndexStillPuts(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var batchGets, puts atomic.Int64
	srv := emptyIndexServer(t, &batchGets, &puts, nil)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	require.True(t, b.indexEmpty)

	body := []byte("a freshly compiled object")
	err = b.Put("deadbeefdeadbeef", testOutputID(string(body)), bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	// Put is async; Close drains the buffered upload as a single /_batch/put before we assert the remote received it.
	require.NoError(t, b.Close())

	require.Equal(t, int64(1), puts.Load(),
		"an empty-index run must still upload PUTs to populate the remote")
}

// TestGet_IndexedKeyUsesBatch pins the fetch routing: a key the index lists
// (an expected hit) is fetched through /_batch/get — a coalesced round-trip
// with prefetch — not via a per-key individual GET.
func TestGet_IndexedKeyUsesBatch(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	compressed, _ := Compress([]byte("indexed hit"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{"outputid": testOutputID("indexed hit")}
	primeIndex(b, "aabbccdd11223344")

	outputID, body, _, _, miss, _, err := b.Get("aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss, "an indexed key served by the batch endpoint must hit")
	require.Equal(t, testOutputID("indexed hit"), outputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, "indexed hit", string(data))
}

// TestWebBackend_Reclaims404IndexedKey is the regression for the permanent
// forced miss: the index advertises a key the server no longer has (stale
// index, or evicted server-side). The not-found must drop the key from the known-
// keys set so the PUT path re-uploads it — previously the standing claim made
// Put skip as already-present, so the key stayed missing on every future build.
func TestWebBackend_Reclaims404IndexedKey(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	// A single deterministic /_batch/put flushed by Close, not the timer.
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "5000")
	const actionID = "aabbccdd11223344"

	var uploaded atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/_batch/put") {
			manifest, _ := parsePutTarSafe(r.Body)
			uploaded.Add(int64(len(manifest.Entries)))
			writePutResults(w, manifest, func(string) string { return "stored" })
			return
		}
		if r.Method == http.MethodPut {
			// Single-PUT fallback path (not expected here, but count it the same).
			uploaded.Add(1)
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(404) // object GET, batch GET, index: all absent
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	primeIndex(b, actionID) // the (stale) index claims this key
	key := b.key(actionID)

	// GET routes via batch, batch 404s, the fallback individual GET 404s too: authoritative absence.
	_, _, _, _, miss, _, err := b.Get(actionID)
	require.NoError(t, err)
	require.True(t, miss)
	require.Equal(t, uint32(1), b.Reclaimed404.Load(), "the stale index claim must be counted as reclaimed")
	require.False(t, b.keyKnown(key), "the stale index claim must be dropped")

	// PUT must now re-upload, not skip as already-present; Close drains the buffered upload before we assert receipt.
	body := largePayload(256)
	require.NoError(t, b.Put(actionID, testOutputID(body), nopReader(body), int64(len(body))))
	require.NoError(t, b.Close())
	require.Equal(t, int64(1), uploaded.Load(), "a 404'd indexed key must be re-uploaded on the next Put")
	require.Equal(t, uint32(1), b.Stats.Puts.Load())
	require.Equal(t, uint32(0), b.PutSkippedKnown.Load())
}

// TestSendBatch_TransientFailureDoesNotMarkKnownMiss is the regression for
// upload/probe freezing: a TRANSIENT batch failure (5xx, network error) must
// not mark its keys as confirmed-absent. Before the fix, a single blip added every
// in-flight key to knownMiss, so they were never re-probed for the rest of
// the run even after the backend recovered.
func TestSendBatch_TransientFailureDoesNotMarkKnownMiss(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("GO_TOOLCHAIN_CACHE_MAX_RETRIES", "0") // deterministic: a single request per op

	const actionID = "aabbccdd11223344"
	const key = "go-buildcache/v1" + actionID
	body := "recovered entry"

	var failing atomic.Bool
	failing.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_batch/get" {
			w.WriteHeader(404) // index fetch => not authoritative => probing on
			return
		}
		if failing.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		// Healthy again: serve the requested entry.
		compressed, _ := Compress([]byte(body))
		manifest := batchGetManifest{Entries: []batchGetManifestEntry{{
			Key: key, Size: int64(len(compressed)),
			Metadata: map[string]string{"outputid": testOutputID(body)},
		}}}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(200)
		tw := tar.NewWriter(w)
		mdata, _ := json.Marshal(manifest)
		tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
		tw.Write(mdata)
		tw.WriteHeader(&tar.Header{Name: "data/" + key, Size: int64(len(compressed)), Mode: 0644})
		tw.Write(compressed)
		tw.Close()
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	// The opening Get: the batch fails → miss, but the key must NOT become knownMiss.
	_, _, _, _, miss, _, err := b.Get(actionID)
	require.NoError(t, err)
	require.True(t, miss)
	b.missesMu.RLock()
	marked := b.knownMiss.Contains(key)
	b.missesMu.RUnlock()
	require.False(t, marked, "a transient failure must not mark the key as confirmed-absent")

	// Backend recovers: the SAME key must be re-probed and now hit.
	failing.Store(false)
	outputID, rc, _, _, miss, _, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss, "after recovery the key must be re-probed, not frozen as a known miss")
	require.Equal(t, testOutputID(body), outputID)
	data, _ := io.ReadAll(rc)
	require.Equal(t, body, string(data))
}
