package cacheclient

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// emptyIndexServer mimics a reachable but empty shared cache: GET /_index
// serves either a not-found (no blob yet) or, when indexBlob is non-nil, a real GBCI
// blob. It counts /_batch/get and PUT requests so tests can assert that cold
// Gets do (or do not) issue a batch round-trip. PUTs are accepted.
func emptyIndexServer(t *testing.T, batchGets, puts *atomic.Int64, indexBlob []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/testbucket/_index" && r.Method == http.MethodGet:
			if indexBlob == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			etag := indexETag(indexBlob)
			if r.Header.Get("If-None-Match") == etag {
				w.Header().Set("ETag", etag)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.Write(indexBlob)
		case r.URL.Path == "/testbucket/_batch/get" && (r.Method == http.MethodGet || r.Method == http.MethodPost):
			batchGets.Add(1)
			// Empty remote: return a manifest with no entries.
			manifest := batchGetManifest{}
			w.Header().Set("Content-Type", "application/x-tar")
			w.WriteHeader(200)
			tw := tar.NewWriter(w)
			mdata, _ := json.Marshal(manifest)
			tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
			tw.Write(mdata)
			tw.Close()
		case r.Method == http.MethodPut:
			puts.Add(1)
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(200)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// TestWebBackend_EmptyBatchBackoffStopsProbing covers the probing recovery
// path under a useless remote: the index fetch FAILED (so cold keys are
// batch-probed — with an authoritative index they would fast-miss instead),
// and the server has none of this build's keys, so every /_batch/get returns
// no entries. The consecutive-empty-batch backoff must trip after the
// threshold and stop probing, bounding the wasted round-trips instead of
// paying a batch per cold key.
func TestWebBackend_EmptyBatchBackoffStopsProbing(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	// Low threshold so the test trips quickly and deterministically.
	t.Setenv("GO_TOOLCHAIN_CACHE_EMPTY_BATCH_BACKOFF", "4")
	var batchGets, puts atomic.Int64

	srv := emptyIndexServer(t, &batchGets, &puts, nil) // missing index => fetch failure
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()
	require.False(t, b.indexAuthoritative, "fetch failure => non-authoritative => probing enabled")
	require.Equal(t, 4, b.emptyBatchBackoffThreshold)

	// Each blocking Get issues an empty /_batch/get; after the threshold, the backoff skips the network.
	const nKeys = 40
	for i := 0; i < nKeys; i++ {
		id := fmt.Sprintf("%016x", 0xb0110000+i)
		_, _, _, _, miss, err := b.Get(id)
		require.NoError(t, err)
		require.True(t, miss, "a cold key the remote does not have must be a clean miss")
	}

	require.True(t, b.batchProbingDisabled.Load(),
		"consecutive empty batches must trip the backoff")
	require.Greater(t, b.SkippedBatchBackoff.Load(), uint32(0),
		"once tripped, cold Gets must record a batch-backoff skip")
	// Batch round-trips must be bounded by the threshold, NOT paid per key. Allow a
	// little slack for the trip boundary, but it must be far below nKeys.
	require.LessOrEqual(t, batchGets.Load(), int64(b.emptyBatchBackoffThreshold+2),
		"batch GETs must be bounded by the backoff threshold, not one per cold key")
	require.Less(t, batchGets.Load(), int64(nKeys),
		"the backoff must prevent a round-trip per cold key")
	// Skips + actual batch round-trips must account for every cold key.
	require.Equal(t, uint32(nKeys), b.MissNotInIndex.Load(),
		"every cold key counts as not-in-index regardless of skip vs round-trip")
}

// TestWebBackend_BackoffResetsOnNonEmptyBatch verifies the backoff does NOT trip
// when the remote IS serving: a non-empty batch resets the consecutive-empty
// streak, so an interleaving of hits keeps batch probing enabled.
func TestWebBackend_BackoffResetsOnNonEmptyBatch(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("GO_TOOLCHAIN_CACHE_EMPTY_BATCH_BACKOFF", "4")

	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)

	// Seed a served key so a batch requesting it comes back non-empty.
	hotID := "00000000000000ff"
	hotKey := "go-buildcache/v1" + hotID
	hotBody := []byte("served object that resets the streak")
	hotComp, err := compressData(hotBody)
	require.NoError(t, err)
	store[hotKey] = hotComp
	meta[hotKey] = map[string]string{"outputid": testOutputID(string(hotBody))}

	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()
	// fakeBatchServer 404s on /_index, so cold keys take the batch-probe path this test exercises.
	require.False(t, b.indexAuthoritative)

	// Interleave: a few cold (empty-batch) keys, then a hot key that resets the
	// streak, repeated. The streak never reaches the threshold, so probing stays on.
	for round := 0; round < 6; round++ {
		for j := 0; j < 3; j++ {
			cold := fmt.Sprintf("%016x", 0xc01d0000+round*10+j)
			_, _, _, _, miss, err := b.Get(cold)
			require.NoError(t, err)
			require.True(t, miss)
		}
		// Hot key: served, non-empty batch → resets the empty streak.
		_, body, _, _, miss, err := b.Get(hotID)
		require.NoError(t, err)
		require.False(t, miss, "the seeded hot key must hit")
		if body != nil {
			io.Copy(io.Discard, body)
			body.Close()
		}
	}

	require.False(t, b.batchProbingDisabled.Load(),
		"a non-empty batch every few requests must keep the streak below threshold")
	require.Equal(t, uint32(0), b.SkippedBatchBackoff.Load(),
		"batch probing must stay enabled, so no backoff skips")
}
