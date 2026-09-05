package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putObject(t *testing.T, ts *http.Client, url, key string, data []byte, meta map[string]string) {
	t.Helper()
	req, _ := http.NewRequest("PUT", url+"/testbucket/"+key, bytes.NewReader(data))
	for k, v := range meta {
		req.Header.Set("X-Cache-Meta-"+k, v)
	}
	resp, err := ts.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

func doBatchGet(client *http.Client, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

func parseBatchResponse(t *testing.T, body io.Reader) (batchGetManifest, map[string][]byte) {
	t.Helper()
	tr := tar.NewReader(body)

	var manifest batchGetManifest
	data := make(map[string][]byte)

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
		} else if len(hdr.Name) > 5 && hdr.Name[:5] == "data/" {
			data[hdr.Name[5:]] = raw
		}
	}

	return manifest, data
}

func TestBatchGet_Basic(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	// Upload three entries.
	putObject(t, client, ts.URL, "cache/v1aaa", []byte("data-a"), map[string]string{"Outputid": "out-a"})
	putObject(t, client, ts.URL, "cache/v1bbb", []byte("data-b"), map[string]string{"Outputid": "out-b"})
	putObject(t, client, ts.URL, "cache/v1ccc", []byte("data-c"), map[string]string{"Outputid": "out-c"})

	// Batch GET two of them.
	reqBody, _ := json.Marshal(batchGetRequest{
		Keys: []string{"cache/v1aaa", "cache/v1ccc"},
	})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, data := parseBatchResponse(t, resp.Body)

	require.Len(t, manifest.Entries, 2)
	assert.Equal(t, "cache/v1aaa", manifest.Entries[0].Key)
	assert.Equal(t, "cache/v1ccc", manifest.Entries[1].Key)
	assert.Equal(t, "out-a", manifest.Entries[0].Metadata["outputid"])
	assert.Equal(t, "out-c", manifest.Entries[1].Metadata["outputid"])
	assert.False(t, manifest.Entries[0].Prefetch)
	assert.False(t, manifest.Entries[1].Prefetch)

	assert.Equal(t, "data-a", string(data["cache/v1aaa"]))
	assert.Equal(t, "data-c", string(data["cache/v1ccc"]))
}

// TestBatchGet_SelfHealRepairsMissingOutputID verifies the batch path applies the
// same in-place repair as a single GET: an outputid-less entry has its outputid
// reconstructed from the body and is returned in the manifest with that outputid
// (not evicted, not skipped), while well-formed entries are unaffected.
func TestBatchGet_SelfHealRepairsMissingOutputID(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts := testSetup(t)
	client := ts.Client()

	// One good entry (has outputid) and one relic (lz4 body, no outputid
	// metadata). Self-heal only applies to indexed cacheprog keys
	// (go-buildcache/v1<64-hex>), so the relic must use that form.
	goodKey := "go-buildcache/v1" + strings.Repeat("a", 64)
	putObject(t, client, ts.URL, goodKey, []byte("good"), map[string]string{"Outputid": "g"})

	raw := []byte("relic body missing its outputid")
	compressed := lz4Compress(t, raw)
	sum := sha256.Sum256(raw)
	wantOutputID := hex.EncodeToString(sum[:])
	relicKey := "go-buildcache/v1" + strings.Repeat("b", 64)
	req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/"+relicKey, bytes.NewReader(compressed))
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	repairsBefore := testutil.ToFloat64(selfHealRepairsTotal)

	reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{goodKey, relicKey}})
	resp, err = doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, data := parseBatchResponse(t, resp.Body)

	// Both entries come back; the relic was repaired in place, not skipped.
	require.Len(t, manifest.Entries, 2)
	byKey := map[string]batchGetManifestEntry{}
	for _, e := range manifest.Entries {
		byKey[e.Key] = e
	}
	require.Contains(t, byKey, relicKey)
	assert.Equal(t, wantOutputID, byKey[relicKey].Metadata["outputid"],
		"the relic's reconstructed outputid must appear in the manifest")
	assert.Equal(t, compressed, data[relicKey], "the relic body must be streamed untouched")
	assert.Equal(t, "good", string(data[goodKey]))

	assert.Greater(t, testutil.ToFloat64(selfHealRepairsTotal), repairsBefore,
		"batch self-heal should increment the repair counter")
}

func TestBatchGet_MissingKeys(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	putObject(t, client, ts.URL, "cache/v1exists", []byte("here"), map[string]string{"Outputid": "x"})

	reqBody, _ := json.Marshal(batchGetRequest{
		Keys: []string{"cache/v1exists", "cache/v1missing"},
	})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, data := parseBatchResponse(t, resp.Body)

	// Only the existing key should be in the response.
	require.Len(t, manifest.Entries, 1)
	assert.Equal(t, "cache/v1exists", manifest.Entries[0].Key)
	assert.Equal(t, "here", string(data["cache/v1exists"]))
}

func TestBatchGet_EmptyRequest(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{}})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, 400, resp.StatusCode)
}

func TestBatchGet_PrefetchSuppression(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	// Upload a cluster of entries close together in time.
	putObject(t, client, ts.URL, "cache/v1key1", []byte("data1"), map[string]string{"Outputid": "o1"})
	putObject(t, client, ts.URL, "cache/v1key2", []byte("data2"), map[string]string{"Outputid": "o2"})
	putObject(t, client, ts.URL, "cache/v1key3", []byte("data3"), map[string]string{"Outputid": "o3"})

	batchURL := ts.URL + "/testbucket/_batch/get"

	// First request: ask for key1 with prefetch. The server should return key1
	// plus key2 and key3 as prefetch.
	req1, _ := json.Marshal(batchGetRequest{Keys: []string{"cache/v1key1"}, Prefetch: true})
	resp1, err := doBatchGet(client, batchURL, req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	require.Equal(t, 200, resp1.StatusCode)
	manifest1, _ := parseBatchResponse(t, resp1.Body)
	require.GreaterOrEqual(t, len(manifest1.Entries), 2, "first request should include prefetch entries")

	// Collect which keys were prefetched in the first response.
	prefetchedInFirst := map[string]bool{}
	for _, e := range manifest1.Entries {
		if e.Prefetch {
			prefetchedInFirst[e.Key] = true
		}
	}
	require.NotEmpty(t, prefetchedInFirst, "first request should have prefetched some entries")

	// Second request: ask for key2 (a different key in the same cluster) with prefetch.
	// The tracker should suppress keys already sent in the first response.
	req2, _ := json.Marshal(batchGetRequest{Keys: []string{"cache/v1key2"}, Prefetch: true})
	resp2, err := doBatchGet(client, batchURL, req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, 200, resp2.StatusCode)
	manifest2, _ := parseBatchResponse(t, resp2.Body)

	// None of the prefetch entries from the first response should reappear as
	// prefetch in the second response.
	for _, e := range manifest2.Entries {
		if e.Prefetch {
			assert.False(t, prefetchedInFirst[e.Key],
				"key %q was already prefetched in first response; should be suppressed", e.Key)
		}
	}
}

func TestBatchGet_Prefetch(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	// Upload entries with similar modification times (they're uploaded sequentially so close together).
	putObject(t, client, ts.URL, "cache/v1one", []byte("first"), map[string]string{"Outputid": "o1"})
	putObject(t, client, ts.URL, "cache/v1two", []byte("second"), map[string]string{"Outputid": "o2"})
	putObject(t, client, ts.URL, "cache/v1three", []byte("third"), map[string]string{"Outputid": "o3"})

	// Wait to create a time gap, then upload an unrelated entry.
	time.Sleep(100 * time.Millisecond)

	// Request only one entry with prefetch enabled.
	reqBody, _ := json.Marshal(batchGetRequest{
		Keys:     []string{"cache/v1one"},
		Prefetch: true,
	})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, _ := parseBatchResponse(t, resp.Body)

	// Should have the requested entry plus prefetched entries from the same time window.
	require.GreaterOrEqual(t, len(manifest.Entries), 1, "at least the requested entry")

	// The requested entry should not be marked as prefetch.
	assert.False(t, manifest.Entries[0].Prefetch)
	assert.Equal(t, "cache/v1one", manifest.Entries[0].Key)

	// Any additional entries should be marked as prefetch.
	for _, e := range manifest.Entries[1:] {
		assert.True(t, e.Prefetch, "extra entries should be marked prefetch")
	}
}

// TestBatchGet_AcceptsPOST: POST is the semantically sound method for a
// body-carrying batch lookup (GET-with-a-body is proxy-hostile), so the
// endpoint accepts both. Same request, same response shape.
func TestBatchGet_AcceptsPOST(t *testing.T) {
	ts := testSetup(t)
	client := ts.Client()

	key := "go-buildcache/v1" + strings.Repeat("f", 64)
	body := []byte("posted-body")
	putObject(t, client, ts.URL, key, body, map[string]string{"Outputid": "p"})

	reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{key}})
	req, err := http.NewRequest("POST", ts.URL+"/testbucket/_batch/get", bytes.NewReader(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, data := parseBatchResponse(t, resp.Body)
	require.Len(t, manifest.Entries, 1)
	require.Equal(t, key, manifest.Entries[0].Key)
	require.Equal(t, body, data[key])

	// Other methods on the endpoint are still rejected.
	req, err = http.NewRequest("PATCH", ts.URL+"/testbucket/_batch/get", bytes.NewReader(reqBody))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 405, resp.StatusCode)
}

// TestBatchGet_SingleOpenPerServedKey pins the double-open fix: serving a batch
// of N found keys performs exactly N storage "get" operations (the phase-2
// streaming opens). The guard peek and the self-heal use raw opens that are
// neither counted as ops nor recorded as access, so they no longer double
// every key's metrics or stamp last-access onto keys that are never served.
func TestBatchGet_SingleOpenPerServedKey(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)
	client := ts.Client()
	storage.EnableAccessTracking()

	var keys []string
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("go-buildcache/v1%02x%s", 0xe0+i, strings.Repeat("0", 62))
		putObject(t, client, ts.URL, key, []byte("body"), map[string]string{"Outputid": "o"})
		keys = append(keys, key)
	}

	opsBefore := testutil.ToFloat64(storageOpsTotal.WithLabelValues("get", "ok"))

	reqBody, _ := json.Marshal(batchGetRequest{Keys: keys})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	manifest, _ := parseBatchResponse(t, resp.Body)
	require.Len(t, manifest.Entries, 3)

	opsAfter := testutil.ToFloat64(storageOpsTotal.WithLabelValues("get", "ok"))
	require.Equal(t, float64(3), opsAfter-opsBefore,
		"a batch of 3 served keys must perform exactly 3 counted get ops (no peek double-count)")
}
