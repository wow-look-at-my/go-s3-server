package main

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// getOutcome reads the s3_get_requests_total counter for one outcome label.
func getOutcome(outcome string) float64 {
	return testutil.ToFloat64(getRequestsTotal.WithLabelValues(outcome))
}

// TestPutObject_RefusedIndexNotAdvertised locks in the load-bearing property of
// the PUT module-index guard: a refused PUT (200-but-store-nothing) must NOT
// add the key to /_index. If it did, every client would be told the key exists
// (and skip re-uploading it) while every GET 404s — the permanent forced-miss
// wedge behind the "404s on indexed keys" incidents. Index.Put is only called
// by PutStream after a successful rename, which the refusal path never reaches;
// this test keeps it that way.
func TestPutObject_RefusedIndexNotAdvertised(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	const actionHex = "5555000000000000000000000000000000000000000000000000000000005555"
	key := "go-buildcache/v1" + actionHex
	hash, ok := extractActionHash(key)
	require.True(t, ok)
	hashBytes, err := hex.DecodeString(actionHex)
	require.NoError(t, err)

	index := lz4Compress(t, incompressibleIndexBody(t, 4096))
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, index, map[string]string{
		"X-Cache-Meta-Outputid":    "deadbeef",
		"X-Cache-Meta-Compression": "lz4",
	})
	require.Equal(t, 200, resp.StatusCode, "the index PUT is accepted on the wire")
	resp.Body.Close()

	// The refused key must be advertised NOWHERE: not in the in-memory index...
	require.False(t, storage.Index.Contains(hash), "a refused PUT must not enter the index")

	// ... and not in the serialized /_index blob.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	blob, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.False(t, bytes.Contains(blob, hashBytes), "a refused PUT must not appear in the /_index blob")

	// A GET of the refused key is a PLAIN not-found — specifically NOT the
	// advertised-but-unservable divergence signature.
	notFoundBefore := getOutcome("miss_not_found")
	divergedBefore := getOutcome("miss_advertised_unservable")
	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, notFoundBefore+1, getOutcome("miss_not_found"))
	require.Equal(t, divergedBefore, getOutcome("miss_advertised_unservable"))
}

// TestGetOutcome_AdvertisedUnservable proves the item-of-record counter: when a
// key IS advertised in /_index but its object is gone from disk (index/store
// divergence, planted here by unlinking the file behind storage's back), the
// GET 404 is counted as miss_advertised_unservable — distinguishable from a
// plain miss.
func TestGetOutcome_AdvertisedUnservable(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("6", 64)
	hash, ok := extractActionHash(key)
	require.True(t, ok)
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "x"}, nil))
	require.True(t, storage.Index.Contains(hash))

	// Unlink the file directly: the index keeps advertising the key.
	require.NoError(t, os.Remove(storage.keyToPath(key)))
	require.True(t, storage.Index.Contains(hash), "the index still advertises the vanished key")

	divergedBefore := getOutcome("miss_advertised_unservable")
	hitBefore := getOutcome("hit")
	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, divergedBefore+1, getOutcome("miss_advertised_unservable"),
		"a 404 on an advertised key must be counted as index/store divergence")
	require.Equal(t, hitBefore, getOutcome("hit"))

	// Control: a hit counts as a hit.
	goodKey := "go-buildcache/v1" + strings.Repeat("7", 64)
	require.NoError(t, storage.Put(goodKey, []byte("body"), map[string]string{"outputid": "y"}, nil))
	resp = doRequest(t, ts, "GET", "/testbucket/"+goodKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, hitBefore+1, getOutcome("hit"))
}

// TestSelfhealFailure_DeadvertisesKey: an object whose outputid cannot be
// reconstructed (garbage body that does not lz4-decompress) is unservable
// forever; the GET must 404 (counted miss_selfheal_failed), DROP the key from
// /_index so consumers re-upload a good body, and leave the file on disk for
// forensics/eviction.
func TestSelfhealFailure_DeadvertisesKey(t *testing.T) {
	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("8", 64)
	hash, ok := extractActionHash(key)
	require.True(t, ok)

	// Garbage body tagged lz4, with NO outputid: the read guard fails open (not
	// an index), then the self-heal cannot decompress it to reconstruct one.
	garbage := []byte("definitely not a valid lz4 frame ................")
	require.NoError(t, storage.PutStream(key, bytes.NewReader(garbage),
		map[string]string{"compression": "lz4"}, nil))
	require.True(t, storage.Index.Contains(hash))

	failedBefore := getOutcome("miss_selfheal_failed")
	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, failedBefore+1, getOutcome("miss_selfheal_failed"))

	// De-advertised, so clients stop skipping the re-upload...
	require.False(t, storage.Index.Contains(hash),
		"an unrepairable object must be dropped from the index")

	// ... but the body stays on disk (repair-not-evict: forensics + the normal
	// eviction policy own the file).
	_, err := storage.Stat(key)
	require.NoError(t, err, "the unrepairable body must stay on disk")

	// A re-upload of a good body through the normal PUT path heals the key.
	raw := []byte("!<arch>\nrecovered body")
	resp = doRequest(t, ts, "PUT", "/testbucket/"+key, lz4Compress(t, raw), map[string]string{
		"X-Cache-Meta-Outputid":    "cafe",
		"X-Cache-Meta-Compression": "lz4",
	})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	require.True(t, storage.Index.Contains(hash), "the re-upload re-advertises the key")
	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
}

// TestIndexRemoveKeys: the batch removal used by the eviction sweeper drops
// mtime entries, master hashes, and pending hashes in one pass, and marks the
// blob dirty so /_index stops advertising the victims immediately.
func TestIndexRemoveKeys(t *testing.T) {
	idx := &Index{}

	keyA := "go-buildcache/v1" + strings.Repeat("a", 64)
	keyB := "go-buildcache/v1" + strings.Repeat("b", 64)
	keyC := "go-buildcache/v1" + strings.Repeat("c", 64)
	idx.Put(keyA, 1)
	idx.Put(keyB, 1)
	// Serialize so A and B move from pending into the sorted master list.
	idx.Blob()
	// C stays in pending.
	idx.Put(keyC, 1)

	hashA, _ := extractActionHash(keyA)
	hashB, _ := extractActionHash(keyB)
	hashC, _ := extractActionHash(keyC)
	require.True(t, idx.Contains(hashA))
	require.True(t, idx.Contains(hashC))

	idx.RemoveKeys([]string{keyA, keyC, "not-a-cacheprog-key"})

	require.False(t, idx.Contains(hashA), "a master-list hash must be removed")
	require.False(t, idx.Contains(hashC), "a pending hash must be removed")
	require.True(t, idx.Contains(hashB), "unrelated keys must survive")

	blob, _ := idx.Blob()
	require.False(t, bytes.Contains(blob, hashA[:]))
	require.False(t, bytes.Contains(blob, hashC[:]))
	require.True(t, bytes.Contains(blob, hashB[:]))

	// The mtime entries are gone too: nothing nearby except B.
	keys := idx.NearbyKeys(0, 1<<62, 100, nil)
	require.Equal(t, []string{keyB}, keys)
}
