package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// testSetupWithStorage mirrors testSetup but also hands back the underlying
// *Storage. The read-path module-index tests need to plant a poisoned
// module-index blob DIRECTLY on disk (storage.PutStream) -- the HTTP PUT guard
// refuses such a blob, so the only way to reproduce "already stored before the
// guard existed" is to bypass the handler and write it straight to storage.
func testSetupWithStorage(t *testing.T) (*httptest.Server, *Storage) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "allow"},
		DisableAuth: true,
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts, storage
}

// plantModuleIndexBlob writes a module-index blob straight to storage under an
// indexed cacheprog key, bypassing the HTTP PUT guard. A real index carries an
// outputid (so the outputid self-heal would happily pass it -- proving the
// module-index guard, not the self-heal, is what catches it) and is lz4
// compressed on the wire, so we store the lz4 frame and tag compression=lz4.
//
// The payload is REALISTICALLY incompressible (magic + crypto/rand entropy), so
// its single lz4 block compresses to several KB -- far past the old 512-byte
// peek that masked the bug. This is the exact shape of the real production
// poison blobs (compressed 600 B - ~36 KB), so these read-path tests genuinely
// exercise "the compressed first block is bigger than the peek window".
func plantModuleIndexBlob(t *testing.T, storage *Storage, key string) {
	t.Helper()
	raw := incompressibleIndexBody(t, 16384)
	compressed := lz4Compress(t, raw)
	require.Greater(t, len(compressed), 512, "a realistic index must compress to more than the old 512-byte peek")
	sum := sha256.Sum256(raw)
	meta := map[string]string{
		"outputid":    hex.EncodeToString(sum[:]),
		"compression": "lz4",
	}
	require.NoError(t, storage.PutStream(key, bytes.NewReader(compressed), meta, nil))
}

// TestGetObject_EvictsModuleIndexOnRead is the read-path half of the poison fix:
// a module-index blob already on disk (planted directly, as if uploaded before
// the PUT guard existed) is detected on the first GET, evicted (file + /_index
// entry), and reported as a 404 miss -- the client then recomputes the index
// locally. The PUT guard alone could never remove it; this is what sheds the
// already-stored poison, lazily, on first fetch.
func TestGetObject_EvictsModuleIndexOnRead(t *testing.T) {
	if !forkMetrics(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	const actionHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	hashBytes, err := hex.DecodeString(actionHex)
	require.NoError(t, err)
	key := "go-buildcache/v1" + actionHex

	plantModuleIndexBlob(t, storage, key)

	// Sanity: the planted key really is on disk and advertised in /_index.
	_, err = storage.Stat(key)
	require.NoError(t, err, "the poison must be on disk before the read")
	resp := doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxBefore, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.True(t, bytes.Contains(idxBefore, hashBytes), "index lists the poison before the read")

	evictBefore := testutil.ToFloat64(moduleIndexEvictionsTotal)

	// First GET detects + evicts + reports a miss (404, the normal not-found path),
	// NOT the served index body.
	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "a stored module index must be refused on read")
	require.Equal(t, "not_found", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()

	require.Greater(t, testutil.ToFloat64(moduleIndexEvictionsTotal), evictBefore,
		"the module-index eviction counter should increase")

	// The object is gone from disk ...
	_, err = storage.Stat(key)
	require.ErrorIs(t, err, ErrNotFound, "the poison must be evicted from disk")

	// ... and its key is dropped from the /_index blob.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxAfter, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.False(t, bytes.Contains(idxAfter, hashBytes), "index must not list the evicted poison")

	// A subsequent GET is a plain miss (already gone), no double-count.
	evictAfter := testutil.ToFloat64(moduleIndexEvictionsTotal)
	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, evictAfter, testutil.ToFloat64(moduleIndexEvictionsTotal),
		"an already-evicted key must not be counted again")
}

// TestGetObject_NonIndexBodyServedUnchanged is the regression guard that the
// read-path peek does not corrupt or partially consume the served stream: a
// normal (non-index) cacheprog object is served byte-for-byte, exactly as before
// the guard. The peek rewinds the file to byte 0, so io.Copy reads the whole
// body from the start.
func TestGetObject_NonIndexBodyServedUnchanged(t *testing.T) {
	ts := testSetup(t)

	// The served body must be LONGER than any peek the guard does so a botched
	// peek (one that read but failed to rewind) would visibly truncate it. The
	// server serves the stored bytes verbatim, so we store a large, random (hence
	// definitely-not-index, and not lz4-shrinkable) body uncompressed -- with no
	// compression hint the guard checks the raw leading bytes, and a multi-KB body
	// dwarfs the read-path probe.
	const actionHex = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	key := "go-buildcache/v1" + actionHex
	payload := make([]byte, 4096)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	require.Greater(t, len(payload), indexMagicProbeBytes, "stored payload must exceed the read probe to catch truncation")

	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, payload,
		map[string]string{"X-Cache-Meta-Outputid": "abc123"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode, "a normal object must still serve")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, payload, got, "the served body must be byte-for-byte unchanged by the peek")
	require.Equal(t, "abc123", resp.Header.Get("X-Cache-Meta-Outputid"))
}

// TestGetObject_NonCacheprogKeyNotInspected proves the read guard is scoped: an
// object whose key is NOT an indexed cacheprog key (go-buildcache/v1<64-hex>) is
// served exactly as stored even when its body begins with the module-index magic
// -- such keys carry no cache-protocol contract, so the guard never inspects or
// evicts them. The index-magic body is planted directly (the unscoped HTTP PUT
// guard would refuse such a body on any key), which is exactly what makes this a
// real test of the read guard's scope rather than the PUT guard's.
func TestGetObject_NonCacheprogKeyNotInspected(t *testing.T) {
	if !forkMetrics(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	key := "misc/some-arbitrary-object"
	body := []byte("go index v2\nthis is a non-cacheprog object whose body looks index-ish")
	require.NoError(t, storage.PutStream(key, bytes.NewReader(body), map[string]string{}, nil))

	evictBefore := testutil.ToFloat64(moduleIndexEvictionsTotal)

	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode, "a non-cacheprog key must be served as stored, never inspected")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, body, got, "the body must be served verbatim, the guard never having looked at it")
	require.Equal(t, evictBefore, testutil.ToFloat64(moduleIndexEvictionsTotal),
		"a non-cacheprog key must never be counted as a module-index eviction")

	// And it is still on disk -- not evicted.
	_, err := storage.Stat(key)
	require.NoError(t, err, "a non-cacheprog object must not be evicted")
}

// TestBatchGet_EvictsModuleIndex covers the batch path: a poisoned module-index
// blob requested in a batch is detected, evicted (disk + /_index), and OMITTED
// from the manifest and tar (the client treats the missing entry as a miss),
// while a normal sibling key in the same batch is still served untouched.
func TestBatchGet_EvictsModuleIndex(t *testing.T) {
	if !forkMetrics(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)
	client := ts.Client()

	const poisonHex = "aaaa000000000000000000000000000000000000000000000000000000000000"
	poisonKey := "go-buildcache/v1" + poisonHex
	poisonHashBytes, err := hex.DecodeString(poisonHex)
	require.NoError(t, err)
	plantModuleIndexBlob(t, storage, poisonKey)

	// A normal sibling object, stored via the regular HTTP PUT path.
	goodKey := "go-buildcache/v1" + strings.Repeat("b", 64)
	goodBody := []byte("data-good")
	putObject(t, client, ts.URL, goodKey, goodBody, map[string]string{"Outputid": "g"})

	evictBefore := testutil.ToFloat64(moduleIndexEvictionsTotal)

	reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{poisonKey, goodKey}})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	manifest, data := parseBatchResponse(t, resp.Body)

	// Only the good key comes back; the poison is omitted from both manifest and tar.
	require.Len(t, manifest.Entries, 1, "the poisoned index must be omitted from the manifest")
	require.Equal(t, goodKey, manifest.Entries[0].Key)
	require.Equal(t, goodBody, data[goodKey], "the sibling object must be streamed untouched")
	_, poisonInTar := data[poisonKey]
	require.False(t, poisonInTar, "the poisoned index body must not be written into the tar")

	require.Greater(t, testutil.ToFloat64(moduleIndexEvictionsTotal), evictBefore,
		"the batch path must count the module-index eviction")

	// The poison is evicted from disk and the index.
	_, err = storage.Stat(poisonKey)
	require.ErrorIs(t, err, ErrNotFound, "the poison must be evicted from disk by the batch path")
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idx, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.False(t, bytes.Contains(idx, poisonHashBytes), "index must not list the evicted poison")
}
