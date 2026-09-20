package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rotBody replaces a stored object's bytes with other bytes, the way a failing
// disk changes a body under a key that keeps all of its metadata. The metadata
// cache entry goes with it, so the next reader stats the file afresh.
func rotBody(t *testing.T, s *Storage, key string, replacement []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(s.keyToPath(key), replacement, 0644))
	s.forgetMeta(key)
}

// An action-keyed body that stopped matching the digest recorded with it is
// refused and evicted. The client's own check cannot catch this: with no
// outputid on the object, a content address would be minted from the bytes
// being served, so it would agree with whatever is there.
func TestGetObject_RefusesRottedActionKeyedBody(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("a", 64)
	stored := lz4Compress(t, []byte("a compiled object body stored under an action key"))
	rotted := lz4Compress(t, []byte("other bytes that still decompress perfectly well"))

	// No outputid on the PUT, which is the shape self-heal reconstructs from.
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, stored, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	before := testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("get"))
	rotBody(t, storage, key, rotted)

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "a body that no longer matches its digest must not be served")
	resp.Body.Close()

	require.Equal(t, before+1, testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("get")),
		"the refusal must be counted against the get path")

	_, statErr := storage.Stat(key)
	require.ErrorIs(t, statErr, ErrNotFound,
		"the object is evicted, so nothing is left to carry a freshly minted outputid")
}

// The same body rot under a key whose outputid is intact. The outputid still
// describes the body that was uploaded, so serving these bytes would hand the
// client a download it can only throw away.
func TestGetObject_RefusesRottedBodyWithOutputIDIntact(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("b", 64)
	raw := []byte("a compiled object body whose outputid survived the rot")
	stored := lz4Compress(t, raw)
	sum := sha256.Sum256(raw)
	rotted := lz4Compress(t, []byte("other bytes that still decompress perfectly well"))

	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, stored,
		map[string]string{"X-Cache-Meta-Outputid": hex.EncodeToString(sum[:])})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	before := testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("get"))
	rotBody(t, storage, key, rotted)

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "an intact outputid does not make rotted bytes servable")
	resp.Body.Close()

	require.Equal(t, before+1, testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("get")))

	_, statErr := storage.Stat(key)
	require.ErrorIs(t, statErr, ErrNotFound)
}

// An object carrying neither an outputid nor a recorded digest predates both
// stamps. Missing evidence is not evidence of rot, so it is repaired in place
// and served, which is what keeps every relic in a live cache usable.
func TestSelfHeal_StillRepairsLegacyObjectWithNoStoredDigest(t *testing.T) {
	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("c", 64)
	raw := []byte("a relic body stored before either stamp existed")
	stored := lz4Compress(t, raw)
	sum := sha256.Sum256(raw)

	// Written straight to the store's own path, so the body arrives with no
	// metadata of any kind.
	path := storage.keyToPath(key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, stored, 0644))
	storage.forgetMeta(key)

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Empty(t, meta.Metadata[outputIDMetaKey])
	require.Empty(t, meta.Metadata[storedDigestMetaKey])

	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	require.Equal(t, hex.EncodeToString(sum[:]), resp.Header.Get("X-Cache-Meta-Outputid"),
		"the outputid must still be reconstructed from the body")
}

// The batch path checks every body it streams. A rotted member is left out of
// the tar -- the client reads a missing member as a miss -- and evicted, while
// its neighbours stream normally.
func TestBatchGet_OmitsAndEvictsRottedBody(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, storage := testSetupWithStorage(t)
	client := ts.Client()

	goodKey := "go-buildcache/v1" + strings.Repeat("d", 64)
	rottedKey := "go-buildcache/v1" + strings.Repeat("e", 64)

	goodRaw := []byte("a batch member that is exactly what was stored")
	goodBody := lz4Compress(t, goodRaw)
	goodSum := sha256.Sum256(goodRaw)
	putObject(t, client, ts.URL, goodKey, goodBody,
		map[string]string{"Outputid": hex.EncodeToString(goodSum[:])})

	rottedRaw := []byte("a batch member a failing disk will rewrite")
	rottedSum := sha256.Sum256(rottedRaw)
	putObject(t, client, ts.URL, rottedKey, lz4Compress(t, rottedRaw),
		map[string]string{"Outputid": hex.EncodeToString(rottedSum[:])})

	before := testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("batch_get"))
	rotBody(t, storage, rottedKey, lz4Compress(t, []byte("bytes nobody asked this key to hold")))

	reqBody, err := json.Marshal(batchGetRequest{Keys: []string{goodKey, rottedKey}})
	require.NoError(t, err)
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	_, data := parseBatchResponse(t, resp.Body)
	require.Equal(t, goodBody, data[goodKey], "the intact member streams as stored")
	require.NotContains(t, data, rottedKey, "the rotted member must not reach the client")

	require.Equal(t, before+1, testutil.ToFloat64(storedDigestMismatchTotal.WithLabelValues("batch_get")))

	_, statErr := storage.Stat(rottedKey)
	require.ErrorIs(t, statErr, ErrNotFound, "and it must be evicted, so the next upload replaces it")
}
