package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestCleanMemo_SkipsReprobeUntilInvalidated proves the known-clean memo does
// what it claims on the live GET path:
//
//  1. the first GET of a clean lz4 object probes it and memoizes the verdict;
//  2. a subsequent GET SKIPS the probe entirely -- demonstrated by swapping the
//     on-disk body for a module index behind storage's back (no PutStream, so
//     no invalidation) and observing the guard NOT fire;
//  3. an overwrite through PutStream invalidates the memo, so the next GET
//     re-probes the new body and the guard fires again.
func TestCleanMemo_SkipsReprobeUntilInvalidated(t *testing.T) {
	serialMetrics(t)

	ts, storage := testSetupWithStorage(t)

	const actionHex = "1111000000000000000000000000000000000000000000000000000000001111"
	key := "go-buildcache/v1" + actionHex
	hash, ok := extractActionHash(key)
	require.True(t, ok)

	// A normal (non-index) lz4 body, planted with outputid + compression meta.
	raw := []byte("!<arch>\nnormal compiled object body for the memo test")
	sum := sha256.Sum256(raw)
	meta := map[string]string{
		"outputid":    hex.EncodeToString(sum[:]),
		"compression": "lz4",
	}
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, raw)), meta, nil))
	require.False(t, storage.keyKnownClean(hash), "a fresh PUT must not be pre-memoized")

	// First GET probes and memoizes.
	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	require.True(t, storage.keyKnownClean(hash), "the first read must memoize the clean verdict")

	// Swap the on-disk body for a module index WITHOUT going through storage
	// (os.WriteFile keeps the inode, hence the xattrs). Because the memo was not
	// invalidated, the next GET must skip the probe and serve the bytes -- the
	// observable proof that no lz4 decode ran.
	poison := lz4Compress(t, incompressibleIndexBody(t, 4096))
	require.NoError(t, os.WriteFile(storage.keyToPath(key), poison, 0644))

	evictBefore := testutil.ToFloat64(moduleIndexEvictionsTotal)
	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode, "a memoized key must be served without re-probing")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, poison, got)
	require.Equal(t, evictBefore, testutil.ToFloat64(moduleIndexEvictionsTotal),
		"the probe must not have run for a memoized key")

	// Overwrite through PutStream: the memo entry is invalidated, so the next
	// GET re-probes, detects the index, evicts it, and reports a miss.
	require.NoError(t, storage.PutStream(key, bytes.NewReader(poison), meta, nil))
	require.False(t, storage.keyKnownClean(hash), "an overwrite PUT must invalidate the memo")

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "after invalidation the probe must run and evict the index")
	resp.Body.Close()
	require.Greater(t, testutil.ToFloat64(moduleIndexEvictionsTotal), evictBefore)
	require.False(t, storage.keyKnownClean(hash), "an evicted key must not stay memoized")
}

// TestCleanMemo_ForgetOnDelete: DELETE invalidates the memo entry, so a
// re-created key under the same hash is re-probed.
func TestCleanMemo_ForgetOnDelete(t *testing.T) {
	ts, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("2", 64)
	hash, ok := extractActionHash(key)
	require.True(t, ok)

	raw := []byte("!<arch>\ndelete-path body")
	sum := sha256.Sum256(raw)
	meta := map[string]string{"outputid": hex.EncodeToString(sum[:]), "compression": "lz4"}
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, raw)), meta, nil))

	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	require.True(t, storage.keyKnownClean(hash))

	require.NoError(t, storage.Delete(key))
	require.False(t, storage.keyKnownClean(hash), "DELETE must drop the memo entry")
}

// TestCleanMemo_ForgetOnEviction: the eviction sweeper's evictOne invalidates
// the memo entry alongside the access record.
func TestCleanMemo_ForgetOnEviction(t *testing.T) {
	_, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("3", 64)
	hash, ok := extractActionHash(key)
	require.True(t, ok)
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "x"}, nil))
	storage.markKeyClean(hash)
	require.True(t, storage.keyKnownClean(hash))

	require.True(t, storage.evictOne(key, 0))
	require.False(t, storage.keyKnownClean(hash), "eviction must drop the memo entry")
}

// BenchmarkGetObjectWarmLz4 measures a repeated GET of one warm lz4 cacheprog
// key, with and without the known-clean memo. Without the memo every GET pays
// the module-index guard's first-block lz4 decode; with it, only the first GET
// does. The nomemo variant simulates the old behavior by nil-ing the memo
// (nil-safe accessors make that the exact pre-memo code path).
func BenchmarkGetObjectWarmLz4(b *testing.B) {
	for _, variant := range []string{"memo", "nomemo"} {
		b.Run(variant, func(b *testing.B) {
			dir := b.TempDir()
			storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
			require.NoError(b, err)
			b.Cleanup(func() { storage.Close() })
			if variant == "nomemo" {
				storage.cleanKeys = nil
			}

			key := "go-buildcache/v1" + strings.Repeat("4", 64)
			raw := make([]byte, 8192)
			copy(raw, "!<arch>\n")
			sum := sha256.Sum256(raw)
			meta := map[string]string{"outputid": hex.EncodeToString(sum[:]), "compression": "lz4"}
			require.NoError(b, storage.PutStream(key, bytes.NewReader(lz4Compress(b, raw)), meta, nil))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest("GET", "/testbucket/"+key, nil)
				rec := httptest.NewRecorder()
				handleGetObject(rec, req, storage, key)
				require.Equal(b, 200, rec.Code)

			}
		})
	}
}

// TestCleanMemo_Bound: exceeding the limit clears the memo wholesale instead of
// growing without bound; subsequent adds start repopulating it.
func TestCleanMemo_Bound(t *testing.T) {
	// A budget of four entries' worth of bytes.
	memo := newCleanKeyMemo(4 * cleanEntryBytes * lruShardCount)

	var hashes [6][gbciHashSize]byte
	for i := range hashes {
		hashes[i][0] = byte(i) // spread across shards
		hashes[i][31] = 0xee
	}

	for i := range hashes {
		memo.Put(hashes[i], struct{}{})
	}
	// Every entry here lands in a different shard, so all of them fit; what the
	// bound guarantees is that the memo never exceeds its budget.
	require.LessOrEqual(t, memo.Bytes(), memo.Budget()+cleanEntryBytes)
	_, ok := memo.Get(hashes[5])
	require.True(t, ok)

	// Shrinking the budget evicts rather than clearing: the most recently used
	// entries survive, which is the whole point of holding less instead of
	// holding nothing.
	memo.SetBudget(cleanEntryBytes) // one entry per shard, still one shard each
	require.LessOrEqual(t, memo.Bytes(), int64(len(hashes))*cleanEntryBytes)

	// Forget removes exactly one.
	before := memo.Len()
	memo.Forget(hashes[5])
	_, ok = memo.Get(hashes[5])
	require.False(t, ok)
	require.Equal(t, before-1, memo.Len())
}

// TestCleanMemo_EvictsLeastRecentlyUsed: within one shard the memo must give up
// the entry nobody has looked at, not the one being used every build.
func TestCleanMemo_EvictsLeastRecentlyUsed(t *testing.T) {
	memo := newCleanKeyMemo(2 * cleanEntryBytes * lruShardCount)

	// Same first four bytes => same shard, so these three compete directly.
	var a, b, c [gbciHashSize]byte
	a[4], b[4], c[4] = 1, 2, 3

	memo.Put(a, struct{}{})
	memo.Put(b, struct{}{})
	_, ok := memo.Get(a) // a is now the most recently used
	require.True(t, ok)

	memo.Put(c, struct{}{}) // evicts the least recently used, which is b
	_, okA := memo.Get(a)
	_, okB := memo.Get(b)
	_, okC := memo.Get(c)
	require.True(t, okA, "the recently used entry must survive")
	require.False(t, okB, "the least recently used entry is the one to drop")
	require.True(t, okC)
}
