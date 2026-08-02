package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetaCache_ServesWarmMetadataWithoutXattrs proves the cache is actually
// consulted: after one read, the attributes are removed from disk behind the
// cache's back and the next read still reports them. (Only a test may do that
// -- the server never rewrites an inode's xattrs in place except through the
// self-heal, which invalidates.)
func TestMetaCache_ServesWarmMetadataWithoutXattrs(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("a", 64)
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "abc", "compression": "lz4"}, nil))

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "abc", meta.Metadata["outputid"])

	stripStoredMetadata(t, storage.keyToPath(key))

	meta, err = storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "abc", meta.Metadata["outputid"], "a warm key must not re-read the kernel")

	// And with the cache off, the same key reads through to the stripped inode.
	storage.metaCache = nil
	meta, err = storage.Stat(key)
	require.NoError(t, err)
	require.Empty(t, meta.Metadata["outputid"])
}

// TestMetaCache_OverwriteIsNotServedStale is the safety property: a new body
// under the same key arrives as a new inode, so the stat the cache validates
// against no longer matches and the previous body's metadata is never served.
func TestMetaCache_OverwriteIsNotServedStale(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("b", 64)

	require.NoError(t, storage.Put(key, []byte("first body"), map[string]string{"outputid": "first"}, nil))
	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "first", meta.Metadata["outputid"])

	require.NoError(t, storage.Put(key, []byte("second body, different size"), map[string]string{"outputid": "second"}, nil))
	meta, err = storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "second", meta.Metadata["outputid"], "an overwrite must invalidate the cached metadata")
}

// TestMetaCache_SetMetaInvalidates covers the one mutation the stat comparison
// cannot see: an xattr written onto a live inode, which leaves mtime and size
// exactly as they were.
func TestMetaCache_SetMetaInvalidates(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("c", 64)
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "old"}, nil))

	_, err := storage.Stat(key) // warm it
	require.NoError(t, err)

	require.NoError(t, storage.SetMeta(key, map[string]string{"outputid": "new"}))
	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "new", meta.Metadata["outputid"])
}

// TestMetaCache_SelfHealInvalidates: the repair stamps the reconstructed
// outputid through an fd, so it too must drop the entry -- otherwise the next
// reader sees the absence that triggered the repair and repairs it again on
// every single read.
func TestMetaCache_SelfHealInvalidates(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("d", 64)
	body := []byte("!<arch>\nrecoverable body")
	sum := sha256.Sum256(body)
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, body)), map[string]string{"compression": "lz4"}, nil))

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.True(t, missingOutputID(meta))
	require.True(t, ensureOutputID(storage, key, meta, nil))

	fresh, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(sum[:]), fresh.Metadata["outputid"], "the repair must be visible to the next reader")
}

// TestMetaCache_DeleteAndEvictionForget: a key that comes back later is a
// different object, and must not inherit the old one's metadata.
func TestMetaCache_DeleteAndEvictionForget(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	for _, tc := range []struct {
		name   string
		remove func(key string)
	}{
		{"delete", func(key string) { require.NoError(t, storage.Delete(key)) }},
		{"eviction", func(key string) { require.True(t, storage.evictOne(key, 0)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "go-buildcache/v1" + strings.Repeat(tc.name[:1], 64)
			require.NoError(t, storage.Put(key, []byte("gone soon"), map[string]string{"outputid": "old"}, nil))
			_, err := storage.Stat(key)
			require.NoError(t, err)

			tc.remove(key)
			require.Zero(t, cacheEntryFor(storage, key), "removal must drop the cached metadata")
		})
	}
}

// TestMetaCache_Bound: exceeding the bound clears the cache wholesale rather
// than growing without limit, and it keeps working afterwards.
func TestMetaCache_Bound(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	storage.metaCache = newMetaCache(3 * maxMetaEntryBytes)

	keys := make([]string, 5)
	for i := range keys {
		keys[i] = "go-buildcache/v1" + strings.Repeat(string(rune('0'+i)), 64)
		require.NoError(t, storage.Put(keys[i], []byte("body"), map[string]string{"outputid": "x"}, nil))
		_, err := storage.Stat(keys[i])
		require.NoError(t, err)
	}
	require.LessOrEqual(t, storage.metaCache.Bytes(), storage.metaCache.Budget()+maxMetaEntryBytes,
		"the cache must stay within its byte budget")

	// Still correct after a clear.
	meta, err := storage.Stat(keys[4])
	require.NoError(t, err)
	require.Equal(t, "x", meta.Metadata["outputid"])
}

// TestAuditXattrsAreNotServedAsMetadata: audit attributes share the metadata
// namespace as a string prefix ("user.s3audit." begins with "user.s3."), and
// were being read back as metadata and emitted as X-Cache-Meta-Audit.* headers
// -- handing the uploader's identity and IP to every client that fetched the
// object, and costing a getxattr per audit attribute on every read.
func TestAuditXattrsAreNotServedAsMetadata(t *testing.T) {
	ts, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("e", 64)
	audit := map[string]string{"uploader": "someone", "client_ip": "10.1.2.3", "user_agent": "go-toolchain"}
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "abc"}, audit))

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "abc", meta.Metadata["outputid"])
	for k := range meta.Metadata {
		require.False(t, strings.HasPrefix(k, "audit."), "audit attribute %q leaked into user metadata", k)
	}

	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	io.Copy(io.Discard, resp.Body)
	for name := range resp.Header {
		require.NotContains(t, strings.ToLower(name), "audit", "audit data must not be emitted as a response header")
	}
	require.Equal(t, "abc", resp.Header.Get("X-Cache-Meta-Outputid"))

	// The audit trail itself is untouched on disk.
	require.Equal(t, "someone", getAudit(storage.keyToPath(key))["uploader"])
	_ = audit
}

// TestOpenBodyMatchesOpen: the batch streaming phase swapped Open for OpenBody,
// so the size it writes into each tar header must still be the open fd's size,
// and the body must still be the whole object.
func TestOpenBodyMatchesOpen(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("f", 64)
	body := bytes.Repeat([]byte("payload"), 1000)
	require.NoError(t, storage.Put(key, body, map[string]string{"outputid": "abc"}, nil))

	f, size, err := storage.OpenBody(key)
	require.NoError(t, err)
	defer f.Close()
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), size)
	require.Equal(t, body, got)

	_, _, err = storage.OpenBody("go-buildcache/v1" + strings.Repeat("9", 64))
	require.ErrorIs(t, err, ErrNotFound)
}

// TestBatchGetServesFullMetadataAndBodies guards the batch path end to end
// after the streaming phase stopped re-reading metadata: the manifest must
// still carry every attribute, and each body must arrive whole.
func TestBatchGetServesFullMetadataAndBodies(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	tracker := newPrefetchTracker()

	body := []byte("!<arch>\n" + strings.Repeat("compiled", 500))
	payload := lz4Compress(t, body)
	sum := sha256.Sum256(body)
	meta := map[string]string{
		"outputid": hex.EncodeToString(sum[:]), "compression": "lz4",
		"pkg": "example.com/p", "object-type": "go-archive",
	}
	var keys []string
	for i := 0; i < 3; i++ {
		key := "go-buildcache/v1" + strings.Repeat(string(rune('A'+i)), 64)
		require.NoError(t, storage.PutStream(key, bytes.NewReader(payload), meta, nil))
		keys = append(keys, key)
	}

	manifest, bodies := batchGetDirect(t, storage, tracker, keys)
	require.Len(t, manifest.Entries, len(keys))
	for _, e := range manifest.Entries {
		require.Equal(t, meta["outputid"], e.Metadata["outputid"])
		require.Equal(t, "example.com/p", e.Metadata["pkg"])
		require.Equal(t, int64(len(payload)), e.Size)
		require.Equal(t, payload, bodies[e.Key], "body must round-trip byte-for-byte")
	}
}

// batchGetDirect issues one /_batch/get against the handler and returns the
// manifest plus each body.
func batchGetDirect(t *testing.T, storage *Storage, tracker *prefetchTracker, keys []string) (batchGetManifest, map[string][]byte) {
	t.Helper()
	reqBody, err := json.Marshal(batchGetRequest{Keys: keys})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	handleBatchGet(rec, httptest.NewRequest(http.MethodPost, "/testbucket/_batch/get", bytes.NewReader(reqBody)), storage, tracker)
	require.Equal(t, 200, rec.Code)
	return parseBatchResponse(t, bytes.NewReader(rec.Body.Bytes()))
}

// maxMetaEntryBytes bounds the single-entry overshoot the cache allows: an
// insert never evicts the entry it just made, so a shard can exceed its budget
// by at most one entry.
const maxMetaEntryBytes = 4096

// cacheEntryFor reports how many cached metadata attributes the cache holds for
// key, regardless of whether the entry still validates.
func cacheEntryFor(storage *Storage, key string) int {
	e, ok := storage.metaCache.Get(key)
	if !ok {
		return 0
	}
	return len(e.kv)
}
