package main

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

func deltaKey(n byte) (string, [gbciHashSize]byte) {
	var h [gbciHashSize]byte
	h[0], h[31] = n, n
	return gbciKeyPrefix + hex.EncodeToString(h[:]), h
}

func newDeltaIndex(t *testing.T) *Index {
	t.Helper()
	return &Index{addEpoch: newIndexEpoch(), entriesOff: true}
}

func gbciHashes(t *testing.T, blob []byte) [][gbciHashSize]byte {
	t.Helper()
	require.GreaterOrEqual(t, len(blob), gbciHeaderSize+32)
	body := blob[gbciHeaderSize : len(blob)-32]
	out := make([][gbciHashSize]byte, len(body)/gbciHashSize)
	for i := range out {
		copy(out[i][:], body[i*gbciHashSize:])
	}
	return out
}

func TestDeltaCarriesOnlyTheKeysAddedSinceTheBlob(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, h1 := deltaKey(1)
	idx.Put(k1, 1)
	blob, _, gen := idx.BlobGen()
	assert.Equal(t, [][gbciHashSize]byte{h1}, gbciHashes(t, blob))
	assert.Equal(t, uint64(1), gen.seq)

	k2, h2 := deltaKey(2)
	idx.Put(k2, 1)
	idx.Put(k2, 1)
	delta, to, ok := idx.Delta(gen)
	require.True(t, ok)
	assert.Equal(t, [][gbciHashSize]byte{h2}, gbciHashes(t, delta), "only the new key, once")
	assert.Equal(t, gen.epoch, to.epoch)
	assert.Equal(t, uint64(3), to.seq)

	empty, _, ok := idx.Delta(to)
	require.True(t, ok)
	assert.Empty(t, gbciHashes(t, empty), "nothing added since the latest position")
}

// A delta can only add keys. After a removal the copy holds a key the server
// no longer has, so only a full fetch is correct.
func TestRemovalEndsTheEpoch(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, _ := deltaKey(1)
	idx.Put(k1, 1)
	_, _, gen := idx.BlobGen()

	idx.Remove(k1)
	_, _, ok := idx.Delta(gen)
	assert.False(t, ok)

	_, _, after := idx.BlobGen()
	assert.NotEqual(t, gen.epoch, after.epoch, "the next blob names the new epoch")
}

func TestDeltaRefusesAPositionAheadOfTheLog(t *testing.T) {
	idx := newDeltaIndex(t)
	_, _, ok := idx.Delta(indexGen{epoch: idx.addEpoch, seq: 5})
	assert.False(t, ok)
	_, _, ok = idx.Delta(indexGen{epoch: idx.addEpoch + 1})
	assert.False(t, ok)
}

func TestFullLogStartsANewEpoch(t *testing.T) {
	idx := newDeltaIndex(t)
	idx.addLogLimit = 2
	_, _, gen := idx.BlobGen()
	for n := byte(1); n <= 3; n++ {
		key, _ := deltaKey(n)
		idx.Put(key, 1)
	}
	_, _, ok := idx.Delta(gen)
	assert.False(t, ok, "the log dropped hashes this position needs")
}

func TestIndexHandlerServesTheDelta(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, _ := deltaKey(1)
	idx.Put(k1, 1)

	full := httptest.NewRecorder()
	handleGetIndex(full, httptest.NewRequest("GET", "/b/_index", nil), idx)
	require.Equal(t, http.StatusOK, full.Code)
	epoch, seq := full.Header().Get(headerIndexEpoch), full.Header().Get(headerIndexSeq)
	require.NotEmpty(t, epoch)
	assert.Equal(t, "1", seq)
	assert.Empty(t, full.Header().Get(headerIndexDelta))

	k2, h2 := deltaKey(2)
	idx.Put(k2, 1)
	delta := httptest.NewRecorder()
	handleGetIndex(delta, httptest.NewRequest("GET", "/b/_index?epoch="+epoch+"&seq="+seq, nil), idx)
	require.Equal(t, http.StatusOK, delta.Code)
	assert.Equal(t, "1", delta.Header().Get(headerIndexDelta))
	assert.Equal(t, "2", delta.Header().Get(headerIndexSeq))
	assert.Equal(t, [][gbciHashSize]byte{h2}, gbciHashes(t, delta.Body.Bytes()))

	gone := httptest.NewRecorder()
	handleGetIndex(gone, httptest.NewRequest("GET", "/b/_index?epoch=7&seq=0", nil), idx)
	assert.Equal(t, http.StatusGone, gone.Code)
}

// prefixEntries splits a version 2 blob's body into its entries.
func prefixEntries(t *testing.T, blob []byte) [][gbciPrefixSize]byte {
	t.Helper()
	require.GreaterOrEqual(t, len(blob), gbciHeaderSize+32)
	require.Equal(t, byte(gbciPrefixVersion), blob[4])
	require.Equal(t, byte(gbciPrefixSize), blob[5])
	body := blob[gbciHeaderSize : len(blob)-32]
	require.Zero(t, len(body)%gbciPrefixSize)
	out := make([][gbciPrefixSize]byte, len(body)/gbciPrefixSize)
	for i := range out {
		copy(out[i][:], body[i*gbciPrefixSize:])
	}
	return out
}

func prefixOf(h [gbciHashSize]byte) [gbciPrefixSize]byte {
	var p [gbciPrefixSize]byte
	copy(p[:], h[:])
	return p
}

// A client that does not ask for a version is the go toolchain's vendored
// client, which reads version 1 only. It must keep getting exactly that.
func TestIndexHandlerServesV1UnlessAsked(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, h1 := deltaKey(1)
	k2, h2 := deltaKey(2)
	idx.Put(k1, 1)
	idx.Put(k2, 1)

	v1 := httptest.NewRecorder()
	handleGetIndex(v1, httptest.NewRequest("GET", "/b/_index", nil), idx)
	require.Equal(t, http.StatusOK, v1.Code)
	assert.Equal(t, marshalGBCI([][gbciHashSize]byte{h1, h2}, gbciFull), v1.Body.Bytes(), "byte for byte the version 1 blob")
	assert.Equal(t, byte(gbciVersion), v1.Body.Bytes()[4])

	other := httptest.NewRecorder()
	handleGetIndex(other, httptest.NewRequest("GET", "/b/_index?v=7", nil), idx)
	assert.Equal(t, v1.Body.Bytes(), other.Body.Bytes(), "a version this server does not know is answered with version 1")

	v2 := httptest.NewRecorder()
	handleGetIndex(v2, httptest.NewRequest("GET", "/b/_index?v=2", nil), idx)
	require.Equal(t, http.StatusOK, v2.Code)
	assert.Equal(t, [][gbciPrefixSize]byte{prefixOf(h1), prefixOf(h2)}, prefixEntries(t, v2.Body.Bytes()))
	assert.Len(t, v2.Body.Bytes(), gbciHeaderSize+2*gbciPrefixSize+32)
	etag1, etag2 := v1.Header().Get("ETag"), v2.Header().Get("ETag")
	require.NotEmpty(t, etag2)
	assert.NotEqual(t, etag1, etag2)
	assert.Equal(t, v1.Header().Get(headerIndexSeq), v2.Header().Get(headerIndexSeq), "both versions name the same position")

	revalidate := func(query, etag string) int {
		req := httptest.NewRequest("GET", "/b/_index"+query, nil)
		req.Header.Set("If-None-Match", etag)
		rec := httptest.NewRecorder()
		handleGetIndex(rec, req, idx)
		return rec.Code
	}
	assert.Equal(t, http.StatusNotModified, revalidate("?v=2", etag2))
	assert.Equal(t, http.StatusNotModified, revalidate("", etag1))
	assert.Equal(t, http.StatusOK, revalidate("?v=2", etag1), "a version 1 copy is replaced by version 2")
	assert.Equal(t, http.StatusOK, revalidate("", etag2))
}

func TestIndexHandlerServesTheV2Delta(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, _ := deltaKey(1)
	idx.Put(k1, 1)

	full := httptest.NewRecorder()
	handleGetIndex(full, httptest.NewRequest("GET", "/b/_index?v=2", nil), idx)
	require.Equal(t, http.StatusOK, full.Code)
	epoch, seq := full.Header().Get(headerIndexEpoch), full.Header().Get(headerIndexSeq)
	require.NotEmpty(t, epoch)

	k2, h2 := deltaKey(2)
	idx.Put(k2, 1)
	delta := httptest.NewRecorder()
	handleGetIndex(delta, httptest.NewRequest("GET", "/b/_index?v=2&epoch="+epoch+"&seq="+seq, nil), idx)
	require.Equal(t, http.StatusOK, delta.Code)
	assert.Equal(t, "1", delta.Header().Get(headerIndexDelta))
	assert.Equal(t, "2", delta.Header().Get(headerIndexSeq))
	assert.Equal(t, [][gbciPrefixSize]byte{prefixOf(h2)}, prefixEntries(t, delta.Body.Bytes()))

	v1Delta := httptest.NewRecorder()
	handleGetIndex(v1Delta, httptest.NewRequest("GET", "/b/_index?epoch="+epoch+"&seq="+seq, nil), idx)
	assert.Equal(t, [][gbciHashSize]byte{h2}, gbciHashes(t, v1Delta.Body.Bytes()), "a delta without v=2 stays version 1")
}

// Hashes that differ only past the prefix are one version 2 entry, so the
// body stays sorted and deduped.
func TestPrefixBlobMergesHashesThatSharePrefix(t *testing.T) {
	var a, b, c [gbciHashSize]byte
	a[0], b[0], c[0] = 1, 1, 2
	b[31] = 9
	hashes := [][gbciHashSize]byte{a, b, c}
	assert.Equal(t, [][gbciPrefixSize]byte{prefixOf(a), prefixOf(c)}, prefixEntries(t, marshalGBCI(hashes, gbciPrefix)))
	assert.Equal(t, hashes, gbciHashes(t, marshalGBCI(hashes, gbciFull)))
}

// Each version is serialized at most once per blob interval, and serving one
// leaves the other's cached blob alone.
func TestEachFormatIsCachedOnItsOwn(t *testing.T) {
	idx := newDeltaIndex(t)
	idx.SetBlobInterval(time.Hour)
	k1, _ := deltaKey(1)
	idx.Put(k1, 1)

	full1, _, _ := idx.BlobGenFormat(gbciFull)
	prefix1, _, _ := idx.BlobGenFormat(gbciPrefix)
	full2, _, _ := idx.BlobGenFormat(gbciFull)
	prefix2, _, _ := idx.BlobGenFormat(gbciPrefix)
	assert.Same(t, &full1[0], &full2[0])
	assert.Same(t, &prefix1[0], &prefix2[0])

	k2, _ := deltaKey(2)
	idx.Put(k2, 1)
	full3, _, _ := idx.BlobGenFormat(gbciFull)
	prefix3, _, _ := idx.BlobGenFormat(gbciPrefix)
	assert.Same(t, &full1[0], &full3[0], "inside the interval the cached blob is served")
	assert.Same(t, &prefix1[0], &prefix3[0])

	idx.SetBlobInterval(0)
	full4, _, _ := idx.BlobGenFormat(gbciFull)
	prefix4, _, _ := idx.BlobGenFormat(gbciPrefix)
	assert.Len(t, gbciHashes(t, full4), 2)
	assert.Len(t, prefixEntries(t, prefix4), 2, "serializing version 1 must not mark version 2 current")
}

func TestIndexHeadersMatchTheClient(t *testing.T) {
	assert.Equal(t, cacheclient.HeaderIndexEpoch, headerIndexEpoch)
	assert.Equal(t, cacheclient.HeaderIndexSeq, headerIndexSeq)
	assert.Equal(t, cacheclient.HeaderIndexDelta, headerIndexDelta)
	assert.Equal(t, cacheclient.HeaderPrefetch, headerPrefetch)
}

// A server with prefetch off says so on every batch answer, so a client's
// look-ahead pool stops after its first request.
func TestBatchAnswerSaysPrefetchIsOff(t *testing.T) {
	ts, _ := testSetupWithStorage(t)
	resp, err := http.Post(ts.URL+"/testbucket/_batch/get", "application/json",
		strings.NewReader(`{"keys":["go-buildcache/v1aa"],"prefetch":true,"prefetch_only":true}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "off", resp.Header.Get(headerPrefetch))
}

// A key another build stores while this process runs is a hit. The process
// must not decide the miss alone from the index it loaded at start.
func TestRunningClientLearnsKeysStoredDuringItsRun(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")
	ts, _ := testSetupWithStorage(t)
	base := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked"}

	readerCfg := base
	readerCfg.IndexDir = t.TempDir()
	reader, err := cacheclient.NewWebBackend(readerCfg)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })
	objs := makeWireObjects(3)
	_, _, _, miss := reader.Get(objs[0].actionID)
	require.True(t, miss, "nothing is stored yet; this loads the reader's index")

	writerCfg := base
	writerCfg.IndexDir = t.TempDir()
	writerCfg.IndexMaxAge = -1
	w, err := cacheclient.NewWebBackend(writerCfg)
	require.NoError(t, err)
	for _, o := range objs[1:] {
		require.NoError(t, w.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, w.Close())

	time.Sleep(cacheclient.LiveDeltaInterval)
	for _, o := range objs[1:] {
		_, got, _, miss := reader.Get(o.actionID)
		require.False(t, miss, "a key stored during the run must be served")
		require.Equal(t, o.body, got)
	}
}

// Consider a key stored after a client. That key wrote its disk copy is a hit
// for the next client that loads that copy while it is still young. Without
// the delta the copy is trusted as it stands, and the key is a miss that never
// reaches the server.
func TestYoungCopyLearnsTheKeysStoredSinceIt(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")
	ts, _ := testSetupWithStorage(t)
	base := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked"}
	copyDir := t.TempDir()

	first := base
	first.IndexDir = copyDir
	seeder, err := cacheclient.NewWebBackend(first)
	require.NoError(t, err)
	objs := makeWireObjects(3)
	_, _, _, miss := seeder.Get(objs[0].actionID)
	require.True(t, miss, "nothing is stored yet")
	require.NoError(t, seeder.Close())

	writer := base
	writer.IndexDir = t.TempDir()
	writer.IndexMaxAge = -1
	w, err := cacheclient.NewWebBackend(writer)
	require.NoError(t, err)
	for _, o := range objs {
		require.NoError(t, w.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, w.Close())

	reader, err := cacheclient.NewWebBackend(first)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })
	for _, o := range objs {
		_, got, _, miss := reader.Get(o.actionID)
		require.False(t, miss, "a key stored after the copy was written must still be served")
		require.Equal(t, o.body, got)
	}
}
