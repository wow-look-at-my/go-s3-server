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

// The short form carries the first gbciPrefixSize bytes of each hash, once
// each, sorted, and is a quarter of the full download.
func TestIndexShortForm(t *testing.T) {
	idx := newDeltaIndex(t)
	for n := byte(1); n <= 4; n++ {
		key, _ := deltaKey(n)
		idx.Put(key, 1)
	}
	// Same first bytes as deltaKey(1), so the short form holds it once.
	var twin [gbciHashSize]byte
	twin[0], twin[31] = 1, 0xee
	idx.Put(gbciKeyPrefix+hex.EncodeToString(twin[:]), 1)

	short := httptest.NewRecorder()
	handleGetIndex(short, httptest.NewRequest("GET", "/b/_index?hash=8", nil), idx)
	require.Equal(t, http.StatusOK, short.Code)
	body := short.Body.Bytes()
	assert.Equal(t, byte(gbciPrefixSize), body[5])
	assert.Len(t, body, gbciHeaderSize+4*gbciPrefixSize+32, "five keys, four distinct prefixes")

	full := httptest.NewRecorder()
	handleGetIndex(full, httptest.NewRequest("GET", "/b/_index", nil), idx)
	assert.Equal(t, byte(gbciHashSize), full.Body.Bytes()[5])
	assert.Len(t, full.Body.Bytes(), gbciHeaderSize+5*gbciHashSize+32)
	assert.NotEqual(t, short.Header().Get("ETag"), full.Header().Get("ETag"), "each form has its own ETag")
	assert.Equal(t, short.Header().Get(headerIndexSeq), full.Header().Get(headerIndexSeq), "both forms name the same position")

	k5, _ := deltaKey(5)
	idx.Put(k5, 1)
	delta := httptest.NewRecorder()
	handleGetIndex(delta, httptest.NewRequest("GET", "/b/_index?hash=8&epoch="+short.Header().Get(headerIndexEpoch)+"&seq="+short.Header().Get(headerIndexSeq), nil), idx)
	require.Equal(t, http.StatusOK, delta.Code)
	assert.Len(t, delta.Body.Bytes(), gbciHeaderSize+gbciPrefixSize+32, "the delta is short too")
}

// v=2 names the Rice-coded form. It carries each distinct prefix once at the
// server's width, parses with the client's decoder, has its own ETag, and
// answers a delta in the same form.
func TestIndexV2Form(t *testing.T) {
	idx := newDeltaIndex(t)
	for n := byte(1); n <= 4; n++ {
		key, _ := deltaKey(n)
		idx.Put(key, 1)
	}
	var twin [gbciHashSize]byte
	twin[0], twin[31] = 1, 0xee
	idx.Put(gbciKeyPrefix+hex.EncodeToString(twin[:]), 1)
	counted := idx.WireSize()

	v2 := httptest.NewRecorder()
	handleGetIndex(v2, httptest.NewRequest("GET", "/b/_index?v=2&hash=8", nil), idx)
	require.Equal(t, http.StatusOK, v2.Code)
	body := v2.Body.Bytes()
	values, width, err := cacheclient.ParseIndexV2(body)
	require.NoError(t, err)
	assert.Equal(t, defaultIndexHashBits, width)
	assert.Equal(t, []uint64{1 << 32, 2 << 32, 3 << 32, 4 << 32}, values, "five keys, four distinct prefixes")
	assert.Equal(t, `"`+hex.EncodeToString(body[len(body)-32:])+`"`, v2.Header().Get("ETag"))
	assert.EqualValues(t, len(body), counted, "the size counted before any serialization is the body's")
	assert.EqualValues(t, len(body), idx.WireSize(), "and so is the cached blob's")

	short := httptest.NewRecorder()
	handleGetIndex(short, httptest.NewRequest("GET", "/b/_index?hash=8", nil), idx)
	assert.Less(t, len(body), short.Body.Len())
	assert.NotEqual(t, short.Header().Get("ETag"), v2.Header().Get("ETag"), "each form has its own ETag")
	assert.Equal(t, short.Header().Get(headerIndexSeq), v2.Header().Get(headerIndexSeq), "both forms name the same position")

	cond := httptest.NewRequest("GET", "/b/_index?v=2", nil)
	cond.Header.Set("If-None-Match", v2.Header().Get("ETag"))
	notModified := httptest.NewRecorder()
	handleGetIndex(notModified, cond, idx)
	assert.Equal(t, http.StatusNotModified, notModified.Code)

	k5, _ := deltaKey(5)
	idx.Put(k5, 1)
	delta := httptest.NewRecorder()
	handleGetIndex(delta, httptest.NewRequest("GET", "/b/_index?v=2&epoch="+v2.Header().Get(headerIndexEpoch)+"&seq="+v2.Header().Get(headerIndexSeq), nil), idx)
	require.Equal(t, http.StatusOK, delta.Code)
	assert.Equal(t, "1", delta.Header().Get(headerIndexDelta))
	values, _, err = cacheclient.ParseIndexV2(delta.Body.Bytes())
	require.NoError(t, err)
	assert.Equal(t, []uint64{5 << 32}, values, "the delta is v2 too")

	later := httptest.NewRecorder()
	handleGetIndex(later, httptest.NewRequest("GET", "/b/_index?v=9", nil), idx)
	assert.Equal(t, byte(cacheclient.IndexV2Version), later.Body.Bytes()[4], "a version this server lacks gets the newest it has")

	idx.SetHashBits(48)
	wide := httptest.NewRecorder()
	handleGetIndex(wide, httptest.NewRequest("GET", "/b/_index?v=2", nil), idx)
	values, width, err = cacheclient.ParseIndexV2(wide.Body.Bytes())
	require.NoError(t, err)
	assert.Equal(t, 48, width)
	assert.Equal(t, []uint64{1 << 40, 2 << 40, 3 << 40, 4 << 40, 5 << 40}, values)
}

// Prefixes that agree at the served width collapse into one entry, whichever
// list they arrive from.
func TestV2FormFoldsSharedPrefixesAcrossPendingAndMaster(t *testing.T) {
	idx := newDeltaIndex(t)
	k1, _ := deltaKey(1)
	idx.Put(k1, 1)
	idx.BlobGenForm(idx.FormV2())
	var twin [gbciHashSize]byte
	twin[0], twin[31] = 1, 0xee
	idx.Put(gbciKeyPrefix+hex.EncodeToString(twin[:]), 1)
	k2, _ := deltaKey(2)
	idx.Put(k2, 1)
	idx.SetBlobInterval(0)

	counted := idx.WireSize()
	blob, _, _ := idx.BlobGenForm(idx.FormV2())
	assert.EqualValues(t, len(blob), counted)
	values, _, err := cacheclient.ParseIndexV2(blob)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1 << 32, 2 << 32}, values)
}

func TestLoadConfigBoundsIndexHashBits(t *testing.T) {
	dir := t.TempDir()
	for _, bits := range []int{8, 31, 65} {
		path := writeConfigFile(t, dir, "cfg.json", map[string]any{
			"bucket": "b", "data_dir": dir, "disable_auth": true, "index_hash_bits": bits,
		})
		_, err := LoadConfig(path)
		require.ErrorContains(t, err, "index_hash_bits", "%d bits", bits)
	}
	path := writeConfigFile(t, dir, "cfg.json", map[string]any{
		"bucket": "b", "data_dir": dir, "disable_auth": true, "index_hash_bits": 48,
	})
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, 48, cfg.IndexHashBits)
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
	_, storage := testSetupWithStorage(t)
	body := `{"keys":["go-buildcache/v1aa"],"prefetch":true,"prefetch_only":true}`

	off := httptest.NewRecorder()
	handleBatchGet(off, httptest.NewRequest(http.MethodPost, "/testbucket/_batch/get", strings.NewReader(body)), storage, nil, false)
	require.Equal(t, http.StatusOK, off.Code)
	assert.Equal(t, "off", off.Header().Get(headerPrefetch))

	on := httptest.NewRecorder()
	handleBatchGet(on, httptest.NewRequest(http.MethodPost, "/testbucket/_batch/get", strings.NewReader(body)), storage, nil, true)
	require.Equal(t, http.StatusOK, on.Code)
	assert.Empty(t, on.Header().Get(headerPrefetch), "a server with prefetch on sends no such header")
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
