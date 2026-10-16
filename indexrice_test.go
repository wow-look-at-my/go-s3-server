package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// riceValues decodes a compact-form blob into its width and its values. It
// checks the header, the trailer and the padding as the client does.
func riceValues(t testing.TB, blob []byte) (int, []uint64) {
	t.Helper()
	require.GreaterOrEqual(t, len(blob), gbciHeaderSize+sha256.Size)
	require.Equal(t, gbciMagic[:], blob[0:4])
	require.Equal(t, byte(gbciRiceVersion), blob[4])
	width := int(blob[5])
	k := uint(blob[6])
	count := binary.LittleEndian.Uint64(blob[16:24])
	end := len(blob) - sha256.Size
	digest := sha256.Sum256(blob[:end])
	require.Equal(t, digest[:], blob[end:], "the trailer is the sha256 of header and body")
	body := blob[gbciHeaderSize:end]
	bit := 0
	read := func() uint64 {
		require.Less(t, bit, len(body)*8)

		v := uint64(body[bit/8]>>(7-bit%8)) & 1
		bit++
		return v
	}
	values := make([]uint64, 0, count)
	var prev uint64
	for range count {
		var q uint64
		for read() == 1 {
			q++
		}
		var low uint64
		for range k {
			low = low<<1 | read()
		}
		prev += q<<k | low
		values = append(values, prev)
	}
	require.Equal(t, (bit+7)/8, len(body), "the body ends in the last value's byte")
	for ; bit < len(body)*8; bit++ {
		require.Zero(t, read(), "the padding is zero bits")
	}
	return width, values
}

// hashWithPrefix is a hash whose first width bytes are v and whose tail is tail.
func hashWithPrefix(v uint64, width int, tail byte) [gbciHashSize]byte {
	var h [gbciHashSize]byte
	binary.BigEndian.PutUint64(h[:8], v<<(64-8*width))
	h[31] = tail
	return h
}

func TestRiceRoundTripsAtBoundaryCounts(t *testing.T) {
	top := uint64(1)<<(8*gbciCompactWidth) - 1
	cases := map[string][]uint64{
		"none":      nil,
		"one zero":  {0},
		"one top":   {top},
		"two":       {3, 4},
		"huge gaps": {0, top / 2, top},
		"small top": {0, 1, 2, 3, 4, 5, 6, 7},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			hashes := make([][gbciHashSize]byte, len(want))
			for i, v := range want {
				hashes[i] = hashWithPrefix(v, gbciCompactWidth, 0)
			}
			width, got := riceValues(t, marshalGBCIRice(hashes, gbciCompactWidth))
			assert.Equal(t, gbciCompactWidth, width)
			assert.Equal(t, len(want), len(got))
			for i := range want {
				assert.Equal(t, want[i], got[i])
			}
		})
	}
}

// Hashes that share their first gbciCompactWidth bytes are one value.
func TestRiceCollapsesSharedPrefixes(t *testing.T) {
	hashes := [][gbciHashSize]byte{
		hashWithPrefix(9, gbciCompactWidth, 1),
		hashWithPrefix(9, gbciCompactWidth, 2),
		hashWithPrefix(10, gbciCompactWidth, 0),
		hashWithPrefix(10, gbciCompactWidth, 7),
	}
	_, got := riceValues(t, marshalGBCIRice(hashes, gbciCompactWidth))
	assert.Equal(t, []uint64{9, 10}, got)
}

func randomHashes(seed uint64, n int) [][gbciHashSize]byte {
	rng := rand.New(rand.NewPCG(seed, seed))
	hashes := make([][gbciHashSize]byte, n)
	for i := range hashes {
		for off := 0; off < gbciHashSize; off += 8 {
			binary.LittleEndian.PutUint64(hashes[i][off:], rng.Uint64())
		}
	}
	return sortDedupeHashes(hashes)
}

func TestRiceBodyIsSmall(t *testing.T) {
	// The cost a key is the log of the mean gap plus a couple of bits, so
	// a small index costs more a key.
	for _, n := range []int{100_000, 500_000} {
		hashes := randomHashes(uint64(n), n)
		blob := marshalGBCIRice(hashes, gbciCompactWidth)
		body := len(blob) - gbciHeaderSize - sha256.Size
		perKey := float64(body) / float64(n)
		assert.Less(t, perKey, 3.2, "%d keys: %.3f bytes a key", n, perKey)
		_, got := riceValues(t, blob)
		require.Len(t, got, n, "random 40-bit prefixes of %d keys stay distinct with this seed", n)
		for i, h := range hashes {
			require.Equal(t, prefixValue(&h, gbciCompactWidth), got[i])
		}
	}
}

// riceParam stays near the mean gap, and is zero for a dense list.
func TestRiceParam(t *testing.T) {
	assert.Equal(t, uint(0), riceParam(0, 0))
	assert.Equal(t, uint(0), riceParam(8, 7))
	assert.Equal(t, uint(bits.Len64((1<<40/3_000_000)*11/16)-1), riceParam(3_000_000, 1<<40))
}

// riceWireSize is the length the encoder writes for the merged list.
func TestRiceWireSizeMatchesTheEncoder(t *testing.T) {
	a := randomHashes(1, 5000)
	b := randomHashes(2, 300)
	b = append(b, a[:40]...)
	b = sortDedupeHashes(b)
	merged := mergeSortedHashes(a, b)
	assert.EqualValues(t, len(marshalGBCIRice(merged, gbciCompactWidth)), riceWireSize(a, b, gbciCompactWidth))
	assert.EqualValues(t, len(marshalGBCIRice(nil, gbciCompactWidth)), riceWireSize(nil, nil, gbciCompactWidth))
}

// enc=rice gets the compact form, whole and as a delta. hash=8 alone still
// gets the 8-byte form.
func TestIndexServesTheCompactForm(t *testing.T) {
	idx := newDeltaIndex(t)
	var want []uint64
	for n := byte(1); n <= 4; n++ {
		key, h := deltaKey(n)
		idx.Put(key, 1)
		want = append(want, prefixValue(&h, gbciCompactWidth))
	}

	compact := httptest.NewRecorder()
	handleGetIndex(compact, httptest.NewRequest("GET", "/b/_index?hash=8&enc=rice", nil), idx)
	require.Equal(t, http.StatusOK, compact.Code)
	width, got := riceValues(t, compact.Body.Bytes())
	assert.Equal(t, gbciCompactWidth, width)
	assert.Equal(t, want, got)
	sum := compact.Body.Bytes()[compact.Body.Len()-sha256.Size:]
	assert.Equal(t, `"`+hex.EncodeToString(sum)+`"`, compact.Header().Get("ETag"), "the ETag is the trailer")

	short := httptest.NewRecorder()
	handleGetIndex(short, httptest.NewRequest("GET", "/b/_index?hash=8", nil), idx)
	assert.Equal(t, byte(gbciVersion), short.Body.Bytes()[4])
	assert.Equal(t, byte(gbciPrefixSize), short.Body.Bytes()[5])
	assert.NotEqual(t, short.Header().Get("ETag"), compact.Header().Get("ETag"))
	assert.Equal(t, short.Header().Get(headerIndexSeq), compact.Header().Get(headerIndexSeq))

	k5, h5 := deltaKey(5)
	idx.Put(k5, 1)
	delta := httptest.NewRecorder()
	handleGetIndex(delta, httptest.NewRequest("GET", "/b/_index?hash=8&enc=rice&epoch="+compact.Header().Get(headerIndexEpoch)+"&seq="+compact.Header().Get(headerIndexSeq), nil), idx)
	require.Equal(t, http.StatusOK, delta.Code)
	assert.Equal(t, "1", delta.Header().Get(headerIndexDelta))
	_, added := riceValues(t, delta.Body.Bytes())
	assert.Equal(t, []uint64{prefixValue(&h5, gbciCompactWidth)}, added)
}

// The real client fetches the compact form from this server, keeps it on
// disk, and finds every key it holds.
func TestCacheClientFetchesTheCompactForm(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	ts, storage := testSetupWithStorage(t)
	objs := makeWireObjects(6)
	for _, o := range objs {
		require.NoError(t, storage.Put("go-buildcache/v1"+o.actionID, o.body, nil, nil))
	}
	cfg := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked", IndexDir: dir}
	b, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	defer b.Close()
	b.WaitIndex()
	for _, o := range objs {
		require.True(t, b.Present(o.actionID), "the compact index holds %s", o.actionID)
	}
	absent := sha256.Sum256([]byte("never-stored"))
	require.False(t, b.Present(hex.EncodeToString(absent[:])))

	copies, err := filepath.Glob(filepath.Join(dir, "gocache-web-index-*.bin"))
	require.NoError(t, err)
	require.Len(t, copies, 1)
	disk, err := os.ReadFile(copies[0])
	require.NoError(t, err)
	assert.Equal(t, byte(gbciRiceVersion), disk[4], "the disk copy is the form that arrived")
}

// BenchmarkIndexForms builds an Index from the keys of a client's disk copy
// of the index, named by GBCI_INDEX_COPY. BenchmarkIndexForms serves it in
// each form. With no copy named it uses 2.9M random keys. It reports the body
// size of each form.
func BenchmarkIndexForms(b *testing.B) {
	hashes := indexCopyHashes(b, os.Getenv("GBCI_INDEX_COPY"))
	idx := &Index{addEpoch: newIndexEpoch(), entriesOff: true}
	for _, h := range hashes {
		idx.Put(gbciKeyPrefix+hex.EncodeToString(h[:]), 1)
	}
	body := func(query string) int {
		rec := httptest.NewRecorder()
		handleGetIndex(rec, httptest.NewRequest("GET", "/b/_index?"+query, nil), idx)
		require.Equal(b, http.StatusOK, rec.Code)
		return rec.Body.Len()
	}
	old, compact := body("hash=8"), body("hash=8&enc=rice")
	b.ResetTimer()
	for range b.N {
		marshalGBCIRice(idx.hashes, gbciCompactWidth)
	}
	b.ReportMetric(float64(len(hashes)), "keys")
	b.ReportMetric(float64(old), "old-bytes")
	b.ReportMetric(float64(compact), "compact-bytes")
	b.ReportMetric(float64(compact-gbciHeaderSize-sha256.Size)/float64(len(hashes)), "compact-bytes/key")
}

// indexCopyHashes reads the keys of a v1 index copy at path as hashes padded
// with zero bytes. An empty path gives 2.9M random hashes.
func indexCopyHashes(b *testing.B, path string) [][gbciHashSize]byte {
	if path == "" {
		return randomHashes(7, 2_900_000)
	}
	blob, err := os.ReadFile(path)
	require.NoError(b, err)
	require.Equal(b, byte(gbciVersion), blob[4], "the copy is a v1 blob")
	width := int(blob[5])
	count := int(binary.LittleEndian.Uint64(blob[16:24]))
	require.Equal(b, gbciHeaderSize+count*width+sha256.Size, len(blob), fmt.Sprintf("a v1 blob of %d keys", count))
	hashes := make([][gbciHashSize]byte, count)
	for i := range hashes {
		copy(hashes[i][:], blob[gbciHeaderSize+i*width:gbciHeaderSize+(i+1)*width])
	}
	return slices.Clip(hashes)
}
