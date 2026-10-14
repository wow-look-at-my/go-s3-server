package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// compactKey is v as a key of gbciCompactWidth bytes.
func compactKey(v uint64) hashKey { return v << (64 - 8*gbciCompactWidth) }

// compactHash is a hash whose first gbciCompactWidth bytes are v.
func compactHash(v uint64, tail byte) actionHash {
	var h actionHash
	binary.BigEndian.PutUint64(h[:8], compactKey(v))
	h[31] = tail
	return h
}

func randomCompactKeys(seed uint64, n int) []hashKey {
	rng := rand.New(rand.NewPCG(seed, seed))
	keys := make([]hashKey, n)
	for i := range keys {
		keys[i] = compactKey(rng.Uint64() >> (64 - 8*gbciCompactWidth))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func TestRiceIndexRoundTrips(t *testing.T) {
	top := uint64(1)<<(8*gbciCompactWidth) - 1
	cases := map[string][]uint64{
		"none":      {},
		"one zero":  {0},
		"one top":   {top},
		"two":       {1, 2},
		"huge gaps": {0, top / 3, top},
		"dense":     {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			want := make([]hashKey, len(values))
			for i, v := range values {
				want[i] = compactKey(v)
			}
			blob := marshalRiceIndex(want, gbciCompactWidth)
			assert.Equal(t, byte(gbciRiceVersion), blob[4])
			keys, etag, err := parseIndexBlob(blob)
			require.NoError(t, err)
			assert.Equal(t, indexETag(blob), etag)
			assert.Equal(t, gbciCompactWidth, keys.width())
			got := slices.Sorted(keys.All())
			if len(want) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestRiceIndexIsSmall(t *testing.T) {
	keys := randomCompactKeys(3, 500_000)
	blob := marshalRiceIndex(keys, gbciCompactWidth)
	perKey := float64(len(blob)-gbciHeaderSize-sha256.Size) / float64(len(keys))
	assert.Less(t, perKey, 3.2, "%.3f bytes a key", perKey)
}

// A blob whose body is cut, extended, or padded with one bits is refused.
func TestRiceIndexRefusesABadBody(t *testing.T) {
	good := marshalRiceIndex(randomCompactKeys(5, 1000), gbciCompactWidth)
	reseal := func(header, body []byte) []byte {
		blob := slices.Concat(header, body)
		sum := sha256.Sum256(blob)
		return append(blob, sum[:]...)
	}
	header := good[:gbciHeaderSize]
	body := good[gbciHeaderSize : len(good)-sha256.Size]

	_, _, err := parseIndexBlob(reseal(header, body[:len(body)-1]))
	assert.Error(t, err, "a cut body")
	_, _, err = parseIndexBlob(reseal(header, append(slices.Clone(body), 0)))
	assert.Error(t, err, "a byte past the last key")
	moreKeys := slices.Clone(header)
	binary.LittleEndian.PutUint64(moreKeys[16:24], 1001)
	_, _, err = parseIndexBlob(reseal(moreKeys, body))
	assert.Error(t, err, "a count past the body")
	huge := slices.Clone(header)
	binary.LittleEndian.PutUint64(huge[16:24], 1<<40)
	_, _, err = parseIndexBlob(reseal(huge, body))
	assert.Error(t, err, "a count no body can hold")
	wide := slices.Clone(header)
	wide[5] = 9
	_, _, err = parseIndexBlob(reseal(wide, body))
	assert.Error(t, err, "a width past gbciPrefixSize")

	tamper := slices.Clone(good)
	tamper[gbciHeaderSize] ^= 1
	_, _, err = parseIndexBlob(tamper)
	assert.Error(t, err, "a trailer that does not match")

	// The last byte of a body whose bits do not fill it carries zero padding.
	odd := marshalRiceIndex([]hashKey{compactKey(5)}, gbciCompactWidth)
	oddBody := slices.Clone(odd[gbciHeaderSize : len(odd)-sha256.Size])
	oddBody[len(oddBody)-1] |= 1
	_, _, err = parseIndexBlob(reseal(odd[:gbciHeaderSize], oddBody))
	assert.Error(t, err, "one bits in the padding")
}

// A set of narrow keys answers full-hash lookups by their first bytes, and
// claims and removals act at that width.
func TestCompactSetActsAtItsWidth(t *testing.T) {
	keys, _, err := parseIndexBlob(marshalRiceIndex([]hashKey{compactKey(1), compactKey(2)}, gbciCompactWidth))
	require.NoError(t, err)
	require.True(t, keys.Contains(compactHash(1, 0xaa)))
	require.False(t, keys.Contains(compactHash(3, 0)))

	keys.Add(compactHash(4, 0x55))
	require.True(t, keys.Contains(compactHash(4, 0x99)))
	keys.Add(compactHash(4, 0x77))
	require.Equal(t, 3, keys.Len(), "hashes that share the prefix are one key")
	keys.Remove(compactHash(1, 0x11))
	require.False(t, keys.Contains(compactHash(1, 0)))
	require.Equal(t, 2, keys.Len())
}

// Merges of sets of each width hold every key at the narrower width, through
// pending and removed keys alike.
func TestMixedWidthMerge(t *testing.T) {
	full := func() *hashSet {
		s := newHashSet(0)
		for v := uint64(1); v <= 600; v++ {
			s.Add(compactHash(v, 1))
		}
		// Same prefix as a key above, a different tail: one key at a later width.
		s.Add(compactHash(7, 2))
		s.Remove(compactHash(9, 1))
		s.Add(compactHash(5000, 3)) // still pending
		return s
	}
	compact := func() *hashSet {
		s, _, err := parseIndexBlob(marshalRiceIndex([]hashKey{compactKey(600), compactKey(700)}, gbciCompactWidth))
		require.NoError(t, err)
		s.Add(compactHash(800, 0))
		s.Remove(compactHash(700, 9))
		return s
	}
	check := func(t *testing.T, s *hashSet) {
		assert.Equal(t, gbciCompactWidth, s.width())
		for v := uint64(1); v <= 600; v++ {
			assert.Equal(t, v != 9, s.Contains(compactHash(v, 0xf0)), "key %d", v)
		}
		assert.True(t, s.Contains(compactHash(5000, 0)))
		assert.True(t, s.Contains(compactHash(800, 4)))
		assert.False(t, s.Contains(compactHash(700, 0)))
		assert.Equal(t, 599+2, s.Len())
		seen := set.New[hashKey]()
		for k := range s.All() {
			assert.False(t, seen.Contains(k), "a key yields one time")
			assert.Zero(t, k<<(8*gbciCompactWidth), "a key carries no bits past the width")
			seen.Add(k)
		}
		blob := marshalIndex(s)
		assert.Equal(t, byte(gbciRiceVersion), blob[4], "a narrow set writes the compact form")
		back, _, err := parseIndexBlob(blob)
		require.NoError(t, err)
		assert.Equal(t, slices.Sorted(s.All()), slices.Sorted(back.All()))
	}
	t.Run("narrow into full", func(t *testing.T) {
		s := full()
		s.addAll(compact())
		check(t, s)
	})
	t.Run("full into narrow", func(t *testing.T) {
		s := compact()
		s.addAll(full())
		check(t, s)
	})
}

// A blob merged on disk keeps the narrower width of those.
func TestMergeIndexBlobAcrossWidths(t *testing.T) {
	wide := newHashSet(0)
	wide.Add(compactHash(1, 1))
	wide.Add(compactHash(1, 2))
	wide.Add(compactHash(2, 0))
	narrowDelta, _, err := parseIndexBlob(marshalRiceIndex([]hashKey{compactKey(3)}, gbciCompactWidth))
	require.NoError(t, err)

	merged, err := mergeIndexBlob(marshalIndex(wide), narrowDelta)
	require.NoError(t, err)
	keys, _, err := parseIndexBlob(merged)
	require.NoError(t, err)
	assert.Equal(t, []hashKey{compactKey(1), compactKey(2), compactKey(3)}, slices.Sorted(keys.All()))

	wideDelta := newHashSet(0)
	wideDelta.Add(compactHash(9, 9))
	merged, err = mergeIndexBlob(marshalRiceIndex([]hashKey{compactKey(1)}, gbciCompactWidth), wideDelta)
	require.NoError(t, err)
	keys, _, err = parseIndexBlob(merged)
	require.NoError(t, err)
	assert.Equal(t, []hashKey{compactKey(1), compactKey(9)}, slices.Sorted(keys.All()))

	_, err = mergeIndexBlob([]byte("nope"), wideDelta)
	assert.Error(t, err)
}

// An absent hash matches a 3M-key compact set by chance a few times in a
// lookups.
func TestCompactSetFalsePositiveRate(t *testing.T) {
	keys := newHashSetFromSorted(randomCompactKeys(11, 3_000_000))
	keys.drop = 64 - 8*gbciCompactWidth
	rng := rand.New(rand.NewPCG(12, 12))
	const probes = 1_000_000
	hits := 0
	for range probes {
		var h actionHash
		for off := 0; off < hashSize; off += 8 {
			binary.LittleEndian.PutUint64(h[off:], rng.Uint64())
		}
		if keys.Contains(h) {
			hits++
		}
	}
	assert.LessOrEqual(t, hits, 15, "%d chance matches in %d lookups", hits, probes)
}

// The client asks for the compact form, and a server that answers the 8-byte
// form serves it as before.
func TestClientAsksForTheCompactFormAndTakesTheOldOne(t *testing.T) {
	keys := sevenKeys()
	full := marshalIndex(keySetToHashes(keys))
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("ETag", indexETag(full))
		w.Write(full)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	defer b.Close()
	b.awaitIndex()
	for k := range keys.All() {
		require.True(t, b.Present(idOf(k)))
	}
	require.False(t, b.Present(idOf(extraKey())))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{riceQuery}, queries)
}

// A compact delta merges into an 8-byte copy. The live set and the saved
// copy then hold every key at the compact width.
func TestCompactDeltaOverAnOldCopy(t *testing.T) {
	t.Serial() // persistDeltaMin is package state
	saved := persistDeltaMin
	persistDeltaMin = 1
	defer func() { persistDeltaMin = saved }()

	dir := t.TempDir()
	keys := sevenKeys()
	full := marshalIndex(keySetToHashes(keys))
	extra := hashOfKey(extraKey())
	delta := marshalRiceIndex([]hashKey{keyOf(extra) >> 24 << 24}, gbciCompactWidth)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set(HeaderIndexEpoch, "9")
		if r.URL.Query().Get("seq") == "" {
			w.Header().Set(HeaderIndexSeq, "1")
			w.Header().Set("ETag", indexETag(full))
			w.Write(full)
			return
		}
		w.Header().Set(HeaderIndexSeq, "2")
		w.Header().Set(HeaderIndexDelta, "1")
		w.Write(delta)
	}))
	defer srv.Close()

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: time.Hour}
	var path string
	for run := range 2 {
		b, err := NewWebBackend(cfg)
		require.NoError(t, err)
		path = b.indexCachePath()
		b.awaitIndex()
		require.Equal(t, run > 0, b.Present(idOf(extraKey())))
		for k := range keys.All() {
			require.True(t, b.Present(idOf(k)))
		}
		b.Close()
	}
	blob, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte(gbciRiceVersion), blob[4], "the saved copy takes the narrower width")
	disk, _, err := parseIndexBlob(blob)
	require.NoError(t, err)
	require.Equal(t, keys.Len()+1, disk.Len())
	require.True(t, disk.Contains(extra))
}

// The decode a cold go command makes of a compact index the size of the
// shared cache's.
func BenchmarkParseRiceIndex(b *testing.B) {
	keys := randomCompactKeys(21, 2_960_000)
	blob := marshalRiceIndex(keys, gbciCompactWidth)
	b.ReportMetric(float64(len(keys)), "keys")
	b.ReportMetric(float64(len(blob)), "bytes")
	b.SetBytes(int64(len(blob)))
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := parseIndexBlob(blob)
		require.NoError(b, err)
	}
}
