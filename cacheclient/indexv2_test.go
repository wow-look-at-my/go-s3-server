package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prefixesOf(n, bits int) []uint64 {
	values := make([]uint64, n)
	for i := range values {
		sum := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(i)))
		values[i] = binary.BigEndian.Uint64(sum[:8]) >> (64 - bits)
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func TestIndexV2RoundTrip(t *testing.T) {
	for _, bits := range []int{32, 40, 48, 64} {
		values := prefixesOf(10_000, bits)
		blob := MarshalIndexV2(bits, values)
		assert.Equal(t, []byte("GBCI"), blob[:4])
		assert.Equal(t, byte(IndexV2Version), blob[4])
		assert.Equal(t, byte(bits), blob[5])
		assert.Equal(t, len(blob), IndexV2Size(bits, slices.Values(values)), "the size is counted the way it is written")

		got, width, err := ParseIndexV2(blob)
		require.NoError(t, err)
		assert.Equal(t, bits, width)
		assert.Equal(t, values, got)
	}
}

func TestIndexV2EdgeValues(t *testing.T) {
	for _, values := range [][]uint64{
		{},
		{0},
		{1<<40 - 1},
		{0, 1<<40 - 1},
		{0, 1, 2, 3},
		{5, 1<<40 - 2, 1<<40 - 1},
	} {
		blob := MarshalIndexV2(40, values)
		got, _, err := ParseIndexV2(blob)
		require.NoError(t, err, "%v", values)
		assert.Equal(t, values, got)
		assert.Equal(t, len(blob), IndexV2Size(40, slices.Values(values)))
	}
	whole := []uint64{0, 1<<63 - 1, 1 << 63, ^uint64(0)}
	got, _, err := ParseIndexV2(MarshalIndexV2(64, whole))
	require.NoError(t, err)
	assert.Equal(t, whole, got)
}

func TestIndexV2RefusesABrokenBlob(t *testing.T) {
	values := prefixesOf(100, 40)
	blob := MarshalIndexV2(40, values)

	short := append([]byte(nil), blob[:len(blob)-1]...)
	_, _, err := ParseIndexV2(short)
	assert.ErrorContains(t, err, "trailer hash mismatch")

	flipped := append([]byte(nil), blob...)
	flipped[gbciHeaderSize+3] ^= 0x80
	_, _, err = ParseIndexV2(flipped)
	assert.ErrorContains(t, err, "trailer hash mismatch")

	v1 := append([]byte(nil), blob...)
	v1[4] = gbciVersion
	_, _, err = ParseIndexV2(v1)
	assert.ErrorContains(t, err, "unsupported version")

	// A trailer that matches a lying header must still fail: the count asks
	// for more values than the body can hold.
	lying := append([]byte(nil), blob...)
	binary.LittleEndian.PutUint64(lying[16:24], 1<<40)
	bodyEnd := len(lying) - sha256.Size
	digest := sha256.Sum256(lying[:bodyEnd])
	copy(lying[bodyEnd:], digest[:])
	_, _, err = ParseIndexV2(lying)
	assert.ErrorContains(t, err, "does not fit")

	// A body cut inside a value, resigned, ends inside that value.
	cut := append([]byte(nil), blob[:len(blob)-sha256.Size-2]...)
	cut = append(cut, make([]byte, sha256.Size)...)
	bodyEnd = len(cut) - sha256.Size
	digest = sha256.Sum256(cut[:bodyEnd])
	copy(cut[bodyEnd:], digest[:])
	_, _, err = ParseIndexV2(cut)
	assert.ErrorContains(t, err, "body ends inside value")
}

func TestIndexV2PanicsOnUnsortedInput(t *testing.T) {
	assert.Panics(t, func() { MarshalIndexV2(40, []uint64{2, 1}) })
	assert.Panics(t, func() { MarshalIndexV2(40, []uint64{1, 1}) })
	assert.Panics(t, func() { MarshalIndexV2(40, []uint64{1 << 40}) })
	assert.Panics(t, func() { MarshalIndexV2(16, nil) })
}

// The Rice parameter is chosen from the width and the count alone, so a size
// can be counted without a trial encode. On real hash prefixes that choice is
// within a percent of the best parameter, and the whole blob costs about
// width-log2(n)+2 bits per entry.
func TestIndexV2RiceParameterIsNearOptimal(t *testing.T) {
	const n = 200_000
	for _, bits := range []int{40, 64} {
		values := prefixesOf(n, bits)
		k := riceParam(bits, len(values))
		cost := func(k uint) uint64 {
			var total uint64
			var prev uint64
			for i, v := range values {
				g := v
				if i > 0 {
					g = v - prev - 1
				}
				total += g>>k + uint64(k) + 1
				prev = v
			}
			return total
		}
		chosen := cost(k)
		for d := -2; d <= 2; d++ {
			other := cost(uint(int(k) + d))
			assert.LessOrEqual(t, float64(chosen), float64(other)*1.01, "bits=%d k=%d against k=%d", bits, k, int(k)+d)
		}
		perEntry := float64(len(MarshalIndexV2(bits, values))-gbciHeaderSize-sha256.Size) * 8 / float64(len(values))
		assert.Less(t, perEntry, float64(bits)-17.6+2.5, "bits=%d: %.2f bits per entry", bits, perEntry)
	}
}

// The sizes the docs quote, for a keys. A change here is a change to the
// wire format and must be measured again.
func TestIndexV2SizeAtAMillionKeys(t *testing.T) {
	const n = 1_000_000
	for _, c := range []struct {
		bits int
		size int
	}{
		{40, 2703495},
		{48, 3703495},
		{64, 5703495},
	} {
		values := prefixesOf(n, c.bits)
		assert.Equal(t, c.size, len(MarshalIndexV2(c.bits, values)), "bits=%d", c.bits)
	}
}

// A set parsed from a v2 blob holds the blob's width: a lookup with a full
// hash decides on those bits alone.
func TestV2IndexAnswersFullHashLookups(t *testing.T) {
	keys, etag, err := parseIndexBlob(MarshalIndexV2(40, []uint64{1 << 32, 2 << 32}))
	require.NoError(t, err)
	assert.Len(t, etag, 66)
	assert.Equal(t, 40, keys.width())
	assert.True(t, keys.Contains(fullHash(1, 0xaa)), "the first 40 bits decide")
	assert.True(t, keys.Contains(fullHash(2, 0)))
	assert.False(t, keys.Contains(fullHash(3, 0)))

	var low actionHash
	low[0], low[5], low[31] = 1, 0xff, 0xff
	assert.True(t, keys.Contains(low), "bits past the width are not read")
	keys.Remove(low)
	assert.False(t, keys.Contains(fullHash(1, 0)), "nor on removal")
}

// A delta of a narrower width narrows the set it merges into; one of a wider
// width is cut to the set's width. Either way every key keeps the same bits.
func TestDeltaMergesAcrossWidths(t *testing.T) {
	live, _, err := parseIndexBlob(fullWidthBlob(fullHash(1, 7), fullHash(2, 7)))
	require.NoError(t, err)
	assert.Equal(t, 64, live.width())
	narrow, _, err := parseIndexBlob(MarshalIndexV2(40, []uint64{3 << 32}))
	require.NoError(t, err)
	live.addAll(narrow)
	assert.Equal(t, 40, live.width())
	assert.Equal(t, 3, live.Len())
	for _, h := range []actionHash{fullHash(1, 7), fullHash(2, 7), fullHash(3, 9)} {
		assert.True(t, live.Contains(h))
	}

	wide, _, err := parseIndexBlob(shortBlob(fullHash(4, 0x55)))
	require.NoError(t, err)
	live.addAll(wide)
	assert.Equal(t, 40, live.width())
	assert.True(t, live.Contains(fullHash(4, 0x99)))
	assert.Equal(t, 4, live.Len())

	// Narrowing folds keys that agree on the kept bits, pending ones included.
	var twin actionHash
	twin[0], twin[7] = 1, 0xee
	live.Add(twin)
	assert.Equal(t, 4, live.Len())
}

// marshalIndex round-trips a set of any width through the v2 form.
func TestMarshalIndexKeepsTheWidth(t *testing.T) {
	s := newKeySet(0)
	s.narrowTo(24)
	for i := range 50 {
		var h actionHash
		h[0], h[1], h[2] = byte(i), byte(i*7), byte(i*13)
		s.Add(h)
	}
	got, _, err := parseIndexBlob(marshalIndex(s))
	require.NoError(t, err)
	assert.Equal(t, 40, got.width())
	assert.Equal(t, s.Len(), got.Len())
	for k := range s.All() {
		assert.True(t, got.Contains(hashOfKey64(binary.BigEndian.Uint64(k[:]))))
	}
}

// hashOfKey64 is a full hash whose first several bytes are k.
func hashOfKey64(k uint64) actionHash {
	var h actionHash
	binary.BigEndian.PutUint64(h[:8], k)
	return h
}

// BenchmarkParseV2Index is the parse of a disk copy in the v2 form, which every
// go command pays before its first lookup, for a keys at many bits.
func BenchmarkParseV2Index(b *testing.B) {
	blob := MarshalIndexV2(40, prefixesOf(1_000_000, 40))
	b.ReportAllocs()
	b.SetBytes(int64(len(blob)))
	b.ResetTimer()
	for range b.N {
		_, _, err := parseIndexBlob(blob)
		require.Nil(b, err)
	}
}

func BenchmarkMarshalV2Index(b *testing.B) {
	values := prefixesOf(1_000_000, 40)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		MarshalIndexV2(40, values)
	}
}

func TestIndexV2RandomRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		bits := 32 + r.IntN(33)
		n := r.IntN(300)
		values := make([]uint64, n)
		for i := range values {
			values[i] = r.Uint64() >> (64 - bits)
		}
		slices.Sort(values)
		values = slices.Compact(values)
		got, _, err := ParseIndexV2(MarshalIndexV2(bits, values))
		require.NoError(t, err)
		assert.Equal(t, values, got)
	}
}
