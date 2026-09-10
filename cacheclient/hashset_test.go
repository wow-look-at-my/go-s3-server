package cacheclient

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

func hashN(n int) actionHash {
	var h actionHash
	h[0] = byte(n >> 24)
	h[1] = byte(n >> 16)
	h[2] = byte(n >> 8)
	h[3] = byte(n)
	return h
}

// The set answers the same questions a map did, through every compaction the
// mutation counts force. A build adds thousands of keys after startup, so the
// merge path runs many times over a run and a wrong merge is a silent wrong
// answer: a cache hit reported as a miss, or a Put skipped for a key the remote
// does not hold.
func TestHashSetMatchesAMapThroughEveryCompaction(t *testing.T) {
	s := newHashSet(0)
	want := set.New[actionHash]()
	rng := rand.New(rand.NewSource(1))

	for range 20000 {
		h := hashN(rng.Intn(3000))
		if rng.Intn(3) == 0 {
			s.Remove(h)
			want.Remove(h)
		} else {
			s.Add(h)
			want.Add(h)
		}
		require.Equal(t, want.Len(), s.Len())
	}
	for h := range want.All() {
		require.True(t, s.Contains(h), "missing a key the set was told to hold")
	}
	seen := map[actionHash]int{}
	for h := range s.All() {
		seen[h]++
	}
	require.Len(t, seen, want.Len())
	for h, n := range seen {
		assert.Equal(t, 1, n, "yielded a key twice")
		assert.True(t, want.Contains(h), "yielded a key that was removed")
	}
	for i := range 3000 {
		h := hashN(i)
		assert.Equal(t, want.Contains(h), s.Contains(h))
	}
}

// The sorted slice must stay sorted, because every lookup binary-searches it. A
// merge that left one element out of place would answer "absent" for a key the
// set holds, and nothing else in the type would notice.
func TestHashSetStaysSortedAcrossMerges(t *testing.T) {
	s := newHashSet(0)
	rng := rand.New(rand.NewSource(2))
	for range 5000 {
		s.Add(hashN(rng.Intn(100000)))
	}
	s.compact()
	require.True(t, slices.IsSortedFunc(s.sorted, compareHash))
	require.Equal(t, len(s.sorted), len(slices.CompactFunc(slices.Clone(s.sorted), func(a, b actionHash) bool { return a == b })))
}

// Re-adding a removed key restores it rather than duplicating it: the key is
// still in the sorted slice, and the removal is what goes away.
func TestHashSetReAddCancelsARemoval(t *testing.T) {
	s := newHashSet(0)
	h := hashN(7)
	s.Add(h)
	s.compact()
	s.Remove(h)
	require.False(t, s.Contains(h))
	s.Add(h)
	require.True(t, s.Contains(h))
	require.Equal(t, 1, s.Len())
	s.compact()
	require.Equal(t, 1, s.Len())
	require.Len(t, s.sorted, 1)
}

// The index blob's body arrives ascending, so adopting it costs no sort. The
// constructor must still sort a body that does not, because a set built out of
// order answers wrongly for most of its keys.
func TestHashSetFromSortedFixesAnUnsortedBody(t *testing.T) {
	in := []actionHash{hashN(9), hashN(3), hashN(3), hashN(5)}
	s := newHashSetFromSorted(in)
	require.True(t, slices.IsSortedFunc(s.sorted, compareHash))
	require.Equal(t, 3, s.Len())
	for _, n := range []int{3, 5, 9} {
		assert.True(t, s.Contains(hashN(n)))
	}
	assert.False(t, s.Contains(hashN(4)))
}

// What the type is for: the keys and nothing else. A map charged about eight
// times the key material to hold the same hashes, which is what put a windows
// runner into "Out of memory" with an empty log.
func TestHashSetHoldsOnlyTheKeyMaterial(t *testing.T) {
	const n = 50000
	hashes := make([]actionHash, n)
	for i := range hashes {
		hashes[i] = hashN(i)
	}
	s := newHashSetFromSorted(hashes)
	require.Equal(t, n, s.Len())
	assert.LessOrEqual(t, cap(s.sorted), n+compactAt, "the sorted slice carries growth slack it never earned")
	assert.Equal(t, 0, s.pending.Len())
	assert.Equal(t, 0, s.removed.Len())
}

// Both buffers are bounded, so the map overhead this type avoids cannot come
// back through them: an unbounded pending buffer is the same map by another name.
func TestHashSetBuffersStayBounded(t *testing.T) {
	s := newHashSet(0)
	for i := range 10000 {
		s.Add(hashN(i))
		require.Less(t, s.pending.Len(), compactAt)
	}
	for i := range 10000 {
		s.Remove(hashN(i))
		require.Less(t, s.removed.Len(), compactAt)
	}
	require.Equal(t, 0, s.Len())
}
