package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func nearbyIndex(n int) *Index {
	idx := &Index{}
	for i := range n {
		idx.entries = append(idx.entries, indexEntry{
			compactKey: newCompactKey(footprintKey(i)),
			mtimeUnix:  int64(i),
		})
	}
	return idx
}

// nearbyKeyNum recovers i from the key footprintKey(i) produced.
func nearbyKeyNum(t *testing.T, key string) int {
	t.Helper()
	h, ok := extractActionHash(key)
	require.True(t, ok, "%q is not an indexed key", key)
	return int(h[0]) | int(h[1])<<8 | int(h[2])<<16
}

// The window's nearest entries come back taken from both sides of the
// midpoint. This is the ordering prefetch depends on: the objects written
// closest in time to the ones the build just asked for.
func TestNearbyKeysReturnsNearestFirst(t *testing.T) {
	idx := nearbyIndex(100)
	got := idx.NearbyKeys(20, 40, 5, nil, nil)
	require.Len(t, got, 5)

	nums := make([]int, len(got))
	for i, k := range got {
		nums[i] = nearbyKeyNum(t, k)
	}
	require.Equal(t, 30, nums[0], "the midpoint itself is nearest")
	require.ElementsMatch(t, []int{28, 29, 30, 31, 32}, nums, "the five nearest the midpoint")

	// Distance must not decrease along the result.
	for i := 1; i < len(nums); i++ {
		prev := abs(nums[i-1] - 30)
		cur := abs(nums[i] - 30)
		require.LessOrEqual(t, prev, cur, "result %v is not nearest-first", nums)
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// Nothing outside the window is ever offered, whatever the limit asks for.
func TestNearbyKeysStaysInsideTheWindow(t *testing.T) {
	idx := nearbyIndex(100)
	got := idx.NearbyKeys(40, 44, 50, nil, nil)
	require.Len(t, got, 5, "the window holds exactly five entries")
	for _, k := range got {
		n := nearbyKeyNum(t, k)
		require.GreaterOrEqual(t, n, 40)
		require.LessOrEqual(t, n, 44)
	}

	require.Empty(t, idx.NearbyKeys(1000, 2000, 10, nil, nil), "an empty window offers nothing")
	require.Empty(t, idx.NearbyKeys(0, 10, 0, nil, nil), "a zero limit asks for nothing")
}

// The requested keys are the anchor, not a candidate: a key the caller named
// is never offered back to it as a neighbour.
func TestNearbyKeysExcludesTheRequestedKeys(t *testing.T) {
	idx := nearbyIndex(100)
	exclude := map[string]bool{footprintKey(30): true, footprintKey(31): true}
	got := idx.NearbyKeys(20, 40, 4, exclude, nil)
	require.Len(t, got, 4)
	for _, k := range got {
		n := nearbyKeyNum(t, k)
		require.NotEqual(t, 30, n)
		require.NotEqual(t, 31, n)
	}
	require.Equal(t, 29, nearbyKeyNum(t, got[0]), "the nearest key that was not asked for")
}

// A skipped candidate does not stop the walk: it steps past it, so a client
// that already holds the nearest keys is still handed new ones.
func TestNearbyKeysWalksPastSkippedKeys(t *testing.T) {
	idx := nearbyIndex(100)
	held := map[int]bool{29: true, 30: true, 31: true}
	var examined int
	got := idx.NearbyKeys(0, 60, 3, nil, func(key string) bool {
		examined++
		return held[nearbyKeyNum(t, key)]
	})
	require.Len(t, got, 3)
	for _, k := range got {
		require.False(t, held[nearbyKeyNum(t, k)], "a skipped key must not be offered")
	}
	require.Equal(t, 6, examined, "the three held keys plus the three offered")
}

func TestNearbyKeysStopsAtTheScanBudget(t *testing.T) {
	idx := nearbyIndex(10000)
	var examined int
	got := idx.NearbyKeys(0, 10000, 10, nil, func(string) bool {
		examined++
		return true // the client holds everything
	})
	require.Empty(t, got)
	require.Equal(t, 10*nearbyScanFactor, examined, "the walk stops at limit*nearbyScanFactor")
}

// The selection must not allocate per candidate examined.
func TestNearbyKeysAllocatesOnlyItsResult(t *testing.T) {
	const (
		keys  = 50_000
		limit = 200
	)
	idx := nearbyIndex(keys)
	var got []string
	allocs := testing.AllocsPerRun(3, func() {
		got = idx.NearbyKeys(0, int64(keys), limit, nil, nil)
	})
	require.Len(t, got, limit)
	// A single slice for the result plus a single string per key returned.
	require.LessOrEqual(t, allocs, float64(limit+2),
		"selection must allocate its result and nothing per candidate examined")
}

// BenchmarkNearbyKeysWideWindow is the per-request cost of prefetch selection
// over a window holding far more entries than the limit, which is the shape a
// busy cache has.
func BenchmarkNearbyKeysWideWindow(b *testing.B) {
	for _, keys := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("%dkeys", keys), func(b *testing.B) {
			idx := nearbyIndex(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				idx.NearbyKeys(0, int64(keys), maxPrefetchEntries, nil, nil)
			}
		})
	}
}
