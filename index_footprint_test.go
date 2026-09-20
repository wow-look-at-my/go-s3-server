package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// indexMaxBytesPerKey and blobMaxBytesPerKey are the ceilings the tests below
// hold the index to.
const (
	indexMaxBytesPerKey = 130.0
	// With it off, which is what a server with prefetch off runs.
	indexNoEntriesMaxBytesPerKey = 72.0
	// The blob is a copy of the hash list, so serializing costs a single
	// hash per key and no more.
	blobMaxBytesPerKey = 40.0
)

// The index is the server's largest resident structure by an order of
// magnitude, and it has no profile in production. These measure it instead:
// build any of a known size, read the live heap, and divide.
//
// footprintKeys is small enough to run in a unit test and large enough that
// the per-key figure is not dominated by fixed overhead.
const footprintKeys = 200_000

// heapLive is the live heap after a full collection. GCs, because the
// earliest can leave finalizer-reachable objects the next reclaims.
func heapLive() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// footprintKey is a cacheprog key for entry n.
func footprintKey(n int) string {
	var h [gbciHashSize]byte
	h[0] = byte(n)
	h[1] = byte(n >> 8)
	h[2] = byte(n >> 16)
	return gbciKeyPrefix + hex.EncodeToString(h[:])
}

// fillIndex puts n keys into a fresh index.
func fillIndex(n int) *Index {
	return fillIndexTracking(n, true)
}

// fillIndexTracking builds an index of n keys with the mtime list on or off.
func fillIndexTracking(n int, entries bool) *Index {
	idx := &Index{entriesOff: !entries}
	idx.blobMinInterval.Store(0)
	for i := range n {
		idx.Put(footprintKey(i), 4096)
	}
	return idx
}

// The bound is per key over the whole index: the mtime-sorted entry list, the
// sorted hash list, and the serialized blob the /_index endpoint serves
// between rebuilds.
func TestIndexPerKeyFootprint(t *testing.T) {
	before := heapLive()
	idx := fillIndex(footprintKeys)
	// Serializing is part of steady state: the blob is held until the next
	// a single replaces it.
	blob, _ := idx.Blob()
	require.NotEmpty(t, blob)
	// NearbyKeys drains the pending entry buffer into the sorted list, which
	// is the shape the index spends its life in.
	idx.NearbyKeys(0, time.Now().Add(time.Hour).Unix(), 10, set.Set[string]{}, nil)

	after := heapLive()
	runtime.KeepAlive(idx)
	runtime.KeepAlive(blob)

	perKey := float64(after-before) / float64(footprintKeys)
	t.Logf("index footprint: %d keys, %.1f MB live, %.1f bytes/key",
		footprintKeys, float64(after-before)/(1<<20), perKey)
	t.Logf("  projected at 1.7M keys: %.0f MB", perKey*1_700_000/(1<<20))

	require.Less(t, perKey, indexMaxBytesPerKey,
		"an indexed key must not cost more than %.0f bytes of RAM; see index.go", indexMaxBytesPerKey)

	blobPerKey := float64(len(blob)) / float64(footprintKeys)
	require.Less(t, blobPerKey, blobMaxBytesPerKey,
		"the blob is the hash list plus a fixed header and trailer, so a key must cost about one hash in it")
}

// TestIndexWithoutEntriesCostsLess pins the saving a server with prefetch off
// gets: the mtime list is the largest of each structures, and nothing reads
// it when no window is ever selected.
func TestIndexWithoutEntriesCostsLess(t *testing.T) {
	before := heapLive()
	idx := fillIndexTracking(footprintKeys, false)
	blob, _ := idx.Blob()
	require.NotEmpty(t, blob)
	after := heapLive()
	runtime.KeepAlive(idx)
	runtime.KeepAlive(blob)

	perKey := float64(after-before) / float64(footprintKeys)
	t.Logf("index footprint without the mtime list: %.1f bytes/key (%.0f MB at 1.7M keys)",
		perKey, perKey*1_700_000/(1<<20))
	require.Less(t, perKey, indexNoEntriesMaxBytesPerKey,
		"with the mtime list off, a key must cost no more than the hash plus the blob")

	require.Empty(t, idx.entries)
	require.Empty(t, idx.pendingEntries)
	require.Empty(t, idx.NearbyKeys(0, time.Now().Add(time.Hour).Unix(), 10, set.Set[string]{}, nil),
		"no list means no candidates, which is what nobody asking for a window gets anyway")
	require.False(t, idx.EntryTrackingEnabled())

	// The hashes are all still there: /_index advertises exactly as much.
	require.Len(t, blob, gbciHeaderSize+footprintKeys*gbciHashSize+sha256.Size)
}

// TestIndexEntrySize pins the per-entry struct.
func TestIndexEntrySize(t *testing.T) {
	require.Equal(t, uintptr(indexEntryBytes), unsafe.Sizeof(indexEntry{}),
		"an index entry grew; that is %d bytes per key across the whole cache", indexEntryBytes)
	require.Equal(t, uintptr(indexHashBytes), unsafe.Sizeof([gbciHashSize]byte{}))
}

func BenchmarkIndexPut(b *testing.B) {
	idx := &Index{}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = footprintKey(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		idx.Put(keys[i%len(keys)], 4096)
	}
}

// BenchmarkIndexBlob is what a serialization costs, which happens at most
// a single time per blob interval but allocates the whole blob when it does.
func BenchmarkIndexBlob(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("%dkeys", n), func(b *testing.B) {
			idx := fillIndex(n)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				idx.dirty.Store(true)
				idx.builtAt = time.Time{}
				idx.Blob()
			}
		})
	}
}

// BenchmarkIndexNearbyKeys is the prefetch selection, which runs on every
// batch GET that asks for a window.
func BenchmarkIndexNearbyKeys(b *testing.B) {
	idx := fillIndex(100_000)
	idx.NearbyKeys(0, time.Now().Add(time.Hour).Unix(), 1, set.Set[string]{}, nil)
	end := time.Now().Add(time.Hour).Unix()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		idx.NearbyKeys(0, end, maxPrefetchEntries, set.Set[string]{}, nil)
	}
}
