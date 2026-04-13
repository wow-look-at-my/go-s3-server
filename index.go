package main

import (
	"log"
	"sort"
	"sync"
	"time"
)

// Index maintains an in-memory map of S3 keys to modification times.
// It's rebuilt from the filesystem on startup and updated on every PUT.
// Since it's derived from the actual storage, it can never be out of sync
// (at worst, a crash loses index entries for in-flight PUTs, which are
// re-added on the next startup rebuild).
type Index struct {
	mu      sync.RWMutex
	entries []indexEntry // sorted by mtime for binary search
}

type indexEntry struct {
	key       string
	mtimeUnix int64
}

// NewIndex builds the index by scanning the filesystem.
func NewIndex(storage *Storage) *Index {
	idx := &Index{}
	idx.rebuild(storage)
	return idx
}

// Put records a key with the current time.
func (idx *Index) Put(key string, size int64) {
	now := time.Now().Unix()
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Append and re-sort. This is O(n log n) but PUTs are infrequent
	// relative to the index size, and the sort is over a pre-sorted slice
	// (one element out of place → nearly O(n)).
	idx.entries = append(idx.entries, indexEntry{key: key, mtimeUnix: now})
	sort.Slice(idx.entries, func(i, j int) bool {
		return idx.entries[i].mtimeUnix < idx.entries[j].mtimeUnix
	})
}

// NearbyKeys returns up to limit keys whose modification time falls within
// [startUnix, endUnix], sorted by distance from the midpoint, excluding
// keys in the exclude set.
func (idx *Index) NearbyKeys(startUnix, endUnix int64, limit int, exclude map[string]bool) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Binary search for the start of the time window.
	lo := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].mtimeUnix >= startUnix
	})

	// Collect candidates within the window.
	mid := (startUnix + endUnix) / 2
	type candidate struct {
		key  string
		dist int64
	}
	var candidates []candidate
	for i := lo; i < len(idx.entries) && idx.entries[i].mtimeUnix <= endUnix; i++ {
		e := idx.entries[i]
		if exclude[e.key] {
			continue
		}
		d := e.mtimeUnix - mid
		if d < 0 {
			d = -d
		}
		candidates = append(candidates, candidate{key: e.key, dist: d})
	}

	// Sort by distance from center.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	keys := make([]string, len(candidates))
	for i, c := range candidates {
		keys[i] = c.key
	}
	return keys
}

func (idx *Index) rebuild(storage *Storage) {
	start := time.Now()
	result, err := storage.List("", 1000000, "")
	if err != nil {
		log.Printf("index: rebuild failed: %v", err)
		return
	}

	idx.mu.Lock()
	idx.entries = make([]indexEntry, 0, len(result.Objects))
	for _, obj := range result.Objects {
		idx.entries = append(idx.entries, indexEntry{
			key:       obj.Key,
			mtimeUnix: obj.LastModified.Unix(),
		})
	}
	sort.Slice(idx.entries, func(i, j int) bool {
		return idx.entries[i].mtimeUnix < idx.entries[j].mtimeUnix
	})
	idx.mu.Unlock()

	log.Printf("index: built %d entries in %v", len(idx.entries), time.Since(start).Round(time.Millisecond))
}
