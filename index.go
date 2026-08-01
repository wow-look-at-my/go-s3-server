package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// gbciKeyPrefix is the constant leading portion of every cacheprog cache key.
// The binary index format ships only the 32-byte action-ID hash that follows
// this prefix; keys not matching the pattern are skipped.
const gbciKeyPrefix = "go-buildcache/v1"

// gbciHashSize is the number of bytes per entry in the binary index body.
const gbciHashSize = 32

// gbciHeaderSize is the fixed header size in bytes.
const gbciHeaderSize = 24

// gbciVersion is the wire-format version stored in the header.
const gbciVersion = 1

// gbciMagic is the four-byte file-format identifier "GBCI".
var gbciMagic = [4]byte{'G', 'B', 'C', 'I'}

// Index maintains an in-memory map of S3 keys to modification times, plus
// a precomputed binary blob of all action-ID hashes for the GET /_index
// endpoint.
//
// It's rebuilt from the filesystem on startup and updated on every PUT.
// Since it's derived from the actual storage, it can never be out of sync
// (at worst, a crash loses index entries for in-flight PUTs, which are
// re-added on the next startup rebuild).
type Index struct {
	mu      sync.RWMutex
	entries []indexEntry // sorted by mtime for NearbyKeys binary search

	// pendingEntries is an unsorted append-only buffer of new mtime entries
	// added by Put. Like pending (for hashes), it keeps the per-PUT path to a
	// single O(1) mutex-guarded append; the O(n log n) merge+sort is deferred to
	// the next reader (NearbyKeys/Remove). Previously Put re-sorted the whole
	// mtime list on every call under this same lock, which serialized writers
	// into a lock convoy under the concurrent CI matrix load — the upstream
	// stall a fronting proxy reported as a 502.
	pendingEntries []indexEntry

	// hashes is the sorted, deduplicated master list of action-ID hashes
	// extracted from S3 keys matching gbciKeyPrefix. Only mutated inside
	// Blob() under mu.Lock().
	hashes [][gbciHashSize]byte

	// pending is an unsorted append-only buffer of new hashes added by Put.
	// Drained into hashes during the next Blob() call. Keeping the per-PUT
	// path to a single mutex-guarded slice append (microseconds) is what
	// lets the server absorb bursts of ~1000 PUTs/sec without contention.
	pending [][gbciHashSize]byte

	// dirty is set true whenever pending grows or the master is rebuilt;
	// cleared by Blob() after a successful serialization.
	dirty atomic.Bool

	// cachedBlob and cachedETag hold the most recently built output. Read
	// under mu.RLock on the fast path when dirty is false.
	cachedBlob []byte
	cachedETag string
}

type indexEntry struct {
	key       string
	mtimeUnix int64
}

// extractActionHash decodes the 32-byte action ID from a cacheprog cache key.
// Returns (hash, true) if key matches `^go-buildcache/v1[0-9a-f]{64}$`,
// otherwise (zero, false).
func extractActionHash(key string) ([gbciHashSize]byte, bool) {
	var zero [gbciHashSize]byte
	if !strings.HasPrefix(key, gbciKeyPrefix) {
		return zero, false
	}
	hex64 := key[len(gbciKeyPrefix):]
	if len(hex64) != gbciHashSize*2 {
		return zero, false
	}
	var h [gbciHashSize]byte
	if _, err := hex.Decode(h[:], []byte(hex64)); err != nil {
		return zero, false
	}
	return h, true
}

// NewIndex builds the index by scanning the filesystem.
func NewIndex(storage *Storage) *Index {
	idx := &Index{}
	idx.rebuild(storage)
	return idx
}

// Put records a key with the current time and queues its action-ID hash
// (if the key is well-formed) for inclusion in the next /_index serialization.
//
// The hot path is a single mutex-guarded slice append: microseconds at
// any reasonable cache size. Sorting and serialization are deferred to
// the next Blob() call.
func (idx *Index) Put(key string, size int64) {
	now := time.Now().Unix()
	hash, hashOK := extractActionHash(key)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Append to the unsorted pending buffer only — O(1). The merge+sort into the
	// mtime-ordered list is deferred to the next reader (see drainEntriesLocked),
	// so a burst of concurrent PUTs no longer convoys behind a full re-sort.
	idx.pendingEntries = append(idx.pendingEntries, indexEntry{key: key, mtimeUnix: now})

	if hashOK {
		idx.pending = append(idx.pending, hash)
		idx.dirty.Store(true)
	}
	idx.updateGaugesLocked()
}

// updateGaugesLocked refreshes the index-size gauges. Caller must hold idx.mu.
// Three atomic stores — negligible next to the xattr writes on the PUT path.
func (idx *Index) updateGaugesLocked() {
	indexEntriesGauge.Set(float64(len(idx.entries) + len(idx.pendingEntries)))
	indexHashesGauge.Set(float64(len(idx.hashes)))
	indexPendingGauge.Set(float64(len(idx.pending)))
}

// drainEntriesLocked merges any pending mtime entries into the sorted master
// list and re-sorts. Must be called under idx.mu.Lock by any reader that needs
// idx.entries to be complete and mtime-ordered (NearbyKeys, Remove).
func (idx *Index) drainEntriesLocked() {
	if len(idx.pendingEntries) == 0 {
		return
	}
	idx.entries = append(idx.entries, idx.pendingEntries...)
	idx.pendingEntries = idx.pendingEntries[:0]
	sort.Slice(idx.entries, func(i, j int) bool {
		return idx.entries[i].mtimeUnix < idx.entries[j].mtimeUnix
	})
}

// Remove drops key from the index: its mtime entry and, when the key is a
// well-formed cacheprog key, its action-ID hash from the GBCI blob. Called when
// an object is deleted so the index stops advertising a key the store no longer
// has. Best-effort and O(n) in the index size; deletes are rare (operator
// eviction of a poisoned entry), so the linear scan is not on any hot path.
func (idx *Index) Remove(key string) {
	hash, hashOK := extractActionHash(key)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain first so a key still sitting in pendingEntries is removable too.
	idx.drainEntriesLocked()
	for i := range idx.entries {
		if idx.entries[i].key == key {
			idx.entries = append(idx.entries[:i], idx.entries[i+1:]...)
			break
		}
	}

	if hashOK {
		idx.pending = removeHash(idx.pending, hash)
		idx.hashes = removeHash(idx.hashes, hash)
		idx.dirty.Store(true)
	}
	idx.updateGaugesLocked()
}

// RemoveKeys drops a batch of keys from the index in one pass: their mtime
// entries and (for well-formed cacheprog keys) their action-ID hashes. The
// eviction sweeper calls it with the full victim set BEFORE unlinking any file,
// so the index stops advertising a key strictly before its body disappears —
// the opposite ordering (delete files during the sweep, rebuild the index only
// at sweep end) left every already-deleted key advertised for the remainder of
// the sweep, a window in which a GET of that key is a 404 on an indexed key
// (the miss_advertised_unservable signature). One filter pass over the index
// is O(n + len(keys)); the per-key Remove would be O(n) each.
func (idx *Index) RemoveKeys(keys []string) {
	if len(keys) == 0 {
		return
	}
	victimKeys := make(map[string]bool, len(keys))
	victimHashes := make(map[[gbciHashSize]byte]bool, len(keys))
	for _, k := range keys {
		victimKeys[k] = true
		if h, ok := extractActionHash(k); ok {
			victimHashes[h] = true
		}
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain first so keys still sitting in pendingEntries are removable too.
	idx.drainEntriesLocked()
	w := 0
	for _, e := range idx.entries {
		if !victimKeys[e.key] {
			idx.entries[w] = e
			w++
		}
	}
	idx.entries = idx.entries[:w]

	if len(victimHashes) > 0 {
		idx.hashes = filterHashes(idx.hashes, victimHashes)
		idx.pending = filterHashes(idx.pending, victimHashes)
		idx.dirty.Store(true)
	}
	idx.updateGaugesLocked()
}

// filterHashes returns s with every hash present in victims filtered out,
// reusing s's backing array (the result is always a prefix of s), so a sorted
// input stays sorted.
func filterHashes(s [][gbciHashSize]byte, victims map[[gbciHashSize]byte]bool) [][gbciHashSize]byte {
	out := s[:0]
	for _, x := range s {
		if !victims[x] {
			out = append(out, x)
		}
	}
	return out
}

// Contains reports whether the action hash is currently in the index (the
// sorted master list or the pending buffer) — i.e. whether the key is, or will
// be on the next serialization, advertised by /_index. Used to classify a GET
// 404 as "advertised but unservable" (index/store divergence) versus a plain
// not-found. O(log n) on the master plus O(pending); pending is bounded by the
// PUT burst since the last Blob().
func (idx *Index) Contains(h [gbciHashSize]byte) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	// hashes is always sorted: only Blob() writes it (sort+dedupe) and the
	// removal paths do order-preserving filtering.
	i := sort.Search(len(idx.hashes), func(i int) bool {
		return bytes.Compare(idx.hashes[i][:], h[:]) >= 0
	})
	if i < len(idx.hashes) && idx.hashes[i] == h {
		return true
	}
	for _, p := range idx.pending {
		if p == h {
			return true
		}
	}
	return false
}

// removeHash returns s with every occurrence of h filtered out, reusing s's
// backing array (the result is always a prefix of s). The action-ID hash is a
// 1:1 function of the key, so at most one entry matches.
func removeHash(s [][gbciHashSize]byte, h [gbciHashSize]byte) [][gbciHashSize]byte {
	out := s[:0]
	for _, x := range s {
		if x != h {
			out = append(out, x)
		}
	}
	return out
}

// NearbyKeys returns up to limit keys whose modification time falls within
// [startUnix, endUnix], sorted by distance from the midpoint, excluding
// keys in the exclude set.
func (idx *Index) NearbyKeys(startUnix, endUnix int64, limit int, exclude map[string]bool) []string {
	// Fast path: nothing pending means the sorted list is current — a read lock
	// lets concurrent batch GETs run in parallel. Slow path takes the write lock
	// once to drain+sort, then searches.
	idx.mu.RLock()
	if len(idx.pendingEntries) == 0 {
		keys := idx.nearbyKeysLocked(startUnix, endUnix, limit, exclude)
		idx.mu.RUnlock()
		return keys
	}
	idx.mu.RUnlock()

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.drainEntriesLocked()
	return idx.nearbyKeysLocked(startUnix, endUnix, limit, exclude)
}

// nearbyKeysLocked is the search itself. The caller must hold idx.mu (read or
// write) and must have ensured idx.entries is drained and mtime-sorted.
func (idx *Index) nearbyKeysLocked(startUnix, endUnix int64, limit int, exclude map[string]bool) []string {
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

// DropCachedBlob discards the serialized /_index body, keeping the hashes it
// was built from. At a million keys that blob is ~32 MB of the process's
// memory, held purely so repeat index fetches are free; the next fetch after a
// drop pays a re-serialization instead. That is the right trade when the
// alternative is an OOM kill, so the memory watcher's trim calls this.
func (idx *Index) DropCachedBlob() {
	idx.mu.Lock()
	idx.cachedBlob = nil
	idx.cachedETag = ""
	idx.mu.Unlock()
	// The hashes themselves are untouched, so the next Blob() rebuilds the same
	// bytes; marking dirty just keeps the fast path from trusting a nil blob.
	idx.dirty.Store(true)
}

// Blob returns the precomputed GBCI v1 binary index and its strong ETag
// (hex-encoded SHA-256 of the blob, surrounded by quotes per RFC 7232).
//
// Fast path: if the cached blob is up-to-date (dirty == false), return it
// under a read lock. Slow path: drain pending into hashes, re-sort, dedupe,
// serialize header + body + trailer, cache the result, clear dirty.
func (idx *Index) Blob() ([]byte, string) {
	if !idx.dirty.Load() {
		idx.mu.RLock()
		blob, etag := idx.cachedBlob, idx.cachedETag
		idx.mu.RUnlock()
		if blob != nil {
			return blob, etag
		}
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Re-check: another caller may have rebuilt the blob while we were
	// waiting on the lock.
	if !idx.dirty.Load() && idx.cachedBlob != nil {
		return idx.cachedBlob, idx.cachedETag
	}

	if len(idx.pending) > 0 {
		idx.hashes = append(idx.hashes, idx.pending...)
		idx.pending = idx.pending[:0]
	}
	sort.Slice(idx.hashes, func(i, j int) bool {
		return bytes.Compare(idx.hashes[i][:], idx.hashes[j][:]) < 0
	})
	// Dedupe sorted slice in place.
	w := 0
	for r := 0; r < len(idx.hashes); r++ {
		if w == 0 || idx.hashes[r] != idx.hashes[w-1] {
			idx.hashes[w] = idx.hashes[r]
			w++
		}
	}
	idx.hashes = idx.hashes[:w]

	count := uint64(len(idx.hashes))

	blob := make([]byte, gbciHeaderSize+int(count)*gbciHashSize+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = gbciHashSize
	binary.LittleEndian.PutUint16(blob[6:8], 0)
	binary.LittleEndian.PutUint64(blob[16:24], count)
	off := gbciHeaderSize
	for i := range idx.hashes {
		copy(blob[off:off+gbciHashSize], idx.hashes[i][:])
		off += gbciHashSize
	}
	// The generation field is CONTENT-DERIVED (the first 8 bytes of the hash
	// body's digest), not a serialization counter. That makes the whole blob —
	// and therefore the ETag (the trailer hash) — a pure function of the
	// advertised key set. The old monotonic counter sat inside the hashed
	// region, so duplicate-only PUT traffic (hash set unchanged) produced a
	// brand-new ETag on every reserialization and forced every client into a
	// pointless multi-MB /_index re-download with zero informational gain.
	// Identical key sets now serialize byte-identically, so If-None-Match
	// keeps answering 304 across duplicate PUTs and even server restarts.
	// (The client's parseIndexBlob validates magic/version/hashSize/length/
	// trailer and never reads this field, so the semantic change is invisible
	// to it; anything wanting a change-detection token still gets one, since
	// the value changes exactly when the key set does.)
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:off])
	binary.LittleEndian.PutUint64(blob[8:16], binary.LittleEndian.Uint64(bodyDigest[:8]))
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])

	idx.cachedBlob = blob
	idx.cachedETag = `"` + hex.EncodeToString(digest[:]) + `"`
	idx.dirty.Store(false)
	idx.updateGaugesLocked()
	return idx.cachedBlob, idx.cachedETag
}

func (idx *Index) rebuild(storage *Storage) {
	start := time.Now()
	objects, err := storage.Snapshot()
	if err != nil {
		log.Printf("index: rebuild failed: %v", err)
		return
	}
	entries, hashes := idx.applyRebuild(objects)
	indexRebuildDuration.Observe(time.Since(start).Seconds())
	log.Printf("index: built %d entries (%d hashes) in %v",
		entries, hashes, time.Since(start).Round(time.Millisecond))
}

// applyRebuild replaces the index's master state with a filesystem snapshot
// while PRESERVING the pending buffers. The snapshot walk (storage.List) runs
// with no index lock held and takes seconds on a large cache, so PUTs complete
// concurrently; each lives only in pending/pendingEntries until drained. The
// old code nil'd both buffers here, silently dropping every PUT that finished
// after the walk passed its shard — those keys then vanished from /_index (and
// from prefetch) until the NEXT rebuild, i.e. the next eviction sweep, forcing
// misses and duplicate re-uploads right after every sweep.
//
// Instead the walked hashes are prepended to the surviving pending buffer
// (Blob() sorts + dedupes, so a PUT the walk also saw costs nothing) and
// pendingEntries is left alone (drainEntriesLocked merges + sorts it into the
// fresh entries on the next read; a duplicate mtime entry is the same benign
// shape an overwrite PUT already produces). A PUT can therefore never be lost
// to a rebuild: it either lands in pending before the lock (merged here) or
// after (normal append path).
//
// Returns the entry and hash counts for logging.
func (idx *Index) applyRebuild(objects []ListObject) (int, int) {
	entries := make([]indexEntry, 0, len(objects))
	walked := make([][gbciHashSize]byte, 0, len(objects))
	for _, obj := range objects {
		entries = append(entries, indexEntry{
			key:       obj.Key,
			mtimeUnix: obj.LastModified.Unix(),
		})
		if h, ok := extractActionHash(obj.Key); ok {
			walked = append(walked, h)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].mtimeUnix < entries[j].mtimeUnix
	})

	idx.mu.Lock()
	idx.entries = entries
	// pendingEntries intentionally survives (see doc comment above).
	idx.hashes = idx.hashes[:0]
	idx.pending = append(walked, idx.pending...)
	idx.cachedBlob = nil
	idx.cachedETag = ""
	idx.dirty.Store(true)
	hashCount := len(idx.pending)
	idx.updateGaugesLocked()
	idx.mu.Unlock()
	return len(objects), hashCount
}
