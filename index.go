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

// indexEntry is one indexed object. The key is held as a compactKey so a
// million-object index costs no per-entry allocation; see compactkey.go.
type indexEntry struct {
	compactKey
	mtimeUnix int64
}

// maxRetainedPending caps how much pending-buffer capacity survives a drain. A
// rebuild or a PUT burst can grow these to the size of the whole cache, and
// reslicing to [:0] holds that array for the life of the process; re-growing a
// small buffer on the next burst is cheaper than keeping tens of megabytes
// permanently.
const maxRetainedPending = 4096

func resetPending[T any](s []T) []T {
	if cap(s) > maxRetainedPending {
		return nil
	}
	return s[:0]
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
	ck := newCompactKey(key)
	hash, hashOK := ck.actionHash()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Append to the unsorted pending buffer only — O(1). The merge+sort into the
	// mtime-ordered list is deferred to the next reader (see drainEntriesLocked),
	// so a burst of concurrent PUTs no longer convoys behind a full re-sort.
	idx.pendingEntries = append(idx.pendingEntries, indexEntry{compactKey: ck, mtimeUnix: now})

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
	idx.pendingEntries = resetPending(idx.pendingEntries)
	sortEntriesByMtime(idx.entries)
}

func sortEntriesByMtime(entries []indexEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].mtimeUnix < entries[j].mtimeUnix
	})
}

// Remove drops key from the index: its mtime entry and, when the key is a
// well-formed cacheprog key, its action-ID hash from the GBCI blob. Called when
// an object is deleted so the index stops advertising a key the store no longer
// has. Best-effort and O(n) in the index size; deletes are rare (operator
// eviction of a poisoned entry), so the linear scan is not on any hot path.
func (idx *Index) Remove(key string) {
	ck := newCompactKey(key)
	hash, hashOK := ck.actionHash()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain first so a key still sitting in pendingEntries is removable too.
	idx.drainEntriesLocked()
	for i := range idx.entries {
		if idx.entries[i].compactKey == ck {
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
	victimKeys := make(map[compactKey]bool, len(keys))
	victimHashes := make(map[[gbciHashSize]byte]bool, len(keys))
	for _, k := range keys {
		ck := newCompactKey(k)
		victimKeys[ck] = true
		if h, ok := ck.actionHash(); ok {
			victimHashes[h] = true
		}
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain first so keys still sitting in pendingEntries are removable too.
	idx.drainEntriesLocked()
	w := 0
	for _, e := range idx.entries {
		if !victimKeys[e.compactKey] {
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

// sortDedupeHashes sorts s ascending and drops duplicates in place, which is
// what Contains's binary search and the serialized blob both require.
func sortDedupeHashes(s [][gbciHashSize]byte) [][gbciHashSize]byte {
	sort.Slice(s, func(i, j int) bool {
		return bytes.Compare(s[i][:], s[j][:]) < 0
	})
	w := 0
	for r := 0; r < len(s); r++ {
		if w == 0 || s[r] != s[w-1] {
			s[w] = s[r]
			w++
		}
	}
	return s[:w]
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
	// The exclusion set arrives keyed by key string; convert it once (it is
	// bounded by the batch request that produced it) so the scan below can
	// compare compact keys instead of rebuilding a string per candidate.
	var excluded map[compactKey]bool
	if len(exclude) > 0 {
		excluded = make(map[compactKey]bool, len(exclude))
		for k := range exclude {
			excluded[newCompactKey(k)] = true
		}
	}

	// Fast path: nothing pending means the sorted list is current — a read lock
	// lets concurrent batch GETs run in parallel. Slow path takes the write lock
	// once to drain+sort, then searches.
	idx.mu.RLock()
	if len(idx.pendingEntries) == 0 {
		keys := idx.nearbyKeysLocked(startUnix, endUnix, limit, excluded)
		idx.mu.RUnlock()
		return keys
	}
	idx.mu.RUnlock()

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.drainEntriesLocked()
	return idx.nearbyKeysLocked(startUnix, endUnix, limit, excluded)
}

// nearbyKeysLocked is the search itself. The caller must hold idx.mu (read or
// write) and must have ensured idx.entries is drained and mtime-sorted.
//
// Candidates are carried as positions in idx.entries, not as keys: the window
// can hold far more entries than the limit, and only the survivors are worth
// rebuilding a key string for.
func (idx *Index) nearbyKeysLocked(startUnix, endUnix int64, limit int, excluded map[compactKey]bool) []string {
	// Binary search for the start of the time window.
	lo := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].mtimeUnix >= startUnix
	})

	// Collect candidates within the window.
	mid := (startUnix + endUnix) / 2
	type candidate struct {
		pos  int
		dist int64
	}
	var candidates []candidate
	for i := lo; i < len(idx.entries) && idx.entries[i].mtimeUnix <= endUnix; i++ {
		e := idx.entries[i]
		if excluded[e.compactKey] {
			continue
		}
		d := e.mtimeUnix - mid
		if d < 0 {
			d = -d
		}
		candidates = append(candidates, candidate{pos: i, dist: d})
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
		keys[i] = idx.entries[c.pos].Key()
	}
	return keys
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
		idx.pending = resetPending(idx.pending)
	}
	idx.hashes = sortDedupeHashes(idx.hashes)

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
	b := newIndexBuild(idx.entryCount())
	if err := storage.Walk(b.add); err != nil {
		log.Printf("index: rebuild failed: %v", err)
		return
	}
	b.finish()
	entries, hashes := idx.applyRebuild(b)
	indexRebuildDuration.Observe(time.Since(start).Seconds())
	log.Printf("index: built %d entries (%d hashes) in %v",
		entries, hashes, time.Since(start).Round(time.Millisecond))
}

// entryCount is the current number of indexed objects, used to size the next
// rebuild's buffers: a cache does not change size much between rebuilds, and
// growing a million-element slice by doubling costs an extra copy of itself.
func (idx *Index) entryCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries) + len(idx.pendingEntries)
}

// indexBuild accumulates a rebuild's state as the data_dir walk produces it, so
// a rebuild never materializes a second full copy of the cache's keys the way
// walking into a []ListObject did.
type indexBuild struct {
	entries []indexEntry
	hashes  [][gbciHashSize]byte
	sorted  bool
}

func newIndexBuild(sizeHint int) *indexBuild {
	return &indexBuild{
		entries: make([]indexEntry, 0, sizeHint),
		hashes:  make([][gbciHashSize]byte, 0, sizeHint),
	}
}

func (b *indexBuild) add(obj ListObject) {
	ck := newCompactKey(obj.Key)
	b.entries = append(b.entries, indexEntry{compactKey: ck, mtimeUnix: obj.LastModified.Unix()})
	if h, ok := ck.actionHash(); ok {
		b.hashes = append(b.hashes, h)
	}
}

// finish orders what the walk collected: entries by mtime for NearbyKeys, and
// hashes sorted+deduped so Contains can binary-search the master list the
// instant it is installed. Sorting here, off the index lock, is what keeps a
// million-element sort out of the critical section every rebuild.
func (b *indexBuild) finish() {
	if b.sorted {
		return
	}
	sortEntriesByMtime(b.entries)
	b.hashes = sortDedupeHashes(b.hashes)
	b.sorted = true
}

// applyRebuild replaces the index's master state with a filesystem walk's
// result while PRESERVING the pending buffers. The walk (Storage.Walk) runs
// with no index lock held and takes seconds on a large cache, so PUTs complete
// concurrently; each lives only in pending/pendingEntries until drained. The
// old code nil'd both buffers here, silently dropping every PUT that finished
// after the walk passed its shard — those keys then vanished from /_index (and
// from prefetch) until the NEXT rebuild, i.e. the next eviction sweep, forcing
// misses and duplicate re-uploads right after every sweep.
//
// Instead the walked state replaces only the master lists: the surviving
// pending buffers are left alone (Blob() sorts + dedupes pending into the
// hashes, and drainEntriesLocked merges + sorts pendingEntries into the fresh
// entries on the next read; a duplicate is the same benign shape an overwrite
// PUT already produces). A PUT can therefore never be lost to a rebuild: it
// either lands in pending before the lock (kept here) or after (normal append
// path).
//
// The walked hashes go straight into the sorted master list rather than through
// pending, which is both one full-size copy cheaper and immediately searchable
// by Contains -- b.finish() has already sorted and deduped them.
//
// Returns the entry and hash counts for logging.
func (idx *Index) applyRebuild(b *indexBuild) (int, int) {
	// Contains binary-searches the master list the moment it is installed, so an
	// unsorted build would answer wrongly. finish() is a no-op if the caller
	// already did it off-lock, which is where it belongs.
	b.finish()

	idx.mu.Lock()
	idx.entries = b.entries
	idx.hashes = b.hashes
	// pending and pendingEntries intentionally survive (see doc comment above).
	idx.cachedBlob = nil
	idx.cachedETag = ""
	idx.dirty.Store(true)
	hashCount := len(idx.hashes) + len(idx.pending)
	idx.updateGaugesLocked()
	idx.mu.Unlock()
	return len(b.entries), hashCount
}
