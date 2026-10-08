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

	"github.com/wow-look-at-my/go-containers/set"
)

// gbciKeyPrefix is the constant leading portion of every cacheprog cache key.
const gbciKeyPrefix = "go-buildcache/v1"

// gbciHashSize is the number of bytes per entry in the binary index body.
const gbciHashSize = 32

// gbciHeaderSize is the fixed header size in bytes.
const gbciHeaderSize = 24

// gbciVersion is the wire-format version stored in the header.
const gbciVersion = 1

var gbciMagic = [4]byte{'G', 'B', 'C', 'I'}

// Index maintains an in-memory map of S3 keys to modification times. Plus
// a precomputed binary blob of all action-ID hashes for the GET /_index
// endpoint.
type Index struct {
	mu      sync.RWMutex
	entries []indexEntry // sorted by mtime for NearbyKeys binary search

	// entriesOff turns the mtime list off.
	entriesOff bool

	// pendingEntries is an unsorted append-only buffer of new mtime entries added by Put.
	pendingEntries []indexEntry

	// hashes is the sorted, deduplicated master list of action-ID hashes extracted from S3 keys matching gbciKeyPrefix.
	hashes [][gbciHashSize]byte

	// pending is an unsorted append-only buffer of new hashes added by Put.
	pending [][gbciHashSize]byte

	// dirty is set true whenever pending grows or the master is rebuilt.
	dirty atomic.Bool

	// cachedBlob and cachedETag hold the most recently built output.
	cachedBlob []byte
	cachedETag string
	// builtAt is when cachedBlob was serialized. Blob serves the cached blob for blobMinInterval after it, dirty or not.
	builtAt time.Time
	// blobMinInterval is the least time between serializations, in nanoseconds.
	blobMinInterval atomic.Int64
}

// defaultIndexBlobInterval is the least time between serializations of the index unless the config says otherwise.
const defaultIndexBlobInterval = 15 * time.Second

// indexEntry is a single indexed object.
type indexEntry struct {
	compactKey
	mtimeUnix int64
}

// maxRetainedPending caps how much pending-buffer capacity survives a drain.
const indexEntryBytes = gbciHashSize + 16 + 8

// indexHashBytes is what a single key costs in the sorted hash list, which is also what it costs in the serialized blob.
const indexHashBytes = gbciHashSize

// indexEntriesDefault is whether a new Index maintains the mtime list.
var indexEntriesDefault = true

// SetIndexEntryTracking decides whether indexes built after it maintain the mtime list. Call it before NewStorage.
func SetIndexEntryTracking(on bool) { indexEntriesDefault = on }

// maxRetainedPending caps how much pending-buffer capacity survives a drain.
const maxRetainedPending = 4096

func resetPending[T any](s []T) []T {
	if cap(s) > maxRetainedPending {
		return nil
	}
	return s[:0]
}

// extractActionHash decodes the 32-byte action ID from a cacheprog cache key.
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
	idx := &Index{entriesOff: !indexEntriesDefault}
	idx.blobMinInterval.Store(int64(defaultIndexBlobInterval))
	idx.rebuild(storage)
	return idx
}

// SetBlobInterval sets the least time between serializations. empty serializes every PUT on the next read.
func (idx *Index) SetBlobInterval(d time.Duration) { idx.blobMinInterval.Store(int64(d)) }

// DisableEntryTracking stops the index maintaining the mtime-sorted entry
// list and releases what it holds.
func (idx *Index) DisableEntryTracking() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entriesOff = true
	idx.entries, idx.pendingEntries = nil, nil
	idx.updateGaugesLocked()
}

// EntryTrackingEnabled reports whether the mtime list is maintained.
func (idx *Index) EntryTrackingEnabled() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return !idx.entriesOff
}

// BlobInterval reports the least time between serializations.
func (idx *Index) BlobInterval() time.Duration { return time.Duration(idx.blobMinInterval.Load()) }

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

	// drainEntriesLocked merges and sorts it for the next reader.
	if !idx.entriesOff {
		idx.pendingEntries = append(idx.pendingEntries, indexEntry{compactKey: ck, mtimeUnix: now})
	}

	if hashOK {
		idx.pending = append(idx.pending, hash)
		idx.dirty.Store(true)
	}
	idx.updateGaugesLocked()
}

// updateGaugesLocked refreshes the index-size gauges. Caller must hold idx.mu.
// atomic stores — negligible next to the xattr writes on the PUT path.
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

// Remove drops key from the index. Its mtime entry and, when the key is a
// well-formed cacheprog key, its action-ID hash from the GBCI blob. Called when
// an object is deleted so the index stops advertising a key the store no longer
// has. Best-effort and O(n) in the index size; deletes are rare (operator
// eviction of a poisoned entry), so the linear scan is not on any hot path.
func (idx *Index) Remove(key string) {
	ck := newCompactKey(key)
	hash, hashOK := ck.actionHash()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain earliest so a key still sitting in pendingEntries is removable too.
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
		idx.builtAt = time.Time{} // a removed key is not advertised for the interval
	}
	idx.updateGaugesLocked()
}

// RemoveKeys drops a batch of keys from the index in a single pass: their mtime
// entries and (for well-formed cacheprog keys) their action-ID hashes. a single
// filter pass over the index is O(n + len(keys)); the per-key Remove would be
// O(n) each.
func (idx *Index) RemoveKeys(keys []string) {
	if len(keys) == 0 {
		return
	}
	victimKeys := set.New[compactKey](len(keys))
	victimHashes := make(map[[gbciHashSize]byte]bool, len(keys))
	for _, k := range keys {
		ck := newCompactKey(k)
		victimKeys.Add(ck)
		if h, ok := ck.actionHash(); ok {
			victimHashes[h] = true
		}
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Drain earliest so keys still sitting in pendingEntries are removable too.
	idx.drainEntriesLocked()
	w := 0
	for _, e := range idx.entries {
		if !victimKeys.Contains(e.compactKey) {
			idx.entries[w] = e
			w++
		}
	}
	idx.entries = idx.entries[:w]

	if len(victimHashes) > 0 {
		idx.hashes = filterHashes(idx.hashes, victimHashes)
		idx.pending = filterHashes(idx.pending, victimHashes)
		idx.dirty.Store(true)
		idx.builtAt = time.Time{} // removed keys are not advertised for the interval
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

// Contains reports whether the action hash is in the index (the sorted master
// list or the pending buffer) — i.e. whether the key is. Or will be on the
// next serialization, advertised by /_index. O(log n) on the master plus
// O(pending); pending is bounded by the PUT burst since the last Blob().
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

// mergeSortedHashes returns the union of sorted, deduplicated hash lists,
// sorted and deduplicated, in a single pass.
func mergeSortedHashes(a, b [][gbciHashSize]byte) [][gbciHashSize]byte {
	if len(b) == 0 {
		return a
	}
	out := make([][gbciHashSize]byte, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := bytes.Compare(a[i][:], b[j][:]); {
		case c < 0:
			out = append(out, a[i])
			i++
		case c > 0:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// removeHash returns s with every occurrence of h filtered out, reusing s's
// backing array (the result is always a prefix of s).
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
// [startUnix, endUnix], sorted by distance from the midpoint, excluding keys in
// the exclude set. Any key skip reports as unwanted.
func (idx *Index) NearbyKeys(startUnix, endUnix int64, limit int, exclude set.Set[string], skip func(string) bool) []string {
	// The exclusion set arrives keyed by key string.
	excluded := set.New[compactKey](exclude.Len())
	for key := range exclude.All() {
		excluded.Add(newCompactKey(key))
	}

	// Fast path.
	idx.mu.RLock()
	if len(idx.pendingEntries) == 0 {
		keys := idx.nearbyKeysLocked(startUnix, endUnix, limit, excluded, skip)
		idx.mu.RUnlock()
		return keys
	}
	idx.mu.RUnlock()

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.drainEntriesLocked()
	return idx.nearbyKeysLocked(startUnix, endUnix, limit, excluded, skip)
}

// nearbyScanFactor bounds how many candidates a skip-heavy scan examines, as a multiple of the limit.
const nearbyScanFactor = 8

// nearbyKeysLocked is the search itself. The caller must hold idx.mu (read or
// write) and must have ensured idx.entries is drained and mtime-sorted.
//
// It walks OUTWARD from the window's midpoint rather than collecting the
// window and sorting it. Entries are mtime-sorted, so distance from the
// midpoint rises monotonically in each direction. Taking whichever side is
// nearer, a single at a time, yields exactly the nearest-earliest order a
// sort would, and stops as soon. As the limit is full.
//
// Only a survivor is turned into a key string: rebuilding a single per
// examined candidate is the other allocation this bounds.
func (idx *Index) nearbyKeysLocked(startUnix, endUnix int64, limit int, excluded set.Set[compactKey], skip func(string) bool) []string {
	if limit <= 0 || len(idx.entries) == 0 {
		return nil
	}
	// The window's bounds, as positions: lo is its earliest entry, hi a
	// single past its last.
	lo := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].mtimeUnix >= startUnix
	})
	hi := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].mtimeUnix > endUnix
	})
	if lo >= hi {
		return nil
	}

	// left and right are the next candidates on each side of the midpoint.
	mid := (startUnix + endUnix) / 2
	right := sort.Search(hi-lo, func(i int) bool {
		return idx.entries[lo+i].mtimeUnix >= mid
	}) + lo
	left := right - 1

	// Take the nearest candidates the caller still wants. Without skip this is
	// the earliest limit of them.
	dist := func(pos int) int64 {
		d := idx.entries[pos].mtimeUnix - mid
		if d < 0 {
			return -d
		}
		return d
	}

	keys := make([]string, 0, limit)
	examined := 0
	// The scan budget bounds a skip-heavy request: a client deep into a build has been sent most of the window already.
	budget := limit * nearbyScanFactor
	for len(keys) < limit && (left >= lo || right < hi) {
		// Take whichever side is nearer; with only a single side left, take it.
		var pos int
		switch {
		case left < lo:
			pos, right = right, right+1
		case right >= hi:
			pos, left = left, left-1
		case dist(left) <= dist(right):
			pos, left = left, left-1
		default:
			pos, right = right, right+1
		}

		if excluded.Contains(idx.entries[pos].compactKey) {
			continue
		}
		if skip == nil {
			keys = append(keys, idx.entries[pos].Key())
			continue
		}
		if examined == budget {
			// Out of scan budget with the limit unfilled.
			nearbyScanExhaustedTotal.Inc()
			break
		}
		examined++
		key := idx.entries[pos].Key()
		if skip(key) {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

// blobServableLocked reports whether the cached blob may be handed out: it is
// current, or it is younger than the blob interval. The caller holds idx.mu.
func (idx *Index) blobServableLocked() bool {
	if idx.cachedBlob == nil {
		return false
	}
	return !idx.dirty.Load() || time.Since(idx.builtAt) < idx.BlobInterval()
}

// Blob answers the serialized index and its ETag.
//
// Fast path: a servable cached blob is returned under a read lock. Slow path: merge pending into
// hashes, serialize header + body + trailer, cache the result, clear dirty. Callers arriving
// during a serialization wait on the read lock and all receive the blob it produces. A burst of
// GETs costs a single serialization.
func (idx *Index) Blob() ([]byte, string) {
	idx.mu.RLock()
	cached, etag, fresh := idx.cachedBlob, idx.cachedETag, idx.blobServableLocked()
	idx.mu.RUnlock()
	if fresh {
		return cached, etag
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Re-check: another caller may have rebuilt the blob while we were
	// waiting on the lock.
	if idx.blobServableLocked() {
		return idx.cachedBlob, idx.cachedETag
	}

	lockedAt := time.Now()
	pendingCount := len(idx.pending)
	log.Printf("index: serialize locked (hashes=%d pending=%d)", len(idx.hashes), pendingCount)
	defer func() {
		log.Printf("index: serialize unlocked after %v (hashes=%d)", time.Since(lockedAt), len(idx.hashes))
	}()

	// hashes is sorted and deduplicated already. The pending buffer is small
	// next to it, so a merge costs O(n) where a re-sort cost O(n log n).
	if pendingCount > 0 {
		idx.hashes = mergeSortedHashes(idx.hashes, sortDedupeHashes(idx.pending))
		idx.pending = resetPending(idx.pending)
	}

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
	// That makes the whole blob — and therefore the ETag.
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:off])
	binary.LittleEndian.PutUint64(blob[8:16], binary.LittleEndian.Uint64(bodyDigest[:8]))
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])

	idx.cachedBlob = blob
	idx.cachedETag = `"` + hex.EncodeToString(digest[:]) + `"`
	idx.builtAt = time.Now()
	idx.dirty.Store(false)
	idx.updateGaugesLocked()
	return idx.cachedBlob, idx.cachedETag
}

func (idx *Index) rebuild(storage *Storage) {
	start := time.Now()
	b := newIndexBuild(idx.hashCount(), !idx.EntryTrackingEnabled())
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

// hashCount sizes a rebuild's buffers.
func (idx *Index) hashCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.hashes) + len(idx.pending)
}

// indexBuild accumulates a rebuild's state as the data_dir walk produces it.
type indexBuild struct {
	entries []indexEntry
	hashes  [][gbciHashSize]byte
	sorted  bool
	// entriesOff carries the index's setting into the walk.
	entriesOff bool
}

func newIndexBuild(sizeHint int, entriesOff bool) *indexBuild {
	b := &indexBuild{
		hashes:     make([][gbciHashSize]byte, 0, sizeHint),
		entriesOff: entriesOff,
	}
	if !entriesOff {
		b.entries = make([]indexEntry, 0, sizeHint)
	}
	return b
}

func (b *indexBuild) add(obj ListObject) {
	ck := newCompactKey(obj.Key)
	if !b.entriesOff {
		b.entries = append(b.entries, indexEntry{compactKey: ck, mtimeUnix: obj.LastModified.Unix()})
	}
	if h, ok := ck.actionHash(); ok {
		b.hashes = append(b.hashes, h)
	}
}

// finish orders what the walk collected.
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
// with no index lock held and takes seconds on a large cache, so PUTs
// complete concurrently. Each lives only in pending/pendingEntries until
// drained.
func (idx *Index) applyRebuild(b *indexBuild) (int, int) {
	// Contains binary-searches the master list the moment it is installed.
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
