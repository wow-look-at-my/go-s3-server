package main

import (
	"errors"
	"io/fs"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Access tracking is the IN-MEMORY half of eviction's least-recently-used
// decisions, and the fallback half: the durable record of a read is the
// filesystem's own access time (atime.go), which survives restarts. This map is
// enabled only when the data_dir turns out not to record access times, because
// one entry per key read since startup costs real memory at a million keys.
// See the accessShards field on Storage.

const accessShardCount = 256

type accessShard struct {
	mu sync.Mutex
	m  map[compactKey]int64 // key -> last-access unix seconds
}

// EnableAccessTracking turns on per-key last-access bookkeeping for eviction.
// Call once at startup, before serving, when eviction is configured and the
// data_dir's filesystem does not record access times itself.
func (s *Storage) EnableAccessTracking() {
	shards := make([]*accessShard, accessShardCount)
	for i := range shards {
		shards[i] = &accessShard{m: make(map[compactKey]int64)}
	}
	s.accessShards = shards
}

// accessShardFor returns the shard owning ck. Action IDs are uniformly
// distributed, so their first byte shards evenly; other keys hash their string.
func (s *Storage) accessShardFor(ck compactKey) *accessShard {
	if h, ok := ck.actionHash(); ok {
		return s.accessShards[uint32(h[0])%accessShardCount]
	}
	return s.accessShards[fnv1a(ck.raw)%accessShardCount]
}

// recordAccess stamps key as used now. No-op unless access tracking is enabled.
func (s *Storage) recordAccess(key string) {
	if s.accessShards == nil {
		return
	}
	ck := newCompactKey(key)
	sh := s.accessShardFor(ck)
	now := time.Now().Unix()
	sh.mu.Lock()
	sh.m[ck] = now
	sh.mu.Unlock()
}

// lastAccess returns the last-access time (unix seconds) of key, if recorded.
func (s *Storage) lastAccess(key string) (int64, bool) {
	return s.lastAccessOf(newCompactKey(key))
}

func (s *Storage) lastAccessOf(ck compactKey) (int64, bool) {
	if s.accessShards == nil {
		return 0, false
	}
	sh := s.accessShardFor(ck)
	sh.mu.Lock()
	t, ok := sh.m[ck]
	sh.mu.Unlock()
	return t, ok
}

// forgetAccess drops key's access record (called when the key is removed).
func (s *Storage) forgetAccess(key string) {
	if s.accessShards == nil {
		return
	}
	ck := newCompactKey(key)
	sh := s.accessShardFor(ck)
	sh.mu.Lock()
	delete(sh.m, ck)
	sh.mu.Unlock()
}

// pruneAccess drops access records for keys not present in live, so the map
// cannot accumulate entries for objects that have since disappeared.
func (s *Storage) pruneAccess(live map[compactKey]bool) {
	if s.accessShards == nil {
		return
	}
	for _, sh := range s.accessShards {
		sh.mu.Lock()
		for k := range sh.m {
			if !live[k] {
				delete(sh.m, k)
			}
		}
		sh.mu.Unlock()
	}
}

// evictionVictim is one object the sweep has decided to remove. Victims are
// carried in bounded batches, so this is the only per-object state a sweep ever
// holds -- the scan itself keeps no list of the cache's contents.
type evictionVictim struct {
	compactKey
	size      int64
	mtimeUnix int64 // on-disk mtime at scan time; re-checked before deletion
	byAge     bool  // selected by the age pass rather than the size budget
}

// EvictStats summarizes a single eviction sweep.
type EvictStats struct {
	Scanned     int   // objects examined
	EvictedAge  int   // removed for exceeding max_age
	EvictedSize int   // removed to get under max_bytes
	BytesFreed  int64 // total bytes removed
	BytesTotal  int64 // total cache size before this sweep
}

// evictionBucketSeconds is the resolution of the size pass's last-use
// histogram. The pass has to answer "how far back must I evict to free N
// bytes?" without holding a sorted list of the whole cache in memory, so it
// buckets last-use times and finds the bucket where the running total reaches
// N. The cutoff therefore lands on a bucket edge and the sweep may free up to
// one bucket more than strictly needed -- ten minutes of last-use activity,
// which is a low-water margin, and finer than the once-a-day resolution the
// kernel's relatime gives the access times feeding it.
const evictionBucketSeconds = 600

// evictionScan is what one pass over the data_dir tells the sweeper: how big
// the cache is, how its last-use times are distributed, and (only when
// in-memory access records exist to prune) which keys are still there.
type evictionScan struct {
	scanned    int
	totalBytes int64
	byBucket   map[int64]int64 // last-use bucket -> bytes
	live       map[compactKey]bool
}

// Evict runs one eviction pass over the data_dir. If maxAge > 0, entries not
// used within maxAge are removed. If maxBytes > 0, the least-recently-used
// entries are removed until the total size is within budget. now is injected so
// tests can control the clock.
//
// It walks the data_dir twice rather than building a list of candidates: at a
// million objects that list, with a key string apiece, was the largest
// allocation this process ever made, and it was made on a schedule. The first
// walk only measures (a bounded histogram), and from it the sweep derives ONE
// number -- the last-use cutoff below which entries must go. The second walk
// deletes what falls under the cutoff, in bounded batches. Peak memory is
// therefore a batch, not the cache.
//
// Both passes are the same rule at different cutoffs (evict everything last
// used before T), so age and size eviction combine into max(ageCutoff,
// sizeCutoff) instead of running as two sequential selections.
//
// Deletions are done directly (os.Remove) and the in-memory index is rebuilt
// once at the end rather than calling the O(n) Index.Remove per victim, which
// would make a large sweep O(n^2).
func (s *Storage) Evict(maxAge time.Duration, maxBytes int64, now time.Time) (EvictStats, error) {
	var stats EvictStats

	scan, err := s.scanForEviction(maxBytes > 0)
	if err != nil {
		return stats, err
	}
	stats.Scanned = scan.scanned
	stats.BytesTotal = scan.totalBytes

	var ageCutoff int64
	if maxAge > 0 {
		ageCutoff = now.Unix() - int64(maxAge.Seconds())
	}
	cutoff := max(ageCutoff, scan.sizeCutoff(maxBytes))

	if cutoff > 0 {
		if err := s.sweepBelow(cutoff, ageCutoff, &stats); err != nil {
			return stats, err
		}
	}

	// Drop access records for keys that vanished out from under us.
	s.pruneAccess(scan.live)

	evicted := stats.EvictedAge + stats.EvictedSize
	if evicted > 0 {
		evictionsTotal.Add(float64(evicted))
		evictedBytesTotal.Add(float64(stats.BytesFreed))
		// One rebuild so the index stops advertising evicted keys.
		if s.Index != nil {
			s.Index.rebuild(s)
		}
	}
	cacheBytes.Set(float64(stats.BytesTotal - stats.BytesFreed))
	return stats, nil
}

// scanForEviction measures the cache: total size, and the distribution of
// last-use times needed to place the size cutoff. It also collects the live key
// set, but only when there are in-memory access records to prune against it --
// that set is per-object memory, and it exists solely so records for keys that
// vanished out-of-band do not accumulate.
func (s *Storage) scanForEviction(needHistogram bool) (*evictionScan, error) {
	scan := &evictionScan{byBucket: make(map[int64]int64)}
	if s.accessShards != nil {
		scan.live = make(map[compactKey]bool)
	}
	err := s.Walk(func(obj ListObject) {
		ck := newCompactKey(obj.Key)
		scan.scanned++
		scan.totalBytes += obj.Size
		if scan.live != nil {
			scan.live[ck] = true
		}
		if needHistogram {
			memAccess, _ := s.lastAccessOf(ck)
			scan.byBucket[lastUsedUnix(obj, memAccess)/evictionBucketSeconds] += obj.Size
		}
	})
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// sizeCutoff returns the last-use time below which entries must be evicted for
// the cache to fit maxBytes, or 0 when it already fits (or has no budget).
func (sc *evictionScan) sizeCutoff(maxBytes int64) int64 {
	if maxBytes <= 0 || sc.totalBytes <= maxBytes {
		return 0
	}
	need := sc.totalBytes - maxBytes
	buckets := make([]int64, 0, len(sc.byBucket))
	for b := range sc.byBucket {
		buckets = append(buckets, b)
	}
	slices.Sort(buckets)

	var freed int64
	for _, b := range buckets {
		freed += sc.byBucket[b]
		if freed >= need {
			// The whole bucket goes, so the cutoff is its exclusive upper edge.
			return (b + 1) * evictionBucketSeconds
		}
	}
	// Unreachable: the buckets sum to totalBytes, which exceeds need. Evicting
	// everything is the honest answer if it ever is reached.
	return (buckets[len(buckets)-1] + 1) * evictionBucketSeconds
}

// evictionBatchSize is how many victims are de-advertised and deleted per
// round. It trades a few MiB of sweep memory against index passes: each batch
// costs one Index.RemoveKeys, which walks the whole index under its write lock,
// so a sweep clearing a million entries should do that ten-odd times rather
// than hundreds.
const evictionBatchSize = 65536

// sweepBelow deletes every object last used before cutoff. Each batch is
// de-advertised from the index BEFORE its files are unlinked: the opposite
// ordering (unlink first, rebuild the index at sweep end) left each deleted key
// advertised in /_index for the rest of the sweep -- a window in which every
// GET of it was a 404 on an indexed key, the exact index/store divergence the
// miss_advertised_unservable counter tracks. Advertised-but-already-deleted is
// a forced miss; present-but-unadvertised, the state this ordering produces
// instead, is at worst a redundant re-upload.
//
// It re-reads each object's last-use time rather than trusting the scan's, so
// anything read between the two passes is spared.
func (s *Storage) sweepBelow(cutoff, ageCutoff int64, stats *EvictStats) error {
	// Grown as needed rather than preallocated: most sweeps evict a handful of
	// entries and should not reserve the full batch to do it.
	var batch []evictionVictim
	flush := func() {
		if len(batch) == 0 {
			return
		}
		keys := make([]string, len(batch))
		for i, v := range batch {
			keys[i] = v.Key()
		}
		if s.Index != nil {
			s.Index.RemoveKeys(keys)
		}
		for i, v := range batch {
			if !s.evictOne(keys[i], v.mtimeUnix) {
				continue
			}
			if v.byAge {
				stats.EvictedAge++
			} else {
				stats.EvictedSize++
			}
			stats.BytesFreed += v.size
		}
		batch = batch[:0]
	}

	err := s.Walk(func(obj ListObject) {
		ck := newCompactKey(obj.Key)
		memAccess, _ := s.lastAccessOf(ck)
		lastUsed := lastUsedUnix(obj, memAccess)
		if lastUsed >= cutoff {
			return
		}
		batch = append(batch, evictionVictim{
			compactKey: ck,
			size:       obj.Size,
			mtimeUnix:  obj.LastModified.Unix(),
			byAge:      ageCutoff > 0 && lastUsed < ageCutoff,
		})
		if len(batch) >= evictionBatchSize {
			flush()
		}
	})
	flush()
	return err
}

// evictOne removes a single object's file by key, but only if its on-disk
// mtime still matches what the sweep's scan recorded. The scan snapshot can be
// minutes stale by the time a victim is deleted; a concurrent overwrite PUT in
// that window renames FRESH content onto the same path, and unconditionally
// removing it would evict an object that was just written (the snapshot-then-
// remove TOCTOU). Re-stat'ing first and skipping on any mtime change bounds the
// race to the stat-to-remove instant. expectMtime <= 0 skips the check (for
// callers that hold no snapshot). It reports whether a file was actually
// removed (false if already gone or freshly overwritten). The index is
// intentionally not touched here; Evict de-advertises victims up front and
// rebuilds once at the end.
func (s *Storage) evictOne(key string, expectMtime int64) bool {
	path := s.keyToPath(key)
	if expectMtime > 0 {
		info, err := os.Stat(path)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				log.Printf("eviction: stat %s: %v", key, err)
			}
			return false
		}
		if info.ModTime().Unix() != expectMtime {
			// Overwritten since the scan: this is now a fresh object, not a victim.
			return false
		}
	}
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("eviction: remove %s: %v", key, err)
		}
		return false
	}
	removeSidecars(path)
	s.forgetAccess(key)
	s.forgetClean(key)
	s.forgetMeta(key)
	return true
}

// Eviction-loop timing. The sweep schedule is kept in the data_dir, not in
// this process: a restart-heavy deployment (rolling updates are the production
// model) that restarted the clock on every boot would simply never sweep, and
// one that swept on every boot would walk the whole disk on every rolling
// update. So the loop asks the marker when the last sweep was and sweeps
// immediately -- after a jitter that spreads replicas restarting together --
// when it is at least one interval old. The s3_cache_bytes gauge is refreshed
// on its own faster cadence in between so operators are not looking at a value
// a whole interval old.
const (
	evictionStartupDelayMin   = 1 * time.Minute
	evictionStartupDelayMax   = 5 * time.Minute
	cacheBytesRefreshInterval = 15 * time.Minute
)

// evictionStartupDelay returns the jittered delay before the first sweep.
func evictionStartupDelay() time.Duration {
	spread := evictionStartupDelayMax - evictionStartupDelayMin
	return evictionStartupDelayMin + time.Duration(rand.Int64N(int64(spread)))
}

// lastSweepTime reads the recorded end of the last eviction sweep. A missing
// marker (a new data_dir, or one from a server that predates it) reports no
// recorded sweep, which schedules one at startup.
func (s *Storage) lastSweepTime() (time.Time, bool) {
	data, err := os.ReadFile(filepath.Join(s.dataDir, sweepMarkerFile))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("eviction: cannot read %s: %v (treating the cache as never swept)", sweepMarkerFile, err)
		}
		return time.Time{}, false
	}
	sec, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || sec <= 0 {
		log.Printf("eviction: %s is corrupt (%q); treating the cache as never swept", sweepMarkerFile, strings.TrimSpace(string(data)))
		return time.Time{}, false
	}
	return time.Unix(sec, 0), true
}

// recordSweepTime stamps the marker so the next startup can tell whether a
// sweep is due. A failure here only costs an extra sweep, but it is logged:
// silently losing the schedule is how a deployment ends up either never
// sweeping or sweeping on every restart.
func (s *Storage) recordSweepTime(t time.Time) {
	path := filepath.Join(s.dataDir, sweepMarkerFile)
	if err := os.WriteFile(path, []byte(strconv.FormatInt(t.Unix(), 10)+"\n"), 0644); err != nil {
		log.Printf("eviction: cannot write %s: %v (the next startup will sweep again)", sweepMarkerFile, err)
	}
}

// firstSweepDelay returns how long to wait before the first sweep of this
// process: the jittered startup delay when a sweep is due (never swept, or the
// recorded sweep is a full interval old), otherwise the time remaining until
// the recorded sweep comes due.
func (s *Storage) firstSweepDelay(interval time.Duration) time.Duration {
	jitter := evictionStartupDelay()
	last, ok := s.lastSweepTime()
	if !ok {
		return jitter
	}
	remaining := time.Until(last.Add(interval))
	if remaining <= 0 {
		return jitter
	}
	// A marker stamped in the future (a clock that jumped) must not push the
	// first sweep past one interval, which would stop eviction indefinitely.
	return min(remaining, interval) + jitter
}

// runSweep executes one eviction sweep, records when it finished, and logs it
// if anything was evicted.
func (s *Storage) runSweep(maxAge time.Duration, maxBytes int64) {
	start := time.Now()
	stats, err := s.Evict(maxAge, maxBytes, start)
	if err != nil {
		log.Printf("eviction: sweep failed: %v", err)
		return
	}
	s.recordSweepTime(time.Now())
	if stats.EvictedAge > 0 || stats.EvictedSize > 0 {
		log.Printf("eviction: swept in %v scanned=%d evicted_age=%d evicted_size=%d freed_bytes=%d remaining_bytes=%d",
			time.Since(start).Round(time.Millisecond), stats.Scanned,
			stats.EvictedAge, stats.EvictedSize, stats.BytesFreed,
			stats.BytesTotal-stats.BytesFreed)
	}
}

// RefreshCacheBytes recomputes the total stored size (a size-only WalkDir; no
// xattrs, no bodies) and updates the s3_cache_bytes gauge. Runs between sweeps
// so the gauge tracks growth instead of staying frozen at the last sweep's
// value for up to the whole eviction interval.
func (s *Storage) RefreshCacheBytes() {
	var total int64
	filepath.WalkDir(s.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if isReservedFile(d.Name()) {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	cacheBytes.Set(float64(total))
}

// RunEvictionLoop sweeps until the process exits. Run it in its own goroutine.
// The first sweep is scheduled from the recorded last sweep (see
// firstSweepDelay), and each subsequent one an interval after the previous
// finished; the size gauge is refreshed on its own faster cadence in between.
func (s *Storage) RunEvictionLoop(maxAge time.Duration, maxBytes int64, interval time.Duration) {
	next := time.NewTimer(s.firstSweepDelay(interval))
	defer next.Stop()
	refresh := time.NewTicker(cacheBytesRefreshInterval)
	defer refresh.Stop()
	for {
		select {
		case <-next.C:
			s.runSweep(maxAge, maxBytes)
			next.Reset(interval)
		case <-refresh.C:
			s.RefreshCacheBytes()
		}
	}
}
