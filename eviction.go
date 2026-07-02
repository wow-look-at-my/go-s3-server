package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Access tracking feeds eviction's least-recently-used decisions. It records
// the last time each key was read so the sweeper can keep hot-but-old entries
// and prune genuinely idle ones. See the accessShards field on Storage.

const accessShardCount = 256

type accessShard struct {
	mu sync.Mutex
	m  map[string]int64 // key -> last-access unix seconds
}

// EnableAccessTracking turns on per-key last-access bookkeeping for eviction.
// Call once at startup, before serving, when an eviction policy is configured.
func (s *Storage) EnableAccessTracking() {
	shards := make([]*accessShard, accessShardCount)
	for i := range shards {
		shards[i] = &accessShard{m: make(map[string]int64)}
	}
	s.accessShards = shards
}

// accessShardFor returns the shard owning key, using FNV-1a (cheap, no allocs).
func (s *Storage) accessShardFor(key string) *accessShard {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return s.accessShards[h%accessShardCount]
}

// recordAccess stamps key as used now. No-op unless access tracking is enabled.
func (s *Storage) recordAccess(key string) {
	if s.accessShards == nil {
		return
	}
	sh := s.accessShardFor(key)
	now := time.Now().Unix()
	sh.mu.Lock()
	sh.m[key] = now
	sh.mu.Unlock()
}

// lastAccess returns the last-access time (unix seconds) of key, if recorded.
func (s *Storage) lastAccess(key string) (int64, bool) {
	if s.accessShards == nil {
		return 0, false
	}
	sh := s.accessShardFor(key)
	sh.mu.Lock()
	t, ok := sh.m[key]
	sh.mu.Unlock()
	return t, ok
}

// forgetAccess drops key's access record (called when the key is removed).
func (s *Storage) forgetAccess(key string) {
	if s.accessShards == nil {
		return
	}
	sh := s.accessShardFor(key)
	sh.mu.Lock()
	delete(sh.m, key)
	sh.mu.Unlock()
}

// pruneAccess drops access records for keys not present in live, so the map
// cannot accumulate entries for objects that have since disappeared.
func (s *Storage) pruneAccess(live map[string]bool) {
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

// evictionCandidate is one stored object considered for eviction. lastUsed is
// the later of the object's mtime (write time) and its recorded last-access
// time, so a frequently-read entry is not evicted just because it was written
// long ago.
type evictionCandidate struct {
	key      string
	size     int64
	lastUsed int64 // unix seconds
}

// EvictStats summarizes a single eviction sweep.
type EvictStats struct {
	Scanned     int   // objects examined
	EvictedAge  int   // removed for exceeding max_age
	EvictedSize int   // removed to get under max_bytes
	BytesFreed  int64 // total bytes removed
	BytesTotal  int64 // total cache size before this sweep
}

// Evict runs one eviction pass over the data_dir. If maxAge > 0, entries not
// used within maxAge are removed. If maxBytes > 0, the least-recently-used
// remaining entries are then removed until the total size is within budget.
// now is injected so tests can control the clock.
//
// Deletions are done directly (os.Remove) and the in-memory index is rebuilt
// once at the end rather than calling the O(n) Index.Remove per victim, which
// would make a large sweep O(n^2).
func (s *Storage) Evict(maxAge time.Duration, maxBytes int64, now time.Time) (EvictStats, error) {
	var stats EvictStats

	// Enumerate stored objects (metadata only; no bodies are read).
	result, err := s.List("", 1<<30, "")
	if err != nil {
		return stats, err
	}

	cands := make([]evictionCandidate, 0, len(result.Objects))
	live := make(map[string]bool, len(result.Objects))
	for _, obj := range result.Objects {
		lastUsed := obj.LastModified.Unix()
		if at, ok := s.lastAccess(obj.Key); ok && at > lastUsed {
			lastUsed = at
		}
		cands = append(cands, evictionCandidate{key: obj.Key, size: obj.Size, lastUsed: lastUsed})
		live[obj.Key] = true
		stats.BytesTotal += obj.Size
	}
	stats.Scanned = len(cands)

	nowUnix := now.Unix()
	maxAgeSec := int64(maxAge.Seconds())

	// Age pass: select anything idle longer than maxAge as a victim; keep the
	// rest as survivors (reusing the candidate backing array) for the size pass.
	// Selection only — no deletion yet, see below.
	var ageVictims []evictionCandidate
	survivors := cands[:0]
	var survivorBytes int64
	for _, c := range cands {
		if maxAge > 0 && nowUnix-c.lastUsed > maxAgeSec {
			ageVictims = append(ageVictims, c)
			continue
		}
		survivors = append(survivors, c)
		survivorBytes += c.size
	}

	// Size pass: if still over budget, select least-recently-used first.
	var sizeVictims []evictionCandidate
	if maxBytes > 0 && survivorBytes > maxBytes {
		sort.Slice(survivors, func(i, j int) bool {
			return survivors[i].lastUsed < survivors[j].lastUsed
		})
		for i := 0; survivorBytes > maxBytes && i < len(survivors); i++ {
			sizeVictims = append(sizeVictims, survivors[i])
			survivorBytes -= survivors[i].size
		}
	}

	// Stop advertising every victim BEFORE unlinking any file. The previous
	// ordering (unlink during the passes, rebuild the index once at sweep end)
	// left each deleted key advertised in /_index for the rest of the sweep —
	// a window in which every GET of it was a 404 on an indexed key, the exact
	// index/store divergence the miss_advertised_unservable counter tracks.
	// Inverting the order makes the transient state "present but unadvertised",
	// whose worst case is a redundant re-upload rather than a forced miss.
	if s.Index != nil && len(ageVictims)+len(sizeVictims) > 0 {
		keys := make([]string, 0, len(ageVictims)+len(sizeVictims))
		for _, c := range ageVictims {
			keys = append(keys, c.key)
		}
		for _, c := range sizeVictims {
			keys = append(keys, c.key)
		}
		s.Index.RemoveKeys(keys)
	}

	for _, c := range ageVictims {
		if s.evictOne(c.key) {
			stats.EvictedAge++
			stats.BytesFreed += c.size
		}
	}
	for _, c := range sizeVictims {
		if s.evictOne(c.key) {
			stats.EvictedSize++
			stats.BytesFreed += c.size
		}
	}

	// Drop access records for keys that vanished out from under us.
	s.pruneAccess(live)

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

// evictOne removes a single object's file by key. It reports whether a file was
// actually removed (false if it was already gone). The index is intentionally
// not touched here; Evict rebuilds it once after the whole sweep.
func (s *Storage) evictOne(key string) bool {
	path := s.keyToPath(key)
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("eviction: remove %s: %v", key, err)
		}
		return false
	}
	s.forgetAccess(key)
	s.forgetClean(key)
	return true
}

// RunEvictionLoop sweeps on a fixed interval until the process exits. Run it in
// its own goroutine. The first sweep happens after one interval, not at
// startup, so a fresh server is not immediately churning the disk.
func (s *Storage) RunEvictionLoop(maxAge time.Duration, maxBytes int64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		start := time.Now()
		stats, err := s.Evict(maxAge, maxBytes, time.Now())
		if err != nil {
			log.Printf("eviction: sweep failed: %v", err)
			continue
		}
		if stats.EvictedAge > 0 || stats.EvictedSize > 0 {
			log.Printf("eviction: swept in %v scanned=%d evicted_age=%d evicted_size=%d freed_bytes=%d remaining_bytes=%d",
				time.Since(start).Round(time.Millisecond), stats.Scanned,
				stats.EvictedAge, stats.EvictedSize, stats.BytesFreed,
				stats.BytesTotal-stats.BytesFreed)
		}
	}
}
