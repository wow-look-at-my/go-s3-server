package main

import (
	"os"
	"sync"
	"sync/atomic"
)

// An object's user metadata lives in extended attributes, and reading it costs
// one listxattr plus one getxattr per attribute -- around a dozen syscalls for
// a typical entry (outputid, compression, object-type, pkg, src, module,
// go-version, target, toolchain-version, created), measured at ~42us per key.
// Every Stat and every Open pays it, so one batch GET of the client's 128-key
// chunk paid it 128 times before this cache existed, and twice per key before
// the streaming phase stopped re-reading metadata it already had.
//
// The metadata of a stored object never changes in place: a new body arrives as
// a fresh inode renamed over the path, so the mtime and size that come back
// from the stat every caller ALREADY performs are enough to tell a cached entry
// from a stale one. That makes the cache self-validating rather than
// invalidation-dependent: a hit is only served when the fresh stat matches the
// stat the entry was recorded under, so an overwrite, a restore, or a rewritten
// body can never be served with the previous body's metadata.
//
// The one mutation that does NOT move mtime is an xattr write onto a live inode
// -- the outputid self-heal's fsetxattr. Those call sites drop the entry
// explicitly (forgetMeta), the same way they already drop the known-clean memo.
//
// Bounded like the known-clean memo: past the limit the cache is cleared
// wholesale, which costs a re-read of the warm working set once. Entries are
// small (the kv pairs of one object) and the working set of a CI build is a few
// thousand keys, so the bound is reached only pathologically.
//
// The bound is SIZED FROM THE PROCESS MEMORY BUDGET (memlimit.go) rather than
// being a flat constant: a cache that assumes a large host is exactly how a
// small one gets OOM-killed. metaCacheEntryBytes is the measured-ish cost of
// one entry -- ten short kv pairs plus map overhead -- and the cache may claim
// metaCacheBudgetFraction of the budget, never more than the old constant.
const (
	metaCacheEntryBytes     = 600
	metaCacheBudgetFraction = 0.05
	maxMetaCacheEntriesCap  = 1 << 16
	minMetaCacheEntries     = 1 << 10
)

// maxMetaCacheEntries is the bound this process runs with.
var maxMetaCacheEntries = budgetFraction(metaCacheBudgetFraction, metaCacheEntryBytes, minMetaCacheEntries, maxMetaCacheEntriesCap)

// kvPair is one metadata attribute. Entries hold a slice rather than a map so a
// cached entry is immutable and shareable: callers get a fresh map built from
// it (ObjectMeta.Metadata is mutable -- the self-heal writes into it), and no
// caller can reach back into the cache.
type kvPair struct{ k, v string }

type metaEntry struct {
	modNano int64
	size    int64
	kv      []kvPair
}

// metaCache maps a storage key to the metadata last read for it, tagged with
// the stat it was read under. Sharded 256 ways so concurrent batch handlers do
// not convoy on one lock, the same shape as cleanKeyMemo and accessShards.
type metaCache struct {
	limit  int64
	count  atomic.Int64
	shards [256]metaShard
}

type metaShard struct {
	mu sync.Mutex
	m  map[string]metaEntry
}

func newMetaCache(limit int64) *metaCache {
	c := &metaCache{limit: limit}
	for i := range c.shards {
		c.shards[i].m = make(map[string]metaEntry)
	}
	return c
}

// shardFor picks a shard from an FNV-1a byte of the key. Storage keys share a
// long constant prefix (go-buildcache/v1...), so hashing the whole key rather
// than sampling a byte of it is what keeps the shards even.
func (c *metaCache) shardFor(key string) *metaShard {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &c.shards[byte(h^(h>>16))]
}

// get returns the metadata recorded for key IF it was recorded under the same
// mtime and size the caller just stat'ed. Any mismatch is a miss.
func (c *metaCache) get(key string, info os.FileInfo) ([]kvPair, bool) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	e, ok := sh.m[key]
	sh.mu.Unlock()
	if !ok || e.modNano != info.ModTime().UnixNano() || e.size != info.Size() {
		return nil, false
	}
	return e.kv, true
}

// put records key's metadata against the stat it was read under.
func (c *metaCache) put(key string, info os.FileInfo, kv []kvPair) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	_, existed := sh.m[key]
	sh.m[key] = metaEntry{modNano: info.ModTime().UnixNano(), size: info.Size(), kv: kv}
	sh.mu.Unlock()
	if existed {
		return
	}
	if c.count.Add(1) > c.limit {
		c.clear()
	}
}

// forget drops key's entry. Required only for a mutation that leaves mtime and
// size untouched (an xattr stamped onto a live inode); every other change is
// caught by the stat comparison.
func (c *metaCache) forget(key string) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	if _, existed := sh.m[key]; existed {
		delete(sh.m, key)
		c.count.Add(-1)
	}
	sh.mu.Unlock()
}

func (c *metaCache) clear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n := len(sh.m)
		if n > 0 {
			sh.m = make(map[string]metaEntry)
		}
		sh.mu.Unlock()
		c.count.Add(int64(-n))
	}
}

func (c *metaCache) size() int64 { return c.count.Load() }

// loadMetadata fills meta.Metadata for the object at path, from the cache when
// the entry matches info, otherwise by reading the xattrs and recording them.
// Nil-safe for directly-constructed Storage values (tests), which then read
// through every time.
func (s *Storage) loadMetadata(key, path string, info os.FileInfo, meta *ObjectMeta) {
	if s.metaCache == nil {
		getMetadata(path, meta)
		return
	}
	if kv, ok := s.metaCache.get(key, info); ok {
		metaCacheHitsTotal.Inc()
		for _, p := range kv {
			meta.Metadata[p.k] = p.v
		}
		return
	}
	metaCacheMissesTotal.Inc()
	getMetadata(path, meta)
	kv := make([]kvPair, 0, len(meta.Metadata))
	for k, v := range meta.Metadata {
		kv = append(kv, kvPair{k, v})
	}
	s.metaCache.put(key, info, kv)
}

// forgetMeta drops key's cached metadata. Called wherever an object's xattrs
// change without its mtime changing (the self-heal's fsetxattr), and alongside
// the other per-key invalidations on delete and eviction.
func (s *Storage) forgetMeta(key string) {
	if s.metaCache != nil {
		s.metaCache.forget(key)
	}
}
