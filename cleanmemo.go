package main

import (
	"sync"
	"sync/atomic"
)

// maxCleanMemoEntries bounds the known-clean memo. At 32 bytes per hash plus
// map overhead, one million entries is on the order of 100 MB worst case and
// covers the production cache's entire key population; past the bound the memo
// is cleared wholesale (a rare, cheap event that only costs re-probing warm
// keys once) rather than tracking any eviction order.
const maxCleanMemoEntries = 1 << 20

// cleanKeyMemo remembers indexed cacheprog keys whose stored body has already
// passed the read-path module-index probe, so repeat GET/batch/prefetch reads
// of a warm key skip the lz4 decode entirely. The probe costs a file open plus
// a first-block decompress per read; on a warm cache the same keys are read on
// every build, so memoizing the verdict removes the per-GET decode from the
// steady state.
//
// Scope and safety: only keys in the guard's scope (go-buildcache/v1<64-hex>)
// are ever memoized, keyed by their 32-byte action hash. A key is forgotten
// whenever its body could have changed -- overwrite PUT, DELETE, and eviction
// (storage.forgetClean call sites) -- so a memo hit always refers to a body
// that entered through PutStream's module-index guard or was probed directly.
// The memo is a probe-skip optimization, not the safety boundary: the PUT
// guard remains the gate that keeps new module indexes out of the store.
//
// Sharded 256 ways on the hash's first byte (action IDs are uniformly
// distributed), so concurrent readers and writers do not convoy on one lock --
// the same shape as storage's accessShards.
type cleanKeyMemo struct {
	limit  int64
	count  atomic.Int64
	shards [256]cleanMemoShard
}

type cleanMemoShard struct {
	mu sync.Mutex
	m  map[[gbciHashSize]byte]struct{}
}

func newCleanKeyMemo(limit int64) *cleanKeyMemo {
	c := &cleanKeyMemo{limit: limit}
	for i := range c.shards {
		c.shards[i].m = make(map[[gbciHashSize]byte]struct{})
	}
	return c
}

// has reports whether the hash was memoized as clean.
func (c *cleanKeyMemo) has(h [gbciHashSize]byte) bool {
	sh := &c.shards[h[0]]
	sh.mu.Lock()
	_, ok := sh.m[h]
	sh.mu.Unlock()
	return ok
}

// add memoizes the hash as clean. When the memo exceeds its bound it is
// cleared wholesale; the next reads simply re-probe and re-memoize.
func (c *cleanKeyMemo) add(h [gbciHashSize]byte) {
	sh := &c.shards[h[0]]
	sh.mu.Lock()
	_, existed := sh.m[h]
	if !existed {
		sh.m[h] = struct{}{}
	}
	sh.mu.Unlock()
	if existed {
		return
	}
	if c.count.Add(1) > c.limit {
		c.clear()
	}
}

// forgetKey drops the hash for key (no-op for non-cacheprog keys). Called
// whenever the stored body under key may have changed or vanished.
func (c *cleanKeyMemo) forgetKey(key string) {
	h, ok := extractActionHash(key)
	if !ok {
		return
	}
	sh := &c.shards[h[0]]
	sh.mu.Lock()
	if _, existed := sh.m[h]; existed {
		delete(sh.m, h)
		c.count.Add(-1)
	}
	sh.mu.Unlock()
}

// clear empties every shard. Concurrent adds racing with a clear are either
// kept or re-probed on the next read; both are correct.
func (c *cleanKeyMemo) clear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n := len(sh.m)
		if n > 0 {
			sh.m = make(map[[gbciHashSize]byte]struct{})
		}
		sh.mu.Unlock()
		c.count.Add(int64(-n))
	}
}

// size returns the approximate number of memoized hashes.
func (c *cleanKeyMemo) size() int64 {
	return c.count.Load()
}

// keyKnownClean reports whether the action hash already passed the read-path
// module-index probe. Nil-safe for directly-constructed Storage values.
func (s *Storage) keyKnownClean(h [gbciHashSize]byte) bool {
	return s.cleanKeys != nil && s.cleanKeys.has(h)
}

// markKeyClean memoizes an action hash whose body was just probed and is not a
// module index. Nil-safe for directly-constructed Storage values.
func (s *Storage) markKeyClean(h [gbciHashSize]byte) {
	if s.cleanKeys != nil {
		s.cleanKeys.add(h)
	}
}

// forgetClean invalidates the known-clean memo entry for key. Must be called
// whenever the body stored under key changes or is removed (overwrite PUT,
// DELETE, eviction), so the next read re-probes the new body.
func (s *Storage) forgetClean(key string) {
	if s.cleanKeys != nil {
		s.cleanKeys.forgetKey(key)
	}
}
