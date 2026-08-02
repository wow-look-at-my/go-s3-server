package main

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// A cache server answers requests. That is the whole job, and memory pressure
// is never a reason to stop doing it: a build cache that refuses reads under
// load is worse than no cache at all, because every client that would have hit
// it now rebuilds AND waits on a timeout first.
//
// So the in-memory caches here are bounded in BYTES and evict their
// least-recently-used entries to stay inside that bound. Memory pressure
// shrinks the bound, which evicts more; it never reaches the request path.
// Every entry in every one of these caches is reconstructible from disk, so
// eviction costs a re-read and nothing else.
//
// Sharded so concurrent readers and writers do not convoy on one lock, with
// each shard holding an equal slice of the budget. Storage keys are uniformly
// distributed (they are hashes), so per-shard accounting stays even and no
// shard needs to know about any other -- an insert evicts only within its own
// shard, which keeps the hot path lock-local.

// lruShardCount is the number of independent shards. 64 keeps per-shard
// budgets meaningful even at small totals (a 4 MiB budget is 64 KiB a shard)
// while still spreading contention across every core the server runs on.
const lruShardCount = 64

// lruCache is a byte-bounded, LRU-evicting, sharded cache. The budget is
// adjustable at runtime (SetBudget), which is how memory pressure translates
// into "hold less" rather than "serve less".
type lruCache[K comparable, V any] struct {
	shardOf func(K) uint32
	sizeOf  func(K, V) int64

	budget    atomic.Int64 // total bytes allowed across all shards
	bytes     atomic.Int64 // total bytes held
	evictions atomic.Int64

	shards [lruShardCount]lruShard[K, V]
}

type lruShard[K comparable, V any] struct {
	mu    sync.Mutex
	ll    *list.List // front = most recently used
	items map[K]*list.Element
	bytes int64
}

type lruEntry[K comparable, V any] struct {
	key  K
	val  V
	size int64
}

// newLRUCache builds a cache holding at most budget bytes. sizeOf reports an
// entry's cost -- an estimate is fine and expected; what matters is that it
// scales with the entry, so a cache of large entries holds fewer of them.
func newLRUCache[K comparable, V any](budget int64, shardOf func(K) uint32, sizeOf func(K, V) int64) *lruCache[K, V] {
	c := &lruCache[K, V]{shardOf: shardOf, sizeOf: sizeOf}
	c.budget.Store(budget)
	for i := range c.shards {
		c.shards[i].ll = list.New()
		c.shards[i].items = make(map[K]*list.Element)
	}
	return c
}

func (c *lruCache[K, V]) shard(k K) *lruShard[K, V] {
	return &c.shards[c.shardOf(k)%lruShardCount]
}

// shardBudget is each shard's slice of the total. At least one byte, so a
// pathologically small budget degenerates to "hold almost nothing" rather than
// "divide by zero".
func (c *lruCache[K, V]) shardBudget() int64 {
	b := c.budget.Load() / lruShardCount
	if b < 1 {
		return 1
	}
	return b
}

// Get returns the value for k and marks it most-recently-used.
func (c *lruCache[K, V]) Get(k K) (V, bool) {
	var zero V
	sh := c.shard(k)
	sh.mu.Lock()
	el, ok := sh.items[k]
	if !ok {
		sh.mu.Unlock()
		return zero, false
	}
	sh.ll.MoveToFront(el)
	v := el.Value.(*lruEntry[K, V]).val
	sh.mu.Unlock()
	return v, true
}

// Put stores v under k, evicting least-recently-used entries from this shard
// until it is back inside its budget.
func (c *lruCache[K, V]) Put(k K, v V) {
	size := c.sizeOf(k, v)
	sh := c.shard(k)
	budget := c.shardBudget()

	sh.mu.Lock()
	if el, ok := sh.items[k]; ok {
		e := el.Value.(*lruEntry[K, V])
		sh.bytes += size - e.size
		c.bytes.Add(size - e.size)
		e.val, e.size = v, size
		sh.ll.MoveToFront(el)
	} else {
		el := sh.ll.PushFront(&lruEntry[K, V]{key: k, val: v, size: size})
		sh.items[k] = el
		sh.bytes += size
		c.bytes.Add(size)
	}
	c.evictLocked(sh, budget)
	sh.mu.Unlock()
}

// evictLocked drops least-recently-used entries until the shard fits. The
// newest entry is never evicted even if it alone exceeds the budget: a cache
// that refuses to hold anything is a cache with a permanent miss rate, and the
// single-entry overshoot is bounded by the entry's own size.
func (c *lruCache[K, V]) evictLocked(sh *lruShard[K, V], budget int64) {
	for sh.bytes > budget && sh.ll.Len() > 1 {
		back := sh.ll.Back()
		e := back.Value.(*lruEntry[K, V])
		sh.ll.Remove(back)
		delete(sh.items, e.key)
		sh.bytes -= e.size
		c.bytes.Add(-e.size)
		c.evictions.Add(1)
	}
}

// Forget drops k, if present.
func (c *lruCache[K, V]) Forget(k K) {
	sh := c.shard(k)
	sh.mu.Lock()
	if el, ok := sh.items[k]; ok {
		e := el.Value.(*lruEntry[K, V])
		sh.ll.Remove(el)
		delete(sh.items, k)
		sh.bytes -= e.size
		c.bytes.Add(-e.size)
	}
	sh.mu.Unlock()
}

// SetBudget changes the total byte budget and immediately evicts down to it.
// This is the lever memory pressure pulls: hold less, keep serving.
func (c *lruCache[K, V]) SetBudget(budget int64) {
	if budget < 0 {
		budget = 0
	}
	c.budget.Store(budget)
	per := c.shardBudget()
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		c.evictLocked(sh, per)
		sh.mu.Unlock()
	}
}

// Clear drops everything.
func (c *lruCache[K, V]) Clear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		c.bytes.Add(-sh.bytes)
		sh.bytes = 0
		sh.ll.Init()
		sh.items = make(map[K]*list.Element)
		sh.mu.Unlock()
	}
}

func (c *lruCache[K, V]) Bytes() int64     { return c.bytes.Load() }
func (c *lruCache[K, V]) Budget() int64    { return c.budget.Load() }
func (c *lruCache[K, V]) Evictions() int64 { return c.evictions.Load() }

// Len is the number of entries held, for tests and diagnostics.
func (c *lruCache[K, V]) Len() int {
	var n int
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n += len(sh.items)
		sh.mu.Unlock()
	}
	return n
}

// fnv1a is the shard hash for string keys. Storage keys share a long constant
// prefix (go-buildcache/v1...), so the whole key is hashed rather than a byte
// of it sampled, which is what keeps the shards even.
func fnv1a(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
