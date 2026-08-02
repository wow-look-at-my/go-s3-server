package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestLRU is a string->string cache whose entries cost their own bytes plus
// a fixed overhead, so a test can reason in exact numbers.
func newTestLRU(budget int64) *lruCache[string, string] {
	return newLRUCache(budget, fnv1a, func(k, v string) int64 { return int64(len(k) + len(v)) })
}

// oneShardKeys returns n keys that all land in the same shard, so a test can
// exercise eviction inside one shard deterministically.
func oneShardKeys(t *testing.T, n int) []string {
	t.Helper()
	// Fixed-width keys so every entry costs the same, which lets a test reason
	// about the budget in whole entries.
	want := fnv1a("seed") % lruShardCount
	var out []string
	for i := 0; len(out) < n; i++ {
		k := fmt.Sprintf("key-%08d", i)
		if fnv1a(k)%lruShardCount == want {
			out = append(out, k)
		}
		require.Less(t, i, 1_000_000, "could not find enough same-shard keys")
	}
	return out
}

// TestLRU_StaysWithinBudget is the property the whole memory story rests on:
// the cache holds what it is allowed to hold and not more, no matter how much
// is put into it.
func TestLRU_StaysWithinBudget(t *testing.T) {
	const budget = 64 * lruShardCount // one 64-byte entry per shard
	c := newTestLRU(budget)

	for i := 0; i < 10000; i++ {
		c.Put(fmt.Sprintf("key-%06d", i), "0123456789012345678901234567890123456789012345678")
	}
	// Each shard may exceed its slice by at most the single entry it just made
	// (evictLocked never drops the newest), so the bound is per-shard budget
	// plus one entry, times shards.
	require.LessOrEqual(t, c.Bytes(), int64(budget)+int64(lruShardCount)*128,
		"a cache that can exceed its budget is not a bound")
	require.Positive(t, c.Evictions(), "the load must actually have evicted")
	require.Positive(t, c.Len(), "and it must still be holding something useful")
}

// TestLRU_EvictsLeastRecentlyUsed: the entry given up is the one nobody is
// using, which is what makes eviction cheap -- a warm working set survives.
func TestLRU_EvictsLeastRecentlyUsed(t *testing.T) {
	keys := oneShardKeys(t, 3)
	// Room for two of these entries in the shard they share.
	c := newTestLRU(int64(2*(len(keys[0])+1)) * lruShardCount)

	c.Put(keys[0], "a")
	c.Put(keys[1], "b")
	_, ok := c.Get(keys[0]) // keys[0] is now the most recently used
	require.True(t, ok)

	c.Put(keys[2], "c")

	_, ok0 := c.Get(keys[0])
	_, ok1 := c.Get(keys[1])
	_, ok2 := c.Get(keys[2])
	require.True(t, ok0, "a recently used entry must survive")
	require.False(t, ok1, "the least recently used entry is the one to drop")
	require.True(t, ok2)
}

// TestLRU_SetBudgetEvictsImmediately is how memory pressure reaches the caches:
// lowering the budget must free memory now, not at some future insert.
func TestLRU_SetBudgetEvictsImmediately(t *testing.T) {
	c := newTestLRU(1 << 20)
	for i := 0; i < 2000; i++ {
		c.Put(fmt.Sprintf("key-%06d", i), "value-value-value-value")
	}
	before := c.Bytes()
	require.Positive(t, before)

	c.SetBudget(before / 8)
	require.LessOrEqual(t, c.Bytes(), before/8+int64(lruShardCount)*128,
		"lowering the budget must evict down to it")
	require.Positive(t, c.Len(), "shrinking is not clearing")

	// And raising it back does not resurrect anything, but does let the cache
	// fill again.
	c.SetBudget(1 << 20)
	for i := 2000; i < 3000; i++ {
		c.Put(fmt.Sprintf("key-%06d", i), "value-value-value-value")
	}
	require.Greater(t, c.Bytes(), before/8)
}

// TestLRU_OversizedEntryIsStillHeld: an entry larger than its shard's whole
// budget is kept rather than dropped on arrival. A cache that refuses to hold
// anything has a permanent miss rate, and the overshoot is bounded by the one
// entry.
func TestLRU_OversizedEntryIsStillHeld(t *testing.T) {
	c := newTestLRU(lruShardCount) // one byte per shard
	c.Put("k", "a value far larger than the budget")
	v, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, "a value far larger than the budget", v)
}

// TestLRU_ForgetAndClear cover the two invalidation paths callers use.
func TestLRU_ForgetAndClear(t *testing.T) {
	c := newTestLRU(1 << 20)
	c.Put("a", "1")
	c.Put("b", "2")

	c.Forget("a")
	_, ok := c.Get("a")
	require.False(t, ok)
	_, ok = c.Get("b")
	require.True(t, ok)
	require.Equal(t, 1, c.Len())

	c.Forget("missing") // must be a no-op, not a panic or a negative count
	require.Equal(t, 1, c.Len())

	c.Clear()
	require.Zero(t, c.Len())
	require.Zero(t, c.Bytes())
}

// TestLRU_UpdateReplacesRatherThanAccumulates: re-putting a key must not double
// count its bytes, or the accounting drifts until the cache starves itself.
func TestLRU_UpdateReplacesRatherThanAccumulates(t *testing.T) {
	c := newTestLRU(1 << 20)
	c.Put("k", "short")
	first := c.Bytes()
	c.Put("k", "short")
	require.Equal(t, first, c.Bytes(), "re-putting the same value must not grow the accounting")

	c.Put("k", "a considerably longer value")
	require.Greater(t, c.Bytes(), first)
	require.Equal(t, 1, c.Len())
}

// TestLRU_ConcurrentUse exercises the shards under the race detector: readers,
// writers, invalidations and a budget change all at once, which is exactly what
// a batch handler plus the memory controller do.
func TestLRU_ConcurrentUse(t *testing.T) {
	c := newTestLRU(64 << 10)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("key-%d-%d", w, i%50)
				c.Put(k, "value")
				c.Get(k)
				if i%10 == 0 {
					c.Forget(k)
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.SetBudget(int64(8<<10) + int64(i)*1024)
		}
	}()
	wg.Wait()

	require.GreaterOrEqual(t, c.Bytes(), int64(0), "byte accounting must never go negative")
	require.LessOrEqual(t, c.Bytes(), c.Budget()+int64(lruShardCount)*128)
}
