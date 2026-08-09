package main

import (
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// withBudget runs f with the process memory budget overridden, restoring it
// afterwards. The budget is a package var resolved once at startup, so this is
// how a test asks "what would this server do in a 512 MiB container".
func withBudget(t *testing.T, budget int64, f func()) {
	t.Helper()
	prev := memoryBudget
	memoryBudget = budget
	defer func() { memoryBudget = prev }()
	f()
}

// TestDetectMemoryBudget_UsesRuntimeLimit: the authoritative ceiling is the
// runtime's own limit -- GOMEMLIMIT, or whatever go-toolchain's injected cgroup
// guard installed -- so the server and the GC agree on one number instead of
// computing two.
func TestDetectMemoryBudget_UsesRuntimeLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	debug.SetMemoryLimit(512 << 20)
	got, source := detectMemoryBudget()
	require.EqualValues(t, 512<<20, got)
	require.Equal(t, "GOMEMLIMIT", source)

	debug.SetMemoryLimit(math.MaxInt64)
	prevPaths := cgroupMemoryLimitPaths
	cgroupMemoryLimitPaths = []string{filepath.Join(t.TempDir(), "nope")}
	t.Cleanup(func() { cgroupMemoryLimitPaths = prevPaths })

	got, source = detectMemoryBudget()
	require.Zero(t, got)
	require.Equal(t, "unknown", source, "an undiscoverable limit must not become an invented one")
}

// TestReadCgroupMemoryLimit covers the fallback for a binary built without the
// injected guard, including both spellings of "no limit": v2's literal "max"
// and v1's saturated sentinel.
func TestReadCgroupMemoryLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    int64
		ok      bool
	}{
		{"v2-limit", "536870912\n", 536870912, true},
		{"v2-unlimited", "max\n", 0, false},
		{"v1-unlimited", "9223372036854771712\n", 0, false},
		{"garbage", "not a number\n", 0, false},
		{"empty", "\n", 0, false},
		{"zero", "0\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "memory.max")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0644))
			prev := cgroupMemoryLimitPaths
			cgroupMemoryLimitPaths = []string{path}
			defer func() { cgroupMemoryLimitPaths = prev }()

			got, ok := readCgroupMemoryLimit()
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestCacheBudget: caches take a share of a known ceiling, and fall back to
// their fixed defaults when there is none -- so discovering nothing changes
// nothing.
func TestCacheBudget(t *testing.T) {
	withBudget(t, 0, func() {
		require.EqualValues(t, defaultMetaCacheBytes, cacheBudget(metaCacheBudgetFraction, defaultMetaCacheBytes))
	})
	withBudget(t, 1<<30, func() {
		gib := float64(int64(1) << 30)
		require.EqualValues(t, int64(gib*metaCacheBudgetFraction), cacheBudget(metaCacheBudgetFraction, defaultMetaCacheBytes))
	})
	withBudget(t, 1024, func() {
		require.Positive(t, cacheBudget(metaCacheBudgetFraction, defaultMetaCacheBytes),
			"even a tiny ceiling must leave a positive budget, not a zero-capacity cache")
	})
}

// fakeCache is a shrinkable that records what the controller does to it.
type fakeCache struct {
	budget int64
	held   int64
}

func (f *fakeCache) SetBudget(b int64) {
	f.budget = b
	if f.held > b {
		f.held = b // "evicting" down to the new budget
	}
}
func (f *fakeCache) Bytes() int64 { return f.held }

// newTestController builds a controller whose sampler and clock the test drives.
func newTestController(budget int64, inUse *int64, now *time.Time) *memController {
	c := newMemController(budget)
	c.sample = func() int64 { return *inUse }
	c.freeOS = func() {}
	c.now = func() time.Time { return *now }
	return c
}

// TestMemController_ShrinksCachesUnderPressure is the requirement in one test:
// when memory climbs, the server holds LESS. Nothing else changes -- there is
// no request path here to change.
func TestMemController_ShrinksCachesUnderPressure(t *testing.T) {
	const budget = 1000
	inUse := int64(0)
	now := time.Now()
	c := newTestController(budget, &inUse, &now)

	cache := &fakeCache{held: 100}
	c.Register("test", 100, cache)
	require.EqualValues(t, 100, cache.budget, "a registered cache starts fully grown")

	inUse = 500 // 50%: nothing to do
	c.poll()
	require.EqualValues(t, 100, cache.budget)
	require.EqualValues(t, 1, c.Scale())

	inUse = 900 // 90%: over the shrink threshold
	c.poll()
	require.EqualValues(t, 50, cache.budget, "pressure must halve the budget")
	require.EqualValues(t, 50, cache.Bytes(), "and the cache must actually have evicted down to it")

	// Still under pressure, past the cooldown: shrink again.
	now = now.Add(memShrinkCooldown + time.Second)
	c.poll()
	require.EqualValues(t, 25, cache.budget)

	// Inside the cooldown, nothing moves -- one burst must not walk the caches
	// straight to their floor.
	c.poll()
	require.EqualValues(t, 25, cache.budget)
}

// TestMemController_GrowsBackAfterPressure: a transient burst must not leave the
// server permanently cold.
func TestMemController_GrowsBackAfterPressure(t *testing.T) {
	inUse := int64(900)
	now := time.Now()
	c := newTestController(1000, &inUse, &now)
	cache := &fakeCache{held: 1000}
	c.Register("test", 1000, cache)

	for i := 0; i < 3; i++ {
		c.poll()
		now = now.Add(memShrinkCooldown + time.Second)
	}
	require.Less(t, c.Scale(), 1.0)
	shrunk := cache.budget

	inUse = 100 // recovered
	for i := 0; i < 20; i++ {
		c.poll()
		now = now.Add(memGrowCooldown + time.Second)
	}
	require.EqualValues(t, 1, c.Scale(), "budgets must return to full once memory recovers")
	require.Greater(t, cache.budget, shrunk)

	// And growth stops at full: the controller never hands out more than the
	// share it was given.
	c.poll()
	require.EqualValues(t, 1000, cache.budget)
}

// TestMemController_FloorsAndWarns: once the caches are as small as they go,
// there is nothing left for the server to give up -- the remaining memory is
// the index and in-flight work. The controller must stop shrinking (rather than
// spin) and say so, because that is the one case only the operator can fix.
func TestMemController_FloorsAndWarns(t *testing.T) {
	inUse := int64(990)
	now := time.Now()
	c := newTestController(1000, &inUse, &now)
	cache := &fakeCache{held: 1000}
	c.Register("test", 1000, cache)

	for i := 0; i < 40; i++ {
		c.poll()
		now = now.Add(memShrinkCooldown + time.Second)
	}
	require.InDelta(t, memMinScale, c.Scale(), 1e-9, "shrinking must stop at the floor")
	require.Positive(t, cache.budget, "the floor is a small cache, never a zero-capacity one")

	// Still at the floor and still under pressure: no panic, no runaway.
	c.poll()
	require.InDelta(t, memMinScale, c.Scale(), 1e-9)
}

// TestMemController_NoBudgetDoesNothing: with no discoverable ceiling the
// controller must not run at all, leaving the caches on their defaults.
func TestMemController_NoBudgetDoesNothing(t *testing.T) {
	c := newMemController(0)
	cache := &fakeCache{held: 100}
	c.Register("test", 100, cache)
	require.EqualValues(t, 100, cache.budget)

	done := make(chan struct{})
	go func() { c.Run(make(chan struct{})); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return immediately when there is no budget")
	}
}

// TestMemSampler_ReadsRuntimeMemory: the sampled quantity must be the one
// GOMEMLIMIT governs (mapped minus released), not a heap-only figure that
// misses stacks and runtime metadata.
func TestMemSampler_ReadsRuntimeMemory(t *testing.T) {
	s := newMemSampler()
	first := s.read()
	require.Positive(t, first)

	ballast := make([]byte, 32<<20)
	for i := range ballast {
		ballast[i] = byte(i + 1)
	}
	require.Greater(t, s.read(), first, "allocating must move the sample")
	require.NotZero(t, ballast[0], "keep the ballast alive across the second sample")
}

// TestMemController_ShrinkEvictsRealCaches wires the controller to the actual
// caches a server runs with, rather than a fake, and checks the whole chain:
// pressure -> smaller budget -> entries actually gone -> the data still
// available from disk.
func TestMemController_ShrinkEvictsRealCaches(t *testing.T) {
	_, storage := testSetupWithStorage(t)

	var keys []string
	for i := 0; i < 200; i++ {
		key := loadTestKey(i)
		require.NoError(t, storage.Put(key, []byte("body"), map[string]string{
			"outputid": "abc", "pkg": "example.com/some/package/path",
		}, nil))
		_, err := storage.Stat(key) // populate the metadata cache
		require.NoError(t, err)
		keys = append(keys, key)
	}
	require.Positive(t, storage.metaCache.Bytes())

	inUse := int64(900)
	now := time.Now()
	c := newTestController(1000, &inUse, &now)
	// Register at what the cache actually holds, so shrinking bites: the real
	// server's full budget is a share of a container's memory, which on a test
	// fixture is far more than these 200 entries occupy.
	before := storage.metaCache.Bytes()
	c.Register(metaCacheKind, before, storage.metaCache)
	for i := 0; i < 12; i++ {
		c.poll()
		now = now.Add(memShrinkCooldown + time.Second)
	}
	require.Less(t, storage.metaCache.Bytes(), before, "pressure must actually free cached bytes")

	// Everything is still served correctly -- eviction cost a re-read, nothing
	// more.
	for _, key := range keys {
		meta, err := storage.Stat(key)
		require.NoError(t, err)
		require.Equal(t, "abc", meta.Metadata["outputid"])
	}
}

// TestMemoryBudgetIsTheEnforcedCeiling: the budget must be the limit the GC is
// actually enforcing, not the container limit somebody could have derived it
// from. go-toolchain's injected init() installs GOMEMLIMIT at a fraction of the
// cgroup limit, and resolving the budget before that init ran reported the raw
// cgroup number -- a ceiling nothing enforced, which reads as "GOMEMLIMIT is
// not set" to anyone diagnosing an OOM from the metric.
func TestMemoryBudgetIsTheEnforcedCeiling(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	installed := int64(512) << 20
	debug.SetMemoryLimit(installed)

	budget, source := detectMemoryBudget()
	require.Equal(t, installed, budget, "the runtime's own limit wins over the cgroup file")
	require.Equal(t, "GOMEMLIMIT", source)

	prevBudget, prevSource := memoryBudget, memoryBudgetSource
	t.Cleanup(func() { memoryBudget, memoryBudgetSource = prevBudget, prevSource })
	resolveMemoryBudget()
	require.Equal(t, installed, memoryBudget, "resolveMemoryBudget is what publishes it")
	require.Equal(t, "GOMEMLIMIT", memoryBudgetSource)
}
