package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// withBudget runs f with the process memory budget overridden, restoring it
// afterwards. The budget is a package var resolved once at startup, so this is
// how a test exercises "what would this server do in a 512 MiB container".
func withBudget(t *testing.T, budget int64, f func()) {
	t.Helper()
	prev := memoryBudget
	memoryBudget = budget
	defer func() { memoryBudget = prev }()
	f()
}

// TestDetectMemoryBudget_UsesRuntimeLimit: the authoritative ceiling is the
// runtime's own limit -- GOMEMLIMIT, or whatever go-toolchain's injected cgroup
// guard installed at startup -- so the server and the GC agree on one number
// instead of computing two.
func TestDetectMemoryBudget_UsesRuntimeLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	debug.SetMemoryLimit(512 << 20)
	got, source := detectMemoryBudget()
	require.EqualValues(t, 512<<20, got)
	require.Equal(t, "GOMEMLIMIT", source)

	// No runtime limit: fall through to the cgroup files (absent here, in this
	// test's fixture-less state) and report an unknown budget rather than
	// inventing one.
	debug.SetMemoryLimit(math.MaxInt64)
	prevPaths := cgroupMemoryLimitPaths
	cgroupMemoryLimitPaths = []string{filepath.Join(t.TempDir(), "nope")}
	t.Cleanup(func() { cgroupMemoryLimitPaths = prevPaths })

	got, source = detectMemoryBudget()
	require.Zero(t, got)
	require.Equal(t, "unknown", source)
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

// TestBudgetFraction: caches scale with the budget, clamped at both ends, and
// an unknown budget keeps the pre-existing fixed size so discovering nothing
// changes nothing.
func TestBudgetFraction(t *testing.T) {
	withBudget(t, 0, func() {
		require.EqualValues(t, 1000, budgetFraction(0.05, 100, 10, 1000), "unknown budget keeps the old constant")
	})
	withBudget(t, 1<<30, func() {
		require.EqualValues(t, 1000, budgetFraction(0.05, 100, 10, 1000), "a large budget is capped")
	})
	withBudget(t, 8<<10, func() {
		require.EqualValues(t, 10, budgetFraction(0.05, 100, 10, 1000), "a tiny budget floors at the minimum")
	})
	withBudget(t, 1<<20, func() {
		require.EqualValues(t, 524, budgetFraction(0.05, 100, 10, 1000), "otherwise it is the budget share")
	})
}

// TestConcurrencyForBudget: the request limiter is capped by what the budget
// can hold, and only ever downward -- an operator asking for less than the
// budget allows is not overridden upward.
func TestConcurrencyForBudget(t *testing.T) {
	withBudget(t, 0, func() {
		require.Equal(t, 128, concurrencyForBudget(128), "unknown budget leaves the configured value alone")
	})
	withBudget(t, 64<<30, func() {
		require.Equal(t, 128, concurrencyForBudget(128), "a large budget does not raise the configured value")
	})
	withBudget(t, 32<<20, func() {
		got := concurrencyForBudget(128)
		require.Less(t, got, 128, "a small budget must lower the limit")
		require.GreaterOrEqual(t, got, 8)
	})
	withBudget(t, 4<<20, func() {
		require.Equal(t, 8, concurrencyForBudget(128), "never zero: a server that serves nothing is not safer")
	})
}

// newTestWatcher builds a watcher whose sampler and clock are under the test's
// control, so pressure is exercised without allocating gigabytes or spinning a
// real GC.
func newTestWatcher(budget int64, inUse *int64, now *time.Time) *memWatcher {
	return newTestWatcherGC(budget, inUse, new(float64), now)
}

func newTestWatcherGC(budget int64, inUse *int64, gcShare *float64, now *time.Time) *memWatcher {
	w := newMemWatcher(budget)
	w.sample = func() (int64, float64) { return *inUse, *gcShare }
	w.trimNow = nil // no forced GC in tests
	w.now = func() time.Time { return *now }
	return w
}

// TestMemWatcher_ShedsOnGCThrash covers the pressure shape a memory gauge
// cannot see: Go does not OOM when the working set will not fit, it collects
// harder, so memory in use can sit BELOW the shed threshold -- the GC is
// "succeeding" -- while the process spends its time collecting instead of
// serving. GC CPU is therefore a pressure signal in its own right.
func TestMemWatcher_ShedsOnGCThrash(t *testing.T) {
	inUse := int64(700) // 70%: comfortably below both memory thresholds
	gcShare := 0.0
	now := time.Now()
	w := newTestWatcherGC(1000, &inUse, &gcShare, &now)

	var trims int
	w.AddTrimmer(func() { trims++ })

	w.poll()
	require.False(t, w.ShouldShed())
	require.Zero(t, trims)

	gcShare = 0.30 // the GC is working hard but the server is still serving
	w.poll()
	require.Equal(t, 1, trims, "a GC eating a quarter of the CPU must trim")
	require.False(t, w.ShouldShed(), "trimming first, shedding only if that is not enough")

	gcShare = 0.75 // more collecting than working: this is the hang
	now = now.Add(memTrimCooldown + time.Second)
	w.poll()
	require.True(t, w.ShouldShed(), "a thrashing GC must shed even with memory below the threshold")
	require.InDelta(t, 0.75, w.GCShare(), 0.001)

	gcShare = 0.05
	w.poll()
	require.False(t, w.ShouldShed(), "recovery must not need a restart")
}

// TestMemSampler_GCFractionIsPerInterval: the first read has no interval to
// measure and must report 0 rather than a lifetime average, and subsequent
// reads must report a share in [0,1].
func TestMemSampler_GCFractionIsPerInterval(t *testing.T) {
	s := newMemSampler()
	_, first := s.read()
	require.Zero(t, first, "the first sample has no interval")

	for i := 0; i < 50; i++ {
		_ = make([]byte, 1<<20)
	}
	runtime.GC()
	_, second := s.read()
	require.GreaterOrEqual(t, second, 0.0)
	require.LessOrEqual(t, second, 1.0)
}

// TestMemWatcher_TrimsBeforeShedding is the ordering that matters: dropping a
// rebuildable cache costs a re-read, shedding costs a client its request, so
// the trim threshold must be crossed (and acted on) first.
func TestMemWatcher_TrimsBeforeShedding(t *testing.T) {
	const budget = 1000
	inUse := int64(0)
	now := time.Now()
	w := newTestWatcher(budget, &inUse, &now)

	var trims int
	w.AddTrimmer(func() { trims++ })

	inUse = 500 // 50%: nothing to do
	w.poll()
	require.Zero(t, trims)
	require.False(t, w.ShouldShed())

	inUse = 850 // 85%: past the trim threshold, below the shed threshold
	w.poll()
	require.Equal(t, 1, trims, "crossing the trim threshold must drop the caches")
	require.False(t, w.ShouldShed(), "trimming is not shedding")

	inUse = 950 // 95%: shed
	now = now.Add(memTrimCooldown + time.Second)
	w.poll()
	require.Equal(t, 2, trims)
	require.True(t, w.ShouldShed(), "above the shed threshold new work must be refused")

	// Recovery is immediate once memory comes back down.
	inUse = 400
	w.poll()
	require.False(t, w.ShouldShed())
}

// TestMemWatcher_TrimCooldown: under sustained pressure the sampler fires four
// times a second, and a trim drops warm caches and forces a collection. Without
// a cooldown that would cost more than it recovers.
func TestMemWatcher_TrimCooldown(t *testing.T) {
	inUse := int64(900)
	now := time.Now()
	w := newTestWatcher(1000, &inUse, &now)

	var trims int
	w.AddTrimmer(func() { trims++ })

	for i := 0; i < 10; i++ {
		w.poll()
		now = now.Add(memSampleInterval)
	}
	require.Equal(t, 1, trims, "repeated polls inside the cooldown must trim once")

	now = now.Add(memTrimCooldown)
	w.poll()
	require.Equal(t, 2, trims, "past the cooldown a trim may run again")
}

// TestMemWatcher_NoBudgetNeverSheds: an undiscoverable limit must not become an
// invented one. A server that cannot tell how much memory it has behaves
// exactly as it did before any of this existed.
func TestMemWatcher_NoBudgetNeverSheds(t *testing.T) {
	w := newMemWatcher(0)
	require.False(t, w.ShouldShed())
	w.inUse.Store(math.MaxInt64)
	require.False(t, w.ShouldShed(), "with no budget there is no threshold to cross")

	var nilWatcher *memWatcher
	require.False(t, nilWatcher.ShouldShed(), "a nil watcher must be safe on the request path")

	// Run returns immediately rather than spinning a pointless ticker.
	done := make(chan struct{})
	go func() { w.Run(make(chan struct{})); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must exit immediately when there is no budget")
	}
}

// TestMemSampler_ReadsRuntimeMemory: the sampled quantity must be the one
// GOMEMLIMIT governs (mapped minus released), not a heap-only figure that
// misses stacks and runtime metadata.
func TestMemSampler_ReadsRuntimeMemory(t *testing.T) {
	s := newMemSampler()
	first, _ := s.read()
	require.Positive(t, first)

	ballast := make([]byte, 32<<20)
	for i := range ballast {
		ballast[i] = byte(i + 1)
	}
	after, _ := s.read()
	require.Greater(t, after, first, "allocating must move the sample")
	require.NotZero(t, ballast[0], "keep the ballast alive across the second sample")
}

// TestServer_ShedsUnderMemoryPressure: the whole point, at the HTTP boundary.
// A request that would be served normally is refused with 503 + Retry-After
// while memory is above the shed threshold, and served again once it is not.
func TestServer_ShedsUnderMemoryPressure(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	cfg := &Config{Bucket: "testbucket", DisableAuth: true}
	srv := NewServer(cfg, storage)

	inUse := int64(0)
	now := time.Now()
	srv.mem = newTestWatcher(1000, &inUse, &now)

	key := "go-buildcache/v1" + strings.Repeat("1", 64)
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "abc"}, nil))
	get := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/testbucket/"+key, nil))
		return rec
	}

	inUse = 100
	require.Equal(t, 200, get().Code)

	inUse = 990
	srv.mem.poll()
	rec := get()
	require.Equal(t, 503, rec.Code)
	require.Equal(t, "2", rec.Header().Get("Retry-After"), "a shed response must tell the client when to come back")
	require.Contains(t, rec.Body.String(), "memory pressure")

	// The health probe is answered before admission control, so an orchestrator
	// still sees a live server it can route to once pressure passes -- shedding
	// must not read as "this instance is dead".
	health := httptest.NewRecorder()
	srv.ServeHTTP(health, httptest.NewRequest(http.MethodGet, healthPath, nil))
	require.Equal(t, 200, health.Code)

	inUse = 100
	srv.mem.poll()
	require.Equal(t, 200, get().Code, "pressure passing must restore service without a restart")
}

// TestTrimCaches_DropsRebuildableStateOnly: a trim must leave the server
// correct -- every cache it drops is an optimization over what is on disk, so
// afterwards the same requests still answer identically.
func TestTrimCaches_DropsRebuildableStateOnly(t *testing.T) {
	ts, storage := testSetupWithStorage(t)

	var keys []string
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("go-buildcache/v1%064x", i)
		body := []byte("!<arch>\nobject " + key)
		require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, body)), map[string]string{
			"outputid": "abc", "compression": "lz4",
		}, nil))
		keys = append(keys, key)
		hash, ok := extractActionHash(key)
		require.True(t, ok)
		storage.markKeyClean(hash)
		_, err := storage.Stat(key)
		require.NoError(t, err)
	}
	blobBefore, etagBefore := storage.Index.Blob()
	require.NotEmpty(t, blobBefore)
	require.Positive(t, storage.metaCache.size())

	storage.TrimCaches()

	require.Zero(t, storage.metaCache.size(), "the metadata cache is rebuildable from xattrs")
	require.Zero(t, storage.cleanKeys.size(), "the known-clean memo is rebuildable by re-probing")

	// Identical answers afterwards: the index serves the same bytes and every
	// object still reads back with its metadata.
	blobAfter, etagAfter := storage.Index.Blob()
	require.Equal(t, blobBefore, blobAfter, "a dropped blob must rebuild byte-identically")
	require.Equal(t, etagBefore, etagAfter)
	for _, key := range keys {
		meta, err := storage.Stat(key)
		require.NoError(t, err)
		require.Equal(t, "abc", meta.Metadata["outputid"])
		resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
		require.Equal(t, 200, resp.StatusCode)
		resp.Body.Close()
	}
}
