package main

import (
	"log"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The cache is a streaming server -- bodies never sit in the heap whole -- but
// "streaming" bounds the per-request cost, not the total. Under a CI burst the
// total is what kills it: concurrency times the per-request working set, plus
// the in-memory structures that grow with the CACHE (the key index, the
// metadata cache, the known-clean memo), plus whatever the GC has not gotten
// to yet. Those were fixed constants sized for a large host, so on a smaller
// one the process simply grew until the kernel killed it -- and an OOM kill is
// the worst possible failure here: every in-flight upload and download dies,
// the client sees a 502 from the proxy, and the next build starts cold.
//
// So the server reads its own memory ceiling and behaves accordingly:
//
//   - the caches are SIZED from it, instead of from constants;
//   - the request limiter is CAPPED by it, so admitted concurrency times the
//     per-request worst case cannot exceed the budget;
//   - and at runtime a watcher samples memory in use and, as it climbs, first
//     TRIMS what is discardable (every in-memory cache here is an optimization,
//     never the source of truth) and then SHEDS new requests with 503 until it
//     recovers.
//
// Shedding is the last resort and it is deliberately visible: a 503 with
// Retry-After is a client waiting two seconds, an OOM kill is a failed build.
// When no ceiling can be discovered, none of this engages and the server keeps
// its previous fixed-constant behavior -- an unknown limit must not become an
// invented one.

const (
	// memTrimFraction: above this fraction of the budget, drop what is
	// discardable. Optimization caches only -- correctness never depends on
	// them, so the cost of being wrong here is some re-reading, not an error.
	memTrimFraction = 0.80
	// memShedFraction: above this fraction, refuse new requests with 503 +
	// Retry-After. Above it the process is close enough to the ceiling that
	// admitting more work risks the kill.
	memShedFraction = 0.92
	// A memory limit does not produce an OOM kill in Go -- it produces a GC that
	// works harder to honor it. Pushed far enough (a working set that simply does
	// not fit) that becomes a collector running continuously: memory in use sits
	// just under the ceiling, so the in-use fraction reports everything as fine,
	// while throughput collapses. A server that answers nothing is no better than
	// one that died, and the memory gauge cannot see the difference -- so GC CPU
	// is a second, independent pressure signal. (Under the loads measured for
	// docs/memory-limits.md the memory thresholds always fired first; this is the
	// backstop for the shape they cannot see.)
	//
	// memGCTrimFraction: GC taking this share of the process's CPU means the
	// heap is too big for the budget; drop what is discardable.
	memGCTrimFraction = 0.25
	// memGCShedFraction: at this share the process is doing more collecting than
	// working. Shedding turns a hang into a 503 the client can act on.
	memGCShedFraction = 0.50
	// memSampleInterval is how often memory in use is re-sampled. The request
	// path reads the last sample (one atomic load), never the runtime.
	memSampleInterval = 250 * time.Millisecond
	// memTrimCooldown bounds how often a trim runs: a trim drops warm caches
	// and forces a collection, so doing it in a tight loop would cost more than
	// the memory it recovers.
	memTrimCooldown = 30 * time.Second
	// memPerRequestBytes is the per-request working set the concurrency cap is
	// computed from: the PUT guard's small head read, the pooled copy buffer,
	// and room for headers and manifest churn. (It used to be dominated by the
	// guard's 1 MiB bounded prefix; that read is now the rare fallback, not the
	// per-PUT cost -- see storeOneObject.)
	memPerRequestBytes = 256 << 10
	// memRequestBudgetFraction is how much of the budget in-flight requests may
	// claim. The rest belongs to the index, the caches, and GC headroom.
	memRequestBudgetFraction = 0.25
)

// memoryBudget is the process's memory ceiling in bytes, or 0 when none could
// be discovered. Resolved once at startup; see detectMemoryBudget.
var memoryBudget, memoryBudgetSource = detectMemoryBudget()

// detectMemoryBudget returns the process's memory ceiling and where it came
// from.
//
// The authoritative answer is the runtime's own limit: GOMEMLIMIT if the
// operator set it, or the value go-toolchain's injected cgroup guard installed
// at startup (it reads the cgroup v2/v1 limit and applies a ratio). Reading it
// back means this server agrees with the GC about the ceiling rather than
// computing a second, different one.
//
// The cgroup files are a fallback for a binary built without that guard. An
// undiscoverable limit returns 0, which disables every behavior in this file.
func detectMemoryBudget() (int64, string) {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit != math.MaxInt64 {
		return limit, "GOMEMLIMIT"
	}
	if v, ok := readCgroupMemoryLimit(); ok {
		return v, "cgroup"
	}
	return 0, "unknown"
}

// cgroupMemoryLimitPaths are read in order: cgroup v2's unified file first,
// then v1's. Both are the container's own limit when the process runs in its
// own cgroup namespace, which is the normal container case.
var cgroupMemoryLimitPaths = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

func readCgroupMemoryLimit() (int64, bool) {
	for _, path := range cgroupMemoryLimitPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(data))
		if s == "" || s == "max" {
			continue // v2 spells "no limit" as "max"
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil || v <= 0 {
			continue
		}
		// v1 spells "no limit" as a value near the top of the range.
		if v >= 1<<62 {
			continue
		}
		return v, true
	}
	return 0, false
}

// budgetFraction returns n bytes of the budget, clamped between lo and hi. With
// no budget it returns hi -- the fixed size the server used before it knew its
// ceiling, so an undiscoverable limit changes nothing.
func budgetFraction(fraction float64, perEntryBytes, lo, hi int64) int64 {
	if memoryBudget <= 0 {
		return hi
	}
	n := int64(float64(memoryBudget) * fraction / float64(perEntryBytes))
	return max(lo, min(n, hi))
}

// concurrencyForBudget caps a configured request limit so that the admitted
// requests' worst-case working set fits the budget. It only ever LOWERS the
// limit: an operator asking for less than the budget allows is respected.
func concurrencyForBudget(configured int) int {
	if memoryBudget <= 0 {
		return configured
	}
	allowed := int(float64(memoryBudget) * memRequestBudgetFraction / memPerRequestBytes)
	if allowed < 8 {
		allowed = 8 // a server that can serve nothing is not a safer server
	}
	return min(configured, allowed)
}

// memSampler reads the memory the runtime counts against its limit: everything
// it has mapped and not released back to the OS. This is the same quantity
// GOMEMLIMIT governs, so comparing it to the budget compares like with like --
// unlike heap-only figures, which miss stacks and runtime metadata.
type memSampler struct {
	samples []metrics.Sample
	// Cumulative CPU seconds at the previous read, so the reported GC fraction
	// is the fraction over the last interval rather than since process start --
	// a lifetime average would hide a spiral that started a minute ago.
	prevGC, prevTotal float64
	primed            bool
}

func newMemSampler() *memSampler {
	return &memSampler{samples: []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}}
}

// read returns memory in use and the share of CPU the GC took since the
// previous call (0 on the first call, which has no interval to measure).
func (m *memSampler) read() (int64, float64) {
	metrics.Read(m.samples)
	total := m.samples[0].Value.Uint64()
	released := m.samples[1].Value.Uint64()
	inUse := int64(total)
	if released <= total {
		inUse = int64(total - released)
	}

	gcCPU := m.samples[2].Value.Float64()
	allCPU := m.samples[3].Value.Float64()
	var fraction float64
	if m.primed {
		dGC, dAll := gcCPU-m.prevGC, allCPU-m.prevTotal
		if dAll > 0 && dGC > 0 {
			fraction = dGC / dAll
		}
	}
	m.prevGC, m.prevTotal, m.primed = gcCPU, allCPU, true
	return inUse, fraction
}

// memWatcher samples memory in use in the background and answers, in O(1) on
// the request path, whether the server should shed. Crossing the trim
// threshold runs the registered trimmers (bounded by a cooldown) BEFORE any
// shedding starts, because dropping a discardable cache costs a re-read while
// shedding costs a client its request.
type memWatcher struct {
	budget  int64
	trimAt  int64
	shedAt  int64
	sample  func() (int64, float64) // seam for tests: memory in use, GC CPU share
	trimNow func()                  // seam for tests: what a trim does beyond the trimmers

	inUse    atomic.Int64
	gcShare  atomic.Uint64 // math.Float64bits of the last GC CPU share
	shedding atomic.Bool
	mu       sync.Mutex
	trimmers []func()
	lastTrim time.Time
	now      func() time.Time // seam for tests
}

func newMemWatcher(budget int64) *memWatcher {
	s := newMemSampler()
	w := &memWatcher{
		budget:  budget,
		trimAt:  int64(float64(budget) * memTrimFraction),
		shedAt:  int64(float64(budget) * memShedFraction),
		sample:  s.read,
		trimNow: debug.FreeOSMemory,
		now:     time.Now,
	}
	memoryLimitBytes.Set(float64(budget))
	return w
}

// AddTrimmer registers something to drop when memory gets tight. Every trimmer
// must be safe to call at any time and must only discard rebuildable state.
func (w *memWatcher) AddTrimmer(f func()) {
	w.mu.Lock()
	w.trimmers = append(w.trimmers, f)
	w.mu.Unlock()
}

// Run samples until ctx-less stop; started as a goroutine at boot. A watcher
// with no budget never runs.
func (w *memWatcher) Run(stop <-chan struct{}) {
	if w.budget <= 0 {
		return
	}
	t := time.NewTicker(memSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			w.poll()
		}
	}
}

// poll takes one sample and updates the two pressure verdicts. Either signal
// alone is enough: a heap near the ceiling, or a GC that is eating the process.
func (w *memWatcher) poll() {
	in, gcShare := w.sample()
	if w.applySample(in, gcShare) {
		w.trim()
	}
}

// applySample records one sample, flips the shed verdict (logging each
// transition, so the reason a client saw a 503 is in the log), and reports
// whether a trim is due. Both signals are checked here and nowhere else, so a
// re-sample can never disagree with a poll about what pressure means.
func (w *memWatcher) applySample(in int64, gcShare float64) (trimDue bool) {
	w.inUse.Store(in)
	w.gcShare.Store(math.Float64bits(gcShare))
	memoryInUseBytes.Set(float64(in))
	memoryGCCPUFraction.Set(gcShare)

	shed := in >= w.shedAt || gcShare >= memGCShedFraction
	if w.shedding.Swap(shed) != shed {
		if shed {
			log.Printf("memory: shedding new requests (%d MiB in use of a %d MiB budget, GC using %.0f%% of CPU); clients get 503 + Retry-After until this clears",
				in>>20, w.budget>>20, gcShare*100)
		} else {
			log.Printf("memory: pressure cleared (%d MiB in use of a %d MiB budget, GC using %.0f%% of CPU); serving normally",
				in>>20, w.budget>>20, gcShare*100)
		}
	}
	return in >= w.trimAt || gcShare >= memGCTrimFraction
}

// trim drops every registered cache and returns memory to the OS, at most once
// per cooldown. Rate limiting matters: under sustained pressure the sampler
// would otherwise trim every 250ms, and a forced collection is not cheap.
func (w *memWatcher) trim() {
	w.mu.Lock()
	if !w.lastTrim.IsZero() && w.now().Sub(w.lastTrim) < memTrimCooldown {
		w.mu.Unlock()
		return
	}
	w.lastTrim = w.now()
	trimmers := append([]func(){}, w.trimmers...)
	w.mu.Unlock()

	for _, f := range trimmers {
		f()
	}
	if w.trimNow != nil {
		w.trimNow()
	}
	memoryTrimsTotal.Inc()
	log.Printf("memory: %d MiB in use against a %d MiB budget; dropped in-memory caches (they are optimizations, not state) and returned memory to the OS",
		w.inUse.Load()>>20, w.budget>>20)

	// Re-sample so a recovered trim is visible to the request path immediately
	// rather than up to one interval later, when it would still be shedding.
	// Through applySample, so the recovery verdict uses both signals -- a trim
	// frees memory but does not stop a GC that is thrashing.
	if w.sample != nil {
		w.applySample(w.sample())
	}
}

// ShouldShed reports whether new work must be refused right now. False whenever
// no budget is known, so an undiscoverable limit can never shed.
func (w *memWatcher) ShouldShed() bool {
	if w == nil || w.budget <= 0 {
		return false
	}
	return w.shedding.Load()
}

// InUse is the last memory sample, for logging and tests.
func (w *memWatcher) InUse() int64 { return w.inUse.Load() }

// GCShare is the last GC CPU share, for logging and tests.
func (w *memWatcher) GCShare() float64 { return math.Float64frombits(w.gcShare.Load()) }
