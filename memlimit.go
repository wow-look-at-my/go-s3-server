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
	"time"
)

// How this server stays inside its memory limit: it holds less, never serves
// less.
//
// Bodies are already streamed, so a request's own cost is small and bounded.
// What grows is what the server CACHES in memory -- object metadata, the
// known-clean verdicts, the prefetch suppression records, the serialized
// /_index blob. Every one of those is reconstructible from disk, which makes
// them the correct thing to give up under pressure: dropping one costs a
// re-read, and the client never learns it happened.
//
// So each cache is bounded in BYTES (lrucache.go), sized from the process's
// memory ceiling, and evicts its least-recently-used entries to stay inside
// that bound. This file adds the feedback loop on top: sample memory in use,
// and when it climbs, SHRINK the caches' budgets -- which evicts -- then let
// them grow back as memory recovers.
//
// Nothing here touches request handling. A cache server that answers "no"
// because of its own bookkeeping is useless: every client it refuses rebuilds
// anyway, having first paid for the round trip. Refusing service is not a
// memory strategy, it is a failure.
//
// When no ceiling can be discovered, the caches keep fixed default budgets and
// the controller does not run. An unknown limit must not become an invented
// one.

const (
	// Fractions of the process budget each cache may claim when it is fully
	// grown. They are deliberately modest: the cache's real value is on disk,
	// and these only save syscalls and re-probes.
	metaCacheBudgetFraction = 0.10
	cleanMemoBudgetFraction = 0.03
	prefetchBudgetFraction  = 0.02
	// Defaults when there is no discoverable ceiling: what the previous
	// entry-count bounds worked out to in bytes.
	defaultMetaCacheBytes = 32 << 20
	defaultCleanMemoBytes = 16 << 20
	defaultPrefetchBytes  = 8 << 20

	// memShrinkFraction: above this share of the budget, shrink the caches.
	memShrinkFraction = 0.85
	// memGrowFraction: below this share, let them grow back. The gap between
	// the two is hysteresis -- without it the controller would oscillate every
	// sample.
	memGrowFraction = 0.65
	// Multipliers applied to the cache scale on each shrink/grow step. Shrink
	// fast (memory pressure is urgent), grow slowly (do not re-create the
	// pressure you just relieved).
	memShrinkStep = 0.5
	memGrowStep   = 1.25
	// memMinScale floors the shrinking: below this the caches are effectively
	// off and shrinking further only costs re-reads without freeing anything
	// meaningful.
	memMinScale = 1.0 / 32
	// memSampleInterval is how often memory in use is sampled.
	memSampleInterval = 250 * time.Millisecond
	// memShrinkCooldown / memGrowCooldown bound how often the scale moves, so
	// one burst does not walk the caches to their floor and a recovery does not
	// snap them straight back.
	memShrinkCooldown = 2 * time.Second
	memGrowCooldown   = 30 * time.Second
)

// memoryBudget is the process's memory ceiling in bytes, or 0 when none could
// be discovered. Resolved once at startup; see detectMemoryBudget.
var memoryBudget, memoryBudgetSource = detectMemoryBudget()

// gomemlimitHeadroom is the share of the container's limit handed to the GC as
// its own soft limit. The remainder covers what the Go heap accounting does not
// see (thread stacks the runtime has not mapped, the allocator's own
// fragmentation, anything else in the container) so the GC works harder before
// the kernel's limit, rather than after it.
const gomemlimitHeadroom = 0.9

// applyRuntimeMemoryLimit hands the discovered container limit to the GC.
//
// Without it the GC only targets a multiple of the live heap (GOGC=100 means
// roughly double it) and has no idea a ceiling exists: a 400 MiB live heap
// happily grows toward 800 MiB of heap goal inside a 1 GiB container, and a
// burst of concurrent requests on top of that is an OOM kill, not a GC. Setting
// the limit makes the GC run harder as the total approaches the ceiling.
//
// It returns the limit it installed, or 0 when it installed none: an operator's
// own GOMEMLIMIT is left exactly as set, and with no discoverable ceiling there
// is nothing honest to install.
func applyRuntimeMemoryLimit() int64 {
	limit := runtimeMemoryLimitFor(memoryBudget, memoryBudgetSource)
	if limit > 0 {
		debug.SetMemoryLimit(limit)
	}
	return limit
}

// runtimeMemoryLimitFor is applyRuntimeMemoryLimit's decision, without the
// process-wide side effect. A budget that came from GOMEMLIMIT is already the
// runtime's limit -- re-deriving one from it would quietly shrink the operator's
// setting on every start.
func runtimeMemoryLimitFor(budget int64, source string) int64 {
	if budget <= 0 || source != "cgroup" {
		return 0
	}
	return int64(float64(budget) * gomemlimitHeadroom)
}

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
// undiscoverable limit returns 0, which leaves the caches on fixed defaults and
// the controller stopped.
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

// cacheBudget returns a cache's fully-grown byte budget: a share of the process
// budget, or the fixed default when no ceiling is known.
func cacheBudget(fraction float64, fallback int64) int64 {
	if memoryBudget <= 0 {
		return fallback
	}
	if n := int64(float64(memoryBudget) * fraction); n > 0 {
		return n
	}
	return 1
}

// shrinkable is a cache the controller can resize. Everything registered must
// be reconstructible from disk -- the controller's only lever is "hold less".
type shrinkable interface {
	// SetBudget sets the cache's byte budget, evicting down to it.
	SetBudget(int64)
	// Bytes reports what it currently holds.
	Bytes() int64
}

// namedCache pairs a cache with its label and fully-grown budget.
type namedCache struct {
	name string
	full int64
	c    shrinkable
}

// memSampler reads the memory the runtime counts against its limit: everything
// mapped and not released back to the OS. This is the quantity GOMEMLIMIT
// governs, so comparing it to the budget compares like with like -- unlike
// heap-only figures, which miss stacks and runtime metadata.
type memSampler struct {
	samples []metrics.Sample
}

func newMemSampler() *memSampler {
	return &memSampler{samples: []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}}
}

func (m *memSampler) read() int64 {
	metrics.Read(m.samples)
	total := m.samples[0].Value.Uint64()
	released := m.samples[1].Value.Uint64()
	if released > total {
		return int64(total)
	}
	return int64(total - released)
}

// memController samples memory in use and scales the registered caches to fit.
// It is a feedback loop over cache SIZE and nothing else: it cannot refuse a
// request, delay one, or change what the server answers.
type memController struct {
	budget   int64
	shrinkAt int64
	growAt   int64
	sample   func() int64 // seam for tests
	freeOS   func()       // seam for tests
	now      func() time.Time

	mu            sync.Mutex
	caches        []namedCache
	scale         float64
	lastShrink    time.Time
	lastGrow      time.Time
	warnedAtFloor bool
}

func newMemController(budget int64) *memController {
	s := newMemSampler()
	memoryLimitBytes.Set(float64(budget))
	return &memController{
		budget:   budget,
		shrinkAt: int64(float64(budget) * memShrinkFraction),
		growAt:   int64(float64(budget) * memGrowFraction),
		sample:   s.read,
		freeOS:   debug.FreeOSMemory,
		now:      time.Now,
		scale:    1,
	}
}

// Register adds a cache to the controller and applies the current scale to it.
func (m *memController) Register(name string, full int64, c shrinkable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.caches = append(m.caches, namedCache{name: name, full: full, c: c})
	c.SetBudget(scaled(full, m.scale))
	cacheBudgetBytes.WithLabelValues(name).Set(float64(scaled(full, m.scale)))
}

func scaled(full int64, scale float64) int64 {
	n := int64(float64(full) * scale)
	if n < 1 {
		return 1
	}
	return n
}

// Run samples until stop is closed. A controller with no budget never runs:
// the caches keep their default budgets, which is exactly the behavior the
// server had before it knew its ceiling.
func (m *memController) Run(stop <-chan struct{}) {
	if m.budget <= 0 {
		return
	}
	t := time.NewTicker(memSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			m.poll()
		}
	}
}

// poll takes one sample and moves the cache scale if warranted.
func (m *memController) poll() {
	in := m.sample()
	memoryInUseBytes.Set(float64(in))
	switch {
	case in >= m.shrinkAt:
		m.shrink(in)
	case in <= m.growAt:
		m.grow()
	}
	m.publish()
}

// shrink cuts every cache's budget, which evicts their least-recently-used
// entries, and returns the freed memory to the OS.
func (m *memController) shrink(inUse int64) {
	m.mu.Lock()
	if !m.lastShrink.IsZero() && m.now().Sub(m.lastShrink) < memShrinkCooldown {
		m.mu.Unlock()
		return
	}
	if m.scale <= memMinScale {
		atFloor, held := !m.warnedAtFloor, m.heldBytesLocked()
		m.warnedAtFloor = true
		m.mu.Unlock()
		if atFloor {
			// Nothing left to give: what remains is the index and in-flight work,
			// neither of which may be dropped. Say so plainly -- this is the one
			// case where the operator, not the server, has to act.
			log.Printf("memory: %d MiB in use of a %d MiB budget with the in-memory caches already at their floor (holding %d MiB). The rest is the key index and in-flight requests, which cannot be dropped without breaking the cache -- this container needs more memory.",
				inUse>>20, m.budget>>20, held>>20)
		}
		return
	}
	m.lastShrink = m.now()
	m.warnedAtFloor = false
	prev := m.scale
	m.scale = math.Max(memMinScale, m.scale*memShrinkStep)
	before := m.heldBytesLocked()
	m.applyLocked()
	after := m.heldBytesLocked()
	caches := len(m.caches)
	m.mu.Unlock()

	m.freeOS()
	memoryShrinksTotal.Inc()
	log.Printf("memory: %d MiB in use of a %d MiB budget; shrank %d in-memory cache(s) to %.0f%% of full (was %.0f%%), releasing %d MiB. Requests are unaffected -- evicted entries are re-read from disk.",
		inUse>>20, m.budget>>20, caches, m.scale*100, prev*100, (before-after)>>20)
}

// grow restores cache budgets as memory recovers, so a transient burst does not
// leave the server permanently cold.
func (m *memController) grow() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scale >= 1 {
		return
	}
	if !m.lastGrow.IsZero() && m.now().Sub(m.lastGrow) < memGrowCooldown {
		return
	}
	m.lastGrow = m.now()
	m.scale = math.Min(1, m.scale*memGrowStep)
	m.applyLocked()
}

func (m *memController) applyLocked() {
	for _, nc := range m.caches {
		b := scaled(nc.full, m.scale)
		nc.c.SetBudget(b)
		cacheBudgetBytes.WithLabelValues(nc.name).Set(float64(b))
	}
}

func (m *memController) heldBytesLocked() int64 {
	var n int64
	for _, nc := range m.caches {
		n += nc.c.Bytes()
	}
	return n
}

// publish updates the per-cache size gauges.
func (m *memController) publish() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, nc := range m.caches {
		cacheMemoryBytes.WithLabelValues(nc.name).Set(float64(nc.c.Bytes()))
	}
}

// Scale is the current fraction of full budget the caches are allowed, for
// logging and tests.
func (m *memController) Scale() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scale
}
