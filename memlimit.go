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

const (
	// Fractions of the process budget each cache may claim when it is fully grown.
	metaCacheBudgetFraction = 0.10
	cleanMemoBudgetFraction = 0.03
	// Defaults when there is no discoverable ceiling: what the entry-count bounds worked out to in bytes.
	defaultMetaCacheBytes = 32 << 20
	defaultCleanMemoBytes = 16 << 20

	// memShrinkFraction: above this share of the budget, shrink the caches.
	memShrinkFraction = 0.85
	// memGrowFraction: below this share, let them grow back.
	memGrowFraction = 0.65
	// Multipliers applied to the cache scale on each shrink/grow step.
	memShrinkStep = 0.5
	memGrowStep   = 1.25
	// memMinScale floors the shrinking.
	memMinScale = 1.0 / 32
	// memSampleInterval is how often memory in use is sampled.
	memSampleInterval = 250 * time.Millisecond
	// memShrinkCooldown / memGrowCooldown bound how often the scale moves.
	memShrinkCooldown = 2 * time.Second
	memGrowCooldown   = 30 * time.Second
)

// Both are empty until resolveMemoryBudget runs.
var memoryBudget int64
var memoryBudgetSource string

// resolveMemoryBudget discovers the ceiling. Call it a single time at
// startup.
func resolveMemoryBudget() {
	memoryBudget, memoryBudgetSource = detectMemoryBudget()
}

// detectMemoryBudget returns the process's memory ceiling and where it came
// from.
func detectMemoryBudget() (int64, string) {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit != math.MaxInt64 {
		return limit, "GOMEMLIMIT"
	}
	if v, ok := readCgroupMemoryLimit(); ok {
		return v, "cgroup"
	}
	return 0, "unknown"
}

// cgroupMemoryLimitPaths are read in order: cgroup v2's unified file then
// v1's.
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

// defaultGCPercent is the heap growth target this server installs when the operator has not set GOGC.
const defaultGCPercent = 50

// tuneGC installs defaultGCPercent unless the operator set GOGC, and reports
// what it did.
func tuneGC() (applied bool, previous int) {
	if os.Getenv("GOGC") != "" {
		// Read nothing and set nothing: SetGCPercent has no read-only form.
		return false, 0
	}
	return true, debug.SetGCPercent(defaultGCPercent)
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

// shrinkable is a cache the controller can resize.
type shrinkable interface {
	// SetBudget sets the cache's byte budget, evicting down to it.
	SetBudget(int64)
	// Bytes reports what it holds.
	Bytes() int64
}

// namedCache pairs a cache with its label and fully-grown budget.
type namedCache struct {
	name string
	full int64
	c    shrinkable
}

// memSampler reads the memory the runtime counts against its limit:
// everything mapped and not released back to the OS.
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
// the caches keep their default budgets. This is exactly the behavior the
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

// poll takes a single sample and moves the cache scale if warranted.
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
			// Nothing left to give: what remains is the index and in-flight work, neither of which may be dropped. Say so plainly -- this is the a single case where the operator, not the server, has to act.
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
