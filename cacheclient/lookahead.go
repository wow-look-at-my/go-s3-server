// The look-ahead pool.

package cacheclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// The look-ahead pool. A build asks for a single key at a time, in dependency order, and it cannot ask for the next a single until this answers. A package's action ID is computed from its dependencies' output IDs. So the number of keys the build has outstanding is its own -p, and no amount of batching on the client changes that. What DOES change it is asking for keys nobody has asked for yet.
//
// The server can name those keys. It stores an object's modification time. The objects a single build writes land next to each other in time. The window around a key this build wanted is mostly keys this build wants next. That is what /_batch/get's prefetch flag returns.
//
// Properties decide where that work runs.
type lookAhead struct {
	b     *WebBackend
	seeds chan []string
	stop  chan struct{}
	wg    sync.WaitGroup

	// seeded guards a key against being seeded again.
	mu     sync.Mutex
	seeded set.Set[string]

	// held is what the workers are carrying right now, in bytes, and budget is what they may carry between them.
	held   atomic.Int64
	budget int64
	chunk  int64 // bytes a single worker may hold between

	// serverOff is set when the server says it sends no look-ahead window. Every later seed is then dropped unsent.
	serverOff atomic.Bool

	// covered is set when a build manifest names this build's keys. The window then only adds bodies the build does not use.
	covered atomic.Bool

	// gate holds the workers while the manifest walk decides whether a manifest covers the build. Nil holds nothing.
	gateMu   sync.Mutex
	gate     chan struct{}
	gateOpen bool

	// brought holds the keys the pool fetched that the build has not used yet. Used counts those it then used.
	brought set.Set[actionHash]
	Used    AtomicCounter
	// wasteful is set when the build uses too little of what the pool fetched. The pool then stops.
	wasteful atomic.Bool

	// sampled counts the entries the windows brought, as they arrive. lookups counts the build's lookups.
	sampled atomic.Int64
	lookups atomic.Int64
	// paused is set while a sample waits for its verdict. The pool sends nothing while it is set.
	paused atomic.Bool
	// judgeMu guards the verdict state below.
	judgeMu   sync.Mutex
	nextJudge int64 // the sampled count of the next verdict; empty means lookAheadJudgeAfter
	pausedAt  int64 // lookups when the pause started

	// winCtx is the context the windows run under. A pause cancels it, so no window goes on past the sample.
	ctxMu     sync.Mutex
	winCtx    context.Context
	winCancel context.CancelFunc

	Requests AtomicCounter // look-ahead round trips issued
	Entries  AtomicCounter // entries they brought back
	Dropped  AtomicCounter // seeds refused: the queue was full, or the budget was spent
	Failed   AtomicCounter // requests that brought back an error, not a window
	Empty    AtomicCounter // requests the server answered with an empty window

	lastFailure atomic.Pointer[string] // the most recent failure, for the report
}

// fail records a request that brought back no window, and why.
func (la *lookAhead) fail(why string) {
	la.Failed.Increment()
	la.lastFailure.Store(&why)
}

// lookAheadBudget is what a single pool may hold in memory at the same time.
func lookAheadBudget() int64 {
	return int64(envInt("GO_TOOLCHAIN_CACHE_LOOKAHEAD_BYTES", 32<<20))
}

// overBudget reports whether the workers are already carrying everything this
// pool may hold.
func (la *lookAhead) overBudget() bool {
	return la.budget > 0 && la.held.Load() >= la.budget
}

// charge records what this worker holds and returns the release. The gate reads
// the same counter, so a window already in memory is what stops the next worker
// from fetching another.
func (la *lookAhead) charge(entries []BatchEntry) func() {
	var n int64
	for i := range entries {
		n += int64(len(entries[i].Data))
	}
	la.held.Add(n)
	return func() { la.held.Add(-n) }
}

// lookAheadChunk is how many entries a worker hands the populator at the same time.
const lookAheadChunk = 16

// minChunkBudget keeps a worker's hand-off from degenerating to a single entry at a time.
const minChunkBudget = 256 << 10

// chunkBudget is the bytes a single worker may accumulate before handing them
// over.
func chunkBudget(budget int64, workers int) int64 {
	if budget <= 0 || workers <= 0 {
		return 0 // unbounded, as an unset budget asks.
	}
	per := budget / int64(workers)
	if per < minChunkBudget {
		per = minChunkBudget
	}
	return per
}

// HeaderPrefetch is "off" on a batch answer from a server that never sends a look-ahead window.
const HeaderPrefetch = "X-Cache-Prefetch"

// defaultLookAheadWorkers is the pool an unset GO_TOOLCHAIN_CACHE_LOOKAHEAD gets.
const defaultLookAheadWorkers = 4

// maxLookAheadWorkers caps the pool however large GO_TOOLCHAIN_CACHE_LOOKAHEAD is.
const maxLookAheadWorkers = 64

// lookAheadDefaults returns the worker count and queue depth. A positive count
// is capped at maxLookAheadWorkers.
func lookAheadDefaults() (workers, depth int) {
	workers = envInt("GO_TOOLCHAIN_CACHE_LOOKAHEAD", defaultLookAheadWorkers)
	if workers > maxLookAheadWorkers {
		workers = maxLookAheadWorkers
	}
	return workers, workers * 8
}

func newLookAhead(b *WebBackend) *lookAhead {
	workers, depth := lookAheadDefaults()
	if workers <= 0 {
		return nil
	}
	budget := lookAheadBudget()
	la := &lookAhead{
		b:       b,
		seeds:   make(chan []string, depth),
		stop:    make(chan struct{}),
		seeded:  set.New[string](),
		brought: set.New[actionHash](),
		budget:  budget,
		chunk:   chunkBudget(budget, workers),
	}
	la.wg.Add(workers)
	for range workers {
		go la.worker()
	}
	return la
}

// Seed offers the keys a batch fetched as anchors to look around. It never
// blocks: a full queue means the pool is already saturated with work of the
// same kind. A build goroutine must never wait on speculation.
func (la *lookAhead) Seed(keys []string) {
	if la == nil || len(keys) == 0 || la.off() {
		return
	}
	fresh := keys[:0:0]
	la.mu.Lock()
	for _, k := range keys {
		if la.seeded.Contains(k) {
			continue
		}
		la.seeded.Add(k)
		fresh = append(fresh, k)
	}
	la.mu.Unlock()
	if len(fresh) == 0 {
		return
	}
	select {
	case la.seeds <- fresh:
	default:
		la.Dropped.Add(uint32(len(fresh)))
	}
}

// cover stops the pool: a build manifest names what this build needs.
func (la *lookAhead) cover() {
	if la != nil {
		la.covered.Store(true)
	}
}

// uncover lets the pool send again: a guess that covered it stopped.
func (la *lookAhead) uncover() {
	if la != nil {
		la.covered.Store(false)
	}
}

// hold keeps the workers from sending until release.
func (la *lookAhead) hold() {
	if la == nil {
		return
	}
	la.gateMu.Lock()
	if la.gate == nil {
		la.gate = make(chan struct{})
	}
	la.gateMu.Unlock()
}

// release lets the held workers go on.
func (la *lookAhead) release() {
	if la == nil {
		return
	}
	la.gateMu.Lock()
	if la.gate != nil && !la.gateOpen {
		la.gateOpen = true
		close(la.gate)
	}
	la.gateMu.Unlock()
}

// wait blocks while the pool is held. It reports false when the pool stops first.
func (la *lookAhead) wait() bool {
	la.gateMu.Lock()
	gate := la.gate
	la.gateMu.Unlock()
	if gate == nil {
		return true
	}
	select {
	case <-gate:
		return true
	case <-la.stop:
		return false
	}
}

// off reports whether the pool sends nothing more.
func (la *lookAhead) off() bool {
	return la.serverOff.Load() || la.covered.Load() || la.wasteful.Load() || la.paused.Load()
}

// lookAheadJudgeAfter is how many entries the pool fetches before the first verdict on its use.
var lookAheadJudgeAfter int64 = 256

// lookAheadMinUse is the share of the fetched entries, in percent, the build must use for the pool to go on.
const lookAheadMinUse = 20

// windowCtx is the context a window runs under. A pause cancels it.
func (la *lookAhead) windowCtx() context.Context {
	la.ctxMu.Lock()
	defer la.ctxMu.Unlock()
	if la.winCtx == nil {
		parent := context.Background()
		if la.b != nil {
			parent = la.b.requestCtx()
		}
		la.winCtx, la.winCancel = context.WithCancel(parent)
	}
	return la.winCtx
}

// cancelWindows stops every window in flight. The next window gets a new context.
func (la *lookAhead) cancelWindows() {
	la.ctxMu.Lock()
	cancel := la.winCancel
	la.winCtx, la.winCancel = nil, nil
	la.ctxMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// noteSampled counts entries a window brought. At the sample size, the pool
// pauses: no window goes on until the build shows whether it uses them.
func (la *lookAhead) noteSampled(n int64) {
	sampled := la.sampled.Add(n)
	la.judgeMu.Lock()
	due := la.nextJudge
	if due == 0 {
		due = lookAheadJudgeAfter
	}
	if sampled < due || la.paused.Load() || la.wasteful.Load() {
		la.judgeMu.Unlock()
		return
	}
	la.paused.Store(true)
	la.pausedAt = la.lookups.Load()
	la.judgeMu.Unlock()
	la.cancelWindows()
	la.judge()
}

// noteLookup counts a lookup the build made.
func (la *lookAhead) noteLookup() {
	if la == nil {
		return
	}
	la.lookups.Add(1)
	if la.paused.Load() {
		la.judge()
	}
}

// noteBrought records the keys a window brought.
func (la *lookAhead) noteBrought(hashes []actionHash) {
	la.mu.Lock()
	for _, h := range hashes {
		la.brought.Add(h)
		if la.b != nil && la.b.manifest != nil {
			la.b.manifest.trace.add("ahead", hex.EncodeToString(h[:]))
		}
	}
	la.mu.Unlock()
}

// noteUsed counts a lookup the local tier answered, and whether the pool brought it.
func (la *lookAhead) noteUsed(h actionHash) {
	if la == nil {
		return
	}
	la.mu.Lock()
	found := la.brought.Contains(h)
	if found {
		la.brought.Remove(h)
	}
	la.mu.Unlock()
	if found {
		la.Used.Increment()
	}
	la.noteLookup()
}

// judge decides a paused sample. The pool goes on when the build uses at least
// lookAheadMinUse percent of what it fetched. It stops for good when the build
// makes half as many lookups as the pool fetched and still uses less. Each
// unused entry costs a download and a disk write the build waits behind.
func (la *lookAhead) judge() {
	la.judgeMu.Lock()
	defer la.judgeMu.Unlock()
	if !la.paused.Load() || la.wasteful.Load() {
		return
	}
	fetched := la.sampled.Load()
	used := int64(la.Used.Load())
	if used*100 >= fetched*lookAheadMinUse {
		la.nextJudge = 2 * fetched
		la.paused.Store(false)
		return
	}
	if la.lookups.Load()-la.pausedAt < (fetched+1)/2 {
		return
	}
	la.wasteful.Store(true)
	logging.Infof("cacheprog: look-ahead: the build used %d of %d entries it fetched; this process stops asking", used, fetched)
}

func (la *lookAhead) Close() {
	if la == nil {
		return
	}
	close(la.stop)
	la.wg.Wait()
	la.report()
}

// report states what the pool did. Without it a build cannot tell a
// look-ahead that covered it from a single that fetched nothing. Both print
// the same batch GET line, because the critical path's own requests carry no
// prefetch.
//
// Entries is what the pool handed the populator, not what the build went on
// to use. Dropped counts seeds refused for a full queue. A large Requests
// against a small Entries means the mtime window around this build's keys
// holds nobody else's work worth having.
func (la *lookAhead) report() {
	reqs := la.Requests.Load()
	if reqs == 0 {
		return
	}
	logging.Infof("cacheprog: look-ahead: %d requests -> %d entries, %d used, %d seeds dropped, %d empty windows",
		reqs, la.Entries.Load(), la.Used.Load(), la.Dropped.Load(), la.Empty.Load())
	if failed := la.Failed.Load(); failed > 0 {
		why := ""
		if last := la.lastFailure.Load(); last != nil {
			why = *last
		}
		logging.Warnf("cacheprog: look-ahead: %d of %d requests failed; the last: %s", failed, reqs, why)
	}
}

func (la *lookAhead) worker() {
	defer la.wg.Done()
	for {
		select {
		case seed := <-la.seeds:
			la.expand(seed)
		case <-la.stop:
			return
		}
	}
}

// expand asks the server for the objects stored around seed and hands them to
// the populator. PrefetchOnly keeps the seed's own bodies off the wire: this
// request wants the window. The caller that asked for the seed already has
// those bytes.
func (la *lookAhead) expand(seed []string) {
	b := la.b
	if b.OnBatchEntries == nil || !la.wait() || la.off() {
		return
	}
	// Over budget, this seed goes on the floor.
	if la.overBudget() {
		la.Dropped.Add(uint32(len(seed)))
		return
	}
	// The request states what this build already has, which is the whole of what the server needs to skip.
	body, err := json.Marshal(batchGetRequest{Keys: seed, Prefetch: true, PrefetchOnly: true, Have: b.heldFilter()})
	if err != nil {
		return
	}
	// A pause cancels the pool context. The window's own cancel ends it when it streams into a pause.
	ctx, stopWindow := context.WithCancel(la.windowCtx())
	defer stopWindow()
	req, err := http.NewRequestWithContext(ctx, "POST", b.endpoint+"/"+b.bucket+"/_batch/get", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	b.signRequest(req)
	// Nobody is blocked on this request.
	req.Header.Set(HeaderKind, KindLookAhead)

	la.Requests.Increment()
	start := time.Now()
	b.Pool.Acquire()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		// A window the pool cancelled did not fail.
		if ctx.Err() == nil {
			la.fail(err.Error())
		}
		return
	}
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		la.fail(fmt.Sprintf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(msg)))
		closeBody(resp.Body)
		b.Pool.Release()
		return
	}
	if resp.Header.Get(HeaderPrefetch) == "off" {
		if !la.serverOff.Swap(true) {
			logging.Warnf("cacheprog: look-ahead: the server does not support look-ahead; GO_TOOLCHAIN_CACHE_LOOKAHEAD is ignored")
		}
		closeBody(resp.Body)
		b.Pool.Release()
		return
	}
	// The window is handed over in chunks as it arrives, so a worker holds a
	// chunk rather than the whole response. The gate above only decides whether
	// to START this fetch, so what bounds the pool is the byte cap on a chunk
	// times the workers. Plus the re-check after each hand-off.
	var (
		chunk     []BatchEntry
		brought   []actionHash
		chunkSize int64
		count     int
		full      bool
		flush     = func() {}
		ingest    = func(e BatchEntry) {
			// The pool filled up while this window was streaming.
			if !full && la.off() {
				// A pause or a stop came while this window streamed. The rest of it is not read.
				full = true
				stopWindow()
			}
			if full {
				la.Dropped.Increment()
				return
			}
			count++
			la.noteSampled(1)
			// Stated as held, so the next window leaves it out.
			if actionID, ok := b.ActionIDFromKey(e.Key); ok {
				if h, ok := parseActionHash(actionID); ok {
					b.noteHeld(h)
					brought = append(brought, h)
				}
			}
			chunk = append(chunk, e)
			chunkSize += int64(len(e.Data))
			if len(chunk) >= lookAheadChunk || (la.chunk > 0 && chunkSize >= la.chunk) {
				flush()
				full = la.overBudget()
			}
		}
	)
	flush = func() {
		if len(chunk) == 0 {
			return
		}
		release := la.charge(chunk)
		// Recorded before the hand-off, so a lookup that lands at once counts as used.
		la.noteBrought(brought)
		// Each worker ingests its own answer.
		b.OnBatchEntries(chunk)
		release()
		chunk = nil
		brought = nil
		chunkSize = 0
	}

	err = streamBatchResponse(resp.Body, ingest)
	closeBody(resp.Body)
	b.Pool.Release()
	// The entries already read are whole. They go to the populator even when the window stopped early.
	flush()
	if count > 0 {
		la.Entries.Add(uint32(count))
		b.batchTiming.recordLookAhead(count, time.Since(start))
	}
	if err != nil {
		if ctx.Err() == nil {
			la.fail("read the window: " + err.Error())
		}
		return
	}
	if count == 0 {
		la.Empty.Increment()
	}
}
