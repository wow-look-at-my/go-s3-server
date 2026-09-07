package cacheclient

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// The look-ahead pool. A build asks for one key at a time, in dependency
// order, and it cannot ask for the next one until this one answers: a
// package's action ID is computed from its dependencies' output IDs. So the
// number of keys the build has outstanding is its own -p, and no amount of
// batching on the client changes that. What DOES change it is asking for keys
// nobody has asked for yet.
//
// The server can name those keys. It stores an object's modification time, and
// the objects one build writes land next to each other in time, so the window
// around a key this build just wanted is mostly keys this build wants next.
// That is what /_batch/get's prefetch flag returns.
//
// Two properties decide where that work runs.
//
// It must not ride the request the build is blocked on. A prefetch answer is a
// window of bodies nobody is waiting for, in front of the four the compiler is
// stalled on, on one connection. That is how a cache ends up slower than no
// cache at all.
//
// And it must not be bounded by the build's parallelism. The build's -p is a
// count of compilers, chosen for the machine's cores. Fetching is not
// compiling: it is a socket read, and the right number of those in flight has
// nothing to do with core count.
//
// So look-ahead runs here, on its own goroutines, over its own requests, and
// the critical path asks for exactly what it needs.
type lookAhead struct {
	b     *WebBackend
	seeds chan []string
	stop  chan struct{}
	wg    sync.WaitGroup

	// seeded guards a key against being seeded twice. A seed is a request the
	// build never made; making it more than once is pure cost.
	mu     sync.Mutex
	seeded set.Set[string]

	// held is what the workers are carrying right now, in bytes, and budget is
	// what they may carry between them. A worker reads a whole batch response
	// into memory before the populator sees any of it, so without this the
	// pool's peak is workers times whatever the server chose to send.
	//
	// The server caps a window at maxPrefetchEntries, which counts ENTRIES. A
	// count is not a size. It also has no idea how many of these pools exist:
	// `dist test` runs many go processes at once and each one builds its own,
	// so the machine's total is this budget times the number of builds. That is
	// how a windows runner reached "Out of memory" with an empty log.
	held   atomic.Int64
	budget int64

	Requests AtomicCounter // look-ahead round trips issued
	Entries  AtomicCounter // entries they brought back
	Dropped  AtomicCounter // seeds refused: the queue was full, or the budget was spent
}

// lookAheadBudget is what one pool may hold in memory at once. Look-ahead is
// speculation, so the answer to a full budget is to drop the seed rather than
// to wait: the build never asked for these bytes.
func lookAheadBudget() int64 {
	return int64(envInt("GO_TOOLCHAIN_CACHE_LOOKAHEAD_BYTES", 32<<20))
}

// overBudget reports whether the workers are already carrying everything this
// pool may hold. A budget of zero or less holds nothing back, which is what a
// caller asking for no bound gets.
func (la *lookAhead) overBudget() bool {
	return la.budget > 0 && la.held.Load() >= la.budget
}

// charge records what this worker holds and returns the release. The gate reads
// the same counter, so a window already in memory is what stops the next worker
// from fetching another one.
func (la *lookAhead) charge(entries []BatchEntry) func() {
	var n int64
	for i := range entries {
		n += int64(len(entries[i].Data))
	}
	la.held.Add(n)
	return func() { la.held.Add(-n) }
}

// lookAheadChunk is how many entries a worker hands the populator at once. It
// trades a few more calls for a resident set that does not grow with whatever
// the server chose to send.
const lookAheadChunk = 16

// lookAheadDefaults returns the worker count and queue depth. Workers scale
// with the machine because each one is a socket read, not a core's worth of
// work, and the floor matters more than the ceiling on a small runner.
func lookAheadDefaults() (workers, depth int) {
	workers = envInt("GO_TOOLCHAIN_CACHE_LOOKAHEAD", 4*runtime.NumCPU())
	if workers < 8 {
		workers = 8
	}
	if workers > 64 {
		workers = 64
	}
	return workers, workers * 8
}

func newLookAhead(b *WebBackend) *lookAhead {
	workers, depth := lookAheadDefaults()
	if workers <= 0 {
		return nil
	}
	la := &lookAhead{
		b:      b,
		seeds:  make(chan []string, depth),
		stop:   make(chan struct{}),
		seeded: set.New[string](),
		budget: lookAheadBudget(),
	}
	la.wg.Add(workers)
	for range workers {
		go la.worker()
	}
	return la
}

// Seed offers the keys a batch just fetched as anchors to look around. It
// never blocks: a full queue means the pool is already saturated with work of
// the same kind, and a build goroutine must never wait on speculation.
func (la *lookAhead) Seed(keys []string) {
	if la == nil || len(keys) == 0 {
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

func (la *lookAhead) Close() {
	if la == nil {
		return
	}
	close(la.stop)
	la.wg.Wait()
	la.report()
}

// report states what the pool actually did. Without it a build cannot tell a
// look-ahead that covered it from one that fetched nothing: both print the same
// batch GET line, because the critical path's own requests carry no prefetch.
//
// Entries is what the pool handed the populator, not what the build went on to
// use. Dropped counts seeds refused for a full queue. A large Requests against
// a small Entries means the mtime window around this build's keys holds nobody
// else's work worth having.
func (la *lookAhead) report() {
	reqs := la.Requests.Load()
	if reqs == 0 {
		return
	}
	logging.Infof("cacheprog: look-ahead: %d requests -> %d entries, %d seeds dropped",
		reqs, la.Entries.Load(), la.Dropped.Load())
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
// request wants the window, and the caller that asked for the seed already has
// those bytes.
func (la *lookAhead) expand(seed []string) {
	b := la.b
	if b.OnBatchEntries == nil {
		return
	}
	// Over budget, this seed goes on the floor. Waiting for room would hold a
	// worker on bodies nobody asked for, and the window is still there to be
	// asked for again from the next key that hits.
	if la.overBudget() {
		la.Dropped.Add(uint32(len(seed)))
		return
	}
	body, err := json.Marshal(batchGetRequest{Keys: seed, Prefetch: true, PrefetchOnly: true})
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", b.endpoint+"/"+b.bucket+"/_batch/get", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	b.signRequest(req)
	// Nobody is blocked on this request. A server that cannot see the
	// difference reads a burst of look-ahead as a build in trouble.
	req.Header.Set(HeaderKind, KindLookAhead)

	la.Requests.Increment()
	start := time.Now()
	b.Pool.Acquire()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		return
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		b.Pool.Release()
		return
	}
	// The window is handed over in chunks as it arrives, so a worker holds a
	// chunk rather than the whole response. The budget above bounds the pool;
	// this bounds each worker inside it.
	var (
		chunk  []BatchEntry
		count  int
		flush  = func() {}
		ingest = func(e BatchEntry) {
			count++
			chunk = append(chunk, e)
			if len(chunk) >= lookAheadChunk {
				flush()
			}
		}
	)
	flush = func() {
		if len(chunk) == 0 {
			return
		}
		release := la.charge(chunk)
		// Each worker ingests its own answer, so verification and the local write
		// run at the pool's width rather than one batch at a time.
		b.OnBatchEntries(chunk)
		release()
		chunk = nil
	}

	err = streamBatchResponse(resp.Body, ingest)
	resp.Body.Close()
	b.Pool.Release()
	if err != nil {
		return
	}
	flush()
	if count == 0 {
		return
	}

	la.Entries.Add(uint32(count))
	b.batchTiming.recordLookAhead(count, time.Since(start))
}
