package cacheclient

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
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
// It must not ride the request the build is blocked on. A prefetch answer is
// up to prefetchBudget bytes of bodies nobody is waiting for, in front of the
// four the compiler is stalled on, on one connection. That is how a cache ends
// up slower than no cache at all.
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

	Requests AtomicCounter // look-ahead round trips issued
	Entries  AtomicCounter // entries they brought back
	Dropped  AtomicCounter // seeds refused because the queue was full
}

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
	entries, err := parseBatchResponse(resp.Body)
	resp.Body.Close()
	b.Pool.Release()
	if err != nil || len(entries) == 0 {
		return
	}
	la.Entries.Add(uint32(len(entries)))
	b.batchTiming.recordLookAhead(len(entries), time.Since(start))
	// Each worker ingests its own answer, so verification and the local write
	// run at the pool's width rather than one batch at a time.
	b.OnBatchEntries(entries)
}
