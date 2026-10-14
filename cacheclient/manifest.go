package cacheclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// The build manifest is this client's own look-ahead. It does not need the server's prefetch window.

// manifestMaxKeys bounds what a manifest names, and so what a build fetches ahead.
const manifestMaxKeys = 20000

// manifestWaitCap is how long a lookup waits for a manifest fetch that has its object on the wire.
var manifestWaitCap = time.Second

// manifestFetchWidth is how many manifest batches go out at the same time.
const manifestFetchWidth = 8

type buildManifest struct {
	id      atomic.Pointer[string] // the manifest's own action ID, once the module is known
	started atomic.Bool

	mu      sync.Mutex
	used    set.Set[string]          // action IDs this process looked up
	order   []string                 // used, in the order of the first lookup
	pending map[string]chan struct{} // action IDs a fetch has on the wire, closed when they land
	cancel  context.CancelFunc       // stops the fetch; Close calls it, because a build that ended needs nothing more

	newest     int
	newestKeys int // the keys that generation names

	Waited AtomicCounter // lookups that waited for a fetch already on the wire
	GaveUp AtomicCounter // lookups that stopped waiting after manifestWaitCap and asked on their own

	Fetched AtomicCounter // entries the manifest brought into the local tier
	Named   AtomicCounter // action IDs the manifest named
}

func newBuildManifest() *buildManifest {
	return &buildManifest{used: set.New[string](), pending: map[string]chan struct{}{}, newest: -1}
}

// AwaitPrefetch waits while the manifest fetch has actionID on the wire. It
// reports whether it waited, so a caller that did reads its local tier again
// rather than asking for the same object a second time.
func (b *WebBackend) AwaitPrefetch(actionID string) bool {
	m := b.manifest
	if m == nil {
		return false
	}
	m.mu.Lock()
	done, ok := m.pending[actionID]
	m.mu.Unlock()
	if !ok {
		return false
	}
	timer := time.NewTimer(manifestWaitCap)
	defer timer.Stop()
	select {
	case <-done:
		m.Waited.Increment()
		return true
	case <-timer.C:
		// The fetch is slow. A lookup of its own is small, and the slow-server rule covers it.
		m.GaveUp.Increment()
		return false
	}
}

// markPending records that a fetch has these action IDs on the wire, and
// returns the call that clears them.
func (m *buildManifest) markPending(ids []string) func() {
	done := make(chan struct{})
	m.mu.Lock()
	for _, id := range ids {
		m.pending[id] = done
	}
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		for _, id := range ids {
			if m.pending[id] == done {
				delete(m.pending, id)
			}
		}
		m.mu.Unlock()
		close(done)
	}
}

// manifestFormat names what a manifest holds: lookups and stores, in first-use order.
const manifestFormat = "3"

// manifestID is the action ID the manifest for this module is stored under.
func manifestID(module, version, target string) string {
	sum := sha256.Sum256([]byte("go-build-manifest\x00" + manifestFormat + "\x00" + module + "\x00" + version + "\x00" + target))
	return hex.EncodeToString(sum[:])
}

// manifestGenerations caps the walk. A write-once server keeps each generation, so a short one gets a successor.
const manifestGenerations = 16

// manifestGenID is the action ID of generation gen of the manifest stored under base. One generation is base.
func manifestGenID(base string, gen int) string {
	if gen == 0 {
		return base
	}
	sum := sha256.Sum256([]byte("go-build-manifest-gen\x00" + base + "\x00" + strconv.Itoa(gen)))
	return hex.EncodeToString(sum[:])
}

// manifestKeys returns the action IDs a manifest body names.
func manifestKeys(data []byte, self string) []string {
	var ids []string
	for _, line := range strings.Split(string(data), "\n") {
		if _, ok := parseActionHash(line); ok && line != self {
			ids = append(ids, line)
		}
	}
	return ids
}

// note records an action ID this build looked up, from any tier, or stored. A cold build stores
// about half its objects without a lookup first, and the next build looks every one of them up.
func (m *buildManifest) note(actionID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.used.Len() < manifestMaxKeys && !m.used.Contains(actionID) {
		m.used.Add(actionID)
		m.order = append(m.order, actionID)
	}
	m.mu.Unlock()
}

// startManifest learns the module and starts the fetch of its manifest. It
// runs one time per process.
func (b *WebBackend) startManifest(module string) {
	m := b.manifest
	if m == nil || module == "" || b.OnBatchEntries == nil || !m.started.CompareAndSwap(false, true) {
		return
	}
	id := manifestID(module, b.version, b.target)
	m.id.Store(&id)
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	b.manifestWG.Add(1)
	go func() {
		defer b.manifestWG.Done()
		defer cancel()
		b.fetchManifest(ctx, id)
	}()
}

// manifestCloseWait is how long Close lets a manifest fetch finish. The fetch
// fills the local tier for the next command of the build, so Close does not
// cancel it at once.
var manifestCloseWait = 30 * time.Second

// awaitManifest waits for the manifest fetch. After manifestCloseWait it
// cancels the fetch, so a stalled request cannot keep the process alive.
func (b *WebBackend) awaitManifest() {
	done := make(chan struct{})
	go func() {
		b.manifestWG.Wait()
		close(done)
	}()
	timer := time.NewTimer(manifestCloseWait)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	}
	logging.Warnf("cacheprog: manifest: the fetch ran past %v at exit, so it is cancelled", manifestCloseWait)
	b.stopManifest()
	<-done
}

// stopManifest cancels a manifest fetch that is still running.
func (b *WebBackend) stopManifest() {
	m := b.manifest
	if m == nil {
		return
	}
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// readManifest reads the manifest over its own request. On the build's batch
// path a slow answer would hold up the lookups that share its batch.
func (b *WebBackend) readManifest(ctx context.Context, id string) ([]byte, bool) {
	h, ok := parseActionHash(id)
	if !ok {
		return nil, false
	}
	b.ensureIndex()
	b.keysMu.RLock()
	authoritative := b.indexAuthoritative
	b.keysMu.RUnlock()
	if authoritative && !b.keyKnown(h) {
		return nil, false
	}
	var data []byte
	found := false
	_, err := b.postNamed(ctx, []string{b.key(id)}, func(e BatchEntry) {
		if body, ok := b.verify("manifest", id, e.OutputID, e.Data, e.RawSize); ok {
			data, found = body, true
		}
	})
	if err != nil {
		logging.Warnf("cacheprog: manifest: read: %v", err)
	}
	return data, found
}

// walkManifest reads each generation of the manifest in turn, newest last.
// It records that generation and its key count for storeManifest.
func (b *WebBackend) walkManifest(ctx context.Context, base string) (int, []string) {
	newest := -1
	var named []string
	for gen := range manifestGenerations {
		data, found := b.readManifest(ctx, manifestGenID(base, gen))
		if !found {
			break
		}
		newest, named = gen, manifestKeys(data, base)
	}
	m := b.manifest
	m.mu.Lock()
	m.newest, m.newestKeys = newest, len(named)
	m.mu.Unlock()
	return newest, named
}

// fetchManifest reads the newest manifest generation and pulls what it names.
func (b *WebBackend) fetchManifest(ctx context.Context, base string) {
	newest, named := b.walkManifest(ctx, base)
	if newest < 0 {
		return
	}
	b.keysMu.RLock()
	authoritative := b.indexAuthoritative
	b.keysMu.RUnlock()
	var ids []string
	for _, line := range named {
		h, _ := parseActionHash(line)
		b.manifest.Named.Increment()
		// Only an authoritative index proves a key absent.
		if (authoritative && !b.keyKnown(h)) || b.isHeld(h) {
			continue
		}
		ids = append(ids, line)
	}
	if b.manifest.Named.Load() > 0 {
		b.lookAhead.cover()
	}
	if len(ids) == 0 {
		return
	}
	type chunk struct {
		keys  []string
		clear func()
	}
	// Every chunk is marked before the first goes out, so a lookup also waits on a chunk still in the queue.
	var chunks []chunk
	for start := 0; start < len(ids); start += batchMaxKeys {
		part := ids[start:min(start+batchMaxKeys, len(ids))]
		keys := make([]string, len(part))
		for idx, actionID := range part {
			keys[idx] = b.key(actionID)
		}
		chunks = append(chunks, chunk{keys: keys, clear: b.manifest.markPending(part)})
	}
	work := make(chan chunk)
	var wg sync.WaitGroup
	for range manifestFetchWidth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next := range work {
				if ctx.Err() == nil {
					b.fetchNamed(ctx, next.keys)
				}
				next.clear()
			}
		}()
	}
	for _, next := range chunks {
		work <- next
	}
	close(work)
	wg.Wait()
}

// isHeld reports whether this client already holds the action.
func (b *WebBackend) isHeld(h actionHash) bool {
	if b.held == nil {
		return false
	}
	b.heldMu.RLock()
	defer b.heldMu.RUnlock()
	return b.held.Contains(h)
}

// postNamed asks for keys by name on a request of its own and hands each entry
// to each. Nobody waits on it, so it rides the look-ahead kind.
func (b *WebBackend) postNamed(ctx context.Context, keys []string, each func(BatchEntry)) (int, error) {
	body, err := json.Marshal(batchGetRequest{Keys: keys})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", b.endpoint+"/"+b.bucket+"/_batch/get", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	b.signRequest(req)
	req.Header.Set(HeaderKind, KindLookAhead)

	start := time.Now()
	b.Pool.Acquire()
	defer b.Pool.Release()
	resp, err := b.doRetryGET(req)
	if err != nil {
		return 0, err
	}
	defer closeBody(resp.Body)
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	count := 0
	err = streamBatchResponse(resp.Body, func(e BatchEntry) {
		count++
		each(e)
	})
	b.batchTiming.recordLookAhead(count, time.Since(start))
	return count, err
}

// fetchNamed asks for keys by name and hands the entries to the local tier.
func (b *WebBackend) fetchNamed(ctx context.Context, keys []string) {
	var chunk []BatchEntry
	flush := func() {
		if len(chunk) > 0 {
			b.OnBatchEntries(chunk)
			chunk = nil
		}
	}
	count, err := b.postNamed(ctx, keys, func(e BatchEntry) {
		if actionID, ok := b.ActionIDFromKey(e.Key); ok {
			if h, ok := parseActionHash(actionID); ok {
				b.noteHeld(h)
			}
		}
		chunk = append(chunk, e)
		if len(chunk) >= lookAheadChunk {
			flush()
		}
	})
	flush()
	if err != nil {
		logging.Warnf("cacheprog: manifest: fetch: %v", err)
	}
	b.manifest.Fetched.Add(uint32(count))
}

// storeManifest writes what this build used, for the next build. It runs
// before the PUT coalescer closes.
func (b *WebBackend) storeManifest() {
	m := b.manifest
	if m == nil {
		return
	}
	id := m.id.Load()
	if id == nil {
		return
	}
	m.mu.Lock()
	// Lookup order is the order the next build needs the objects in, so the fetch brings the first ones first.
	ids := make([]string, 0, len(m.order))
	for _, actionID := range m.order {
		if actionID != *id {
			ids = append(ids, actionID)
		}
	}
	newest, newestKeys := m.newest, m.newestKeys
	m.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	gen := 0
	if newest >= 0 {
		if newestKeys*10 >= len(ids)*9 {
			return
		}
		gen = newest + 1
	}
	if gen >= manifestGenerations {
		logging.Warnf("cacheprog: manifest: generation %d names %d keys of %d, and the walk reads no later generation", newest, newestKeys, len(ids))
		return
	}
	target := manifestGenID(*id, gen)
	data := []byte(strings.Join(ids, "\n"))
	// Put skips a key the index already holds. A write-once server keeps the first manifest under an ID.
	b.ForgetStale(target)
	if err := b.Put(target, StoredDigest(data), data); err != nil {
		logging.Warnf("cacheprog: manifest: store: %v", err)
	}
}

// reportManifest states what the manifest did.
func (b *WebBackend) reportManifest() {
	m := b.manifest
	if m == nil || m.Named.Load() == 0 {
		return
	}
	logging.Infof("cacheprog: manifest: named %d keys, fetched %d ahead, %d lookups waited for them, %d gave up waiting",
		m.Named.Load(), m.Fetched.Load(), m.Waited.Load(), m.GaveUp.Load())
}
