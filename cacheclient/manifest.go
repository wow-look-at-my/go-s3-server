package cacheclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// The build manifest is this client's own look-ahead. It does not need the server's prefetch window.

// manifestMaxKeys bounds what a manifest names, and so what a build fetches ahead.
const manifestMaxKeys = 20000

// manifestFetchWidth is how many manifest batches go out at the same time.
const manifestFetchWidth = 8

type buildManifest struct {
	id      atomic.Pointer[string] // the manifest's own action ID, once the module is known
	started atomic.Bool

	mu          sync.Mutex
	used        set.Set[string] // action IDs this process served or stored
	missed      set.Set[string] // action IDs this process asked for and did not get
	knownMissed set.Set[string] // action IDs the last build asked for and did not get

	AbsentSkips AtomicCounter // lookups answered as misses from the manifest while the index loaded
	pending map[string]chan struct{} // action IDs a fetch has on the wire, closed when they land
	ready   chan struct{}            // closed once the manifest is read and its objects marked

	Waited AtomicCounter // lookups that waited for a fetch already on the wire

	Fetched AtomicCounter // entries the manifest brought into the local tier
	Named   AtomicCounter // action IDs the manifest named
}

func newBuildManifest() *buildManifest {
	return &buildManifest{
		used: set.New[string](), missed: set.New[string](), knownMissed: set.New[string](),
		pending: map[string]chan struct{}{}, ready: make(chan struct{}),
	}
}

// noteMissed records an action ID this build asked for and did not get.
func (m *buildManifest) noteMissed(actionID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.missed.Len() < manifestMaxKeys {
		m.missed.Add(actionID)
	}
	m.mu.Unlock()
}

// missedLastTime reports whether the last build asked for actionID and did
// not get it. It waits briefly for the manifest to be read.
func (m *buildManifest) missedLastTime(actionID string) bool {
	if m == nil || !m.started.Load() {
		return false
	}
	select {
	case <-m.ready:
	case <-time.After(manifestReadWait):
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.knownMissed.Contains(actionID)
}

// manifestReadWait bounds how long a lookup waits for the manifest itself.
const manifestReadWait = 2 * time.Second

// AwaitPrefetch waits while the manifest fetch has actionID on the wire. It
// reports whether it waited, so a caller that did reads its local tier again
// rather than asking for the same object a second time.
func (b *WebBackend) AwaitPrefetch(actionID string) bool {
	m := b.manifest
	if m == nil || !m.started.Load() {
		return false
	}
	// Until the manifest is read, nothing is marked, so a lookup that ran
	// now would go on to wait for the whole index instead.
	select {
	case <-m.ready:
	case <-time.After(manifestReadWait):
	}
	m.mu.Lock()
	done, ok := m.pending[actionID]
	m.mu.Unlock()
	if !ok {
		return false
	}
	<-done
	m.Waited.Increment()
	return true
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

// manifestID is the action ID the manifest for this module is stored under.
func manifestID(module, version, target string) string {
	sum := sha256.Sum256([]byte("go-build-manifest\x00" + module + "\x00" + version + "\x00" + target))
	return hex.EncodeToString(sum[:])
}

// note records an action ID this build used.
func (m *buildManifest) note(actionID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.used.Len() < manifestMaxKeys {
		m.used.Add(actionID)
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
	b.manifestWG.Add(1)
	go func() {
		defer b.manifestWG.Done()
		b.fetchManifest(id)
	}()
}

// fetchManifest reads the manifest and pulls what it names.
func (b *WebBackend) fetchManifest(id string) {
	ready := sync.OnceFunc(func() { close(b.manifest.ready) })
	defer ready()
	h, ok := parseActionHash(id)
	if !ok {
		return
	}
	// A direct batch read, not Get: Get waits for the index.
	r := b.getBatch(id, b.key(id), h)
	if r.miss {
		return
	}
	data := r.data
	b.keysMu.RLock()
	authoritative := b.indexAuthoritative
	b.keysMu.RUnlock()
	var ids []string
	for _, line := range strings.Split(string(data), "\n") {
		if missed, ok := strings.CutPrefix(line, "-"); ok {
			if _, ok := parseActionHash(missed); ok {
				b.manifest.mu.Lock()
				b.manifest.knownMissed.Add(missed)
				b.manifest.mu.Unlock()
			}
			continue
		}
		h, ok := parseActionHash(line)
		if !ok || line == id {
			continue
		}
		b.manifest.Named.Increment()
		// Only an authoritative index proves a key absent.
		if (authoritative && !b.keyKnown(h)) || b.isHeld(h) {
			continue
		}
		ids = append(ids, line)
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
	ready()
	work := make(chan chunk)
	var wg sync.WaitGroup
	for range manifestFetchWidth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next := range work {
				b.fetchNamed(next.keys)
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

// fetchNamed asks for keys by name and hands the entries to the local tier.
// Nobody waits on it, so it rides the look-ahead kind.
func (b *WebBackend) fetchNamed(keys []string) {
	body, err := json.Marshal(batchGetRequest{Keys: keys})
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", b.endpoint+"/"+b.bucket+"/_batch/get", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	b.signRequest(req)
	req.Header.Set(HeaderKind, KindLookAhead)

	start := time.Now()
	b.Pool.Acquire()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		logging.Warnf("cacheprog: manifest: fetch: %v", err)
		return
	}
	if resp.StatusCode != 200 {
		logging.Warnf("cacheprog: manifest: fetch: HTTP %d", resp.StatusCode)
		closeBody(resp.Body)
		b.Pool.Release()
		return
	}
	var chunk []BatchEntry
	count := 0
	flush := func() {
		if len(chunk) > 0 {
			b.OnBatchEntries(chunk)
			chunk = nil
		}
	}
	err = streamBatchResponse(resp.Body, func(e BatchEntry) {
		count++
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
	closeBody(resp.Body)
	b.Pool.Release()
	flush()
	if err != nil {
		logging.Warnf("cacheprog: manifest: read: %v", err)
	}
	b.manifest.Fetched.Add(uint32(count))
	b.batchTiming.recordLookAhead(count, time.Since(start))
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
	ids := make([]string, 0, m.used.Len()+m.missed.Len())
	for actionID := range m.used.All() {
		if actionID != *id {
			ids = append(ids, actionID)
		}
	}
	// A miss the build later stored is a hit for the next one, not a miss.
	for actionID := range m.missed.All() {
		if actionID != *id && !m.used.Contains(actionID) {
			ids = append(ids, "-"+actionID)
		}
	}
	m.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids)
	data := []byte(strings.Join(ids, "\n"))
	// Put skips a key the index already holds. The manifest replaces the last one.
	b.ForgetStale(*id)
	if err := b.Put(*id, StoredDigest(data), data); err != nil {
		logging.Warnf("cacheprog: manifest: store: %v", err)
	}
}

// reportManifest states what the manifest did.
func (b *WebBackend) reportManifest() {
	m := b.manifest
	if m == nil || m.Named.Load() == 0 {
		return
	}
	logging.Infof("cacheprog: manifest: named %d keys, fetched %d ahead, %d lookups waited for them, %d answered as misses while the index loaded",
		m.Named.Load(), m.Fetched.Load(), m.Waited.Load(), m.AbsentSkips.Load())
}
