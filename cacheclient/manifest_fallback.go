package cacheclient

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// A command with no manifest of its own reads the module fallback.

// manifestFallbackGenerations caps the generations of the module fallback.
const manifestFallbackGenerations = 1 << 16

// manifestFallbackIndexWait is how long the fallback waits for the index load.
var manifestFallbackIndexWait = 5 * time.Second

// manifestFallbackID is the action ID of one generation of the module fallback.
func manifestFallbackID(module, version, target string) string {
	sum := sha256.Sum256([]byte("go-build-manifest-module\x00" + manifestFormat + "\x00" + module + "\x00" + version + "\x00" + target))
	return hex.EncodeToString(sum[:])
}

// fallbackBase is one generation of the module fallback. This holds for this
// backend, or "" when the manifest has no command scope or the module is not
// known.
func (b *WebBackend) fallbackBase() string {
	module := b.moduleName()
	if b.command == "" || module == "" {
		return ""
	}
	return manifestFallbackID(module, b.version, b.target)
}

// newestFallbackGen answers the newest generation of the fallback under base
// that the index holds. It answers false when the index is not authoritative.
// Writers add generations in order, so the search doubles, then halves.
func (b *WebBackend) newestFallbackGen(base string) (int, bool) {
	b.ensureIndex()
	b.keysMu.RLock()
	authoritative := b.indexAuthoritative
	b.keysMu.RUnlock()
	if !authoritative {
		return -1, false
	}
	known := func(gen int) bool {
		h, _ := parseActionHash(manifestGenID(base, gen))
		return b.keyKnown(h)
	}
	if !known(0) {
		return -1, true
	}
	low, high := 0, 1
	for high < manifestFallbackGenerations && known(high) {
		low, high = high, high*2
	}
	high = min(high, manifestFallbackGenerations)
	for high-low > 1 {
		mid := (low + high) / 2
		if known(mid) {
			low = mid
		} else {
			high = mid
		}
	}
	return low, true
}

// manifestFallback is the state of one fallback fetch.
type manifestFallback struct {
	mu       sync.Mutex
	gen      int             // the generation read
	named    []string        // the keys it names
	brought  set.Set[string] // keys fetched that the build has not used.
	wake     chan struct{}   // closed when a pause ends
	due      int64           // landed keys at the next verdict
	landed   int64           // keys of the requests that came back
	fetched  int64           // entries that the requests brought
	used     int64           // fetched entries the build then used
	lookups  int64           // lookups the build made
	pausedAt int64           // lookups when the pause started
	paused   bool
	stopped  bool
}

func newManifestFallback(gen int, named []string) *manifestFallback {
	return &manifestFallback{gen: gen, named: named, brought: set.New[string](), wake: make(chan struct{}), due: lookAheadJudgeAfter}
}

// signal ends a wait on wake. The caller holds mu.
func (f *manifestFallback) signal() {
	close(f.wake)
	f.wake = make(chan struct{})
}

// noteLanded records a request that came back with the entries in arrived.
func (f *manifestFallback) noteLanded(keys int, arrived []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.landed += int64(keys)
	for _, id := range arrived {
		if !f.brought.Contains(id) {
			f.brought.Add(id)
			f.fetched++
		}
	}
	if f.landed >= f.due && !f.paused && !f.stopped {
		f.paused = true
		f.pausedAt = f.lookups
		f.judgeLocked()
	}
}

// noteLookup counts a lookup of actionID, and whether the fallback brought it.
func (f *manifestFallback) noteLookup(actionID string, local bool) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if local && f.brought.Contains(actionID) {
		f.brought.Remove(actionID)
		f.used++
	}
	if f.paused {
		f.judgeLocked()
	}
}

// judgeLocked decides a paused sample. The caller holds mu.
func (f *manifestFallback) judgeLocked() {
	if f.used*100 >= f.fetched*lookAheadMinUse {
		f.due *= 2
		f.paused = false
		f.signal()
		return
	}
	if f.lookups-f.pausedAt < (f.fetched+1)/2 {
		return
	}
	f.paused = false
	f.stopped = true
	f.signal()
	logging.Infof("cacheprog: manifest: the build used %d of %d entries the module fallback fetched; this process stops the fetch", f.used, f.fetched)
}

// waitTurn blocks until dispatched keys are below the next verdict. It reports
// false when the fetch stops, or when cancel or closing closes.
func (f *manifestFallback) waitTurn(dispatched int64, cancel, closing <-chan struct{}) bool {
	for {
		f.mu.Lock()
		if f.stopped {
			f.mu.Unlock()
			return false
		}
		if dispatched < f.due && !f.paused {
			f.mu.Unlock()
			return true
		}
		wake := f.wake
		f.mu.Unlock()
		select {
		case <-wake:
		case <-cancel:
			return false
		case <-closing:
			return false
		}
	}
}

// isStopped reports whether the judge stopped the fetch.
func (f *manifestFallback) isStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

// endFallback ends the fallback fetch: the build is over, so nothing uses
// what it brings.
func (m *buildManifest) endFallback() {
	if m != nil {
		m.closeOnce.Do(func() { close(m.closing) })
	}
}

// awaitIndexLoad waits for a running index load, up to manifestFallbackIndexWait.
func (b *WebBackend) awaitIndexLoad() {
	b.ensureIndex()
	load := b.indexLoad.Load()
	if load == nil {
		return
	}
	timer := time.NewTimer(manifestFallbackIndexWait)
	defer timer.Stop()
	select {
	case <-load.done:
	case <-timer.C:
	case <-b.manifestCtx().Done():
	case <-b.manifest.closing:
	}
}

// fetchFallback reads the module fallback for a command with no manifest of
// its own, and fetches what it names while the build uses it.
func (b *WebBackend) fetchFallback() {
	m := b.manifest
	base := b.fallbackBase()
	if base == "" {
		return
	}
	b.awaitIndexLoad()
	gen, known := b.newestFallbackGen(base)
	if !known {
		m.trace.add("fallback-unknown", base)
		return
	}
	if gen < 0 {
		return
	}
	data, found := b.readManifest(manifestGenID(base, gen))
	if !found {
		return
	}
	named := manifestKeys(data, base)
	f := newManifestFallback(gen, named)
	m.fallback.Store(f)
	m.trace.add("fallback", manifestGenID(base, gen))

	b.keysMu.RLock()
	authoritative := b.indexAuthoritative
	b.keysMu.RUnlock()
	var ids []string
	for _, line := range named {
		h, _ := parseActionHash(line)
		if (authoritative && !b.keyKnown(h)) || b.isHeld(h) || b.hasLocal(h) || m.lookedUp(line) {
			continue
		}
		ids = append(ids, line)
	}
	if len(ids) == 0 {
		return
	}
	// The fallback covers the look-ahead while the build uses it.
	b.lookAhead.cover()
	b.lookAhead.release()
	defer func() {
		if f.isStopped() {
			b.lookAhead.uncover()
		}
	}()

	work := make(chan []string)
	var wg sync.WaitGroup
	for range manifestFetchWidth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for part := range work {
				// Only a request on the wire is pending. A lookup never waits for a guess still in the queue.
				chunk := m.markPending(part)
				chunk.send()
				keys := make([]string, len(part))
				for idx, actionID := range part {
					keys[idx] = b.key(actionID)
				}
				_, arrived := b.fetchNamed(keys, chunk.landed)
				f.noteLanded(len(part), arrived)
				chunk.finish()
			}
		}()
	}
	ctxDone := b.manifestCtx().Done()
	var dispatched int64
	for start := 0; start < len(ids); start += batchMaxKeys {
		if !f.waitTurn(dispatched, ctxDone, m.closing) {
			break
		}
		part := ids[start:min(start+batchMaxKeys, len(ids))]
		dispatched += int64(len(part))
		select {
		case work <- part:
		case <-ctxDone:
		case <-m.closing:
		}
	}
	close(work)
	wg.Wait()
}

// fallbackToStore answers the fallback generation this build writes, or false.
// A build writes one when it stores its own manifest, or when the module has
// no fallback yet.
func (b *WebBackend) fallbackToStore(storesOwn bool, body []byte) (manifestWrite, bool) {
	m := b.manifest
	base := b.fallbackBase()
	if base == "" {
		return manifestWrite{}, false
	}
	newest, known := b.newestFallbackGen(base)
	if !known || (!storesOwn && newest >= 0) {
		return manifestWrite{}, false
	}
	gen := newest + 1
	if gen >= manifestFallbackGenerations {
		logging.Warnf("cacheprog: manifest: the module fallback has %d generations, and the walk reads no later one", gen)
		return manifestWrite{}, false
	}
	target := manifestGenID(base, gen)
	m.trace.add("store-fallback", target)
	return manifestWrite{id: target, body: body}, true
}

// reportFallback states what the module fallback did.
func (m *buildManifest) reportFallback() string {
	f := m.fallback.Load()
	if f == nil {
		return ""
	}
	m.mu.Lock()
	used := m.used
	inAll := 0
	for _, id := range f.named {
		if used.Contains(id) {
			inAll++
		}
	}
	m.mu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	state := "not stopped"
	if f.stopped {
		state = "stopped at a low use"
	}
	return fmt.Sprintf("cacheprog: manifest: no manifest for this command; used the module fallback generation %d: named %d keys, fetched %d, the build used %d of the fetched keys and %d of the named keys, %s",
		f.gen, len(f.named), f.fetched, f.used, inAll, state)
}

// manifestBody joins ids into a manifest body.
func manifestBody(ids []string) []byte {
	return []byte(strings.Join(ids, "\n"))
}
