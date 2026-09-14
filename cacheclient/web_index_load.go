package cacheclient

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// IndexWaitDefault is how long the first Get or Put waits for the key index
// when no disk copy exists. Past it the build goes on without one: a key the
// index would have listed is batch-probed instead, and the load keeps running
// in the background.
const IndexWaitDefault = 15 * time.Second

// indexSlowLoad is the load time past which the load is reported at warning
// level, so a slow index shows in a CI log without debug output.
const indexSlowLoad = 5 * time.Second

// indexTiming holds the waits of an index load. It is per backend so a test
// can shorten one without touching another test's backend.
type indexTiming struct {
	// wait bounds how long the first use blocks on a load with no disk copy.
	wait time.Duration
	// lockHeartbeat is how often the process holding the lock touches it.
	lockHeartbeat time.Duration
	// lockStale is the lock age past which its holder is taken to be gone.
	lockStale time.Duration
	// lockPoll is how often a process waiting on the lock checks it.
	lockPoll time.Duration
}

func defaultIndexTiming() indexTiming {
	return indexTiming{
		wait:          IndexWaitDefault,
		lockHeartbeat: 2 * time.Second,
		lockStale:     10 * time.Second,
		lockPoll:      50 * time.Millisecond,
	}
}

// indexLoad is a load running in the background.
type indexLoad struct {
	cancel context.CancelFunc
	done   chan struct{} // closed once the load has installed its result
}

// keysJournal holds the claims and drops made on the live key set while a
// load runs. The set the load installs is replayed through it, so a Put's
// claim or a confirmed absence made meanwhile is not lost.
type keysJournal struct {
	added   set.Set[actionHash]
	dropped set.Set[actionHash]
}

// addKeyLocked claims h in the live key set. keysMu must be held.
func (b *WebBackend) addKeyLocked(h actionHash) {
	b.keys.Add(h)
	if j := b.keysJournal; j != nil {
		j.dropped.Remove(h)
		j.added.Add(h)
	}
}

// dropKeyLocked removes h from the live key set. keysMu must be held.
func (b *WebBackend) dropKeyLocked(h actionHash) {
	b.keys.Remove(h)
	if j := b.keysJournal; j != nil {
		j.added.Remove(h)
		j.dropped.Add(h)
	}
}

// ensureIndex loads the key index the first time the cache is used. Every
// path that reads or claims a key calls it first.
//
// It never blocks on a download while any disk copy exists. A copy younger
// than the max age is authoritative and final. An older one is installed at
// once as non-authoritative, and the refresh runs in the background. With no
// copy at all the first use waits up to indexTiming.wait for the load, then
// goes on non-authoritative while the load continues.
func (b *WebBackend) ensureIndex() {
	b.indexOnce.Do(b.startIndexLoad)
}

func (b *WebBackend) startIndexLoad() {
	start := time.Now()
	path := b.indexCachePath()
	disk := b.readDiskIndex(path)
	maxAge := b.resolveIndexMaxAge(path)
	if disk.blob != nil && maxAge > 0 && disk.age() < maxAge {
		logging.Infof("cacheprog: web index: %d keys from a copy %v old", disk.count, disk.age().Round(time.Second))
		b.keysMu.Lock()
		b.keys = disk.keys
		b.indexAuthoritative = true
		b.indexEmpty = disk.count == 0
		b.indexKeysAtStart = disk.count
		b.keysMu.Unlock()
		return
	}

	b.keysMu.Lock()
	b.keys = disk.keys
	b.indexAuthoritative = false
	b.indexEmpty = disk.count == 0
	b.indexKeysAtStart = disk.count
	b.keysJournal = &keysJournal{added: set.New[actionHash](), dropped: set.New[actionHash]()}
	b.keysMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	load := &indexLoad{cancel: cancel, done: make(chan struct{})}
	b.indexLoad.Store(load)
	go func() {
		defer close(load.done)
		defer cancel()
		keys, authoritative, source := b.refreshIndex(ctx, path, disk)
		b.adoptIndex(keys, authoritative)
		if elapsed := time.Since(start); elapsed >= indexSlowLoad && ctx.Err() == nil {
			logging.Infof("cacheprog: web index: %s", b.indexLoadReport(elapsed, source))
		}
	}()

	if disk.blob != nil {
		logging.Infof("cacheprog: web index: %d keys from a copy %v old; refreshing in the background", disk.count, disk.age().Round(time.Second))
		return
	}
	// Only this wait holds up the build, so it is what reports at warning level.
	timer := time.NewTimer(b.indexTiming.wait)
	defer timer.Stop()
	select {
	case <-load.done:
		if waited := time.Since(start); waited >= indexSlowLoad {
			logging.Warnf("cacheprog: web index: the first lookup waited for it: %s", b.indexLoadReport(waited, "no disk copy"))
		}
	case <-timer.C:
		logging.Warnf("cacheprog: web index: not loaded after %v; going on without it while it loads (lookups probe the server)", b.indexTiming.wait)
	}
}

// indexLoadReport describes a finished load for the log.
func (b *WebBackend) indexLoadReport(elapsed time.Duration, source string) string {
	b.keysMu.RLock()
	n, authoritative := b.indexKeysAtStart, b.indexAuthoritative
	b.keysMu.RUnlock()
	return fmt.Sprintf("load took %v (%s): %d keys, %d bytes downloaded, authoritative=%v",
		elapsed.Round(time.Millisecond), source, n, b.indexBytes.Load(), authoritative)
}

// adoptIndex installs a load's result as the live key set, replaying the
// claims and drops made while it ran. A nil set, from a cancelled load, keeps
// the live set as it is.
func (b *WebBackend) adoptIndex(keys *hashSet, authoritative bool) {
	b.keysMu.Lock()
	defer b.keysMu.Unlock()
	j := b.keysJournal
	b.keysJournal = nil
	if keys == nil {
		return
	}
	b.indexAuthoritative = authoritative
	if keys == b.keys {
		// The load confirmed or fell back to the set already installed, which
		// already carries every claim and drop.
		return
	}
	n := keys.Len()
	for h := range j.added.All() {
		keys.Add(h)
	}
	for h := range j.dropped.All() {
		keys.Remove(h)
	}
	b.keys = keys
	b.indexEmpty = n == 0
	b.indexKeysAtStart = n
}

// refreshIndex brings the disk copy up to date and returns the keys to
// install, whether they are authoritative, and where they came from.
//
// One process at a time downloads into a directory. The first to create the
// lock file next to the copy fetches; any other waits for the lock to go and
// then reads what the holder wrote. A holder that dies leaves a lock nobody
// touches, and past indexTiming.lockStale the lock is removed and the fetch
// taken over. A holder whose fetch failed leaves the copy as it was; the
// waiter then tries once itself.
func (b *WebBackend) refreshIndex(ctx context.Context, path string, disk diskIndex) (*hashSet, bool, string) {
	lockPath := path + ".lock"
	for attempt := 0; ; attempt++ {
		lock, err := acquireIndexLock(lockPath)
		if err == nil {
			keys, authoritative, source := b.fetchIndexLocked(ctx, path, disk, lock)
			return keys, authoritative, source
		}
		if !errors.Is(err, fs.ErrExist) {
			logging.Infof("cacheprog: web index: %v; fetching without the lock", err)
			keys, authoritative := b.fetchIndex(ctx, path, disk)
			return keys, authoritative, "fetched without the lock"
		}
		if !b.awaitIndexLock(ctx, lockPath) {
			return nil, false, "cancelled"
		}
		if cur, ok := b.refreshedSince(path, disk); ok {
			logging.Infof("cacheprog: web index: %d keys from another process's refresh", cur.count)
			return cur.keys, true, "another process fetched it"
		}
		if attempt > 0 {
			logging.Infof("cacheprog: web index: another process's refresh failed; using %d cached keys (batch probing enabled)", disk.count)
			return disk.keys, false, "another process's fetch failed"
		}
	}
}

// fetchIndexLocked fetches under a held lock, touching it as long as the fetch
// runs, and releases it.
func (b *WebBackend) fetchIndexLocked(ctx context.Context, path string, disk diskIndex, lock *indexLock) (*hashSet, bool, string) {
	defer lock.release()
	// A refresh that landed between the disk read and the lock is the one this
	// fetch would repeat.
	if cur, ok := b.refreshedSince(path, disk); ok {
		logging.Infof("cacheprog: web index: %d keys from another process's refresh", cur.count)
		return cur.keys, true, "another process fetched it"
	}
	stop := lock.heartbeat(b.indexTiming.lockHeartbeat)
	defer stop()
	keys, authoritative := b.fetchIndex(ctx, path, disk)
	return keys, authoritative, "fetched"
}

// refreshedSince reads the disk copy again and reports whether it changed
// after disk was read, which only a completed refresh does.
func (b *WebBackend) refreshedSince(path string, disk diskIndex) (diskIndex, bool) {
	cur := b.readDiskIndex(path)
	if cur.blob == nil || cur.mtime.Equal(disk.mtime) {
		return cur, false
	}
	return cur, true
}

// awaitIndexLock waits for the lock at lockPath to be released, or to go
// stale, in which case it removes it. It reports false when ctx ends first.
func (b *WebBackend) awaitIndexLock(ctx context.Context, lockPath string) bool {
	tick := time.NewTicker(b.indexTiming.lockPoll)
	defer tick.Stop()
	for {
		st, err := os.Stat(lockPath)
		if err != nil {
			return true
		}
		if age := time.Since(st.ModTime()); age > b.indexTiming.lockStale {
			logging.Infof("cacheprog: web index: lock %s untouched for %v; taking over", lockPath, age.Round(time.Second))
			os.Remove(lockPath)
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
		}
	}
}

// indexLock is the lock file one process holds while it downloads the index.
// It is a file created exclusively rather than an OS lock, so it works on
// every platform and filesystem the directory can be on.
type indexLock struct {
	path string
}

// acquireIndexLock creates the lock file. An error wrapping fs.ErrExist means
// another process holds it.
func acquireIndexLock(path string) (*indexLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	_, werr := fmt.Fprintf(f, "%d\n", os.Getpid())
	cerr := f.Close()
	if err := errors.Join(werr, cerr); err != nil {
		os.Remove(path)
		return nil, err
	}
	return &indexLock{path: path}, nil
}

func (l *indexLock) release() {
	os.Remove(l.path)
}

// heartbeat touches the lock every interval until stop is called, so a
// waiter can tell a live holder from a dead one.
func (l *indexLock) heartbeat(every time.Duration) (stop func()) {
	tick := time.NewTicker(every)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-quit:
				return
			case now := <-tick.C:
				os.Chtimes(l.path, now, now)
			}
		}
	}()
	return func() {
		tick.Stop()
		close(quit)
	}
}
