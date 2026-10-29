// IndexWaitDefault is how long the earliest Get or Put waits for the key index when no disk copy exists.

package cacheclient

import (
	"context"
	"fmt"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// IndexWaitDefault is how long the earliest Get or Put waits for the key index when no disk copy exists.
const IndexWaitDefault = 0

// indexSlowLoad is the load time past which the load is reported at warning level.
const indexSlowLoad = 5 * time.Second

// indexTiming holds the waits of an index load. It is per backend so a test
// can shorten a single without touching another test's backend.
type indexTiming struct {
	// wait bounds how long the earliest use blocks on a load with no disk copy.
	wait time.Duration
}

// defaultIndexWait is the wait each new backend starts with.
var defaultIndexWait time.Duration = IndexWaitDefault

func defaultIndexTiming() indexTiming {
	return indexTiming{wait: defaultIndexWait}
}

// indexLoad is a load running in the background.
type indexLoad struct {
	cancel context.CancelFunc
	done   chan struct{} // closed a single time the load has installed
}

// keysJournal holds the claims and drops made on the live key set while a
// load runs.
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

// ensureIndex loads the key index the earliest time the cache is used.
// Every path that reads or claims a key calls it earliest.
func (b *WebBackend) ensureIndex() {
	b.indexOnce.Do(b.startIndexLoad)
}

func (b *WebBackend) startIndexLoad() {
	start := time.Now()
	path := b.indexCachePath()
	disk := b.readDiskIndex(path)
	maxAge := b.resolveIndexMaxAge(path)
	// A copy that names a position asks for the keys added since, at any age.
	if disk.blob != nil && disk.gen.epoch != 0 {
		b.startDeltaLoad(path, disk)
		return
	}
	if disk.blob != nil && maxAge > 0 && disk.age() < maxAge {
		// A server that names no position cannot send a delta, so the copy is trusted as it stands.
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
		logging.Infof("cacheprog: web index: loaded %d keys from a copy %v old", disk.count, disk.age().Round(time.Second))
		return
	}
	if b.indexTiming.wait <= 0 {
		return
	}
	// Only this wait holds up the build, so it is what reports at warning level.
	timer := time.NewTimer(b.indexTiming.wait)
	defer timer.Stop()
	select {
	case <-load.done:
		if waited := time.Since(start); waited >= indexSlowLoad {
			logging.Warnf("cacheprog: web index: slow load: %s", b.indexLoadReport(waited, "no disk copy"))
		}
	case <-timer.C:
		logging.Warnf("cacheprog: web index: not loaded after %v", b.indexTiming.wait)
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
func (b *WebBackend) adoptIndex(keys *keySet, authoritative bool) {
	b.keysMu.Lock()
	defer b.keysMu.Unlock()
	j := b.keysJournal
	b.keysJournal = nil
	if keys == nil {
		return
	}
	b.indexAuthoritative = authoritative
	if keys == b.keys {
		// The load confirmed or fell back to the set already installed, which already carries every claim and drop.
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
// A single process at a time downloads into a directory, under cachedisk.LockFile on a file next
// to the copy. A process that finds the lock held blocks in the kernel until the holder lets go,
// which it also does by exiting. And then reads what the holder wrote. A holder whose fetch
// failed leaves the copy as it was, and the next process to take the lock fetches itself.
func (b *WebBackend) refreshIndex(ctx context.Context, path string, disk diskIndex) (*keySet, bool, string) {
	unlock, err := cachedisk.LockFile(ctx, path+".lock")
	if err != nil && ctx.Err() != nil {
		return nil, false, "cancelled"
	}
	if err != nil {
		logging.Infof("cacheprog: web index lock: %v", err)
		keys, authoritative := b.fetchIndex(ctx, path, disk)
		return keys, authoritative, "fetched without the lock"
	}
	defer unlock()
	// A refresh that landed between the disk read and the lock is the thing
	// this fetch would repeat.
	if cur, ok := b.refreshedSince(path, disk); ok {
		logging.Infof("cacheprog: web index: %d keys from another process's refresh", cur.count)
		b.setLiveGen(cur.gen)
		return cur.keys, true, "another process fetched it"
	}
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
