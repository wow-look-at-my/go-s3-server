package cacheclient

import (
	"encoding/hex"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// storeTier puts the shared store under the disk cache. Disk stays
// authoritative: a hit there answers without a request, and a body the store
// serves is written to disk before it is handed back, so a caller reads a file
// either way.
type storeTier struct {
	*DiskCache
	store *WebBackend

	localHits     atomic.Int64
	localHitBytes atomic.Int64
	localPuts     atomic.Int64
	localPutBytes atomic.Int64

	closeOnce  sync.Once
	closeErr   error
	storeOnce  sync.Once
	storeErr   error
}

// layerStore answers the disk cache with the store under it.
func layerStore(disk *DiskCache, store *WebBackend) Cache {
	tier := &storeTier{DiskCache: disk, store: store}
	// The look-ahead pool has somewhere to put what it fetches, which is what
	// turns it on: an object lands on disk before the build asks for it, so
	// the ask is a local read.
	store.OnBatchEntries = tier.populate
	return tier
}

// Get answers from disk, and asks the store only when disk misses.
func (tier *storeTier) Get(id ActionID) (Entry, error) {
	entry, err := tier.DiskCache.Get(id)
	if err == nil {
		tier.localHits.Add(1)
		tier.localHitBytes.Add(entry.Size)
		return entry, nil
	}
	outputID, data, _, miss := tier.store.Get(hex.EncodeToString(id[:]))
	if miss || data == nil {
		return Entry{}, err
	}
	out, decodeErr := decodeOutputID(outputID)
	if decodeErr != nil {
		return Entry{}, err
	}
	tier.keep(id, out, data)
	return tier.DiskCache.Get(id)
}

// Put stores locally, then offers the body to the store. The local store is
// what the build depends on, so its result is what Put reports.
func (tier *storeTier) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	out, size, err := tier.DiskCache.Put(id, file)
	if err != nil {
		return out, size, err
	}
	tier.localPuts.Add(1)
	tier.localPutBytes.Add(size)
	tier.store.PutFile(hex.EncodeToString(id[:]), hex.EncodeToString(out[:]), tier.DiskCache.OutputFile(out))
	return out, size, nil
}

// populate stores what the look-ahead pool fetched before the build asked for
// it. It runs on that pool's goroutines, several at a time.
func (tier *storeTier) populate(entries []BatchEntry) {
	for _, entry := range entries {
		actionID, ok := tier.store.ActionIDFromKey(entry.Key)
		if !ok {
			continue
		}
		var id ActionID
		raw, err := hex.DecodeString(actionID)
		if err != nil || len(raw) != len(id) {
			continue
		}
		copy(id[:], raw)
		if _, err := tier.DiskCache.Get(id); err == nil {
			continue // already local, and a decompress is what that saves
		}
		data, ok := tier.store.Verify(entry, actionID)
		if !ok {
			continue
		}
		out, decodeErr := decodeOutputID(entry.OutputID)
		if decodeErr != nil {
			continue
		}
		tier.keep(id, out, data)
	}
}

// keep writes a body the store served. Its output ID is checked already, so
// this skips the hash Put would compute a second time.
func (tier *storeTier) keep(id ActionID, out OutputID, data []byte) {
	if err := tier.DiskCache.copyFile(bytesReader(data), out, int64(len(data))); err != nil {
		return
	}
	// allowVerify is false: this body came off the network, so the local
	// reproducibility check has nothing to say about it.
	if err := tier.DiskCache.putIndexEntry(id, out, int64(len(data)), false); err != nil {
		return
	}
	tier.localPuts.Add(1)
	tier.localPutBytes.Add(int64(len(data)))
}

// Close drains the store's uploads before the disk cache trims, so an upload
// never loses the file it is reading.
func (tier *storeTier) Close() error {
	tier.closeOnce.Do(func() {
		tier.closeErr = joinErrors(tier.closeStore(), tier.DiskCache.Close())
		tier.report()
	})
	return tier.closeErr
}

// closeStore closes the store tier once: it drains the uploads and gives up
// the index's lock. The disk cache stays open.
func (tier *storeTier) closeStore() error {
	tier.storeOnce.Do(func() { tier.storeErr = tier.store.Close() })
	return tier.storeErr
}

// report names what each tier answered and what it moved. Close drains the
// uploads first, so the totals here are final rather than in flight.
func (tier *storeTier) report() {
	web := tier.store.SummarySnapshot()
	cacheNotice("cache: local %s, %s stored | server %s, %s pushed | index %s",
		countBytes(tier.localHits.Load(), tier.localHitBytes.Load()),
		countBytes(tier.localPuts.Load(), tier.localPutBytes.Load()),
		countBytes(int64(web.Hits), int64(web.HitBytes)),
		countBytes(int64(web.Puts), int64(web.PutBytes)),
		formatMB(int64(web.IndexBytes)))
}

// cacheNotice reports what the tiers did, under the variable that makes the
// client report itself.
func cacheNotice(format string, args ...any) {
	if !cacheDebug() {
		return
	}
	logging.Infof(format, args...)
}

// cacheDebug reports whether the cache is asked to describe itself.
func cacheDebug() bool { return os.Getenv("GOCACHEDEBUG") != "" }
