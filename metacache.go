package main

import (
	"os"
)

// An object's user metadata lives in extended attributes, and reading it costs
// one listxattr plus one getxattr per attribute -- around a dozen syscalls for
// a typical entry (outputid, compression, object-type, pkg, src, module,
// go-version, target, toolchain-version, created), measured at ~42us per key.
// Every Stat and every Open pays it, so one batch GET of the client's 128-key
// chunk paid it 128 times before this cache existed, and twice per key before
// the streaming phase stopped re-reading metadata it already had.
//
// The metadata of a stored object never changes in place: a new body arrives as
// a fresh inode renamed over the path, so the mtime and size that come back
// from the stat every caller ALREADY performs are enough to tell a cached entry
// from a stale one. That makes the cache self-validating rather than
// invalidation-dependent: a hit is only served when the fresh stat matches the
// stat the entry was recorded under, so an overwrite, a restore, or a rewritten
// body can never be served with the previous body's metadata.
//
// The one mutation that does NOT move mtime is an xattr write onto a live inode
// -- the outputid self-heal's fsetxattr. Those call sites drop the entry
// explicitly (forgetMeta), the same way they already drop the known-clean memo.
//
// It is bounded in BYTES and evicts its least-recently-used entries
// (lrucache.go), with the bound sized from the process's memory ceiling and
// shrunk further when memory gets tight (memlimit.go). Evicting one costs the
// syscalls back on the next read of that key -- nothing else, and nothing the
// client can observe.

// kvPair is one metadata attribute. Entries hold a slice rather than a map so a
// cached entry is immutable and shareable: callers get a fresh map built from
// it (ObjectMeta.Metadata is mutable -- the self-heal writes into it), and no
// caller can reach back into the cache.
type kvPair struct{ k, v string }

type metaEntry struct {
	modNano int64
	size    int64
	kv      []kvPair
}

// metaEntryOverhead approximates what one entry costs beyond its strings: the
// map bucket, the list element, the entry header, the slice header. An estimate
// is the right precision here -- it feeds a budget that is itself a
// hand-chosen fraction, so being somewhat off changes how many entries fit, not
// whether the bound holds.
const metaEntryOverhead = 160

func metaEntrySize(key string, e metaEntry) int64 {
	n := int64(len(key) + metaEntryOverhead)
	for _, p := range e.kv {
		n += int64(len(p.k) + len(p.v) + 32)
	}
	return n
}

// metaCacheKind is the label this cache reports its size under.
const metaCacheKind = "metadata"

func newMetaCache(budget int64) *lruCache[string, metaEntry] {
	return newLRUCache(budget, fnv1a, metaEntrySize)
}

// loadMetadata fills meta.Metadata for the object at path, from the cache when
// the entry matches info, otherwise by reading the xattrs and recording them.
// Nil-safe for directly-constructed Storage values (tests), which then read
// through every time.
func (s *Storage) loadMetadata(key, path string, info os.FileInfo, meta *ObjectMeta) {
	if s.metaCache == nil {
		getMetadata(path, meta)
		return
	}
	if e, ok := s.metaCache.Get(key); ok && e.modNano == info.ModTime().UnixNano() && e.size == info.Size() {
		metaCacheHitsTotal.Inc()
		for _, p := range e.kv {
			meta.Metadata[p.k] = p.v
		}
		return
	}
	metaCacheMissesTotal.Inc()
	getMetadata(path, meta)
	kv := make([]kvPair, 0, len(meta.Metadata))
	for k, v := range meta.Metadata {
		kv = append(kv, kvPair{k, v})
	}
	s.metaCache.Put(key, metaEntry{modNano: info.ModTime().UnixNano(), size: info.Size(), kv: kv})
}

// forgetMeta drops key's cached metadata. Called wherever an object's xattrs
// change without its mtime changing (the self-heal's fsetxattr), and alongside
// the other per-key invalidations on delete and eviction.
func (s *Storage) forgetMeta(key string) {
	if s.metaCache != nil {
		s.metaCache.Forget(key)
	}
}
