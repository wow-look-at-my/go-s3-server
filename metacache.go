package main

import (
	"os"
)

// An object's user metadata lives in extended attributes.

// kvPair is a single metadata attribute.
type kvPair struct{ k, v string }

type metaEntry struct {
	modNano int64
	size    int64
	kv      []kvPair
}

// metaEntryOverhead approximates what a single entry costs beyond its strings: the map bucket, the list element, the entry header.
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

// forgetMeta drops key's cached metadata.
func (s *Storage) forgetMeta(key string) {
	if s.metaCache != nil {
		s.metaCache.Forget(key)
	}
}
