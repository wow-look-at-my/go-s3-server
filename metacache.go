package main

import (
	"os"
	"sync"

	"github.com/wow-look-at-my/go-containers/set"
)

// An object's user metadata lives in extended attributes.

// kvPair is a single metadata attribute. shared marks a value held by
// reference to the copy in metaStrings, which the entry does not pay for.
type kvPair struct {
	k, v    string
	shared  bool
	kShared bool
}

// sharedMetaValueKeys are the attributes whose values repeat across objects: every archive of a package carries the same pkg, src, module.
var sharedMetaValueKeys = set.Of("pkg", "src", "module", "target", "go-version", "toolchain-version", "object-type", "compression")

// maxSharedMetaStrings bounds the shared copies.
const maxSharedMetaStrings = 1 << 16

// metaStringTable holds a single copy of each shared key name and value.
type metaStringTable struct {
	mu    sync.Mutex
	m     map[string]string
	bytes int64
}

var metaStrings = &metaStringTable{m: map[string]string{}}

// share answers the copy of s, and whether it is shared.
func (t *metaStringTable) share(s string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.m[s]; ok {
		return c, true
	}
	if len(t.m) >= maxSharedMetaStrings {
		return s, false
	}
	t.m[s] = s
	t.bytes += int64(len(s)) + 16
	cacheMemoryBytes.WithLabelValues(metaStringsKind).Set(float64(t.bytes))
	return s, true
}

// metaStringsKind is the label the shared copies report their size under.
const metaStringsKind = "metadata-shared"

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
		// A shared key name or value is paid for once, in metaStrings.
		n += 32
		if !p.kShared {
			n += int64(len(p.k))
		}
		if !p.shared {
			n += int64(len(p.v))
		}
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
		p := kvPair{k: k, v: v}
		p.k, p.kShared = metaStrings.share(k)
		if sharedMetaValueKeys.Contains(k) {
			p.v, p.shared = metaStrings.share(v)
		}
		kv = append(kv, p)
	}
	s.metaCache.Put(key, metaEntry{modNano: info.ModTime().UnixNano(), size: info.Size(), kv: kv})
}

// forgetMeta drops key's cached metadata.
func (s *Storage) forgetMeta(key string) {
	if s.metaCache != nil {
		s.metaCache.Forget(key)
	}
}
