package main

// cleanKeyMemo remembers indexed cacheprog keys whose stored body has already
// passed the read-path module-index probe, so repeat GET/batch/prefetch reads
// of a warm key skip the file open and the header walk. On a warm cache the
// same keys are read on every build, so memoizing the verdict removes that work
// from the steady state.
//
// Scope and safety: only keys in the guard's scope (go-buildcache/v1<64-hex>)
// are ever memoized, keyed by their 32-byte action hash. A key is forgotten
// whenever its body could have changed -- overwrite PUT, DELETE, and eviction
// (storage.forgetClean call sites) -- so a memo hit always refers to a body
// that entered through PutStream's module-index guard or was probed directly.
// The memo is a probe-skip optimization, not the safety boundary: the PUT guard
// remains the gate that keeps new module indexes out of the store.
//
// Bounded in BYTES with least-recently-used eviction (lrucache.go), sized from
// the process's memory ceiling and shrunk when memory gets tight (memlimit.go).
// Evicting an entry costs one re-probe of that key -- which is why this cache,
// like the others, is the right thing to give up under memory pressure instead
// of service.

// cleanEntryBytes is what one memoized verdict costs: the 32-byte hash, the map
// bucket and the list element. The value carries no data -- membership IS the
// verdict -- so an entry's size is a constant.
const cleanEntryBytes = 96

// cleanMemoKind is the label this cache reports its size under.
const cleanMemoKind = "clean-keys"

type cleanKey = [gbciHashSize]byte

func newCleanKeyMemo(budget int64) *lruCache[cleanKey, struct{}] {
	return newLRUCache(budget,
		func(h cleanKey) uint32 {
			// Action IDs are uniformly distributed, so any four bytes shard
			// evenly and there is nothing to gain from hashing all 32.
			return uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
		},
		func(cleanKey, struct{}) int64 { return cleanEntryBytes })
}

// keyKnownClean reports whether the action hash already passed the read-path
// module-index probe. Nil-safe for directly-constructed Storage values.
func (s *Storage) keyKnownClean(h cleanKey) bool {
	if s.cleanKeys == nil {
		return false
	}
	_, ok := s.cleanKeys.Get(h)
	return ok
}

// markKeyClean memoizes an action hash whose body was just probed and is not a
// module index. Nil-safe for directly-constructed Storage values.
func (s *Storage) markKeyClean(h cleanKey) {
	if s.cleanKeys != nil {
		s.cleanKeys.Put(h, struct{}{})
	}
}

// forgetClean invalidates the known-clean memo entry for key. Must be called
// whenever the body stored under key changes or is removed (overwrite PUT,
// DELETE, eviction), so the next read re-probes the new body.
func (s *Storage) forgetClean(key string) {
	if s.cleanKeys == nil {
		return
	}
	if h, ok := extractActionHash(key); ok {
		s.cleanKeys.Forget(h)
	}
}
