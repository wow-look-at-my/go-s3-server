package main

// cleanKeyMemo remembers indexed cacheprog keys whose stored body has already passed the read-path module-index probe.

// cleanEntryBytes is what a single memoized verdict costs: the 32-byte hash, the map bucket and the list element.
const cleanEntryBytes = 96

// cleanMemoKind is the label this cache reports its size under.
const cleanMemoKind = "clean-keys"

type cleanKey = [gbciHashSize]byte

func newCleanKeyMemo(budget int64) *lruCache[cleanKey, struct{}] {
	return newLRUCache(budget,
		func(h cleanKey) uint32 {
			return uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
		},
		func(cleanKey, struct{}) int64 { return cleanEntryBytes })
}

// keyKnownClean reports whether the action hash already passed the read-path
// module-index probe.
func (s *Storage) keyKnownClean(h cleanKey) bool {
	if s.cleanKeys == nil {
		return false
	}
	_, ok := s.cleanKeys.Get(h)
	return ok
}

// markKeyClean memoizes an action hash whose body was probed and is not a
// module index. Nil-safe for directly-constructed Storage values.
func (s *Storage) markKeyClean(h cleanKey) {
	if s.cleanKeys != nil {
		s.cleanKeys.Put(h, struct{}{})
	}
}

// forgetClean invalidates the known-clean memo entry for key.
func (s *Storage) forgetClean(key string) {
	if s.cleanKeys == nil {
		return
	}
	if h, ok := extractActionHash(key); ok {
		s.cleanKeys.Forget(h)
	}
}
