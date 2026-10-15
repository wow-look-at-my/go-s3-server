package main

import "encoding/hex"

// compactKey holds a cache key without a per-key heap allocation.
type compactKey struct {
	hash [gbciHashSize]byte
	raw  string
}

func newCompactKey(key string) compactKey {
	if h, ok := extractActionHash(key); ok {
		return compactKey{hash: h}
	}
	return compactKey{raw: key}
}

// Key rebuilds the cache key. It allocates, so call it for keys that are
// handed back to a caller -- not while scanning.
func (c compactKey) Key() string {
	if c.raw != "" {
		return c.raw
	}
	// Encoded straight into the buffer that becomes the string.
	b := make([]byte, len(gbciKeyPrefix)+2*gbciHashSize)
	copy(b, gbciKeyPrefix)
	hex.Encode(b[len(gbciKeyPrefix):], c.hash[:])
	return string(b)
}

// actionHash returns the action ID, and whether this is a cacheprog key at all.
func (c compactKey) actionHash() ([gbciHashSize]byte, bool) {
	return c.hash, c.raw == ""
}
