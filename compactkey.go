package main

import "encoding/hex"

// compactKey holds a cache key without a per-key heap allocation.
//
// Nearly every key here is a cacheprog key: the constant gbciKeyPrefix
// followed by a 64-character hex action ID, i.e.
//
// Keys outside the cacheprog pattern -- this server stores arbitrary keys too
// -- keep their string in raw.
//
// It is comparable, so it doubles as a map key.
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

// Key rebuilds the original cache key. It allocates, so call it for keys that
// are handed back to a caller -- not while scanning.
func (c compactKey) Key() string {
	if c.raw != "" {
		return c.raw
	}
	return gbciKeyPrefix + hex.EncodeToString(c.hash[:])
}

// actionHash returns the action ID, and whether this is a cacheprog key at all.
func (c compactKey) actionHash() ([gbciHashSize]byte, bool) {
	return c.hash, c.raw == ""
}
