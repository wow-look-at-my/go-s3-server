package cacheclient

import "sync"

// HashSize is the length of a cache key, in bytes. An action ID and an output
// ID are both one of these.
const HashSize = hashSize

// debugHash makes the hashes a build computes report themselves. The consumer
// computes them, so it also sets this, and the disk cache reads it to decide
// whether an entry mismatch is worth describing.
var debugHash = false

// hashDebug holds what went into a hash the consumer computed, so a mismatch
// can be reported as the description of what should have been there rather
// than as two opaque ids. It is filled under GODEBUG=gocacheverify=1 and is
// empty otherwise.
var hashDebug struct {
	sync.Mutex
	m map[[HashSize]byte]string
}

// RecordHash remembers what produced an id, for the report a mismatch makes.
// A consumer that computes cache keys calls it under gocacheverify.
func RecordHash(id [HashSize]byte, description string) {
	hashDebug.Lock()
	defer hashDebug.Unlock()
	if hashDebug.m == nil {
		hashDebug.m = make(map[[HashSize]byte]string)
	}
	hashDebug.m[id] = description
}

// reverseHash answers what produced an id, or "" when nothing recorded it.
func reverseHash(id [HashSize]byte) string {
	hashDebug.Lock()
	defer hashDebug.Unlock()
	return hashDebug.m[id]
}
