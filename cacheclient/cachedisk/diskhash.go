package cachedisk

import "sync"

// HashSize is the length of a cache key in bytes.
const HashSize = 32

// debugHash is GODEBUG=gocachehash=1, and the consumer prints under it.
var debugHash = false

// hashDebug holds what went into each key, under verify mode alone. A mismatch
// is reported from it, because two opaque ids say nothing.
var hashDebug struct {
	sync.Mutex
	m map[[HashSize]byte]string
}

// Verifying reports whether the cache runs in verify mode.
func Verifying() bool { return verify }

// HashDebug reports whether every hash is asked to report itself.
func HashDebug() bool { return debugHash }

// RecordHash remembers what produced a key. A consumer that computes keys
// calls it while Verifying.
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
