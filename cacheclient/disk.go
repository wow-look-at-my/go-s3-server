// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"io"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// The cache's directory lives one package down, where nothing imports net. A
// consumer that must not depend on net holds that package directly and gets a
// working cache with no store and no broker under it. Everything else holds
// this one, and these names are what makes the two read the same.

// An ActionID is a cache action key, the hash of a complete description of a
// repeatable computation: the command line, the environment, the input file
// contents and the executable contents.
type ActionID = cachedisk.ActionID

// An OutputID is a cache output key, the hash of an output of a computation.
type OutputID = cachedisk.OutputID

// A Cache stores an action's output under that action's key.
type Cache = cachedisk.Cache

// An Entry is what a cache holds for one action.
type Entry = cachedisk.Entry

// A DiskCache is the cache directory itself.
type DiskCache = cachedisk.DiskCache

// A MissError says a cache holds no entry for an action.
type MissError = cachedisk.MissError

// Tiered is implemented by a Cache whose lookup reports which tier answered.
type Tiered = cachedisk.Tiered

// HashSize is the length of a cache key, in bytes.
const HashSize = cachedisk.HashSize

// Tier names, as a cache reports them for a lookup it answered.
const (
	TierDisk   = cachedisk.TierDisk
	TierShared = cachedisk.TierShared
	TierOwner  = cachedisk.TierOwner
)

// DebugTest reports whether GODEBUG=gocachetest=1 asks the test cache to
// explain its decisions.
func DebugTest() bool { return cachedisk.DebugTest }

// Open opens the cache in a directory.
func Open(dir string) (*DiskCache, error) { return cachedisk.Open(dir) }

// GetFile looks up an action and answers the file holding its output.
func GetFile(c Cache, id ActionID) (string, Entry, error) { return cachedisk.GetFile(c, id) }

// GetBytes looks up an action and answers its output. Use it only for output
// that fits in memory.
func GetBytes(c Cache, id ActionID) ([]byte, Entry, error) { return cachedisk.GetBytes(c, id) }

// PutBytes stores bytes as an action's output.
func PutBytes(c Cache, id ActionID, data []byte) error { return cachedisk.PutBytes(c, id, data) }

// PutNoVerify stores an output that is not reproducible, such as test output,
// so GODEBUG=gocacheverify=1 does not hold it to a second identical run.
func PutNoVerify(c Cache, id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	return cachedisk.PutNoVerify(c, id, file)
}

// RecordHash remembers what produced a key, for the report a mismatch makes.
func RecordHash(id [HashSize]byte, description string) { cachedisk.RecordHash(id, description) }

// Verifying reports whether the cache runs in verify mode.
func Verifying() bool { return cachedisk.Verifying() }

// HashDebug reports whether every hash is asked to report itself.
func HashDebug() bool { return cachedisk.HashDebug() }
