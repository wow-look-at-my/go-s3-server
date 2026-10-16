//go:build js || wasip1

package cachedisk

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
)

// js and wasip1 start no other process, so every holder of a path is in this
// process. A lock per path in memory gives each caller full exclusion.
var (
	pathLocksMu sync.Mutex
	pathLocks   = map[string]chan struct{}{}
)

// pathSlot answers the in-memory lock for path.
func pathSlot(path string) chan struct{} {
	key := filepath.Clean(path)
	pathLocksMu.Lock()
	defer pathLocksMu.Unlock()
	slot, ok := pathLocks[key]
	if !ok {
		slot = make(chan struct{}, 1)
		pathLocks[key] = slot
	}
	return slot
}

// TryLockFile is LockFile without the wait: a lock another holder has fails at once.
func TryLockFile(path string) (unlock func(), err error) {
	slot := pathSlot(path)
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	default:
		return nil, errors.New("lock " + path + ": held")
	}
}

// LockFile takes an exclusive lock on path and returns the function that
// releases it. A ctx that ends returns the caller at once.
func LockFile(ctx context.Context, path string) (unlock func(), err error) {
	slot := pathSlot(path)
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
