//go:build js || wasip1

package cachedisk

import (
	"context"
	"path/filepath"
	"sync"
)

// js and wasip1 start no other process, so every holder of a path is in this
// process. A lock per path in memory gives each caller full exclusion.
var (
	pathLocksMu sync.Mutex
	pathLocks   = map[string]chan struct{}{}
)

// LockFile takes an exclusive lock on path and returns the function that
// releases it. A ctx that ends returns the caller at once.
func LockFile(ctx context.Context, path string) (unlock func(), err error) {
	key := filepath.Clean(path)
	pathLocksMu.Lock()
	slot, ok := pathLocks[key]
	if !ok {
		slot = make(chan struct{}, 1)
		pathLocks[key] = slot
	}
	pathLocksMu.Unlock()
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
