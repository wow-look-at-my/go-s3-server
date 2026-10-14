//go:build !js && !wasip1

package cachedisk

import (
	"context"

	"github.com/wow-look-at-my/go-ipc/filelock"
)

// LockFile takes an exclusive OS lock on the file at path and returns the
// function that releases it.
func LockFile(ctx context.Context, path string) (unlock func(), err error) {
	return filelock.Lock(ctx, path)
}
