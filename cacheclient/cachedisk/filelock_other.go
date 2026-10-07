//go:build !unix && !windows

package cachedisk

import (
	"errors"
	"os"
)

// lockExclusive reports that this platform has no OS file lock to block in.
func lockExclusive(f *os.File) error {
	return errors.ErrUnsupported
}

func unlockFile(f *os.File) {}
