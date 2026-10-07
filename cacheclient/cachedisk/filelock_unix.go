//go:build unix

package cachedisk

import (
	"errors"
	"os"
	"syscall"
)

// lockExclusive blocks in flock(LOCK_EX) until f's lock is granted. Under GOOS=cosmo on a Windows host the runtime carries flock to a
// blocking LockFileEx.
func lockExclusive(f *os.File) error {
	fd := int(f.Fd())
	for {
		err := syscall.Flock(fd, syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func unlockFile(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
