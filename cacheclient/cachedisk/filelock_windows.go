//go:build windows

package cachedisk

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const lockfileExclusiveLock = 0x00000002

// allBytes is the length of the range locked: every byte the file can have.
const allBytes = ^uint32(0)

// lockExclusive blocks in LockFileEx until f's lock is granted. With no
// LOCKFILE_FAIL_IMMEDIATELY and a synchronous handle the call returns only
// once the lock is held.
func lockExclusive(f *os.File) error {
	var ov syscall.Overlapped
	r1, _, err := procLockFileEx.Call(f.Fd(), lockfileExclusiveLock, 0, uintptr(allBytes), uintptr(allBytes), uintptr(unsafe.Pointer(&ov)))
	if r1 == 0 {
		return err
	}
	return nil
}

func unlockFile(f *os.File) {
	var ov syscall.Overlapped
	procUnlockFileEx.Call(f.Fd(), 0, uintptr(allBytes), uintptr(allBytes), uintptr(unsafe.Pointer(&ov)))
}
