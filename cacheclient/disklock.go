package cacheclient

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"time"
)

// The disk cache has one file two processes write: the trim stamp. cmd/go
// guarded it with its own lockedfile package, which this module cannot import,
// so the same guarantee is built here from a lock file: the writer is whoever
// creates it, and a lock nobody has touched for lockHold is taken to belong to
// a process that died.

// lockHold is the age past which a lock file is treated as abandoned.
const lockHold = 10 * time.Second

// lockPoll is how often a waiter looks at a lock it did not get.
const lockPoll = 20 * time.Millisecond

// transformFile reads a file, hands its contents to change, and writes back
// what change answers. Only one process does this at a time. An error from
// change is returned as it stands, and the file keeps the bytes it had.
func transformFile(path string, change func([]byte) ([]byte, error)) error {
	release, err := takeLock(path + ".lock")
	if err != nil {
		return err
	}
	defer release()
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	next, err := change(data)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, next)
}

// takeLock creates the lock file, waiting for whoever holds it. A lock left by
// a process that died is removed once it is older than lockHold.
func takeLock(path string) (release func(), err error) {
	deadline := time.Now().Add(lockHold * 2)
	for {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
		if err == nil {
			file.Close()
			return func() { os.Remove(path) }, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		info, statErr := os.Stat(path)
		if statErr == nil && time.Since(info.ModTime()) > lockHold {
			os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			// The holder is alive and slow, or the clock moved. Either way the
			// caller's work is not worth blocking the build for.
			return nil, errors.New("cache: " + path + " is held by another process")
		}
		time.Sleep(lockPoll)
	}
}

// writeFileAtomic writes data to path through a temporary file and a rename.
func writeFileAtomic(path string, data []byte) error {
	temp, err := os.CreateTemp(dirOf(path), "tmp-")
	if err != nil {
		return err
	}
	_, writeErr := temp.Write(data)
	closeErr := temp.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		os.Remove(temp.Name())
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		os.Remove(temp.Name())
		return err
	}
	return nil
}

// dirOf is filepath.Dir without the import, since this file needs nothing else
// from that package.
func dirOf(path string) string {
	for idx := len(path) - 1; idx >= 0; idx-- {
		if os.IsPathSeparator(path[idx]) {
			return path[:idx]
		}
	}
	return "."
}

// isETXTBSY reports whether err says the file is a running program. A cache
// entry that a process is executing cannot be replaced on unix, and the go
// command treats that as a miss rather than a failure.
func isETXTBSY(err error) bool {
	return errors.Is(err, syscall.ETXTBSY)
}
