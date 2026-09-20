package cachedisk

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"time"
)

// lockHold is the age at which a lock belonged to a process that died.
const lockHold = 10 * time.Second

// lockPoll is how often a waiter looks at a lock it did not get.
const lockPoll = 20 * time.Millisecond

// transformFile reads a file, hands its contents to change, and writes back
// what change answers. a single process does this at a time, through a lock
// file whose writer is whoever creates it, because cmd/go's own lockedfile
// package is unreachable from this module. An error from change is returned
// as it stands, and the file keeps its bytes.
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
// a process that died is removed a single time it is older than lockHold.
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
		// The holder is alive and slow, or the clock moved. Neither is worth
		// blocking the build for.
		if time.Now().After(deadline) {
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

// dirOf is filepath.Dir without the import.
func dirOf(path string) string {
	for idx := len(path) - 1; idx >= 0; idx-- {
		if os.IsPathSeparator(path[idx]) {
			return path[:idx]
		}
	}
	return "."
}

// isETXTBSY reports whether a running program holds the file, which unix
// refuses to replace. That is a miss rather than a failure.
func isETXTBSY(err error) bool {
	return errors.Is(err, syscall.ETXTBSY)
}
