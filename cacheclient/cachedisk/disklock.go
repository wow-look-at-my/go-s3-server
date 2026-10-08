package cachedisk

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/wow-look-at-my/go-ipc/filelock"
)

// lockWait bounds how long transformFile waits for another process's lock.
const lockWait = 20 * time.Second

// transformFile reads a file, hands its contents to change, and writes back
// what change answers. A single process does this at a time, through an OS
// lock on path+".lock", because cmd/go's own lockedfile package is
// unreachable from this module. An error from change is returned as it
// stands, and the file keeps its bytes.
func transformFile(path string, change func([]byte) ([]byte, error)) error {
	ctx, cancel := context.WithTimeout(context.Background(), lockWait)
	defer cancel()
	unlock, err := filelock.Lock(ctx, path+".lock")
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("cache: " + path + ".lock is held by another process")
	}
	if err != nil {
		return err
	}
	defer unlock()
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
