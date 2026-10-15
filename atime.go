package main

// Durable last-use tracking.

import (
	"fmt"
	"os"
	"time"
)

// atimeProbeAge backdates the probe file before the test read.
const atimeProbeAge = 48 * time.Hour

// atimeIsRecorded reports whether reading a file in dir advances its access
// time. The probe file carries the temp-file prefix. It is skipped by every
// walk of the data_dir and swept at the next startup even if this process dies
// mid-probe.
func atimeIsRecorded(dir string) (bool, error) {
	f, err := os.CreateTemp(dir, tempFilePrefix+"atime-*")
	if err != nil {
		return false, fmt.Errorf("create probe file: %w", err)
	}
	path := f.Name()
	defer os.Remove(path)

	_, err = f.Write([]byte("probe"))
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, fmt.Errorf("write probe file: %w", err)
	}

	backdated := time.Now().Add(-atimeProbeAge)
	if err := os.Chtimes(path, backdated, backdated); err != nil {
		return false, fmt.Errorf("backdate probe file: %w", err)
	}

	if _, err := os.ReadFile(path); err != nil {
		return false, fmt.Errorf("read probe file: %w", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("stat probe file: %w", err)
	}
	// Anything past the backdate means the read was recorded.
	return fileAccessTime(info).After(backdated.Add(time.Hour)), nil
}

// lastUsedUnix is when an object was last used. This covers the later of its
// write time, the filesystem's access time, and any access this process
// recorded in memory.
func lastUsedUnix(obj ListObject, memAccess int64) int64 {
	used := obj.LastModified.Unix()
	if !obj.LastAccess.IsZero() {
		if at := obj.LastAccess.Unix(); at > used {
			used = at
		}
	}
	if memAccess > used {
		used = memAccess
	}
	return used
}
