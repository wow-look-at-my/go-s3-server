package main

// Durable last-use tracking.
//
// Eviction drops the least recently USED entries, and the only record of a read
// that outlives the process is the filesystem's access time. The kernel
// advances it whenever a body is read; under the default relatime it moves at
// most once a day, which is the resolution a multi-day eviction window needs,
// and -- unlike the in-memory access map, which starts empty on every restart
// -- it survives restarts. Without it a hot object written months ago looks
// idle to the first sweep after a restart and is evicted while still in use.
//
// Not every filesystem records it (noatime mounts, platforms whose file info
// carries no access time), so the server probes the actual data_dir at startup
// rather than inferring from mount options, which overlayfs, bind mounts and
// NFS all make unreliable: write a throwaway file, backdate it, read it, and
// see whether the access time moved. When it did not, the server falls back to
// tracking access in memory and says so.

import (
	"fmt"
	"os"
	"time"
)

// atimeProbeAge backdates the probe file before the test read. Any backdate
// satisfies relatime (which updates when mtime or ctime is not older than
// atime); exceeding relatime's one-day window as well means the probe does not
// depend on which of those rules the kernel applies.
const atimeProbeAge = 48 * time.Hour

// atimeIsRecorded reports whether reading a file in dir advances its access
// time. The probe file carries the temp-file prefix, so it is skipped by every
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
	// Anything past the backdate means the read was recorded; the read happened
	// just now, so the margin only has to exclude clock jitter.
	return fileAccessTime(info).After(backdated.Add(time.Hour)), nil
}

// lastUsedUnix is when an object was last used: the later of its write time,
// the filesystem's access time, and any access this process recorded in memory.
// Each source can be missing, and taking the maximum means a missing one only
// ever makes an entry look older, never younger.
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
