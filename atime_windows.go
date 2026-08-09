package main

import (
	"io/fs"
	"syscall"
	"time"
)

// fileAccessTime reads the access time out of a stat the caller already did, so
// the walk that feeds eviction pays no extra syscall for it. NTFS can be
// configured not to maintain last-access times at all; the startup probe in
// atime.go is what decides whether this value can be trusted.
func fileAccessTime(info fs.FileInfo) time.Time {
	d, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return time.Time{}
	}
	return time.Unix(0, d.LastAccessTime.Nanoseconds())
}
