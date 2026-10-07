package main

import (
	"io/fs"
	"syscall"
	"time"
)

// fileAccessTime reads the access time out of a stat the caller already did,
// so the walk that feeds eviction pays no extra syscall for it.
func fileAccessTime(info fs.FileInfo) time.Time {
	d, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return time.Time{}
	}
	return time.Unix(0, d.LastAccessTime.Nanoseconds())
}
