//go:build linux

package main

import (
	"io/fs"
	"syscall"
	"time"
)

// fileAccessTime reads the access time out of a stat the caller already did, so
// the walk that feeds eviction pays no extra syscall for it.
func fileAccessTime(info fs.FileInfo) time.Time {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return time.Time{}
	}
	return time.Unix(st.Atim.Sec, st.Atim.Nsec)
}
