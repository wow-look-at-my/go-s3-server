//go:build !linux && !windows

package main

import (
	"io/fs"
	"time"
)

// fileAccessTime has no portable form: the access time lives in a
// platform-specific field of what info.Sys() returns.
func fileAccessTime(fs.FileInfo) time.Time {
	return time.Time{}
}
