//go:build !linux && !windows

package main

import (
	"io/fs"
	"time"
)

// fileAccessTime has no portable form: the access time lives in a
// platform-specific field of what info.Sys() returns. Reporting the zero time
// makes the startup probe fail cleanly, and the server then tracks access in
// memory and logs that last use does not survive a restart here.
func fileAccessTime(fs.FileInfo) time.Time {
	return time.Time{}
}
