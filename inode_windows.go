// Size and mtime still name the file version.

package main

import "os"

// Size and mtime still name the file version.
func inodeOf(os.FileInfo) uint64 { return 0 }
