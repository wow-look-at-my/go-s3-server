//go:build windows

package main

// dirIsZFS is always false on Windows: there is no ZFS mount to detect, so the
// compression advisory never fires there.
func dirIsZFS(string) bool { return false }
