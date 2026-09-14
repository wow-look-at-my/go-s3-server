//go:build !windows

package main

import "golang.org/x/sys/unix"

const zfsSuperMagic = 0x2fc12fc1

// dirIsZFS reports whether dir lives on a ZFS filesystem. Cheap and
// dependency-free: a single statfs, no zfs tooling needed -- which matters
// because the expensive probes (which dataset, which properties) only run
// when this says yes.
func dirIsZFS(dir string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return false
	}
	return int64(st.Type) == zfsSuperMagic
}
