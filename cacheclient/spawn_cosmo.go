//go:build cosmo

// spawn_cosmo.go holds the spawn cosmo code.

package cacheclient

import "syscall"

// detachAttrs gives the uploader a session of its own.
func detachAttrs() []*syscall.SysProcAttr {
	return []*syscall.SysProcAttr{{Setsid: true}, {Setpgid: true}}
}
