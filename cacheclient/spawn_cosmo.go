//go:build cosmo

package cacheclient

import "syscall"

// detachAttrs gives the uploader a session of its own.
func detachAttrs() []*syscall.SysProcAttr {
	return []*syscall.SysProcAttr{{Setsid: true}, {Setpgid: true}}
}
