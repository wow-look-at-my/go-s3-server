//go:build unix && !cosmo

package cacheclient

import "syscall"

// detachAttrs puts the uploader in a session of its own, so no signal to the
// build's terminal or process group reaches it.
func detachAttrs() []*syscall.SysProcAttr {
	return []*syscall.SysProcAttr{{Setsid: true}}
}
