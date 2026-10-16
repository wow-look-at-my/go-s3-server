//go:build windows

package cacheclient

import "syscall"

// detachedProcess is DETACHED_PROCESS: the uploader gets no console.
const detachedProcess = 0x00000008

// detachAttrs starts the uploader with no console. And in a process group
// of its own, so a Ctrl+C to the build does not reach it.
func detachAttrs() []*syscall.SysProcAttr {
	return []*syscall.SysProcAttr{{CreationFlags: detachedProcess | syscall.CREATE_NEW_PROCESS_GROUP}}
}
