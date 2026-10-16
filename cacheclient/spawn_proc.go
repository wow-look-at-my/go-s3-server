//go:build !js && !wasip1

package cacheclient

import (
	"os"
	"os/exec"
	"strings"
)

// canSpawn is true: this platform starts processes.
const canSpawn = true

// spawnUploader starts this executable again, detached, on the spool. Its
// output goes to a log beside the spool. Nothing waits for it to finish.
func spawnUploader(spool string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(spool+".log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer logFile.Close()
	env := uploaderEnviron(spool)
	var startErr error
	for _, attr := range detachAttrs() {
		cmd := exec.Command(exe)
		cmd.Env = env
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		cmd.SysProcAttr = attr
		if startErr = cmd.Start(); startErr == nil {
			go cmd.Wait()
			return nil
		}
	}
	return startErr
}

// uploaderEnviron is this process's environment with the spool named and
// with no broker: the uploader serves no build.
func uploaderEnviron(spool string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, UploadSpoolEnv+"=") || strings.HasPrefix(kv, BrokerEnv+"=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, UploadSpoolEnv+"="+spool)
}
