//go:build windows

package main

import (
	"encoding/json"
	"os"
)

func lockExclusive(f *os.File) error {
	// On Windows, use LockFileEx via syscall for exclusive locking.
	// For simplicity, we use a zero-byte range lock which is sufficient.
	return lockFileWindows(f)
}

func unlockFile(f *os.File) {
	unlockFileWindows(f)
}

func setMetadata(path string, meta map[string]string) error {
	if len(meta) == 0 {
		return nil
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(path+".meta", data, 0644)
}

func getMetadata(path string, meta *ObjectMeta) {
	data, err := os.ReadFile(path + ".meta")
	if err != nil {
		return
	}
	json.Unmarshal(data, &meta.Metadata)
}

// setMetadataFd: Windows metadata lives in a path-keyed JSON sidecar, so there
// is no true fd-scoped write; this delegates to the path variant via the open
// file's name. The unix build is where the fd-based race-free semantics hold.
func setMetadataFd(f *os.File, meta map[string]string) error {
	return setMetadata(f.Name(), meta)
}

// getMetadataValueFd reads one metadata value via the sidecar of the open
// file's path ("" if absent).
func getMetadataValueFd(f *os.File, key string) string {
	meta := &ObjectMeta{Metadata: map[string]string{}}
	getMetadata(f.Name(), meta)
	return meta.Metadata[key]
}

const originalKeyFile = ".originalkey"

func setOriginalKey(path, key string) error {
	return os.WriteFile(path+"."+originalKeyFile, []byte(key), 0644)
}

func getOriginalKey(path string) (string, error) {
	data, err := os.ReadFile(path + "." + originalKeyFile)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func setAudit(path string, audit map[string]string) error {
	if len(audit) == 0 {
		return nil
	}
	data, err := json.Marshal(audit)
	if err != nil {
		return err
	}
	return os.WriteFile(path+".audit", data, 0644)
}

func getAudit(path string) map[string]string {
	data, err := os.ReadFile(path + ".audit")
	if err != nil {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}
