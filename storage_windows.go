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
