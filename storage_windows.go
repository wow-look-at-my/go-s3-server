//go:build windows

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// sidecarSuffixes are the per-object companion files that hold metadata on
// Windows (where the unix build uses xattrs): user metadata, audit fields,
// and the original-key record for hashed keys. Every operation that moves or
// removes an object's body must move/remove these alongside it.
var sidecarSuffixes = []string{".meta", ".audit", "." + originalKeyFile}

// finalizeSidecars renames an object's sidecar files from the temp path to
// the final path, after the body rename. PutStream writes metadata against
// the TEMP file; without this, the body rename orphaned every sidecar under
// its .tmp-* name — served objects had no metadata at all (no outputid → the
// self-heal misses them) and the orphans leaked until the startup sweep.
func finalizeSidecars(tmpPath, path string) error {
	for _, suffix := range sidecarSuffixes {
		src := tmpPath + suffix
		if _, err := os.Stat(src); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // this sidecar was never written (e.g. no audit)
			}
			return fmt.Errorf("stat sidecar %s: %w", src, err)
		}
		dst := path + suffix
		// Windows os.Rename replaces an existing destination file.
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("rename sidecar %s: %w", src, err)
		}
	}
	return nil
}

// removeSidecars deletes an object's sidecar files (best-effort; a missing
// sidecar is normal). Delete and eviction previously removed only the body,
// leaking every sidecar that had landed.
func removeSidecars(path string) {
	for _, suffix := range sidecarSuffixes {
		os.Remove(path + suffix)
	}
}

// isSidecarName reports whether a directory entry is a metadata sidecar
// rather than an object body, so List (and thus the index rebuild and
// eviction) does not misread sidecars as stored objects.
func isSidecarName(name string) bool {
	for _, suffix := range sidecarSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func lockExclusive(f *os.File) error {
	// On Windows, use LockFileEx via syscall for exclusive locking.
	// For simplicity, we use a zero-byte range lock which is sufficient.
	return lockFileWindows(f)
}

func unlockFile(f *os.File) {
	unlockFileWindows(f)
}

// setMetadata persists user metadata in the object's JSON sidecar. It
// READ-MERGES-WRITES: only the given keys are added/overwritten and every
// other stored key survives. The old whole-file replace meant any partial
// update (e.g. the self-heal stamping just the outputid) silently destroyed
// the object's other metadata (compression, provenance).
func setMetadata(path string, meta map[string]string) error {
	if len(meta) == 0 {
		return nil
	}
	merged := map[string]string{}
	if data, err := os.ReadFile(path + ".meta"); err == nil {
		json.Unmarshal(data, &merged)
	}
	for k, v := range meta {
		merged[k] = v
	}
	data, err := json.Marshal(merged)
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
