//go:build !windows

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func lockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// metadataProtectedKeys are load-bearing for the cache protocol: outputid is
// the content address every client verifies before consuming a body, and
// compression steers both the module-index guards and client decompression.
// A failure persisting one of these fails the PUT — storing the object
// without them would serve unusable (or unguardable) bytes. Every other
// metadata key is descriptive provenance (src, pkg, go-version, ...).
var metadataProtectedKeys = []string{"outputid", "compression"}

// setMetadata persists user metadata as xattrs. Protected keys are written
// first (so they claim xattr space) and any error on them fails the call.
// Optional keys degrade gracefully under xattr-space pressure: on ext4
// without the ea_inode feature ALL of a file's xattrs share one ~4 KiB EA
// block, and the client's Src header is an uncapped list of source file
// names, so a many-file package can overflow it. Failing the whole PUT for
// that (the old behavior: any xattr error → 500) made exactly the biggest
// packages permanently uncacheable. Instead the oversized optional key is
// dropped, counted, and logged — the object stores and serves normally,
// minus one provenance field.
func setMetadata(path string, meta map[string]string) error {
	for _, k := range metadataProtectedKeys {
		if v, ok := meta[k]; ok {
			attrName := "user.s3." + k
			if err := unix.Setxattr(path, attrName, []byte(v), 0); err != nil {
				return fmt.Errorf("set xattr %s: %w", attrName, err)
			}
		}
	}

	optional := make([]string, 0, len(meta))
	for k := range meta {
		if !slices.Contains(metadataProtectedKeys, k) {
			optional = append(optional, k)
		}
	}
	sort.Strings(optional) // deterministic write order under space pressure

	var dropped []string
	for _, k := range optional {
		attrName := "user.s3." + k
		err := unix.Setxattr(path, attrName, []byte(meta[k]), 0)
		if err == nil {
			continue
		}
		if errors.Is(err, unix.E2BIG) || errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT) {
			dropped = append(dropped, k)
			continue
		}
		return fmt.Errorf("set xattr %s: %w", attrName, err)
	}
	if len(dropped) > 0 {
		metadataXattrsDroppedTotal.Add(float64(len(dropped)))
		log.Printf("metadata: dropped oversized xattr(s) %v for %s (xattr space exhausted; object stored without them)", dropped, path)
	}
	return nil
}

// setMetadataFd writes user-metadata xattrs through an open file descriptor
// rather than a path. This is the race-free variant for repairs computed FROM
// that descriptor: a path-based setxattr can land on a different inode than
// the one that was hashed (a concurrent overwrite PUT renames a new file onto
// the path in between), stamping a stale value onto a fresh body; fsetxattr
// by construction stamps the exact inode the caller read.
func setMetadataFd(f *os.File, meta map[string]string) error {
	for k, v := range meta {
		attrName := "user.s3." + k
		if err := unix.Fsetxattr(int(f.Fd()), attrName, []byte(v), 0); err != nil {
			return fmt.Errorf("fset xattr %s: %w", attrName, err)
		}
	}
	return nil
}

// getMetadataValueFd reads one user-metadata value through an open file
// descriptor ("" if absent or unreadable). Fd-based for the same reason as
// setMetadataFd: it inspects the exact inode the caller holds.
func getMetadataValueFd(f *os.File, key string) string {
	attrName := "user.s3." + key
	sz, err := unix.Fgetxattr(int(f.Fd()), attrName, nil)
	if err != nil || sz == 0 {
		return ""
	}
	buf := make([]byte, sz)
	n, err := unix.Fgetxattr(int(f.Fd()), attrName, buf)
	if err != nil {
		return ""
	}
	return string(buf[:n])
}

func getMetadata(path string, meta *ObjectMeta) {
	attrs, err := listXattrs(path)
	if err != nil {
		return
	}
	for _, attr := range attrs {
		if strings.HasPrefix(attr, "user.s3.") {
			val, err := getXattr(path, attr)
			if err == nil {
				metaKey := strings.TrimPrefix(attr, "user.s3.")
				meta.Metadata[metaKey] = string(val)
			}
		}
	}
}

func listXattrs(path string) ([]string, error) {
	sz, err := unix.Listxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	var attrs []string
	for _, name := range strings.Split(string(buf[:sz]), "\x00") {
		if name != "" {
			attrs = append(attrs, name)
		}
	}
	return attrs, nil
}

func getXattr(path, name string) ([]byte, error) {
	sz, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, sz)
	_, err = unix.Getxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

const originalKeyAttr = "user.s3.originalkey"

func setOriginalKey(path, key string) error {
	if err := unix.Setxattr(path, originalKeyAttr, []byte(key), 0); err != nil {
		return fmt.Errorf("set xattr %s: %w", originalKeyAttr, err)
	}
	return nil
}

func getOriginalKey(path string) (string, error) {
	val, err := getXattr(path, originalKeyAttr)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Server-managed audit attributes are stored under a distinct prefix so that
// user-supplied S3 metadata (user.s3.*) can never spoof them.
const auditAttrPrefix = "user.s3audit."

func setAudit(path string, audit map[string]string) error {
	for k, v := range audit {
		attrName := auditAttrPrefix + k
		if err := unix.Setxattr(path, attrName, []byte(v), 0); err != nil {
			return fmt.Errorf("set xattr %s: %w", attrName, err)
		}
	}
	return nil
}

func getAudit(path string) map[string]string {
	attrs, err := listXattrs(path)
	if err != nil {
		return nil
	}
	out := make(map[string]string)
	for _, attr := range attrs {
		if strings.HasPrefix(attr, auditAttrPrefix) {
			val, err := getXattr(path, attr)
			if err == nil {
				out[strings.TrimPrefix(attr, auditAttrPrefix)] = string(val)
			}
		}
	}
	return out
}
