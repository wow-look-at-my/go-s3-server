//go:build !windows

// metadataProtectedKeys are load-bearing for the cache protocol.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/wow-look-at-my/go-containers/set"
	"golang.org/x/sys/unix"
)

// metadataProtectedKeys are load-bearing for the cache protocol. A write never drops them to make room.
var metadataProtectedKeys = set.Of("outputid", "compression", storedDigestMetaKey)

// objAttr holds all of an object's metadata and audit fields.
const objAttr = "user.s3.obj"

// objAttrVersion is the first byte of the objAttr value.
const objAttrVersion = 1

// objAttrs is the decoded objAttr value.
type objAttrs struct {
	meta  map[string]string
	audit map[string]string
}

func appendKV(buf []byte, kv map[string]string) []byte {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf = binary.AppendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
		buf = binary.AppendUvarint(buf, uint64(len(kv[k])))
		buf = append(buf, kv[k]...)
	}
	return buf
}

// encodeObjAttrs writes the version, both pair counts, then each pair as a
// uvarint-length key and a uvarint-length value.
func encodeObjAttrs(a objAttrs) []byte {
	buf := make([]byte, 0, 256)
	buf = append(buf, objAttrVersion)
	buf = binary.AppendUvarint(buf, uint64(len(a.meta)))
	buf = binary.AppendUvarint(buf, uint64(len(a.audit)))
	buf = appendKV(buf, a.meta)
	return appendKV(buf, a.audit)
}

var errObjAttrCorrupt = errors.New("object attribute does not decode")

func readUvarintString(buf []byte) (string, []byte, error) {
	size, width := binary.Uvarint(buf)
	if width <= 0 || uint64(len(buf)-width) < size {
		return "", nil, errObjAttrCorrupt
	}
	end := width + int(size)
	return string(buf[width:end]), buf[end:], nil
}

func decodePairs(buf []byte, count uint64) (map[string]string, []byte, error) {
	out := make(map[string]string, count)
	for range count {
		key, rest, err := readUvarintString(buf)
		if err != nil {
			return nil, nil, err
		}
		val, rest, err := readUvarintString(rest)
		if err != nil {
			return nil, nil, err
		}
		out[key] = val
		buf = rest
	}
	return out, buf, nil
}

func decodeObjAttrs(buf []byte) (objAttrs, error) {
	if len(buf) == 0 || buf[0] != objAttrVersion {
		return objAttrs{}, errObjAttrCorrupt
	}
	buf = buf[1:]
	nMeta, width := binary.Uvarint(buf)
	if width <= 0 {
		return objAttrs{}, errObjAttrCorrupt
	}
	buf = buf[width:]
	nAudit, width := binary.Uvarint(buf)
	if width <= 0 {
		return objAttrs{}, errObjAttrCorrupt
	}
	buf = buf[width:]
	// Each pair takes a couple of bytes or more, which bounds a corrupt count.
	if nMeta > uint64(len(buf)) || nAudit > uint64(len(buf)) || (nMeta+nAudit)*2 > uint64(len(buf)) {
		return objAttrs{}, errObjAttrCorrupt
	}
	meta, buf, err := decodePairs(buf, nMeta)
	if err != nil {
		return objAttrs{}, err
	}
	audit, buf, err := decodePairs(buf, nAudit)
	if err != nil {
		return objAttrs{}, err
	}
	if len(buf) != 0 {
		return objAttrs{}, errObjAttrCorrupt
	}
	return objAttrs{meta: meta, audit: audit}, nil
}

func isNoSpace(err error) bool {
	return errors.Is(err, unix.E2BIG) || errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT)
}

func wrapObjErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("set xattr %s: %w", objAttr, err)
}

// writeObjAttrs stores attrs with setter. When the value does not fit, it
// drops the audit fields first, then the longest optional metadata value, one
// at a time. It never drops a protected key. Each drop is counted and logged.
func writeObjAttrs(where string, attrs objAttrs, setter func([]byte) error) error {
	err := setter(encodeObjAttrs(attrs))
	if !isNoSpace(err) {
		return wrapObjErr(err)
	}
	var dropped []string
	if len(attrs.audit) > 0 {
		for key := range attrs.audit {
			dropped = append(dropped, "audit."+key)
		}
		attrs.audit = nil
		err = setter(encodeObjAttrs(attrs))
	}
	meta := make(map[string]string, len(attrs.meta))
	for key, val := range attrs.meta {
		meta[key] = val
	}
	attrs.meta = meta
	for isNoSpace(err) {
		longest := ""
		for key, val := range meta {
			if !metadataProtectedKeys.Contains(key) && (longest == "" || len(val) > len(meta[longest])) {
				longest = key
			}
		}
		if longest == "" {
			break
		}
		delete(meta, longest)
		dropped = append(dropped, longest)
		err = setter(encodeObjAttrs(attrs))
	}
	if len(dropped) > 0 {
		metadataXattrsDroppedTotal.Add(float64(len(dropped)))
		log.Printf("metadata: dropped oversized field(s) %v for %s (xattr space exhausted; object stored without them)", dropped, where)
	}
	return wrapObjErr(err)
}

func pathSetter(path string) func([]byte) error {
	return func(val []byte) error { return unix.Setxattr(path, objAttr, val, 0) }
}

func fdSetter(f *os.File) func([]byte) error {
	return func(val []byte) error { return unix.Fsetxattr(int(f.Fd()), objAttr, val, 0) }
}

func emptyObjAttrs() objAttrs {
	return objAttrs{meta: map[string]string{}, audit: map[string]string{}}
}

// readObjAttrs decodes the value getter returns. An absent or corrupt value
// reads as empty maps. The read logs a corrupt value.
func readObjAttrs(where string, getter func([]byte) (int, error)) objAttrs {
	var stack [xattrValueBufSize]byte
	buf := stack[:]
	size, err := getter(buf)
	if errors.Is(err, unix.ERANGE) {
		size, err = getter(nil)
		if err == nil {
			buf = make([]byte, size)
			size, err = getter(buf)
		}
	}
	if err != nil {
		return emptyObjAttrs()
	}
	attrs, err := decodeObjAttrs(buf[:size])
	if err != nil {
		log.Printf("metadata: %s on %s: %v", objAttr, where, err)
		return emptyObjAttrs()
	}
	return attrs
}

func readObjAttrsPath(path string) objAttrs {
	return readObjAttrs(path, func(buf []byte) (int, error) { return unix.Getxattr(path, objAttr, buf) })
}

func readObjAttrsFd(f *os.File) objAttrs {
	return readObjAttrs(f.Name(), func(buf []byte) (int, error) { return unix.Fgetxattr(int(f.Fd()), objAttr, buf) })
}

// setObjectAttrs writes a new object's metadata and audit fields in one
// setxattr. It replaces what the file held.
func setObjectAttrs(path string, meta, audit map[string]string) error {
	return writeObjAttrs(path, objAttrs{meta: meta, audit: audit}, pathSetter(path))
}

// setMetadata merges meta into the object's stored metadata.
func setMetadata(path string, meta map[string]string) error {
	attrs := readObjAttrsPath(path)
	for key, val := range meta {
		attrs.meta[key] = val
	}
	return writeObjAttrs(path, attrs, pathSetter(path))
}

// setMetadataFd merges meta into the stored metadata of the inode f holds.
func setMetadataFd(f *os.File, meta map[string]string) error {
	attrs := readObjAttrsFd(f)
	for key, val := range meta {
		attrs.meta[key] = val
	}
	return writeObjAttrs(f.Name(), attrs, fdSetter(f))
}

// getMetadataValueFd reads one metadata value from the inode f holds ("" if
// absent or unreadable).
func getMetadataValueFd(f *os.File, key string) string {
	return readObjAttrsFd(f).meta[key]
}

func getMetadata(path string, meta *ObjectMeta) {
	for key, val := range readObjAttrsPath(path).meta {
		meta.Metadata[key] = val
	}
}

const (
	xattrValueBufSize = 1024
	xattrNameBufSize  = 1024
)

func listXattrs(path string) ([]string, error) {
	var stack [xattrNameBufSize]byte
	buf := stack[:]
	sz, err := unix.Listxattr(path, buf)
	if errors.Is(err, unix.ERANGE) {
		sz, err = unix.Listxattr(path, nil)
		if err != nil {
			return nil, err
		}
		buf = make([]byte, sz)
		sz, err = unix.Listxattr(path, buf)
	}
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
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
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	size, err = unix.Getxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:size], nil
}

// Sidecar hooks: metadata lives in xattrs on unix, so there are no companion files to move, remove.

func finalizeSidecars(tmpPath, path string) error { return nil }

func removeSidecars(path string) {}

func isSidecarName(name string) bool { return false }

// originalKeyAttr is outside the "user.s3." namespace: it is not an object field.
const originalKeyAttr = "user.s3originalkey"

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

// setAudit merges audit into the object's stored audit fields. Audit fields
// are never served as metadata.
func setAudit(path string, audit map[string]string) error {
	if len(audit) == 0 {
		return nil
	}
	attrs := readObjAttrsPath(path)
	for key, val := range audit {
		attrs.audit[key] = val
	}
	return writeObjAttrs(path, attrs, pathSetter(path))
}

func getAudit(path string) map[string]string {
	return readObjAttrsPath(path).audit
}
