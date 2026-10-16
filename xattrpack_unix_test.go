//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A put writes every metadata and audit field in one attribute. Every key
// reads back, and no audit field reads as metadata.
func TestPutWritesOneAttribute(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("c", 64)
	meta := map[string]string{
		"outputid": "out", "compression": "zstd", "body-size": "10",
		"module": "example.com/m", "pkg": "example.com/m/p", "src": "a.go b.go",
		"go-version": "go1.27", "target": "linux/amd64", "created": "2026-10-12T00:00:00Z",
	}
	audit := map[string]string{"uploader": "someone", "client_ip": "10.0.0.1"}
	require.NoError(t, storage.Put(key, []byte("body"), meta, audit))

	path := storage.keyToPath(key)
	attrs, err := listXattrs(path)
	require.NoError(t, err)
	assert.Equal(t, []string{objAttr}, attrs)

	got := &ObjectMeta{Metadata: map[string]string{}}
	getMetadata(path, got)
	for key, val := range meta {
		assert.Equal(t, val, got.Metadata[key], key)
	}
	assert.NotEmpty(t, got.Metadata[storedDigestMetaKey])
	assert.NotContains(t, got.Metadata, "uploader")
	assert.Equal(t, "someone", getAudit(path)["uploader"])
	assert.Equal(t, "4", getAudit(path)["content_length"])
}

// A metadata merge keeps the fields it does not name, the audit fields too.
func TestSetMetadataMerges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obj")
	require.NoError(t, os.WriteFile(path, []byte("body"), 0o644))
	require.NoError(t, setObjectAttrs(path, map[string]string{"outputid": "a", "pkg": "p"}, map[string]string{"uploader": "u"}))
	require.NoError(t, setMetadata(path, map[string]string{"outputid": "b"}))

	got := &ObjectMeta{Metadata: map[string]string{}}
	getMetadata(path, got)
	assert.Equal(t, map[string]string{"outputid": "b", "pkg": "p"}, got.Metadata)
	assert.Equal(t, map[string]string{"uploader": "u"}, getAudit(path))
}

// A value that does not decode reads as no fields, and never panics.
func TestCorruptObjAttrReadsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obj")
	require.NoError(t, os.WriteFile(path, []byte("body"), 0o644))
	good := encodeObjAttrs(objAttrs{meta: map[string]string{"outputid": "abc"}})
	for _, bad := range [][]byte{{}, {9}, good[:len(good)-1], append(good, 0), {objAttrVersion, 0xff, 0xff, 0xff, 0xff, 0x0f, 0}} {
		require.NoError(t, unix.Setxattr(path, objAttr, bad, 0))
		got := &ObjectMeta{Metadata: map[string]string{}}
		getMetadata(path, got)
		assert.Empty(t, got.Metadata)
	}
}
