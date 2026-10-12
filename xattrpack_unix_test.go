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

// A put writes the protected keys one attribute each and every other key in
// a single packed attribute. A store costs a few xattr writes rather than
// one per key. Every key reads back.
func TestPutPacksOptionalMetadata(t *testing.T) {
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
	assert.ElementsMatch(t, []string{
		"user.s3.outputid", "user.s3.compression", "user.s3." + storedDigestMetaKey,
		packedMetaAttr, packedAuditAttr,
	}, attrs)

	got := &ObjectMeta{Metadata: map[string]string{}}
	getMetadata(path, got)
	for k, v := range meta {
		assert.Equal(t, v, got.Metadata[k], k)
	}
	assert.NotEmpty(t, got.Metadata[storedDigestMetaKey])
	assert.Equal(t, "someone", getAudit(path)["uploader"])
	assert.NotContains(t, got.Metadata, "packed", "the packed attribute is a container, not a key")
}

// An object stored before packing keeps a key per attribute, and still reads.
func TestPerKeyMetadataStillReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy")
	require.NoError(t, os.WriteFile(path, []byte("body"), 0o644))
	for k, v := range map[string]string{"outputid": "out", "pkg": "example.com/p"} {
		require.NoError(t, unix.Setxattr(path, metaAttrPrefix+k, []byte(v), 0))
	}
	require.NoError(t, unix.Setxattr(path, auditAttrPrefix+"uploader", []byte("old"), 0))

	got := &ObjectMeta{Metadata: map[string]string{}}
	getMetadata(path, got)
	assert.Equal(t, map[string]string{"outputid": "out", "pkg": "example.com/p"}, got.Metadata)
	assert.Equal(t, "old", getAudit(path)["uploader"])
}
