//go:build !windows

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// stripStoredMetadata removes an object's stored user metadata behind the
// metadata cache's back, so a subsequent read can only succeed from the
// cache.
func stripStoredMetadata(t *testing.T, path string) {
	t.Helper()
	attrs := readObjAttrsPath(path)
	attrs.meta = nil
	require.NoError(t, unix.Setxattr(path, objAttr, encodeObjAttrs(attrs), 0))
}
