//go:build !windows

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// stripStoredMetadata removes an object's stored user metadata behind the
// metadata cache's back, so a subsequent read can only succeed from the cache.
// Nothing in the server does this -- it exists to prove the cache is consulted.
func stripStoredMetadata(t *testing.T, path string) {
	t.Helper()
	attrs, err := listXattrs(path)
	require.NoError(t, err)
	for _, a := range attrs {
		if isUserMetaAttr(a) {
			require.NoError(t, unix.Removexattr(path, a))
		}
	}
}
