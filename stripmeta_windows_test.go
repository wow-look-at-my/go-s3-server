//go:build windows

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// stripStoredMetadata removes an object's stored user metadata behind the
// metadata cache's back (see the unix counterpart). On Windows that metadata is
// the .meta JSON sidecar.
func stripStoredMetadata(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.Remove(path+".meta"))
}
