package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheVersionPurgesOnMissingMarker simulates an existing cache with no
// version marker (i.e. one populated by a pre-versioning release, which could
// include content written during the auth-bypass exposure window).
// NewStorage must purge it and write the current version marker.
func TestCacheVersionPurgesOnMissingMarker(t *testing.T) {
	dir := t.TempDir()

	// Pretend there's a cache already here, with no .cache_version file.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "go-buildcache", "v1", "aa"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go-buildcache", "v1", "aa", "bbccdd11223344"), []byte("possibly-poisoned"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "loose-file"), []byte("junk"), 0644))

	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// Pre-existing contents must be gone.
	_, err = os.Stat(filepath.Join(dir, "go-buildcache", "v1", "aa", "bbccdd11223344"))
	assert.True(t, os.IsNotExist(err), "old cache file should have been purged")
	_, err = os.Stat(filepath.Join(dir, "loose-file"))
	assert.True(t, os.IsNotExist(err), "loose file should have been purged")

	// Version marker must now reflect the current version.
	data, err := os.ReadFile(filepath.Join(dir, cacheVersionFile))
	require.NoError(t, err)
	v, err := strconv.Atoi(string(data[:len(data)-1])) // trim trailing newline
	require.NoError(t, err)
	assert.Equal(t, currentCacheVersion, v)
}

// TestCacheVersionLeavesCurrentAlone ensures that a data_dir already on the
// current version is not purged — routine restarts must not wipe the cache.
func TestCacheVersionLeavesCurrentAlone(t *testing.T) {
	dir := t.TempDir()

	// First bring the dir to current version.
	s1, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)

	// Write an object.
	key := "go-buildcache/v1aabb000000000001"
	content := []byte("keep me")
	require.NoError(t, s1.Put(key, content, nil, nil))
	require.NoError(t, s1.Close())

	// Re-open — should preserve the object.
	s2, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })

	got, _, err := s2.Get(key)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

// TestCacheVersionPurgesOnMismatch simulates a stored version that is not
// the current version — a downgrade or an operator-set marker — and verifies
// the purge happens.
func TestCacheVersionPurgesOnMismatch(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, cacheVersionFile), []byte("999\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stray"), []byte("x"), 0644))

	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	_, err = os.Stat(filepath.Join(dir, "stray"))
	assert.True(t, os.IsNotExist(err), "stray file should have been purged")

	data, err := os.ReadFile(filepath.Join(dir, cacheVersionFile))
	require.NoError(t, err)
	v, err := strconv.Atoi(string(data[:len(data)-1]))
	require.NoError(t, err)
	assert.Equal(t, currentCacheVersion, v)
}

// TestCacheVersionCorruptMarker ensures a corrupt marker file is an error
// that refuses to start, not a silent purge — something is wrong and the
// operator should look at it.
func TestCacheVersionCorruptMarker(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, cacheVersionFile), []byte("not-an-int"), 0644))

	_, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.Error(t, err)
}
