package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openStored(t *testing.T, s *Storage, key string) *os.File {
	t.Helper()
	f, _, err := s.OpenBody(key)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// A body with an outputid is hashed once per file version. A new version is
// hashed again, so a rewrite that no longer matches is still refused.
func TestDigestMemoSkipsOnlyAVerifiedVersion(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("d", 64)
	body := []byte("a body with an outputid")
	require.NoError(t, storage.PutStream(key, bytes.NewReader(body), map[string]string{outputIDMetaKey: "abc"}, nil))
	meta, err := storage.Stat(key)
	require.NoError(t, err)
	hash, ok := extractActionHash(key)
	require.True(t, ok)

	ok, err = storage.verifyStoredDigestFor(key, openStored(t, storage, key), meta.Metadata)
	require.NoError(t, err)
	require.True(t, ok)
	_, memoized := storage.digestOK.Get(hash)
	assert.True(t, memoized, "a verified body with an outputid is memoized")

	// Another version of the file: other bytes and a later mtime.
	path := storage.keyToPath(key)
	require.NoError(t, os.WriteFile(path, []byte("other bytes, same key"), 0o644))
	later := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(path, later, later))
	ok, err = storage.verifyStoredDigestFor(key, openStored(t, storage, key), meta.Metadata)
	require.NoError(t, err)
	assert.False(t, ok, "a new file version is hashed again")
}

// A body with no outputid is hashed on every read. Self-heal mints its
// outputid from the bytes on disk, so nothing downstream would catch rot.
func TestDigestMemoSkipsNoBodyWithoutAnOutputID(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("e", 64)
	require.NoError(t, storage.PutStream(key, bytes.NewReader([]byte("no outputid")), map[string]string{}, nil))
	meta, err := storage.Stat(key)
	require.NoError(t, err)

	ok, err := storage.verifyStoredDigestFor(key, openStored(t, storage, key), meta.Metadata)
	require.NoError(t, err)
	require.True(t, ok)
	hash, _ := extractActionHash(key)
	_, memoized := storage.digestOK.Get(hash)
	assert.False(t, memoized)
}

// A put or a delete forgets the verdict.
func TestDigestMemoForgetsOnDelete(t *testing.T) {
	_, storage := testSetupWithStorage(t)
	key := "go-buildcache/v1" + strings.Repeat("f", 64)
	require.NoError(t, storage.PutStream(key, bytes.NewReader([]byte("body")), map[string]string{outputIDMetaKey: "abc"}, nil))
	meta, err := storage.Stat(key)
	require.NoError(t, err)
	_, err = storage.verifyStoredDigestFor(key, openStored(t, storage, key), meta.Metadata)
	require.NoError(t, err)
	require.NoError(t, storage.Delete(key))
	hash, _ := extractActionHash(key)
	_, memoized := storage.digestOK.Get(hash)
	assert.False(t, memoized)
}
