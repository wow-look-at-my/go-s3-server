package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// bodyPath finds the thing stored body under dir, so a test can corrupt it
// the way a failing disk would: same name, same length, different bytes.
func bodyPath(t *testing.T, dir string, size int) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), tempFilePrefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if int(info.Size()) == size {
			found = path
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, found, "no stored body of %d bytes under %s", size, dir)
	return found
}

// Every stored object carries a digest of its own bytes, whatever its key and
// whatever the uploader sent: the server computes a single when the upload
// names none. That is what makes the stamp a property of the cache rather
// than of a current client.
func TestPutStream_StampsDigestOnEveryObject(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	body := []byte("an object under a key the cache protocol says nothing about")
	require.NoError(t, s.Put("misc/arbitrary-key", body, nil, nil))

	meta, err := s.Stat("misc/arbitrary-key")
	require.NoError(t, err)
	sum := sha256.Sum256(body)
	require.Equal(t, hex.EncodeToString(sum[:]), meta.Metadata[storedDigestMetaKey])
}

// An upload whose bytes disagree with the digest its own metadata claims is
// refused outright. Storing it would hand the next reader a body to discover
// is wrong.
func TestPutStream_RefusesClaimedDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	meta := map[string]string{storedDigestMetaKey: strings.Repeat("00", sha256.Size)}
	err = s.Put("misc/wrong-digest", []byte("these are not those bytes"), meta, nil)
	require.ErrorIs(t, err, ErrStoredDigestMismatch)

	_, statErr := s.Stat("misc/wrong-digest")
	require.ErrorIs(t, statErr, ErrNotFound, "a refused upload must leave nothing behind")
}

// The same refusal over HTTP, where the digest arrives as a metadata header:
// the client's claim and the bytes it sent disagree, so this is its request
// that is wrong, not the server.
func TestPutObject_RefusesDigestMismatch(t *testing.T) {
	ts := testSetup(t)

	key := "/testbucket/misc/mismatched"
	hdr := map[string]string{"X-Cache-Meta-Storedsha256": strings.Repeat("11", sha256.Size)}
	resp := doRequest(t, ts, "PUT", key, []byte("a body that hashes to something else"), hdr)
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "invalid_request", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "the refused body must not have been stored")
	resp.Body.Close()
}

// A GET echoes the digest, so a reader that wants to check the bytes it just
// received has the value to check them against.
func TestGetObject_EmitsStoredDigest(t *testing.T) {
	ts := testSetup(t)

	body := []byte("a body stored with no digest claimed for it")
	key := "/testbucket/misc/echoed"
	resp := doRequest(t, ts, "PUT", key, body, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	sum := sha256.Sum256(body)
	require.Equal(t, hex.EncodeToString(sum[:]), resp.Header.Get("X-Cache-Meta-Storedsha256"))
}

// A key outside the cacheprog pattern has no reader that verifies it, so the
// server is the only thing standing between rot on disk and a consumer. A body
// that stopped matching its digest is refused and evicted, and the next
// uploader replaces it.
func TestGetObject_EvictsBodyThatStoppedMatchingItsDigest(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	body := []byte("bytes that a failing disk will change under us")
	key := "misc/rots"
	require.NoError(t, s.Put(key, body, nil, nil))

	// Same length, different content: the size and the metadata still agree, so
	// only the digest can tell.
	path := bodyPath(t, dir, len(body))
	rotted := append([]byte(nil), body...)
	rotted[0] ^= 0xff
	require.NoError(t, os.WriteFile(path, rotted, 0644))

	req := httptest.NewRequest("GET", "/testbucket/"+key, nil)
	rec := httptest.NewRecorder()
	handleGetObject(rec, req, s, key, nil)
	require.Equal(t, 404, rec.Code, "a body that no longer matches its digest must not be served")

	_, statErr := s.Stat(key)
	require.ErrorIs(t, statErr, ErrNotFound, "and it must be evicted, so the next upload replaces it")
}

// What the check answers, on each inputs it can get: bytes that still
// match, bytes that stopped matching, and an object stored before the stamp
// existed. The last a single reports good -- missing evidence is not
// evidence of rot, and treating it as rot would evict every relic in the cache.
func TestVerifyStoredDigest(t *testing.T) {
	body := []byte("bytes to check against their own digest")
	sum := sha256.Sum256(body)
	write := func(t *testing.T) *os.File {
		t.Helper()
		f, err := os.CreateTemp(t.TempDir(), "body")
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		_, err = f.Write(body)
		require.NoError(t, err)
		return f
	}

	t.Run("matches", func(t *testing.T) {
		ok, err := verifyStoredDigest(write(t), map[string]string{storedDigestMetaKey: hex.EncodeToString(sum[:])})
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("stopped matching", func(t *testing.T) {
		ok, err := verifyStoredDigest(write(t), map[string]string{storedDigestMetaKey: strings.Repeat("00", sha256.Size)})
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("no digest recorded", func(t *testing.T) {
		ok, err := verifyStoredDigest(write(t), map[string]string{})
		require.NoError(t, err)
		require.True(t, ok)
	})
}

// The check rewinds what it read, so a caller that verifies and then serves
// streams the whole body rather than nothing.
func TestVerifyStoredDigest_RewindsForTheServe(t *testing.T) {
	body := []byte("the bytes a serve copies after the check has read them")
	sum := sha256.Sum256(body)
	path := filepath.Join(t.TempDir(), "body")
	require.NoError(t, os.WriteFile(path, body, 0644))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	ok, err := verifyStoredDigest(f, map[string]string{storedDigestMetaKey: hex.EncodeToString(sum[:])})
	require.NoError(t, err)
	require.True(t, ok)

	served, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(body, served))
}
