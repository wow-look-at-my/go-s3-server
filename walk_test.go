package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Walk is the ground truth both the index rebuild and the eviction sweeper are
// built on, and until it replaced the old paginated List it had no test at all
// -- the pagination it carried was dead code nobody called and nobody checked.

// collectWalk gathers everything Walk reports. The tests below assert on the
// whole set; production callers consume objects as the walk finds them, which
// is the point of Walk taking a callback.
func collectWalk(s *Storage) ([]ListObject, error) {
	var objects []ListObject
	err := s.Walk(func(o ListObject) { objects = append(objects, o) })
	return objects, err
}

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := NewStorage(t.TempDir(), WriteOnceConfig{})
	require.NoError(t, err)
	return s
}

func TestWalkReturnsEveryObjectWithMetadata(t *testing.T) {
	s := newTestStorage(t)
	require.NoError(t, s.Put("go-buildcache/v1"+hex64('a'), []byte("hello"), nil, nil))
	require.NoError(t, s.Put("plain-key", []byte("worldly"), nil, nil))

	objects, err := collectWalk(s)
	require.NoError(t, err)

	bySize := map[string]int64{}
	for _, o := range objects {
		bySize[o.Key] = o.Size
		assert.False(t, o.LastModified.IsZero(), "%s: mtime is what eviction ages on", o.Key)
	}
	assert.Equal(t, int64(5), bySize["go-buildcache/v1"+hex64('a')])
	assert.Equal(t, int64(7), bySize["plain-key"])
	assert.Len(t, objects, 2)
}

func TestWalkHasNoCap(t *testing.T) {
	// The old List took a maxKeys, and both callers faked "everything" with an
	// arbitrary huge number -- one of them 1000000, which would have silently
	// truncated the index rebuild of a larger cache. There is no cap to get
	// wrong now: every stored object comes back.
	s := newTestStorage(t)
	const count = 2500
	for i := range count {
		require.NoError(t, s.Put(fmt.Sprintf("key-%05d", i), []byte("x"), nil, nil))
	}

	objects, err := collectWalk(s)
	require.NoError(t, err)
	assert.Len(t, objects, count)

	seen := make(map[string]bool, len(objects))
	for _, o := range objects {
		seen[o.Key] = true
	}
	assert.Len(t, seen, count, "every key distinct and present")
}

func TestWalkSkipsNonObjects(t *testing.T) {
	// The lock file, the cache-version stamp, in-flight temp files and Windows
	// metadata sidecars all live in the data dir but are not objects. Listing
	// one would advertise a phantom key in /_index and hand the eviction
	// sweeper a file it must not touch.
	s := newTestStorage(t)
	require.NoError(t, s.Put("real-object", []byte("body"), nil, nil))

	dir := s.dataDir
	require.NoError(t, os.WriteFile(filepath.Join(dir, cacheVersionFile), []byte("9"), 0o644))
	tmp, err := os.CreateTemp(dir, ".tmp-")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	objects, err := collectWalk(s)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.Equal(t, "real-object", objects[0].Key)
}

func TestWalkEmptyStore(t *testing.T) {
	objects, err := collectWalk(newTestStorage(t))
	require.NoError(t, err)
	assert.Empty(t, objects)
}

// hex64 builds a 64-character hex string of one repeated digit, so a key can
// look like a real cacheprog action hash.
func hex64(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// TestNoBucketLevelListingRoute pins what actually retired issue #18. The
// reported cliff was a paginated ListObjectsV2 endpoint whose every page
// re-walked and re-stat'd the whole tree, so populating a 103k-key client
// index took ~60s across ~104 round-trips. That endpoint is gone: the protocol
// is no longer S3-shaped, and a client now populates its index from the
// precomputed /_index blob in ONE request.
//
// This asserts the shape that makes the cliff structurally impossible. A
// future listing endpoint is a legitimate thing to want -- but it must not be
// the old walk-per-page, so it should arrive with this test updated
// deliberately, not silently.
func TestNoBucketLevelListingRoute(t *testing.T) {
	cfg := &Config{Bucket: "testbucket", DisableAuth: true}
	s := NewServer(cfg, newTestStorage(t))

	for _, target := range []string{
		"/testbucket/",
		"/testbucket/?list-type=2",
		"/testbucket/?list-type=2&max-keys=1000&continuation-token=x",
	} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code,
			"%s: bucket-level listing must not exist (see issue #18)", target)
	}
}
