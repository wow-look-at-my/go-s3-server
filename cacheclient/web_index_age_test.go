package cacheclient

import (
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// TestMain makes the zero IndexMaxAge revalidate on every load. The tests of
// the revalidation path predate the max age and count requests. The tests of
// the max age below pass one explicitly.
func TestMain(m *testing.M) {
	defaultIndexMaxAge = -1
	os.Exit(m.Run())
}

// sevenKeys is a small index body with known members.
func sevenKeys() set.Set[string] {
	want := set.New[string]()
	for i := 0; i < 7; i++ {
		var h [gbciHashSize]byte
		h[0] = byte(i + 100)
		want.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	}
	return want
}

// TestIndexLoadsOnFirstUse pins that NewWebBackend makes no request. A go
// command that never asks the cache for a key never downloads the index.
func TestIndexLoadsOnFirstUse(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	f := newIndexFixture(t, "bk", sevenKeys())

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	require.Equal(t, int32(0), f.hitsAny.Load(), "construction must not touch the server")

	var absent [gbciHashSize]byte
	absent[0] = 1
	_, _, _, miss := b.Get(hex.EncodeToString(absent[:]))
	require.True(t, miss)
	require.Equal(t, int32(1), f.hits200.Load(), "the first Get loads the index")
	require.Equal(t, 7, b.keys.Len())
}

// TestYoungDiskCopyIsServedWithoutARequest pins the max age. A second process
// over a copy younger than it asks the server for nothing, and the copy is
// authoritative: an absent key misses without a probe.
func TestYoungDiskCopyIsServedWithoutARequest(t *testing.T) {
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	cfg := WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: time.Hour}

	first, err := NewWebBackend(cfg)
	require.NoError(t, err)
	first.ensureIndex()
	first.Close()
	require.Equal(t, int32(1), f.hits200.Load(), "the first process has no copy and fetches")

	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.ensureIndex()
	require.Equal(t, int32(1), f.hitsAny.Load(), "a young copy costs no request")
	require.True(t, b.indexAuthoritative)
	require.Equal(t, 7, b.keys.Len())
}

// TestOldDiskCopyIsRevalidatedAndTouched pins the other side of the max age.
// A copy past it is revalidated, and a not-modified answer restarts its age,
// so the next process inside the window is served from disk again.
func TestOldDiskCopyIsRevalidatedAndTouched(t *testing.T) {
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	cfg := WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: time.Hour}

	first, err := NewWebBackend(cfg)
	require.NoError(t, err)
	first.ensureIndex()
	first.Close()
	path := first.indexCachePath()
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(path, old, old))

	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.ensureIndex()
	require.Equal(t, int32(1), f.hits304.Load(), "a copy past the max age is revalidated")
	require.True(t, b.indexAuthoritative)
	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, time.Since(st.ModTime()), time.Minute, "a not-modified answer restarts the copy's age")

	third, err := NewWebBackend(cfg)
	require.NoError(t, err)
	third.ensureIndex()
	require.Equal(t, int32(2), f.hitsAny.Load(), "the touched copy is served with no request")
}

// TestIndexMaxAgeDefault pins what a consumer gets without setting it.
func TestIndexMaxAgeDefault(t *testing.T) {
	require.Equal(t, time.Minute, IndexMaxAgeDefault)
	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: "http://127.0.0.1:1", AccessKey: "k", SecretKey: "s", IndexMaxAge: IndexMaxAgeDefault})
	require.NoError(t, err)
	require.Equal(t, IndexMaxAgeDefault, b.indexMaxAge)
}
