package cacheclient

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// realIndexMaxAgeDefault restores the real default resolver for one test.
// TestMain replaces it for the tests that predate the default.
func realIndexMaxAgeDefault(t *testing.T) {
	t.Helper()
	prev := defaultIndexMaxAge
	defaultIndexMaxAge = resolveDefaultIndexMaxAge
	t.Cleanup(func() { defaultIndexMaxAge = prev })
}

// seedDiskIndex fetches the index once into dir and returns the copy's path.
// The fetch is forced with an explicit max age, so the default under test is
// not what put the copy there.
func seedDiskIndex(t *testing.T, f *indexFixture, dir string) string {
	t.Helper()
	cfg := WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: -1}
	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.awaitIndex()
	path := b.indexCachePath()
	require.NoError(t, b.Close())
	require.Equal(t, int32(1), f.hits200.Load(), "the seeding process has no copy and fetches")
	return path
}

// backdate moves a file's mtime back by d.
func backdate(t *testing.T, path string, d time.Duration) {
	t.Helper()
	when := time.Now().Add(-d)
	require.NoError(t, os.Chtimes(path, when, when))
}

// startRun writes the run marker for the copy at path as a run that began
// age ago and is in use now.
func startRun(t *testing.T, path string, age time.Duration) {
	t.Helper()
	start := strconv.FormatInt(time.Now().Add(-age).UnixNano(), 10)
	require.NoError(t, os.WriteFile(indexRunMarkerPath(path), []byte(start), 0o644))
}

// defaultCfg is a config that takes the default index max age.
func defaultCfg(f *indexFixture, dir string) WebConfig {
	return WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir}
}

// TestCIDefaultIndexMaxAgeDoesNotExpire pins the CI rule at the resolver: a
// copy is served however old it is.
func TestCIDefaultIndexMaxAgeDoesNotExpire(t *testing.T) {
	stubCI(t, map[string]string{"CI": "true"})
	require.Equal(t, IndexMaxAgeCI, resolveDefaultIndexMaxAge(t.TempDir()+"/index.bin"))
	require.Greater(t, IndexMaxAgeCI, 100*365*24*time.Hour, "the CI default outlives any run")
}

// TestCIDefaultServesAnOldCopyWithNoRequest is the CI rule end to end. A
// second process over a day-old copy asks the server for nothing.
func TestCIDefaultServesAnOldCopyWithNoRequest(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, map[string]string{"GITHUB_ACTIONS": "true"})
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	backdate(t, path, 24*time.Hour)

	b, err := NewWebBackend(defaultCfg(f, dir))
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hitsAny.Load(), "a CI run re-fetches nothing partway through")
	require.True(t, b.indexAuthoritative, "the copy answers absences without a probe")
	require.Equal(t, 7, b.keys.Len())
}

// TestNonCIDefaultExpires pins the other side: outside CI a copy older than
// the window is revalidated.
func TestNonCIDefaultExpires(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, nil)
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	backdate(t, path, 24*time.Hour)

	b, err := NewWebBackend(defaultCfg(f, dir))
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hits304.Load(), "a copy past the window is revalidated")
}

// TestNonCIDefaultFloor pins the floor. A copy older than five minutes is
// still served in a run that has only just started.
func TestNonCIDefaultFloor(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, nil)
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	require.GreaterOrEqual(t, IndexMaxAgeDefault, 5*time.Minute, "the floor never drops below five minutes")
	backdate(t, path, 6*time.Minute)

	b, err := NewWebBackend(defaultCfg(f, dir))
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hitsAny.Load(), "a copy inside the floor costs no request")
	require.True(t, b.indexAuthoritative)
}

// TestRunWindowOutlivesTheFloor is the long-build case: a copy older than the
// floor is still served while the run that fetched it is going on.
func TestRunWindowOutlivesTheFloor(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, nil)
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	backdate(t, path, 30*time.Minute)
	startRun(t, path, time.Hour)

	b, err := NewWebBackend(defaultCfg(f, dir))
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hitsAny.Load(), "one run re-fetches nothing partway through")
	require.True(t, b.indexAuthoritative)
}

// TestNewRunRevalidates pins where a run ends. A marker untouched for longer
// than the idle gap belongs to a finished run, so the next process starts a
// new one and a copy past the floor is revalidated.
func TestNewRunRevalidates(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, nil)
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	backdate(t, path, 30*time.Minute)
	startRun(t, path, time.Hour)
	backdate(t, indexRunMarkerPath(path), indexRunIdle+time.Minute)

	b, err := NewWebBackend(defaultCfg(f, dir))
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hits304.Load(), "a new run revalidates a copy past the floor")
}

// TestExplicitIndexMaxAgeWinsInCI pins that the CI rule applies to the
// default only. A caller asking for revalidation on every load gets it.
func TestExplicitIndexMaxAgeWinsInCI(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, map[string]string{"CI": "true"})
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	seedDiskIndex(t, f, dir)

	cfg := defaultCfg(f, dir)
	cfg.IndexMaxAge = -1
	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hits304.Load(), "an explicit max age beats the CI default")
}

// TestExplicitIndexMaxAgeWinsOutsideCI is the same outside CI, in the other
// direction: a max age longer than the default keeps an old copy.
func TestExplicitIndexMaxAgeWinsOutsideCI(t *testing.T) {
	realIndexMaxAgeDefault(t)
	stubCI(t, nil)
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	path := seedDiskIndex(t, f, dir)
	backdate(t, path, 20*time.Hour)

	cfg := defaultCfg(f, dir)
	cfg.IndexMaxAge = 24 * time.Hour
	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.awaitIndex()
	require.Equal(t, int32(1), f.hitsAny.Load(), "an explicit max age beats the non-CI default")
	require.True(t, b.indexAuthoritative)
}

// TestIndexRunWindow pins the marker itself: a first use starts a run, a use
// inside the idle gap continues it, and a use past the gap starts a new one.
func TestIndexRunWindow(t *testing.T) {
	stubCI(t, nil)
	path := t.TempDir() + "/index.bin"

	require.Less(t, indexRunWindow(path, time.Now()), time.Second, "a first use starts a run")

	startRun(t, path, time.Hour)
	w := indexRunWindow(path, time.Now())
	require.Greater(t, w, 59*time.Minute, "a use inside the idle gap continues the run")
	require.Less(t, w, 61*time.Minute)

	startRun(t, path, time.Hour)
	backdate(t, indexRunMarkerPath(path), indexRunIdle+time.Minute)
	require.Less(t, indexRunWindow(path, time.Now()), time.Second, "a use past the idle gap starts a new run")
}

// TestResolveDefaultIndexMaxAge pins the resolver over the run window.
func TestResolveDefaultIndexMaxAge(t *testing.T) {
	stubCI(t, nil)
	path := t.TempDir() + "/index.bin"
	require.Equal(t, IndexMaxAgeDefault, resolveDefaultIndexMaxAge(path), "a fresh run takes the floor")

	startRun(t, path, time.Hour)
	require.Greater(t, resolveDefaultIndexMaxAge(path), 59*time.Minute, "a long run raises the default to cover itself")
}
