package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLastUsedUnix(t *testing.T) {
	write := time.Now().Add(-72 * time.Hour)
	read := time.Now().Add(-1 * time.Hour)

	obj := ListObject{LastModified: write}
	assert.Equal(t, write.Unix(), lastUsedUnix(obj, 0),
		"with no access signal at all, last use is the write time")

	obj.LastAccess = read
	assert.Equal(t, read.Unix(), lastUsedUnix(obj, 0),
		"a filesystem access time later than the write time wins")

	obj.LastAccess = write.Add(-24 * time.Hour)
	assert.Equal(t, write.Unix(), lastUsedUnix(obj, 0),
		"an access time older than the write time must not age an entry")

	obj.LastAccess = time.Time{}
	assert.Equal(t, read.Unix(), lastUsedUnix(obj, read.Unix()),
		"an in-memory record is used when the filesystem records nothing")
}

// TestAtimeProbeDetectsRecording checks the probe against what the filesystem
// actually does, rather than trusting either side: it reads a backdated file
// itself and compares its own observation with the probe's verdict.
func TestAtimeProbeDetectsRecording(t *testing.T) {
	dir := t.TempDir()

	path := dir + "/observed"
	require.NoError(t, os.WriteFile(path, []byte("body"), 0644))
	backdated := time.Now().Add(-atimeProbeAge)
	require.NoError(t, os.Chtimes(path, backdated, backdated))
	_, err := os.ReadFile(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	observed := fileAccessTime(info).After(backdated.Add(time.Hour))

	probed, err := atimeIsRecorded(dir)
	require.NoError(t, err)
	assert.Equal(t, observed, probed, "the probe must agree with what a real read does")

	// The probe must not leave anything behind for the index to find.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.Equal(t, "observed", e.Name(), "the probe file must be cleaned up")
	}
}

// TestEvictionHonorsFilesystemAccessTime is the regression this whole mechanism
// exists for: an entry written long ago but READ recently must survive, with no
// in-memory access tracking involved -- because after a restart there is none,
// and the old sweep evicted exactly those entries as idle.
func TestEvictionHonorsFilesystemAccessTime(t *testing.T) {
	s := newEvictStorage(t)
	require.Nil(t, s.accessShards, "this test must not rely on in-memory access records")

	hot, cold := gbciKey(1), gbciKey(2)
	require.NoError(t, s.Put(hot, []byte("hot"), nil, nil))
	require.NoError(t, s.Put(cold, []byte("cold"), nil, nil))

	// Both written long ago; the hot one was READ an hour ago. The access time
	// is set explicitly rather than by reading the file, so this asserts what
	// eviction does with the timestamp on every filesystem -- a read-driven
	// version would quietly skip wherever the mount does not advance atime, and
	// a test that skips is a test that proves nothing.
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(s.keyToPath(hot), now.Add(-time.Hour), old))
	require.NoError(t, os.Chtimes(s.keyToPath(cold), old, old))

	stats, err := s.Evict(24*time.Hour, 0, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.EvictedAge)

	_, err = s.Stat(hot)
	assert.NoError(t, err, "an entry read since it was written must survive a sweep, with no in-memory record of the read")
	_, err = s.Stat(cold)
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestReadAdvancesAccessTime closes the loop the test above deliberately does
// not depend on: that an ordinary GET is what moves the access time in the
// first place. It asserts against this filesystem's real behavior, which the
// startup probe reports -- so it checks the two agree instead of skipping.
func TestReadAdvancesAccessTime(t *testing.T) {
	s := newEvictStorage(t)
	key := gbciKey(3)
	require.NoError(t, s.Put(key, []byte("body"), nil, nil))

	path := s.keyToPath(key)
	backdated := time.Now().Add(-atimeProbeAge)
	require.NoError(t, os.Chtimes(path, backdated, backdated))

	body, _, err := s.Get(key)
	require.NoError(t, err)
	require.Equal(t, "body", string(body))

	info, err := os.Stat(path)
	require.NoError(t, err)
	advanced := fileAccessTime(info).After(backdated.Add(time.Hour))

	recorded, err := atimeIsRecorded(s.dataDir)
	require.NoError(t, err)
	assert.Equal(t, recorded, advanced,
		"the startup probe decides whether reads are tracked in memory; it must match what a GET actually does here")
}
