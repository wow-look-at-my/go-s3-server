package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gbciKey returns a well-formed cacheprog cache key (prefix + 64 hex chars) so
// the entry also lands in the GBCI index, letting the index-rebuild path be
// exercised by eviction tests.
func gbciKey(n int) string {
	return gbciKeyPrefix + fmt.Sprintf("%064x", n)
}

// setMtime backdates an object's on-disk mtime so age-based eviction can be
// tested without sleeping.
func setMtime(t *testing.T, s *Storage, key string, mt time.Time) {
	t.Helper()
	path := s.keyToPath(key)
	require.NoError(t, os.Chtimes(path, mt, mt))
}

func newEvictStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := NewStorage(t.TempDir(), WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// TestEvictByAge: an entry idle longer than max_age is removed; a fresh one stays.
func TestEvictByAge(t *testing.T) {
	s := newEvictStorage(t)

	old := gbciKey(1)
	fresh := gbciKey(2)
	require.NoError(t, s.Put(old, []byte("old"), nil, nil))
	require.NoError(t, s.Put(fresh, []byte("fresh"), nil, nil))

	now := time.Now()
	setMtime(t, s, old, now.Add(-48*time.Hour))
	setMtime(t, s, fresh, now.Add(-1*time.Hour))

	stats, err := s.Evict(24*time.Hour, 0, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.EvictedAge)
	assert.Equal(t, 0, stats.EvictedSize)

	_, err = s.Stat(old)
	assert.ErrorIs(t, err, ErrNotFound, "stale entry should be evicted")
	_, err = s.Stat(fresh)
	assert.NoError(t, err, "recent entry should survive")
}

// TestEvictAgeRespectsAccessTime: a recently *read* entry survives age eviction
// even though its mtime (write time) is well past max_age. This is the
// "not-accessed-in-a-while" semantics — write time alone must not condemn a
// hot entry.
func TestEvictAgeRespectsAccessTime(t *testing.T) {
	s := newEvictStorage(t)
	s.EnableAccessTracking()

	hot := gbciKey(1)
	cold := gbciKey(2)
	require.NoError(t, s.Put(hot, []byte("hot"), nil, nil))
	require.NoError(t, s.Put(cold, []byte("cold"), nil, nil))

	now := time.Now()
	// Both were written long ago.
	setMtime(t, s, hot, now.Add(-48*time.Hour))
	setMtime(t, s, cold, now.Add(-48*time.Hour))

	// But "hot" was just read.
	f, _, err := s.Open(hot)
	require.NoError(t, err)
	f.Close()

	stats, err := s.Evict(24*time.Hour, 0, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.EvictedAge)

	_, err = s.Stat(hot)
	assert.NoError(t, err, "recently-accessed entry must survive despite old mtime")
	_, err = s.Stat(cold)
	assert.ErrorIs(t, err, ErrNotFound, "never-accessed old entry should be evicted")
}

// TestEvictBySize evicts least-recently-used entries until under the byte budget.
func TestEvictBySize(t *testing.T) {
	s := newEvictStorage(t)

	a, b, c := gbciKey(1), gbciKey(2), gbciKey(3)
	for _, k := range []string{a, b, c} {
		require.NoError(t, s.Put(k, make([]byte, 100), nil, nil))
	}

	now := time.Now()
	setMtime(t, s, a, now.Add(-3*time.Hour)) // oldest → evicted first
	setMtime(t, s, b, now.Add(-2*time.Hour))
	setMtime(t, s, c, now.Add(-1*time.Hour)) // newest → kept

	// Budget 250 with 300 stored: exactly one 100-byte entry must go.
	stats, err := s.Evict(0, 250, now)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.EvictedAge)
	assert.Equal(t, 1, stats.EvictedSize)
	assert.Equal(t, int64(100), stats.BytesFreed)

	_, err = s.Stat(a)
	assert.ErrorIs(t, err, ErrNotFound, "LRU entry should be evicted to meet the budget")
	_, err = s.Stat(b)
	assert.NoError(t, err)
	_, err = s.Stat(c)
	assert.NoError(t, err)
}

// TestEvictUpdatesIndex: after eviction the GBCI index no longer advertises the
// removed keys.
func TestEvictUpdatesIndex(t *testing.T) {
	s := newEvictStorage(t)

	k1, k2 := gbciKey(1), gbciKey(2)
	require.NoError(t, s.Put(k1, []byte("a"), nil, nil))
	require.NoError(t, s.Put(k2, []byte("b"), nil, nil))

	blob, _ := s.Index.Blob()
	require.Equal(t, uint64(2), binary.LittleEndian.Uint64(blob[16:24]), "both keys indexed")

	now := time.Now()
	setMtime(t, s, k1, now.Add(-48*time.Hour))

	_, err := s.Evict(24*time.Hour, 0, now)
	require.NoError(t, err)

	blob, _ = s.Index.Blob()
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(blob[16:24]), "evicted key dropped from index")
}

// TestEvictNoPolicyNoop: with both limits off, nothing is removed.
func TestEvictNoPolicyNoop(t *testing.T) {
	s := newEvictStorage(t)
	k := gbciKey(1)
	require.NoError(t, s.Put(k, make([]byte, 100), nil, nil))

	stats, err := s.Evict(0, 0, time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, stats.EvictedAge+stats.EvictedSize)
	assert.Equal(t, int64(100), stats.BytesTotal)
	_, err = s.Stat(k)
	assert.NoError(t, err)
}

// TestEvictForgetsAccessRecord: an evicted key's access entry is dropped.
// Eviction is forced via the size budget — a lone over-budget entry is removed
// by the size pass regardless of how recently it was accessed.
func TestEvictForgetsAccessRecord(t *testing.T) {
	s := newEvictStorage(t)
	s.EnableAccessTracking()

	k := gbciKey(1)
	require.NoError(t, s.Put(k, make([]byte, 100), nil, nil))
	f, _, err := s.Open(k)
	require.NoError(t, err)
	f.Close()
	_, ok := s.lastAccess(k)
	require.True(t, ok)

	// Budget of 50 with 100 stored: the entry must go even though it is "hot".
	stats, err := s.Evict(0, 50, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, stats.EvictedSize)

	_, ok = s.lastAccess(k)
	assert.False(t, ok, "access record should be forgotten after eviction")
}

// TestPruneAccessDropsVanishedKeys: access records for keys that disappear
// out-of-band (no eviction, no Delete) are cleaned up by the next sweep.
func TestPruneAccessDropsVanishedKeys(t *testing.T) {
	s := newEvictStorage(t)
	s.EnableAccessTracking()

	k := gbciKey(1)
	require.NoError(t, s.Put(k, []byte("x"), nil, nil))
	f, _, err := s.Open(k)
	require.NoError(t, err)
	f.Close()
	_, ok := s.lastAccess(k)
	require.True(t, ok)

	// Remove the file behind storage's back, then sweep with no policy.
	require.NoError(t, os.Remove(s.keyToPath(k)))
	_, err = s.Evict(0, 0, time.Now())
	require.NoError(t, err)

	_, ok = s.lastAccess(k)
	assert.False(t, ok, "access record for a vanished key should be pruned")
}

// TestDurationUnmarshal covers the JSON forms accepted for durations.
func TestDurationUnmarshal(t *testing.T) {
	var d Duration

	require.NoError(t, json.Unmarshal([]byte(`"720h"`), &d))
	assert.Equal(t, 720*time.Hour, d.Std())

	require.NoError(t, json.Unmarshal([]byte(`"0"`), &d))
	assert.Equal(t, time.Duration(0), d.Std())

	require.NoError(t, json.Unmarshal([]byte(`""`), &d))
	assert.Equal(t, time.Duration(0), d.Std())

	require.NoError(t, json.Unmarshal([]byte(`90`), &d)) // bare number = seconds
	assert.Equal(t, 90*time.Second, d.Std())

	require.Error(t, json.Unmarshal([]byte(`"not-a-duration"`), &d))
}

// TestEvictionConfigDefaults: the cache is an LRU by default -- an absent
// eviction block gets the default SIZE budget and no age limit, so nothing is
// dropped for being old, only for being least-recently-used past the budget.
func TestEvictionConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	cred := `"credentials": [{"username": "u", "password": "p"}]`

	// No eviction block → default size budget, age eviction off, enabled.
	p1 := dir + "/default.json"
	require.NoError(t, os.WriteFile(p1, []byte(`{"bucket":"b","data_dir":"/tmp",`+cred+`}`), 0644))
	cfg, err := LoadConfig(p1)
	require.NoError(t, err)
	assert.Equal(t, int64(defaultEvictionMaxBytes), cfg.Eviction.SizeLimit())
	assert.Equal(t, time.Duration(0), cfg.Eviction.AgeLimit(), "age eviction is opt-in")
	assert.True(t, cfg.Eviction.Enabled())
	assert.Equal(t, defaultEvictionInterval, cfg.Eviction.Interval.Std())

	// Explicit max_bytes 0 with no age limit → eviction disabled.
	p2 := dir + "/off.json"
	require.NoError(t, os.WriteFile(p2, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":0},`+cred+`}`), 0644))
	cfg, err = LoadConfig(p2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), cfg.Eviction.SizeLimit())
	assert.False(t, cfg.Eviction.Enabled())

	// Age limit only → enabled even with the size budget off.
	p3 := dir + "/age.json"
	require.NoError(t, os.WriteFile(p3, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":0,"max_age":"720h"},`+cred+`}`), 0644))
	cfg, err = LoadConfig(p3)
	require.NoError(t, err)
	assert.True(t, cfg.Eviction.Enabled())
	assert.Equal(t, 720*time.Hour, cfg.Eviction.AgeLimit())

	// An explicit budget wins over the default.
	p5 := dir + "/size.json"
	require.NoError(t, os.WriteFile(p5, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":1048576},`+cred+`}`), 0644))
	cfg, err = LoadConfig(p5)
	require.NoError(t, err)
	assert.Equal(t, int64(1048576), cfg.Eviction.SizeLimit())

	// Negative max_bytes is rejected.
	p4 := dir + "/neg.json"
	require.NoError(t, os.WriteFile(p4, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":-1},`+cred+`}`), 0644))
	_, err = LoadConfig(p4)
	require.Error(t, err)
}

// TestMaxBytesEnvVar: the size budget can be set without touching the config
// file, an explicit config value still wins, and a malformed env value fails
// the load instead of silently reverting to the default.
func TestMaxBytesEnvVar(t *testing.T) {
	dir := t.TempDir()
	cred := `"credentials": [{"username": "u", "password": "p"}]`
	plain := dir + "/plain.json"
	require.NoError(t, os.WriteFile(plain, []byte(`{"bucket":"b","data_dir":"/tmp",`+cred+`}`), 0644))
	explicit := dir + "/explicit.json"
	require.NoError(t, os.WriteFile(explicit, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":123},`+cred+`}`), 0644))

	t.Setenv(maxBytesEnvVar, "100GB")
	cfg, err := LoadConfig(plain)
	require.NoError(t, err)
	assert.Equal(t, int64(100)<<30, cfg.Eviction.SizeLimit())

	cfg, err = LoadConfig(explicit)
	require.NoError(t, err)
	assert.Equal(t, int64(123), cfg.Eviction.SizeLimit(), "config max_bytes wins over the env var")

	t.Setenv(maxBytesEnvVar, "plenty")
	_, err = LoadConfig(plain)
	require.Error(t, err, "a set-but-unparseable budget must fail loudly")

	t.Setenv(maxBytesEnvVar, "")
	cfg, err = LoadConfig(plain)
	require.NoError(t, err)
	assert.Equal(t, int64(defaultEvictionMaxBytes), cfg.Eviction.SizeLimit())
}

func TestParseByteSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1073741824", 1 << 30},
		{"50GB", 50 << 30},
		{"50 GiB", 50 << 30},
		{"512m", 512 << 20},
		{"2T", 2 << 40},
		{"4096B", 4096},
	} {
		got, err := parseByteSize(tc.in)
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}
	for _, bad := range []string{"", "plenty", "-5", "5GX", "1.5GB", "GB"} {
		_, err := parseByteSize(bad)
		assert.Error(t, err, bad)
	}
}

// TestEvictOneSkipsFreshlyOverwritten is the snapshot-then-remove TOCTOU guard:
// a victim whose on-disk mtime changed since the sweep's scan (a concurrent
// overwrite PUT renamed fresh content onto the path) must NOT be deleted.
func TestEvictOneSkipsFreshlyOverwritten(t *testing.T) {
	s := newEvictStorage(t)
	key := gbciKey(0x77)
	require.NoError(t, s.Put(key, []byte("fresh body"), nil, nil))

	info, err := os.Stat(s.keyToPath(key))
	require.NoError(t, err)
	current := info.ModTime().Unix()

	// A stale snapshot mtime (as if the object was overwritten after the scan):
	// the eviction must back off and keep the file.
	require.False(t, s.evictOne(key, current-3600), "a freshly-overwritten object must not be evicted")
	_, err = s.Stat(key)
	require.NoError(t, err, "the object must still exist after the skipped eviction")

	// With the matching mtime the eviction proceeds.
	require.True(t, s.evictOne(key, current))
	_, err = s.Stat(key)
	require.ErrorIs(t, err, ErrNotFound)

	// Already-gone: no-op, no error.
	require.False(t, s.evictOne(key, current))
}

// TestEvictionStartupDelay: the first sweep is scheduled a jittered 1-5 minutes
// after startup, never a full interval away.
func TestEvictionStartupDelay(t *testing.T) {
	for i := 0; i < 200; i++ {
		d := evictionStartupDelay()
		require.GreaterOrEqual(t, d, evictionStartupDelayMin)
		require.Less(t, d, evictionStartupDelayMax)
	}
}

// TestSweepScheduleSurvivesRestart: the sweep schedule lives in the data_dir,
// so a deployment that restarts more often than the interval still sweeps, and
// one that restarts constantly does not re-walk the whole disk every boot.
func TestSweepScheduleSurvivesRestart(t *testing.T) {
	s := newEvictStorage(t)
	const interval = 24 * time.Hour

	// Never swept: due now (after the startup jitter).
	_, ok := s.lastSweepTime()
	require.False(t, ok)
	assert.Less(t, s.firstSweepDelay(interval), evictionStartupDelayMax,
		"a cache with no recorded sweep must sweep at startup")

	// Swept an interval ago: due now.
	s.recordSweepTime(time.Now().Add(-25 * time.Hour))
	last, ok := s.lastSweepTime()
	require.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(-25*time.Hour), last, time.Minute)
	assert.Less(t, s.firstSweepDelay(interval), evictionStartupDelayMax,
		"a sweep older than the interval is overdue and must run at startup")

	// Swept an hour ago: not due for another 23.
	s.recordSweepTime(time.Now().Add(-time.Hour))
	delay := s.firstSweepDelay(interval)
	assert.Greater(t, delay, 22*time.Hour, "a recent sweep must not be repeated at startup")
	assert.Less(t, delay, 24*time.Hour)

	// A marker stamped in the future cannot delay eviction past one interval.
	s.recordSweepTime(time.Now().Add(30 * 24 * time.Hour))
	assert.LessOrEqual(t, s.firstSweepDelay(interval), interval+evictionStartupDelayMax)

	// A corrupt marker is treated as never swept rather than trusted.
	require.NoError(t, os.WriteFile(s.dataDir+"/"+sweepMarkerFile, []byte("tomorrow"), 0644))
	_, ok = s.lastSweepTime()
	assert.False(t, ok)
}

// TestRunSweepRecordsMarker: a completed sweep stamps the marker, and the
// marker is not itself mistaken for a cache object.
func TestRunSweepRecordsMarker(t *testing.T) {
	s := newEvictStorage(t)
	require.NoError(t, s.Put(gbciKey(1), make([]byte, 10), nil, nil))

	s.runSweep(0, 0)

	last, ok := s.lastSweepTime()
	require.True(t, ok, "a completed sweep must record when it ran")
	assert.WithinDuration(t, time.Now(), last, time.Minute)

	objects, err := collectWalk(s)
	require.NoError(t, err)
	require.Len(t, objects, 1, "the sweep marker must not be walked as an object")
	assert.Equal(t, gbciKey(1), objects[0].Key)
}

// TestEvictBySizeIsLeastRecentlyUsedFirst: over budget, the sweep evicts from
// the least-recently-used end and keeps the rest -- the cache is an LRU.
func TestEvictBySizeIsLeastRecentlyUsedFirst(t *testing.T) {
	s := newEvictStorage(t)

	const count = 6
	keys := make([]string, count)
	now := time.Now()
	for i := range keys {
		keys[i] = gbciKey(i + 1)
		require.NoError(t, s.Put(keys[i], make([]byte, 100), nil, nil))
		// keys[0] is the oldest use, keys[5] the newest.
		used := now.Add(-time.Duration(count-i) * time.Hour)
		require.NoError(t, os.Chtimes(s.keyToPath(keys[i]), used, used))
	}

	// 600 bytes stored, 350 allowed: the three oldest must go, no more.
	stats, err := s.Evict(0, 350, now)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.EvictedSize)
	assert.Equal(t, 0, stats.EvictedAge, "nothing may be dropped for age when max_age is off")

	for i, k := range keys {
		_, err := s.Stat(k)
		if i < 3 {
			assert.ErrorIs(t, err, ErrNotFound, "key %d is least-recently-used and must go", i)
		} else {
			assert.NoError(t, err, "key %d is recently used and must stay", i)
		}
	}
}

// TestRefreshCacheBytes: the gauge refresher sums stored object sizes only,
// skipping the lock file, version marker, and .tmp- leftovers.
func TestRefreshCacheBytes(t *testing.T) {
	s := newEvictStorage(t)
	require.NoError(t, s.Put(gbciKey(1), make([]byte, 100), nil, nil))
	require.NoError(t, s.Put(gbciKey(2), make([]byte, 50), nil, nil))
	// A stale temp file must not count.
	require.NoError(t, os.WriteFile(s.dataDir+"/.tmp-stale", make([]byte, 999), 0644))

	s.RefreshCacheBytes()
	require.Equal(t, float64(150), testutil.ToFloat64(cacheBytes))
}
