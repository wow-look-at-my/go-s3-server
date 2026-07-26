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

// TestEvictionConfigDefaults: absent eviction config gets the default max_age
// (enabled), while an explicit max_age of 0 disables age eviction.
func TestEvictionConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	cred := `"credentials": [{"username": "u", "password": "p"}]`

	// No eviction block → default max_age applied, eviction enabled.
	p1 := dir + "/default.json"
	require.NoError(t, os.WriteFile(p1, []byte(`{"bucket":"b","data_dir":"/tmp",`+cred+`}`), 0644))
	cfg, err := LoadConfig(p1)
	require.NoError(t, err)
	require.NotNil(t, cfg.Eviction.MaxAge)
	assert.Equal(t, defaultEvictionMaxAge, cfg.Eviction.AgeLimit())
	assert.True(t, cfg.Eviction.Enabled())
	assert.Equal(t, defaultEvictionInterval, cfg.Eviction.Interval.Std())

	// Explicit max_age 0 with no size budget → eviction disabled.
	p2 := dir + "/off.json"
	require.NoError(t, os.WriteFile(p2, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_age":"0"},`+cred+`}`), 0644))
	cfg, err = LoadConfig(p2)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.Eviction.AgeLimit())
	assert.False(t, cfg.Eviction.Enabled())

	// Size budget only → enabled even with age off.
	p3 := dir + "/size.json"
	require.NoError(t, os.WriteFile(p3, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_age":"0","max_bytes":1048576},`+cred+`}`), 0644))
	cfg, err = LoadConfig(p3)
	require.NoError(t, err)
	assert.True(t, cfg.Eviction.Enabled())
	assert.Equal(t, int64(1048576), cfg.Eviction.MaxBytes)

	// Negative max_bytes is rejected.
	p4 := dir + "/neg.json"
	require.NoError(t, os.WriteFile(p4, []byte(`{"bucket":"b","data_dir":"/tmp","eviction":{"max_bytes":-1},`+cred+`}`), 0644))
	_, err = LoadConfig(p4)
	require.Error(t, err)
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
