package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStartupSweepsTempOrphans: .tmp-* files left by interrupted uploads are
// invisible to List (and therefore to eviction and the index), so without the
// startup sweep they leak disk forever. NewStorage removes them while holding
// the exclusive lock, before serving.
func TestStartupSweepsTempOrphans(t *testing.T) {
	dir := t.TempDir()
	// Stamp the current cache version FIRST so NewStorage does not purge the
	// planted files for a version mismatch.
	require.NoError(t, writeCacheVersion(dir, currentCacheVersion))

	shard := filepath.Join(dir, "go-buildcache", "v1", "ab")
	require.NoError(t, os.MkdirAll(shard, 0755))
	orphan1 := filepath.Join(shard, ".tmp-1234567")
	orphan2 := filepath.Join(dir, ".tmp-toplevel")
	keeper := filepath.Join(shard, "cdef")
	require.NoError(t, os.WriteFile(orphan1, []byte("dead"), 0644))
	require.NoError(t, os.WriteFile(orphan2, []byte("dead"), 0644))
	require.NoError(t, os.WriteFile(keeper, []byte("alive"), 0644))

	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	defer s.Close()

	_, err = os.Stat(orphan1)
	require.True(t, errors.Is(err, os.ErrNotExist), "sharded temp orphan must be swept at startup")
	_, err = os.Stat(orphan2)
	require.True(t, errors.Is(err, os.ErrNotExist), "top-level temp orphan must be swept at startup")
	_, err = os.Stat(keeper)
	require.NoError(t, err, "real objects must survive the sweep")
}

// TestPutStreamLargeBodyFsyncPath: a body at/above the fsync threshold takes
// the Sync-before-rename branch and still round-trips byte-for-byte.
func TestPutStreamLargeBodyFsyncPath(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	defer s.Close()

	body := bytes.Repeat([]byte("q"), fsyncThresholdBytes+1)
	key := "go-buildcache/v1" + "00" + "11" + "22" + "33" + "44" + "55" + "66" + "77" +
		"88" + "99" + "aa" + "bb" + "cc" + "dd" + "ee" + "ff" +
		"00" + "11" + "22" + "33" + "44" + "55" + "66" + "77" +
		"88" + "99" + "aa" + "bb" + "cc" + "dd" + "ee" + "ff"
	require.NoError(t, s.PutStream(key, bytes.NewReader(body), nil, nil))

	got, _, err := s.Get(key)
	require.NoError(t, err)
	require.Equal(t, len(body), len(got))
	require.True(t, bytes.Equal(body, got))
}

// failingReader errors on the first read: a stand-in for a dying disk.
type failingReader struct{}

var errDiskDead = errors.New("simulated disk I/O failure")

func (failingReader) Read([]byte) (int, error) { return 0, errDiskDead }

// TestReadProbeDistinguishesIOError: the lz4 read probe must surface a real
// source I/O error instead of swallowing it as "not an index" — the caller
// then refuses the serve (miss) instead of emitting a 200 header and dying
// mid-copy. A garbled-but-readable body remains fail-open (not an index).
func TestReadProbeDistinguishesIOError(t *testing.T) {
	isIndex, err := readIsModuleIndex(failingReader{}, "lz4")
	require.False(t, isIndex)
	require.ErrorIs(t, err, errDiskDead, "a source I/O error must be reported, not swallowed")

	// Control: a readable garbled body is a format problem, not an I/O error.
	isIndex, err = readIsModuleIndex(bytes.NewReader([]byte("not a valid lz4 frame at all")), "lz4")
	require.NoError(t, err)
	require.False(t, isIndex)
}

// TestMetricsServerBusyPortDoesNotExit: a busy metrics port must not take the
// whole cache down. startMetricsServer now logs and returns (the old code
// called log.Fatalf, killing the data path over the monitoring path).
func TestMetricsServerBusyPortDoesNotExit(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	// If this still called log.Fatalf the test binary would exit here.
	startMetricsServer(l.Addr().String())
}
