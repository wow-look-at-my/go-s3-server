package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestSelfHealStampsHashedInodeNotPath reproduces the stale-stamp race the
// fd-based repair closes: the healer opens and hashes an outputid-less relic,
// a concurrent overwrite PUT renames a FRESH body (with its own correct
// outputid) onto the same path, and then the healer persists its result. The
// old path-based setxattr stamped the STALE hash onto the NEW body — leaving
// outputid != sha256(body) forever, a permanent forced miss no self-heal could
// ever fix (an outputid is present) and no client would accept. The fd-based
// stamp lands on the hashed (now-unlinked) inode instead, so the fresh PUT's
// outputid survives untouched.
func TestSelfHealStampsHashedInodeNotPath(t *testing.T) {
	_, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("9", 64)

	// The relic: lz4 body, NO outputid.
	oldRaw := []byte("!<arch>\nthe old relic body")
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, oldRaw)),
		map[string]string{"compression": "lz4"}, nil))

	// The healer opens its handle (this is the inode it will hash).
	f, _, err := storage.Open(key)
	require.NoError(t, err)
	defer f.Close()

	// Concurrent overwrite PUT: fresh body with its own correct outputid.
	newRaw := []byte("!<arch>\nthe fresh replacement body")
	newSum := sha256.Sum256(newRaw)
	newOutputID := hex.EncodeToString(newSum[:])
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, newRaw)),
		map[string]string{"compression": "lz4", "outputid": newOutputID}, nil))

	// The stale repair completes against the old fd.
	oldSum := sha256.Sum256(oldRaw)
	got, err := reconstructOutputID(storage, key, f)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(oldSum[:]), got,
		"the repair must return the hash of the body it actually read")

	// THE regression assertion: the fresh body's outputid was NOT clobbered by
	// the stale stamp. Under the old path-based SetMeta this reads the OLD hash.
	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, newOutputID, meta.Metadata["outputid"],
		"a stale repair must never stamp its hash onto a concurrently-replaced body")
}

// TestSelfHealCountsOutputIDMismatch: a repair that finds an existing outputid
// disagreeing with the freshly-computed body hash (stale-stamp corruption
// already on disk) counts it and corrects it in place.
func TestSelfHealCountsOutputIDMismatch(t *testing.T) {
	serialMetrics(t)

	_, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("d", 64)
	raw := []byte("!<arch>\nbody whose stamp is wrong")
	require.NoError(t, storage.PutStream(key, bytes.NewReader(lz4Compress(t, raw)),
		map[string]string{"compression": "lz4"}, nil))
	// Plant a corrupted stamp directly (the historical race's end state).
	require.NoError(t, storage.SetMeta(key, map[string]string{"outputid": "0badc0de"}))

	before := testutil.ToFloat64(outputIDMismatchTotal)
	got, err := reconstructOutputID(storage, key, nil)
	require.NoError(t, err)

	sum := sha256.Sum256(raw)
	require.Equal(t, hex.EncodeToString(sum[:]), got)
	require.Equal(t, before+1, testutil.ToFloat64(outputIDMismatchTotal),
		"a disagreeing pre-existing outputid must be counted")

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, got, meta.Metadata["outputid"], "the corrupted stamp must be repaired in place")
}

// TestSelfHealRewindsServeHandle: when the GET path hands its own serve handle
// to the repair, the handle must come back rewound to byte 0 so the response
// body is complete (the repair consumed it while hashing).
func TestSelfHealRewindsServeHandle(t *testing.T) {
	_, storage := testSetupWithStorage(t)

	key := "go-buildcache/v1" + strings.Repeat("e", 64)
	raw := []byte("!<arch>\nserve-handle body")
	compressed := lz4Compress(t, raw)
	require.NoError(t, storage.PutStream(key, bytes.NewReader(compressed),
		map[string]string{"compression": "lz4"}, nil))

	f, meta, err := storage.Open(key)
	require.NoError(t, err)
	defer f.Close()

	require.True(t, ensureOutputID(storage, key, meta, f))

	// The handle streams the whole body from byte 0.
	var out bytes.Buffer
	_, err = out.ReadFrom(f)
	require.NoError(t, err)
	require.Equal(t, compressed, out.Bytes(), "the serve handle must be rewound after the repair")

	sum := sha256.Sum256(raw)
	require.Equal(t, hex.EncodeToString(sum[:]), meta.Metadata["outputid"])
}
