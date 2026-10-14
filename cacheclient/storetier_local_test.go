package cacheclient

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// An output stored through Local stays on disk. The store queues no upload,
// names no manifest line and counts no miss for it.
func TestLocalTierNeverReachesTheStore(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	tier := layerStore(disk, b)
	local := cachedisk.Local(tier)
	require.IsType(t, diskOnly{}, local, "a tier with a store answers its disk half")

	var id ActionID
	id[0] = 9
	actionID := hex.EncodeToString(id[:])

	_, err = local.Get(id)
	require.Error(t, err, "nothing is stored yet")
	require.Zero(t, b.SummarySnapshot().MissTotal(), "a local miss asks the store nothing")

	_, _, err = local.Put(id, bytes.NewReader([]byte("go index v2 body")))
	require.NoError(t, err)
	require.Zero(t, b.pendingCount(), "a local store queues no upload")
	require.False(t, b.Present(actionID), "a local store claims no key in the store")

	entry, err := local.Get(id)
	require.NoError(t, err)
	require.Equal(t, int64(len("go index v2 body")), entry.Size)
	require.False(t, b.manifest.used.Contains(actionID), "a local lookup names no manifest line")
	require.Equal(t, int64(1), tier.localPuts.Load())
	require.Equal(t, int64(1), tier.localHits.Load())

	require.NoError(t, local.Close())
	_, err = disk.Get(id)
	require.NoError(t, err, "closing the local half leaves the directory open")
}

// A cache with no store answers itself.
func TestLocalOfDiskIsItself(t *testing.T) {
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	require.Same(t, disk, cachedisk.Local(disk).(*DiskCache))
}
