package cacheclient

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// A lookup for an object the manifest fetch has on the wire waits for it, and
// a lookup for any other object does not.
func TestAwaitPrefetchWaitsForTheFetch(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	onWire := strings.Repeat("e5", 32)
	chunk := b.manifest.markPending([]string{onWire})
	chunk.send()
	require.False(t, b.AwaitPrefetch(strings.Repeat("f6", 32)))

	waited := make(chan bool, 1)
	go func() { waited <- b.AwaitPrefetch(onWire) }()
	select {
	case <-waited:
		t.Fatal("the lookup returned while the fetch was on the wire")
	case <-time.After(50 * time.Millisecond):
	}
	chunk.finish()
	require.True(t, <-waited)
	require.False(t, b.AwaitPrefetch(onWire), "a landed fetch holds nothing up")
	require.Equal(t, uint32(1), b.manifest.Waited.Load())
}

// A lookup stops waiting for a slow manifest fetch after manifestWaitCap.
func TestAwaitPrefetchGivesUpOnASlowFetch(t *testing.T) {
	t.Serial() // manifestWaitCap is package state
	old := manifestWaitCap
	manifestWaitCap = 50 * time.Millisecond
	defer func() { manifestWaitCap = old }()

	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	onWire := strings.Repeat("e7", 32)
	chunk := b.manifest.markPending([]string{onWire})
	chunk.send()
	defer chunk.finish()
	start := time.Now()
	require.False(t, b.AwaitPrefetch(onWire))
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint32(1), b.manifest.GaveUp.Load())
	require.Zero(t, b.manifest.Waited.Load())
}

// A lookup goes on as soon as its own entry lands, while the rest of its chunk is still on the wire.
func TestAwaitPrefetchGoesOnWhenItsEntryLands(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	first := strings.Repeat("e8", 32)
	second := strings.Repeat("e9", 32)
	chunk := b.manifest.markPending([]string{first, second})
	require.Equal(t, []string{first, second}, chunk.send())

	waited := make(chan bool, 1)
	go func() { waited <- b.AwaitPrefetch(first) }()
	select {
	case <-waited:
		t.Fatal("the lookup returned before its entry landed")
	case <-time.After(50 * time.Millisecond):
	}
	chunk.landed([]string{first})
	require.True(t, <-waited, "a landed entry releases its lookup before the chunk ends")

	b.manifest.mu.Lock()
	_, stillPending := b.manifest.pending[second]
	b.manifest.mu.Unlock()
	require.True(t, stillPending, "the rest of the chunk is still on the wire")
	chunk.finish()
	require.False(t, b.AwaitPrefetch(second))
}

// A lookup for an ID whose chunk has not gone out asks on its own at once, and the chunk leaves that ID out.
func TestAwaitPrefetchDoesNotWaitForAQueuedChunk(t *testing.T) {
	b := NewBareBackend("go-buildcache/")
	b.manifest = newBuildManifest()
	queued := strings.Repeat("ea", 32)
	other := strings.Repeat("eb", 32)
	chunk := b.manifest.markPending([]string{queued, other})

	start := time.Now()
	require.False(t, b.AwaitPrefetch(queued))
	require.Less(t, time.Since(start), manifestWaitCap/2, "a queued chunk holds no lookup up")
	require.Zero(t, b.manifest.GaveUp.Load())
	require.Zero(t, b.manifest.Waited.Load())
	require.Equal(t, []string{other}, chunk.send(), "the chunk leaves out the ID the lookup took")
	chunk.finish()
}

// fetchNamed hands each group's action IDs to landed after the local tier has the group, before the batch ends.
func TestFetchNamedReportsEachGroupAsItLands(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	store := map[string][]byte{}
	meta := map[string]map[string]string{}
	var keys []string
	for idx := range lookAheadChunk + 1 {
		var raw [32]byte
		raw[0] = byte(idx + 1)
		actionID := hex.EncodeToString(raw[:])
		seedObject(t, store, meta, actionID, []byte("object "+actionID))
		keys = append(keys, "go-buildcache/v1"+actionID)
	}
	srv := fakeBatchServer(t, store, meta)
	t.Cleanup(srv.Close)

	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWait(10 * time.Millisecond)
	stored := set.New[string]()
	b.OnBatchEntries = func(entries []BatchEntry) {
		for _, e := range entries {
			stored.Add(e.Key)
		}
	}
	var groups [][]string
	count, _ := b.fetchNamed(keys, func(ids []string) {
		for _, actionID := range ids {
			require.True(t, stored.Contains(b.key(actionID)), "an ID is reported only after its entry is stored")
		}
		groups = append(groups, append([]string(nil), ids...))
	})
	require.Equal(t, lookAheadChunk+1, count)
	require.Len(t, groups, 2)
	require.Len(t, groups[0], lookAheadChunk, "the first group lands before the batch ends")
	require.Len(t, groups[1], 1)
	require.NoError(t, b.Close())
}
