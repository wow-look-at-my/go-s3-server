package cacheclient

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAtomicCounter_IncrementDecrement(t *testing.T) {
	var c AtomicCounter
	c.Increment()
	c.Increment()
	c.Increment()
	assert.Equal(t, uint32(3), c.Load())
	c.Decrement()
	assert.Equal(t, uint32(2), c.Load())
}

func TestAtomicCounter_AddStore(t *testing.T) {
	var c AtomicCounter
	c.Add(10)
	assert.Equal(t, uint32(10), c.Load())
	c.Store(42)
	assert.Equal(t, uint32(42), c.Load())
}

func TestAtomicCounter_JSON(t *testing.T) {
	var c AtomicCounter
	c.Store(123)

	data, err := json.Marshal(&c)
	require.NoError(t, err)
	assert.Equal(t, "123", string(data))

	var c2 AtomicCounter
	require.NoError(t, json.Unmarshal(data, &c2))
	assert.Equal(t, uint32(123), c2.Load())
}

func TestCacheStats_JSON(t *testing.T) {
	var s CacheStats
	s.Hits.Store(10)
	s.Puts.Store(5)

	data, err := json.Marshal(&s)
	require.NoError(t, err)

	var s2 CacheStats
	require.NoError(t, json.Unmarshal(data, &s2))
	assert.Equal(t, uint32(10), s2.Hits.Load())
	assert.Equal(t, uint32(5), s2.Puts.Load())
}

func TestLatencyTracker_SingleRecord(t *testing.T) {
	var lt LatencyTracker
	lt.Record(500 * time.Microsecond)

	snap := lt.Snapshot()
	assert.Equal(t, uint64(1), snap.Count)
	assert.Equal(t, uint64(500), snap.MinUs)
	assert.Equal(t, uint64(500), snap.MaxUs)
	assert.InDelta(t, 500.0, snap.AvgUs, 0.01)
}

func TestLatencyTracker_MultipleRecords(t *testing.T) {
	var lt LatencyTracker
	lt.Record(100 * time.Microsecond)
	lt.Record(200 * time.Microsecond)
	lt.Record(300 * time.Microsecond)

	snap := lt.Snapshot()
	assert.Equal(t, uint64(3), snap.Count)
	assert.Equal(t, uint64(100), snap.MinUs)
	assert.Equal(t, uint64(300), snap.MaxUs)
	assert.InDelta(t, 200.0, snap.AvgUs, 0.01)
	assert.Equal(t, uint64(600), snap.SumUs)
}

func TestLatencyTracker_SubMicrosecondFloor(t *testing.T) {
	var lt LatencyTracker
	lt.Record(1 * time.Nanosecond) // sub-microsecond

	snap := lt.Snapshot()
	assert.Equal(t, uint64(1), snap.Count)
	assert.Equal(t, uint64(1), snap.MinUs, "sub-microsecond should floor to 1µs")
}

func TestLatencyTracker_Merge(t *testing.T) {
	var lt1, lt2 LatencyTracker
	lt1.Record(100 * time.Microsecond)
	lt1.Record(500 * time.Microsecond)
	lt2.Record(50 * time.Microsecond)
	lt2.Record(200 * time.Microsecond)

	snap2 := lt2.Snapshot()
	lt1.Merge(snap2)

	merged := lt1.Snapshot()
	assert.Equal(t, uint64(4), merged.Count)
	assert.Equal(t, uint64(50), merged.MinUs)
	assert.Equal(t, uint64(500), merged.MaxUs)
}

func TestLatencyTracker_Zero(t *testing.T) {
	var lt LatencyTracker
	snap := lt.Snapshot()
	assert.Equal(t, uint64(0), snap.Count)
	assert.Equal(t, "-", snap.FormatMs())
}

func TestLatencySnapshot_FormatMs(t *testing.T) {
	tests := []struct {
		name string
		snap LatencySnapshot
		want string
	}{
		{"microseconds", LatencySnapshot{Count: 1, MinUs: 50, MaxUs: 50, AvgUs: 50}, "50µs/50µs/50µs"},
		{"milliseconds", LatencySnapshot{Count: 1, MinUs: 1500, MaxUs: 15000, AvgUs: 8000}, "1.5ms/8.0ms/15.0ms"},
		{"seconds", LatencySnapshot{Count: 1, MinUs: 1_500_000, MaxUs: 2_500_000, AvgUs: 2_000_000}, "1.50s/2.00s/2.50s"},
		{"zero", LatencySnapshot{}, "-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.snap.FormatMs())
		})
	}
}

func TestLatencyStats_SnapshotAndMerge(t *testing.T) {
	var ls LatencyStats
	ls.LockWait.Record(10 * time.Microsecond)
	ls.LocalGet.Record(50 * time.Microsecond)
	ls.LocalPut.Record(100 * time.Microsecond)
	ls.RemoteGet.Record(5 * time.Millisecond)
	ls.RemotePut.Record(10 * time.Millisecond)

	snap := ls.Snapshot()
	assert.Equal(t, uint64(1), snap.LockWait.Count)
	assert.Equal(t, uint64(1), snap.LocalGet.Count)
	assert.Equal(t, uint64(1), snap.LocalPut.Count)
	assert.Equal(t, uint64(1), snap.RemoteGet.Count)
	assert.Equal(t, uint64(1), snap.RemotePut.Count)

	// Merge into a new tracker.
	var ls2 LatencyStats
	ls2.Merge(snap)
	snap2 := ls2.Snapshot()
	assert.Equal(t, uint64(1), snap2.LockWait.Count)
	assert.Equal(t, snap.RemoteGet.MinUs, snap2.RemoteGet.MinUs)
}

func TestLatencySnapshot_JSON(t *testing.T) {
	snap := LatencySnapshot{Count: 3, MinUs: 100, MaxUs: 500, AvgUs: 300, SumUs: 900}
	data, err := json.Marshal(snap)
	require.NoError(t, err)

	var snap2 LatencySnapshot
	require.NoError(t, json.Unmarshal(data, &snap2))
	assert.Equal(t, snap.Count, snap2.Count)
	assert.Equal(t, snap.MinUs, snap2.MinUs)
	assert.Equal(t, snap.MaxUs, snap2.MaxUs)
	assert.Equal(t, snap.SumUs, snap2.SumUs)
}
