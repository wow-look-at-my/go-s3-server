package cacheclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatchTimingsSplitWaitFromRoundTrip(t *testing.T) {
	var timing batchTiming
	timing.recordWait(4, 10*time.Millisecond)
	timing.recordTrip(2 * time.Millisecond)
	timing.recordWait(2, 10*time.Millisecond)
	timing.recordTrip(2 * time.Millisecond)

	got := timing.snapshot()
	assert.Equal(t, uint64(2), got.Batches)
	assert.Equal(t, uint64(6), got.Keys)
	assert.InDelta(t, 20.0, got.WaitMillis, 0.001)
	assert.InDelta(t, 4.0, got.RoundTripMillis, 0.001)
	assert.InDelta(t, 3.0, got.KeysPerBatch(), 0.001)

	// This is the shape the deployment reported: batches capped by the
	// caller's parallelism, flushed by the timer, and most of the latency
	// spent waiting rather than on the wire.
	assert.InDelta(t, 20.0/24.0, got.WaitShare(), 0.001)
}

func TestBatchTimingsAreZeroWhenNothingRan(t *testing.T) {
	var timing batchTiming
	got := timing.snapshot()
	assert.Zero(t, got.Batches)
	assert.Zero(t, got.KeysPerBatch(), "an average over no batches must not divide by zero")
	assert.Zero(t, got.WaitShare(), "a share of no latency must not divide by zero")
}
