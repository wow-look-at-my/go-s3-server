package cacheclient

import (
	"sync/atomic"
	"time"
)

// batchTiming records what a batch GET costs the caller, split into the two
// halves that pull in opposite directions.
//
// A batch GET is synchronous: every caller blocks until its batch returns. So
// the coalescer can never hold more keys than there are blocked callers, which
// is the build's parallelism, NOT batchMaxKeys. In a real deployment the GET
// batches topped out at 4 keys with the mode sitting exactly on 4, the
// GOMAXPROCS of the runner. batchMaxKeys was unreachable by construction, and
// every batch flushed on the coalescing timer instead of on the count.
//
// That makes the window a straight trade: wait, against the round trips the
// wait saves. Server-side handling was about 1ms, but a caller pays the round
// trip, not the handler, and nothing measured the round trip. These counters
// measure it. Wait against round trip, per build, is what says whether the
// window earns its place.
type batchTiming struct {
	batches   atomic.Uint64
	keys      atomic.Uint64
	waitNanos atomic.Uint64 // first key queued -> batch dispatched
	tripNanos atomic.Uint64 // request issued -> response consumed
}

func (t *batchTiming) recordWait(keys int, d time.Duration) {
	t.batches.Add(1)
	t.keys.Add(uint64(keys))
	t.waitNanos.Add(uint64(d))
}

func (t *batchTiming) recordTrip(d time.Duration) { t.tripNanos.Add(uint64(d)) }

// BatchTimings is a snapshot of the batch GET cost split, in the build profile
// so a change to the coalescing window can be judged against measurements.
type BatchTimings struct {
	Batches uint64 `json:"batches"`
	Keys    uint64 `json:"keys"`
	// WaitMillis is the total time callers spent inside the coalescing window.
	WaitMillis float64 `json:"wait_ms"`
	// RoundTripMillis is the total time spent on the HTTP requests themselves.
	RoundTripMillis float64 `json:"round_trip_ms"`
}

// KeysPerBatch reports the average batch size. A value pinned just under the
// build's parallelism means the window is flushing on its timer, never on
// batchMaxKeys, so raising that cap buys nothing.
func (bt BatchTimings) KeysPerBatch() float64 {
	if bt.Batches == 0 {
		return 0
	}
	return float64(bt.Keys) / float64(bt.Batches)
}

// WaitShare reports the fraction of batch GET latency that was coalescing
// wait rather than the round trip it exists to amortize. A share near 1 means
// the window costs more than the requests it saves.
func (bt BatchTimings) WaitShare() float64 {
	total := bt.WaitMillis + bt.RoundTripMillis
	if total == 0 {
		return 0
	}
	return bt.WaitMillis / total
}

func (t *batchTiming) snapshot() BatchTimings {
	return BatchTimings{
		Batches:         t.batches.Load(),
		Keys:            t.keys.Load(),
		WaitMillis:      float64(t.waitNanos.Load()) / float64(time.Millisecond),
		RoundTripMillis: float64(t.tripNanos.Load()) / float64(time.Millisecond),
	}
}
