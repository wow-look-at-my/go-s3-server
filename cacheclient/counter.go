package cacheclient

import (
	"encoding/json"
	"math"
	"sync/atomic"
	"time"
)

// AtomicCounter is a uint32 counter with atomic access and JSON support.
type AtomicCounter struct{ v atomic.Uint32 }

func (c *AtomicCounter) Add(delta uint32) { c.v.Add(delta) }
func (c *AtomicCounter) Increment()       { c.v.Add(1) }
func (c *AtomicCounter) Decrement()       { c.v.Add(^uint32(0)) }
func (c *AtomicCounter) Load() uint32     { return c.v.Load() }
func (c *AtomicCounter) Store(val uint32) { c.v.Store(val) }

func (c *AtomicCounter) MarshalJSON() ([]byte, error) { return json.Marshal(c.v.Load()) }

func (c *AtomicCounter) UnmarshalJSON(data []byte) error {
	var v uint32
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	c.v.Store(v)
	return nil
}

// CacheStats tracks get/put counters for a single cache layer.
type CacheStats struct {
	Hits    AtomicCounter `json:"hits"`
	Puts    AtomicCounter `json:"puts"`
	Corrupt AtomicCounter `json:"corrupt,omitempty"` // entries evicted on a CRC mismatch
}

// LatencyTracker records min/max/sum/count for a single operation type
// using lock-free atomics. Durations are stored as microseconds.
type LatencyTracker struct {
	count atomic.Uint64
	sumUs atomic.Uint64 // total microseconds
	minUs atomic.Uint64 // minimum microseconds
	maxUs atomic.Uint64 // maximum microseconds
}

// Record adds a single duration observation.
func (l *LatencyTracker) Record(d time.Duration) {
	us := uint64(d.Microseconds())
	if us == 0 {
		us = 1 // floor to 1µs: any recorded measurement took nonzero time
	}
	l.count.Add(1)
	l.sumUs.Add(us)

	// CAS loop for min.
	for {
		cur := l.minUs.Load()
		if cur != 0 && us >= cur {
			break
		}
		if l.minUs.CompareAndSwap(cur, us) {
			break
		}
	}
	// CAS loop for max.
	for {
		cur := l.maxUs.Load()
		if us <= cur {
			break
		}
		if l.maxUs.CompareAndSwap(cur, us) {
			break
		}
	}
}

// Snapshot returns a point-in-time copy of the tracker.
func (l *LatencyTracker) Snapshot() LatencySnapshot {
	count := l.count.Load()
	sumUs := l.sumUs.Load()
	minUs := l.minUs.Load()
	maxUs := l.maxUs.Load()
	var avgUs float64
	if count > 0 {
		avgUs = float64(sumUs) / float64(count)
	}
	return LatencySnapshot{
		Count: count,
		MinUs: minUs,
		MaxUs: maxUs,
		AvgUs: avgUs,
		SumUs: sumUs,
	}
}

// Merge adds another snapshot's values into this tracker.
func (l *LatencyTracker) Merge(s LatencySnapshot) {
	if s.Count == 0 {
		return
	}
	l.count.Add(s.Count)
	l.sumUs.Add(s.SumUs)
	for {
		cur := l.minUs.Load()
		if cur != 0 && s.MinUs >= cur {
			break
		}
		if l.minUs.CompareAndSwap(cur, s.MinUs) {
			break
		}
	}
	for {
		cur := l.maxUs.Load()
		if s.MaxUs <= cur {
			break
		}
		if l.maxUs.CompareAndSwap(cur, s.MaxUs) {
			break
		}
	}
}

// LatencySnapshot is a serializable point-in-time copy of a LatencyTracker.
type LatencySnapshot struct {
	Count uint64  `json:"n,omitempty"`
	MinUs uint64  `json:"min,omitempty"` // microseconds
	MaxUs uint64  `json:"max,omitempty"` // microseconds
	AvgUs float64 `json:"avg,omitempty"` // microseconds
	SumUs uint64  `json:"sum,omitempty"` // microseconds
}

// FormatMs returns the snapshot formatted as human-readable milliseconds.
func (s LatencySnapshot) FormatMs() string {
	if s.Count == 0 {
		return "-"
	}
	return formatUs(s.MinUs) + "/" + formatUs(uint64(math.Round(s.AvgUs))) + "/" + formatUs(s.MaxUs)
}

// formatUs formats microseconds as a human-readable duration string.
func formatUs(us uint64) string {
	if us < 1000 {
		return fmtUint(us) + "\u00b5s"
	}
	if us < 1_000_000 {
		return fmtFloat(float64(us)/1000, 1) + "ms"
	}
	return fmtFloat(float64(us)/1_000_000, 2) + "s"
}

func fmtUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func fmtFloat(v float64, prec int) string {
	// Avoid fmt import — simple fixed-precision formatting.
	neg := ""
	if v < 0 {
		neg = "-"
		v = -v
	}
	shift := math.Pow(10, float64(prec))
	rounded := uint64(v*shift + 0.5)
	whole := rounded / uint64(shift)
	frac := rounded % uint64(shift)
	s := neg + fmtUint(whole) + "."
	fs := fmtUint(frac)
	for len(fs) < prec {
		fs = "0" + fs
	}
	return s + fs
}

// ConcurrencyTracker records the high watermark and a running total of
// concurrency samples; peak plus per-sample sum/count give the average
// concurrent usage.
type ConcurrencyTracker struct {
	inFlight atomic.Int64
	peak     atomic.Int64
	samples  atomic.Uint64 // number of Acquire calls
	sumUsage atomic.Uint64 // sum of in-flight values at each Acquire
}

// Acquire increments the in-flight count and updates the peak.
// Returns the new in-flight count.
func (ct *ConcurrencyTracker) Acquire() int64 {
	n := ct.inFlight.Add(1)
	ct.samples.Add(1)
	ct.sumUsage.Add(uint64(n))
	// CAS loop for peak.
	for {
		cur := ct.peak.Load()
		if n <= cur {
			break
		}
		if ct.peak.CompareAndSwap(cur, n) {
			break
		}
	}
	return n
}

// Release decrements the in-flight count.
func (ct *ConcurrencyTracker) Release() {
	ct.inFlight.Add(-1)
}

// ConcurrencySnapshot is a point-in-time copy.
type ConcurrencySnapshot struct {
	Peak     int64  `json:"peak,omitempty"`
	Samples  uint64 `json:"n,omitempty"`
	SumUsage uint64 `json:"sum,omitempty"` // sum of in-flight values at each Acquire
}

// AvgUsed returns the average concurrent usage.
func (s ConcurrencySnapshot) AvgUsed() float64 {
	if s.Samples == 0 {
		return 0
	}
	return float64(s.SumUsage) / float64(s.Samples)
}

// Snapshot returns a point-in-time copy.
func (ct *ConcurrencyTracker) Snapshot() ConcurrencySnapshot {
	return ConcurrencySnapshot{
		Peak:     ct.peak.Load(),
		Samples:  ct.samples.Load(),
		SumUsage: ct.sumUsage.Load(),
	}
}

// Merge incorporates a snapshot into this tracker.
func (ct *ConcurrencyTracker) Merge(s ConcurrencySnapshot) {
	if s.Samples == 0 {
		return
	}
	ct.samples.Add(s.Samples)
	ct.sumUsage.Add(s.SumUsage)
	for {
		cur := ct.peak.Load()
		if s.Peak <= cur {
			break
		}
		if ct.peak.CompareAndSwap(cur, s.Peak) {
			break
		}
	}
}

// LatencyStats holds latency trackers for all cache operations.
type LatencyStats struct {
	LockWait   LatencyTracker // time waiting for per-actionID mutex
	LocalGet   LatencyTracker // local cache get (file read + stat)
	LocalPut   LatencyTracker // local cache put (write + rename)
	RemoteGet  LatencyTracker // remote backend get (total: http + decompress)
	HTTPGet    LatencyTracker // HTTP GET request/response (network + server)
	Decompress LatencyTracker // LZ4 decompression time
	RemotePut  LatencyTracker // remote backend put (total: sem + compress + http)
	SemWait    LatencyTracker // time waiting for upload concurrency slot
	Compress   LatencyTracker // LZ4 compression time
	HTTPPut    LatencyTracker // HTTP PUT request/response (network + server)
}

// LatencyStatsSnapshot is a serializable point-in-time copy.
type LatencyStatsSnapshot struct {
	LockWait   LatencySnapshot     `json:"lw,omitempty"`
	LocalGet   LatencySnapshot     `json:"lg,omitempty"`
	LocalPut   LatencySnapshot     `json:"lp,omitempty"`
	RemoteGet  LatencySnapshot     `json:"rg,omitempty"`
	HTTPGet    LatencySnapshot     `json:"hg,omitempty"`
	Decompress LatencySnapshot     `json:"dc,omitempty"`
	RemotePut  LatencySnapshot     `json:"rp,omitempty"`
	SemWait    LatencySnapshot     `json:"sw,omitempty"`
	Compress   LatencySnapshot     `json:"cp,omitempty"`
	HTTPPut    LatencySnapshot     `json:"hp,omitempty"`
	Pool       ConcurrencySnapshot `json:"pool,omitempty"` // populated from WebBackend, not LatencyStats
}

// Snapshot returns a point-in-time copy. Pool is NOT included here —
// it lives on the WebBackend and is populated by Server.flushLatency.
func (ls *LatencyStats) Snapshot() LatencyStatsSnapshot {
	return LatencyStatsSnapshot{
		LockWait:   ls.LockWait.Snapshot(),
		LocalGet:   ls.LocalGet.Snapshot(),
		LocalPut:   ls.LocalPut.Snapshot(),
		RemoteGet:  ls.RemoteGet.Snapshot(),
		HTTPGet:    ls.HTTPGet.Snapshot(),
		Decompress: ls.Decompress.Snapshot(),
		RemotePut:  ls.RemotePut.Snapshot(),
		SemWait:    ls.SemWait.Snapshot(),
		Compress:   ls.Compress.Snapshot(),
		HTTPPut:    ls.HTTPPut.Snapshot(),
	}
}

// Merge incorporates a snapshot into the live trackers.
func (ls *LatencyStats) Merge(s LatencyStatsSnapshot) {
	ls.LockWait.Merge(s.LockWait)
	ls.LocalGet.Merge(s.LocalGet)
	ls.LocalPut.Merge(s.LocalPut)
	ls.RemoteGet.Merge(s.RemoteGet)
	ls.HTTPGet.Merge(s.HTTPGet)
	ls.Decompress.Merge(s.Decompress)
	ls.RemotePut.Merge(s.RemotePut)
	ls.SemWait.Merge(s.SemWait)
	ls.Compress.Merge(s.Compress)
	ls.HTTPPut.Merge(s.HTTPPut)
}
