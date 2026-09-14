package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

// The bandwidth chart draws the recent past, so its accounting is a ring of
// time buckets rather than a Prometheus counter: a counter is monotonic and
// carries no time axis, and a page that has to show the last five minutes
// cannot recover one from a single number (the histogram beside it in
// /metrics records a size distribution, not a time series). The ring is also
// what keeps this bounded. Every byte the server serves is filed under the
// second it left, and the ring holds only the newest bandwidthBucketCount of
// those seconds, so the store's size is a constant: it does not depend on how
// long the process has been up, nor on how many modules a fleet builds through
// the cache.
const (
	// bandwidthBucketSeconds is the width of one bucket, and so the resolution
	// of the chart. A second is the rate the access log already aggregates at
	// (logagg.go), which is the rate this server's traffic is read at.
	bandwidthBucketSeconds = 1

	// bandwidthBucketCount is how many buckets the ring holds, and so how many
	// points the chart has.
	bandwidthBucketCount = 300

	// bandwidthRetentionSeconds is the window the store answers for: the newest
	// bucket and the bandwidthBucketCount-1 seconds before it.
	bandwidthRetentionSeconds = bandwidthBucketSeconds * bandwidthBucketCount

	// bandwidthModuleCap bounds the module names tracked inside ONE bucket,
	// bandwidthOtherModule included. A bucket that meets more names sums them
	// into the remainder instead of growing, which is what makes the store's
	// worst case the constant bandwidthBucketCount x bandwidthModuleCap
	// counters no matter what the fleet does. A name past the cap is still
	// counted: only its identity is given up.
	bandwidthModuleCap = 32

	// bandwidthOtherModule is the band the dashboard sums every module past its
	// top cut into, and the name a bucket folds overflow onto. A Go module path
	// cannot carry a parenthesis, so this cannot collide with a real module.
	bandwidthOtherModule = "(other)"

	// bandwidthUnknownModule names bytes there is nothing to attribute to: an
	// object whose metadata carries neither a module nor a pkg, served to a
	// client that sent no X-Cache-Module header (an older client, or curl).
	// The bytes crossed the wire, so they count; the name says plainly that
	// they are not attributable.
	bandwidthUnknownModule = "(unknown)"
)

// bandwidthSample is the bytes one response carried, and where they belong.
// They belong in exactly one place: the index series, or the module the served
// cache keys belong to, and never both. A batch response is recorded as one sample
// per streamed entry, because a single batch can carry entries from different
// modules.
type bandwidthSample struct {
	index  bool
	module string
	bytes  int64
}

// bandwidthBucket is one second of accounting. The zero value is an empty
// bucket; start is the Unix second it covers, so a ring slot whose stamp is no
// longer the second being recorded is overwritten rather than merged.
type bandwidthBucket struct {
	start   int64
	index   int64
	modules map[string]int64
}

// bandwidthPoint is one second of the series as the page reads it. Total is
// every byte served in that second, so the module bands and the index series
// add up to it exactly.
type bandwidthPoint struct {
	Start   int64            `json:"t"`
	Total   int64            `json:"total"`
	Index   int64            `json:"index"`
	Modules map[string]int64 `json:"modules,omitempty"`
}

// bandwidthWindow is the retained window read out for the chart: a dense run of
// bandwidthBucketCount points ending at the newest second, the band order to
// stack them in, and the size of the window they cover.
type bandwidthWindow struct {
	BucketSeconds    int              `json:"bucket_seconds"`
	RetentionSeconds int              `json:"retention_seconds"`
	Bands            []string         `json:"bands"`
	Points           []bandwidthPoint `json:"points"`
}

// bandwidthStore keeps the served-byte history, and is safe for concurrent use.
// Every method is safe on a nil store, which records nothing and answers with
// the window's empty shape: a server that was built without one (a test, or a
// handler driven directly) still answers the endpoint the page polls.
type bandwidthStore struct {
	mu sync.Mutex
	// now is the clock the buckets are stamped from. Tests inject one so a
	// window can be walked without sleeping through it.
	now     func() time.Time
	buckets [bandwidthBucketCount]bandwidthBucket
}

func newBandwidthStore() *bandwidthStore {
	return &bandwidthStore{now: time.Now}
}

// bandwidthKey carries the process-wide byte accounting to the handlers that
// know which module's bytes are leaving. It rides with the request for the same
// reason the audit trail does (auditKey): three unrelated handlers need it, and
// none of them is otherwise about accounting.
type bandwidthKey struct{}

// bandwidthFromContext is the store this request records into, or nil when
// there is none, as there is not for a handler driven directly in a test. Every
// method on the store is safe on nil, so no caller has to ask first.
func bandwidthFromContext(ctx context.Context) *bandwidthStore {
	store, _ := ctx.Value(bandwidthKey{}).(*bandwidthStore)
	return store
}

// bandwidthSlot is the ring position one second's bucket lives in. Seconds are
// contiguous, so every second has a slot; a second that comes around again
// after the ring wrapped is told apart from its predecessor by the bucket's
// start stamp, not by the slot.
func bandwidthSlot(unixSecond int64) int {
	slot := unixSecond % bandwidthBucketCount
	if slot < 0 {
		slot += bandwidthBucketCount
	}
	return int(slot)
}

// record files one response's bytes under the second they were served in. It is
// the only way into the store, so a byte cannot be filed twice or land nowhere.
func (b *bandwidthStore) record(s bandwidthSample) {
	if b == nil || s.bytes <= 0 {
		// Nothing crossed the wire (a conditional request answered with 304,
		// say), so there is no bandwidth to draw.
		return
	}
	now := time.Now()
	if b.now != nil {
		now = b.now()
	}
	second := now.Unix()

	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.bucketAt(second)

	if s.index {
		bucket.index += s.bytes
		return
	}
	module := s.module
	if module == "" {
		module = bandwidthUnknownModule
	}
	if bucket.modules == nil {
		bucket.modules = make(map[string]int64, 4)
	}
	if _, tracked := bucket.modules[module]; !tracked && len(bucket.modules) >= bandwidthModuleCap-1 {
		// The last slot is the remainder's, so the fold always has somewhere to
		// land and a busy bucket still stops at bandwidthModuleCap names.
		module = bandwidthOtherModule
	}
	bucket.modules[module] += s.bytes
}

// bucketAt returns the bucket for a second, resetting the slot when the ring
// has advanced past what it last held. Resetting IS the retention policy: the
// oldest second's bytes are dropped by being overwritten, so the store never
// grows and never needs a sweep to stay bounded. The caller holds the lock.
func (b *bandwidthStore) bucketAt(unixSecond int64) *bandwidthBucket {
	bucket := &b.buckets[bandwidthSlot(unixSecond)]
	if bucket.start != unixSecond {
		*bucket = bandwidthBucket{start: unixSecond}
	}
	return bucket
}

// window reads the retained buckets out as the chart needs them: a dense run of
// bandwidthBucketCount points ending at the current second, the module bands
// chosen over the whole window, and the index bytes kept beside them.
//
// topModules is how many modules are named individually; everything else is
// summed into bandwidthOtherModule. A value of zero or less names none of them.
//
// The whole read happens under the lock. The answer is at most
// bandwidthBucketCount x bandwidthModuleCap counters, and copying the per-bucket
// maps out to aggregate them outside the lock would cost more than the
// aggregation it would move out.
func (b *bandwidthStore) window(topModules int) bandwidthWindow {
	newest := time.Now().Unix()
	if b != nil && b.now != nil {
		newest = b.now().Unix()
	}
	out := bandwidthWindow{
		BucketSeconds:    bandwidthBucketSeconds,
		RetentionSeconds: bandwidthRetentionSeconds,
		Points:           make([]bandwidthPoint, bandwidthBucketCount),
	}
	if topModules < 0 {
		topModules = 0
	}
	if b == nil {
		// No store: the window keeps its shape and carries no traffic, so the
		// page draws an empty chart rather than an error. The stamps are still
		// filled in, because a point without one is not a point.
		for i := range out.Points {
			out.Points[i].Start = newest - int64(bandwidthBucketCount-1-i)
		}
		out.Bands = []string{bandwidthOtherModule}
		return out
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Rank the modules over the WHOLE window before drawing any of it: the
	// stack order and the top cut then hold still between polls, instead of a
	// module dropping out of the legend for the one second it was idle.
	totals := make(map[string]int64, bandwidthModuleCap)
	for i := range out.Points {
		bucket := b.bucketAtRead(newest - int64(bandwidthBucketCount-1-i))
		if bucket == nil {
			continue
		}
		for module, n := range bucket.modules {
			totals[module] += n
		}
	}
	ranked := make([]string, 0, len(totals))
	for module := range totals {
		ranked = append(ranked, module)
	}
	// Bytes first, name second: the name makes the order of two modules with
	// the same bytes deterministic, so an unchanged window cannot reshuffle the
	// stack between two polls.
	sort.Slice(ranked, func(i, j int) bool {
		if totals[ranked[i]] != totals[ranked[j]] {
			return totals[ranked[i]] > totals[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})
	if len(ranked) > topModules {
		ranked = ranked[:topModules]
	}
	named := make(map[string]bool, len(ranked))
	for _, module := range ranked {
		named[module] = true
	}
	out.Bands = append(append(make([]string, 0, len(ranked)+1), ranked...), bandwidthOtherModule)

	for i := range out.Points {
		point := &out.Points[i]
		point.Start = newest - int64(bandwidthBucketCount-1-i)
		bucket := b.bucketAtRead(point.Start)
		if bucket == nil {
			// Either the second has not happened yet or the ring has already
			// wrapped past it: both are a second with no traffic to draw.
			continue
		}
		point.Index = bucket.index
		point.Total = bucket.index
		var remainder int64
		for module, n := range bucket.modules {
			point.Total += n
			if !named[module] {
				remainder += n
				continue
			}
			if point.Modules == nil {
				point.Modules = make(map[string]int64, len(ranked))
			}
			point.Modules[module] += n
		}
		if remainder > 0 {
			if point.Modules == nil {
				point.Modules = make(map[string]int64, 1)
			}
			point.Modules[bandwidthOtherModule] = remainder
		}
	}
	return out
}

// bucketAtRead returns the bucket a second is supposed to be in, or nil when
// that second is not what the slot holds: it never happened, or the ring has
// wrapped past it. It does not reset anything: a read must not disturb the
// window it is reading. The caller holds the lock.
func (b *bandwidthStore) bucketAtRead(unixSecond int64) *bandwidthBucket {
	bucket := &b.buckets[bandwidthSlot(unixSecond)]
	if bucket.start != unixSecond {
		return nil
	}
	return bucket
}
