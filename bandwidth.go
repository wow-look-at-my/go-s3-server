package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

// The bandwidth chart draws the recent past, so its accounting is a ring of
// time buckets rather than a Prometheus counter.
const (
	// bandwidthBucketSeconds is the width of one bucket, and so the resolution of the chart.
	bandwidthBucketSeconds = 1

	// bandwidthBucketCount is how many buckets the ring holds, and so how many points the chart has.
	bandwidthBucketCount = 300

	// bandwidthRetentionSeconds is the window the store answers for: the newest bucket and the bandwidthBucketCount-1 seconds before it.
	bandwidthRetentionSeconds = bandwidthBucketSeconds * bandwidthBucketCount

	// bandwidthModuleCap bounds the module names tracked inside ONE bucket, bandwidthOtherModule included.
	bandwidthModuleCap = 32

	// bandwidthOtherModule is the remainder band.
	bandwidthOtherModule = "(other)"

	// bandwidthUnknownModule names bytes there is nothing to attribute to: an object whose metadata carries neither a module nor a pkg.
	bandwidthUnknownModule = "(unknown)"
)

// bandwidthSample is the bytes one response carried, and where they belong.
type bandwidthSample struct {
	index  bool
	module string
	bytes  int64
}

// bandwidthBucket is one second of accounting.
type bandwidthBucket struct {
	start   int64
	index   int64
	modules map[string]int64
}

// bandwidthPoint is one second of the series as the page reads it. Total is every byte served in that second, so the module bands and the
// index series add up to it exactly.
type bandwidthPoint struct {
	Start   int64            `json:"t"`
	Total   int64            `json:"total"`
	Index   int64            `json:"index"`
	Modules map[string]int64 `json:"modules,omitempty"`
}

// bandwidthWindow is the retained window read out for the chart. A dense run
// of bandwidthBucketCount points ending at the newest second.
type bandwidthWindow struct {
	BucketSeconds    int              `json:"bucket_seconds"`
	RetentionSeconds int              `json:"retention_seconds"`
	Bands            []string         `json:"bands"`
	Points           []bandwidthPoint `json:"points"`
}

// bandwidthStore keeps the served-byte history, and is safe for concurrent
// use.
type bandwidthStore struct {
	mu sync.Mutex
	// now is the clock the buckets are stamped from.
	now     func() time.Time
	buckets [bandwidthBucketCount]bandwidthBucket
}

func newBandwidthStore() *bandwidthStore {
	return &bandwidthStore{now: time.Now}
}

// bandwidthKey carries the process-wide byte accounting to the handlers that know which module's bytes are leaving.
type bandwidthKey struct{}

// bandwidthFromContext is the store this request records into, or nil when
// there is none, as there is not for a handler.
func bandwidthFromContext(ctx context.Context) *bandwidthStore {
	store, _ := ctx.Value(bandwidthKey{}).(*bandwidthStore)
	return store
}

// bandwidthSlot is the ring position one second's bucket lives in.
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
		// The last slot is the remainder's.
		module = bandwidthOtherModule
	}
	bucket.modules[module] += s.bytes
}

// bucketAt returns the bucket for a second, resetting the slot when the ring
// has advanced past what it last held.
func (b *bandwidthStore) bucketAt(unixSecond int64) *bandwidthBucket {
	bucket := &b.buckets[bandwidthSlot(unixSecond)]
	if bucket.start != unixSecond {
		*bucket = bandwidthBucket{start: unixSecond}
	}
	return bucket
}

// window reads the retained buckets out as the chart needs them. A dense run of
// bandwidthBucketCount points ending at the current second. The module bands
// chosen over the whole window, and the index bytes kept beside them. topModules
// is how many modules are named individually; everything else is summed into
// bandwidthOtherModule. A value of zero or less names none of them. The shape it
// returns holds things a reader can rely on. The remainder band appears in Bands
// once, and for every point the bands and the index series add up to that
// point's total. That second one is what makes the stack the page draws reach
// exactly the total its axis is scaled to. It is asserted instead of assumed.
// The whole read happens under the lock. The answer is at most
// bandwidthBucketCount x bandwidthModuleCap counters. Copying the per-bucket
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
		// No store: the window keeps its shape and carries no traffic, so the page
		// draws an empty chart rather than an error.
		for i := range out.Points {
			out.Points[i].Start = newest - int64(bandwidthBucketCount-1-i)
		}
		out.Bands = []string{bandwidthOtherModule}
		return out
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Rank the modules over the WHOLE window before drawing any of it.
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
		// The remainder is never a band of its own, whatever it is called.
		if module == bandwidthOtherModule {
			continue
		}
		ranked = append(ranked, module)
	}
	// Bytes first, name second: the name makes the order of modules with the
	// same bytes deterministic.
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
			// Either the second has not happened yet or the ring has already wrapped past it.
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
			// Added, not assigned: the band may already hold the bytes a busy bucket folded onto it. Those are part of the remainder rather than a second.
			point.Modules[bandwidthOtherModule] += remainder
		}
	}
	return out
}

// bucketAtRead returns the bucket a second is supposed to be in, or nil when
// that second is not what the slot holds. It never happened, or the ring has
// wrapped past it.
func (b *bandwidthStore) bucketAtRead(unixSecond int64) *bandwidthBucket {
	bucket := &b.buckets[bandwidthSlot(unixSecond)]
	if bucket.start != unixSecond {
		return nil
	}
	return bucket
}
