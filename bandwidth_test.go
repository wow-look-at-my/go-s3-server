package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBandwidthStore builds a store on a clock this test moves by hand: the
// window is five minutes wide, and a test that had to sleep through one would
// not be run.
func testBandwidthStore(at time.Time) (*bandwidthStore, *time.Time) {
	clock := new(time.Time)
	*clock = at
	store := newBandwidthStore()
	store.now = func() time.Time { return *clock }
	return store, clock
}

// testBandwidthEpoch is an arbitrary but fixed second, so a failure names the
// same numbers every run.
var testBandwidthEpoch = time.Unix(1_700_000_000, 0)

func TestBandwidthBytesLandInTheirModuleAndWindow(t *testing.T) {
	store, clock := testBandwidthStore(testBandwidthEpoch)

	store.record(bandwidthSample{module: "example.com/app", bytes: 300})
	store.record(bandwidthSample{module: "example.com/tool", bytes: 100})
	store.record(bandwidthSample{module: "example.com/app", bytes: 50})

	*clock = testBandwidthEpoch.Add(4 * time.Second)
	store.record(bandwidthSample{module: "example.com/app", bytes: 7})

	window := store.window(5)
	require.Len(t, window.Points, bandwidthBucketCount)

	newest := window.Points[len(window.Points)-1]
	assert.Equal(t, *clock, time.Unix(newest.Start, 0), "the series ends at the second being recorded in")
	assert.Equal(t, int64(7), newest.Total)
	assert.Equal(t, int64(7), newest.Modules["example.com/app"])

	// The second the earliest bytes were served in is four points back, and it
	// holds what was served in it and nothing else.
	earliest := window.Points[len(window.Points)-5]
	assert.Equal(t, testBandwidthEpoch.Unix(), earliest.Start)
	assert.Equal(t, int64(450), earliest.Total)
	assert.Equal(t, int64(350), earliest.Modules["example.com/app"], "two responses in one second are one bucket")
	assert.Equal(t, int64(100), earliest.Modules["example.com/tool"])
	assert.Zero(t, earliest.Index)

	// The quiet seconds between them are reported and empty, so the chart's
	// axis covers the window rather than only the traffic.
	assert.Zero(t, window.Points[len(window.Points)-2].Total)
	assert.Zero(t, window.Points[0].Total)
}

func TestBandwidthIndexFetchesAreTheirOwnSeries(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	store.record(bandwidthSample{module: "example.com/app", bytes: 300})
	store.record(bandwidthSample{index: true, bytes: 2048})

	window := store.window(5)
	assert.Equal(t, []string{"example.com/app", bandwidthOtherModule}, window.Bands, "the index is not a band of a module's")

	point := window.Points[len(window.Points)-1]
	assert.Equal(t, int64(2048), point.Index, "an index fetch is its own series")
	assert.Equal(t, int64(2348), point.Total)
	assert.Equal(t, map[string]int64{"example.com/app": 300}, point.Modules, "no index byte lands in a module")
}

func TestBandwidthNamesTopModulesAndSumsTheRest(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	sizes := map[string]int64{
		"example.com/one":   700,
		"example.com/two":   600,
		"example.com/three": 500,
		"example.com/four":  400,
		"example.com/five":  300,
		"example.com/six":   200,
		"example.com/seven": 100,
	}
	for module, n := range sizes {
		store.record(bandwidthSample{module: module, bytes: n})
	}
	store.record(bandwidthSample{index: true, bytes: 4096})

	window := store.window(5)
	assert.Equal(t, []string{
		"example.com/one",
		"example.com/two",
		"example.com/three",
		"example.com/four",
		"example.com/five",
		bandwidthOtherModule,
	}, window.Bands, "the top five by bytes, then the remainder")

	point := window.Points[len(window.Points)-1]
	assert.Equal(t, int64(300), point.Modules[bandwidthOtherModule], "the two modules past the cut are summed, not dropped")
	assert.Equal(t, int64(2800+4096), point.Total, "the total is every byte served in the second, the index included")
	assert.NotContains(t, point.Modules, "example.com/six", "a module past the cut is named by the remainder instead")

	// Every byte is accounted for exactly once: the named bands and the
	// remainder are the module bytes, and the index series sits beside them at
	// the total the axis is scaled to.
	var bands int64
	for _, n := range point.Modules {
		bands += n
	}
	assert.Equal(t, int64(700+600+500+400+300+200+100), bands)
	assert.Equal(t, point.Total, bands+point.Index)
}

func TestBandwidthBandsWithEqualBytesAreOrderedByName(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	for _, module := range []string{"example.com/zeta", "example.com/alpha"} {
		store.record(bandwidthSample{module: module, bytes: 512})
	}

	// Both modules served the same bytes, so only their names decide the order.
	// An unchanged window must not reshuffle the stack between two polls.
	for range 3 {
		assert.Equal(t, []string{"example.com/alpha", "example.com/zeta", bandwidthOtherModule}, store.window(2).Bands)
	}
}

func TestBandwidthWindowIsBoundedAndDropsTheOldestSecond(t *testing.T) {
	store, clock := testBandwidthStore(testBandwidthEpoch)
	store.record(bandwidthSample{module: "example.com/early", bytes: 1234})

	// One second short of the window, the earliest bytes are still in it: the
	// window is the newest bucket and the count-1 before it.
	*clock = testBandwidthEpoch.Add((bandwidthRetentionSeconds - 1) * time.Second)
	window := store.window(5)
	require.Len(t, window.Points, bandwidthBucketCount)
	assert.Equal(t, bandwidthRetentionSeconds, window.RetentionSeconds)
	assert.Equal(t, bandwidthBucketSeconds, window.BucketSeconds)
	assert.Equal(t, int64(1234), window.Points[0].Total, "the oldest second of the window is still inside it")

	// One second later the ring has wrapped onto that second, so its bytes are
	// gone: dropped by being overwritten, with no sweep to run and no growth to
	// bound afterwards.
	*clock = testBandwidthEpoch.Add(bandwidthRetentionSeconds * time.Second)
	store.record(bandwidthSample{module: "example.com/now", bytes: 1})

	window = store.window(5)
	require.Len(t, window.Points, bandwidthBucketCount, "the window keeps its width")
	var total int64
	for _, point := range window.Points {
		total += point.Total
		assert.NotContains(t, point.Modules, "example.com/early", "a second past the window is not reported")
	}
	assert.Equal(t, int64(1), total, "only what the newest second served is left")

	// The ring itself never grew: the bound is the array, not a policy applied
	// to a map that keeps growing underneath it.
	live := 0
	for i := range store.buckets {
		if store.buckets[i].start != 0 {
			live++
		}
	}
	assert.LessOrEqual(t, live, bandwidthBucketCount)
}

func TestBandwidthBucketFoldsModulesPastTheCap(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	const modules = 200
	var served int64
	for i := range modules {
		n := int64(i) + 1
		store.record(bandwidthSample{module: fmt.Sprintf("example.com/module-%03d", i), bytes: n})
		served += n
	}

	bucket := store.bucketAt(testBandwidthEpoch.Unix())
	require.Len(t, bucket.modules, bandwidthModuleCap, "a bucket stops at the cap, the remainder band included")
	assert.Contains(t, bucket.modules, bandwidthOtherModule)

	// A module past the cap loses its name, never its bytes.
	window := store.window(0)
	assert.Equal(t, []string{bandwidthOtherModule}, window.Bands, "with no names cut there is only the remainder")
	point := window.Points[bandwidthBucketCount-1]
	assert.Equal(t, served, point.Total)
	assert.Equal(t, served, point.Modules[bandwidthOtherModule])
}

func TestBandwidthUnattributableBytesAreNamedNotDropped(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	// An object with no module or pkg metadata, served to a client that sent no
	// X-Cache-Module header: the bytes are real, so they are counted under a
	// name that says nothing could be attributed to them.
	store.record(bandwidthSample{bytes: 99})

	point := store.window(5).Points[bandwidthBucketCount-1]
	assert.Equal(t, int64(99), point.Modules[bandwidthUnknownModule])
	assert.Equal(t, int64(99), point.Total)
}

func TestBandwidthZeroByteResponsesAreNotRecorded(t *testing.T) {
	store, _ := testBandwidthStore(testBandwidthEpoch)
	// A conditional index fetch answered 304 carries no body, so it is not
	// bandwidth and must not appear as a point of its own.
	store.record(bandwidthSample{index: true})
	store.record(bandwidthSample{module: "example.com/app"})

	for _, point := range store.window(5).Points {
		assert.Zero(t, point.Total)
		assert.Empty(t, point.Modules)
		assert.Zero(t, point.Index)
	}
}

// A server built before the store existed, or a handler driven directly in a
// test, has no store: the endpoint still answers the window's shape so the page
// draws an empty chart instead of an error.
func TestBandwidthNilStoreAnswersTheEmptyWindow(t *testing.T) {
	var store *bandwidthStore
	window := store.window(5)
	require.Len(t, window.Points, bandwidthBucketCount)
	assert.Equal(t, []string{bandwidthOtherModule}, window.Bands)
	assert.Equal(t, bandwidthBucketSeconds, window.BucketSeconds)
	assert.Equal(t, bandwidthRetentionSeconds, window.RetentionSeconds)

	var previous int64
	for i, point := range window.Points {
		assert.Zero(t, point.Total)
		if i > 0 {
			assert.Equal(t, previous+bandwidthBucketSeconds, point.Start, "the stamps stay contiguous")
		}
		previous = point.Start
	}

	// Recording into it is a no-op rather than a panic.
	store.record(bandwidthSample{module: "example.com/app", bytes: 10})
}
