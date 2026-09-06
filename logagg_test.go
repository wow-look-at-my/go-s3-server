package main

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureAggregator returns an aggregator whose clock and output the test
// drives, so a second can be crossed without waiting for one.
func captureAggregator(t *testing.T, clock *time.Time) (*logAggregator, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	a := newLogAggregator()
	a.now = func() time.Time { return *clock }
	a.printf = func(format string, v ...any) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "%s", format, "a bucket line is printed verbatim, never re-formatted")
		lines = append(lines, v[0].(string))
	}
	return a, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
}

func TestLogAggregatorReportsOneLinePerActiveSecond(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	a, lines := captureAggregator(t, &clock)

	// One second of traffic: two puts and three gets, most of it batched.
	a.Record(objectEvent{put: true, wire: 1000, raw: 4000, rawKnown: true, project: "example.com/alpha"})
	a.Record(objectEvent{put: true, batched: true, wire: 1000, raw: 4000, rawKnown: true, project: "example.com/alpha"})
	a.Record(objectEvent{batched: true, wire: 500, raw: 2000, rawKnown: true, project: "example.com/beta"})
	a.Record(objectEvent{batched: true, wire: 500, raw: 2000, rawKnown: true, project: "example.com/beta"})
	a.Record(objectEvent{batched: true, wire: 1000, raw: 4000, rawKnown: true, project: "example.com/alpha"})

	// Nothing is emitted while the second is still open: a partial second
	// would report a rate over an interval that has not elapsed.
	a.flush(false)
	assert.Empty(t, lines(), "a second still in progress must not be reported")

	clock = clock.Add(time.Second)
	a.flush(false)

	got := lines()
	require.Len(t, got, 1)
	line := got[0]
	assert.Contains(t, line, "put=2")
	assert.Contains(t, line, "get=3")
	// Four of the five objects moved through a batch endpoint.
	assert.Contains(t, line, "batched=80%")
	assert.Contains(t, line, "compressed=3.9KiB/s")
	assert.Contains(t, line, "uncompressed=16KiB/s")
	// 4000 compressed against 16000 uncompressed.
	assert.Contains(t, line, "ratio=25%")
	assert.Contains(t, line, "projects=example.com/alpha, example.com/beta")
	assert.NotContains(t, line, "sized=", "every object declared a size, so there is no coverage caveat to print")
}

func TestLogAggregatorSaysNothingForASilentSecond(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	a, lines := captureAggregator(t, &clock)

	clock = clock.Add(5 * time.Second)
	a.flush(false)
	assert.Empty(t, lines(), "a second with no traffic must produce no line at all")
}

// An object whose client declared no body-size cannot be folded into the
// compression ratio: doing so would report a compression that never happened.
func TestLogAggregatorReportsUnsizedObjectsSeparately(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	a, lines := captureAggregator(t, &clock)

	a.Record(objectEvent{wire: 1000, raw: 4000, rawKnown: true, project: "example.com/alpha"})
	a.Record(objectEvent{wire: 8000})

	clock = clock.Add(time.Second)
	a.flush(false)

	got := lines()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "ratio=25%", "the ratio must cover only the object that declared a raw size")
	assert.Contains(t, got[0], "uncompressed=3.9KiB/s", "the uncompressed rate covers the same object the ratio does")
	assert.Contains(t, got[0], "sized=1/2", "the line must say the rates cover different object sets")
	assert.Contains(t, got[0], "compressed=8.8KiB/s", "an object with no declared size still crossed the wire")
}

func TestLogAggregatorFlushesTheOpenSecondOnStop(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	a, lines := captureAggregator(t, &clock)
	a.Record(objectEvent{put: true, wire: 10})

	go a.Run()
	a.Stop()

	assert.Len(t, lines(), 1, "shutdown must not drop the second in progress")
}

func TestLogAggregatorBoundsTheProjectList(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	a, lines := captureAggregator(t, &clock)

	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		a.Record(objectEvent{wire: 1, project: "example.com/" + name})
	}
	clock = clock.Add(time.Second)
	a.flush(false)

	got := lines()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "+2 more")
	assert.Equal(t, maxLoggedProjects, strings.Count(got[0], "example.com/"))
}

func TestNilAggregatorRecordsNothing(t *testing.T) {
	var a *logAggregator
	assert.NotPanics(t, func() {
		a.Record(objectEvent{put: true})
		a.Stop()
		recordObject(a, map[string]string{"module": "example.com/x"}, 10, true, false)
	}, "verbose mode installs no aggregator, and every record site must tolerate that")
}

func TestProjectOfPrefersTheModuleThenTrimsTheImportPath(t *testing.T) {
	assert.Equal(t, "example.com/mod", projectOf(map[string]string{"module": "example.com/mod", "pkg": "example.com/mod/deep/pkg"}))
	assert.Equal(t, "example.com/owner/repo", projectOf(map[string]string{"pkg": "example.com/owner/repo/internal/thing"}))
	assert.Equal(t, "std/fmt", projectOf(map[string]string{"pkg": "std/fmt"}))
	assert.Empty(t, projectOf(map[string]string{"outputid": "x"}))
	assert.Empty(t, projectOf(nil))
}

func TestRawSizeOfRejectsWhatItCannotTrust(t *testing.T) {
	n, ok := rawSizeOf(map[string]string{"body-size": "4096"})
	assert.True(t, ok)
	assert.Equal(t, int64(4096), n)

	for _, meta := range []map[string]string{
		nil,
		{},
		{"body-size": ""},
		{"body-size": "not-a-number"},
		{"body-size": "-1"},
	} {
		_, ok := rawSizeOf(meta)
		assert.False(t, ok, "a missing or malformed body-size must read as unsized, never as zero bytes")
	}
}

func TestByteSizeRendersBinaryUnits(t *testing.T) {
	assert.Equal(t, "0B", byteSize(0))
	assert.Equal(t, "512B", byteSize(512))
	assert.Equal(t, "1.0KiB", byteSize(1024))
	assert.Equal(t, "16KiB", byteSize(16384))
	assert.Equal(t, "1.0MiB", byteSize(1<<20))
	assert.Equal(t, "1.5GiB", byteSize(3<<29))
}

func TestPercentSaysNotApplicableInsteadOfDividingByZero(t *testing.T) {
	assert.Equal(t, "n/a", percent(0, 0))
	assert.Equal(t, "50%", percent(1, 2))
	assert.Equal(t, "n/a", percentInt64(0, 0))
	assert.Equal(t, "33%", percentInt64(1, 3))
}

func TestLogModeConfigDefaultsToNormalAndRejectsAnythingElse(t *testing.T) {
	dir := t.TempDir()
	base := func(extra map[string]any) map[string]any {
		cfg := map[string]any{"bucket": "b", "data_dir": dir, "disable_auth": true}
		for k, v := range extra {
			cfg[k] = v
		}
		return cfg
	}

	loaded, err := LoadConfig(writeConfigFile(t, dir, "default.json", base(nil)))
	require.NoError(t, err)
	assert.Equal(t, logModeNormal, loaded.LogMode)

	loaded, err = LoadConfig(writeConfigFile(t, dir, "verbose.json", base(map[string]any{"log_mode": "verbose"})))
	require.NoError(t, err)
	assert.Equal(t, logModeVerbose, loaded.LogMode)

	_, err = LoadConfig(writeConfigFile(t, dir, "bogus.json", base(map[string]any{"log_mode": "quiet"})))
	require.Error(t, err, "an unknown log_mode is a typo the operator needs to hear about")
	assert.Contains(t, err.Error(), "log_mode")
}
