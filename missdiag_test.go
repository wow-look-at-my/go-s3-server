package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func batchMissCount(reason string) float64 {
	return testutil.ToFloat64(batchMissesTotal.WithLabelValues(reason))
}

func cacheprogKey(fill byte) string {
	return gbciKeyPrefix + strings.Repeat(string(fill), 2*gbciHashSize)
}

// A batch asks for keys it believes the server holds. Each the answer leaves
// out is counted under the reason it was left out, so the panel can say why
// rather than how many.
func TestBatchGet_MissesAreCountedByReason(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, storage := testSetupWithStorage(t)

	// The sweep removes every object it finds and rebuilds the index from disk, so the other keys are stored after it.
	evicted := cacheprogKey('4')
	require.NoError(t, storage.Put(evicted, []byte("body"), map[string]string{"outputid": "z"}, nil))
	_, err := storage.Evict(0, 1, time.Now().Add(time.Hour))
	require.NoError(t, err)
	_, err = storage.Stat(evicted)
	require.ErrorIs(t, err, ErrNotFound, "the sweep removed the object")

	stored := cacheprogKey('1')
	require.NoError(t, storage.Put(stored, []byte("body"), map[string]string{"outputid": "x"}, nil))
	unknown := cacheprogKey('2')
	advertised := cacheprogKey('3')
	require.NoError(t, storage.Put(advertised, []byte("body"), map[string]string{"outputid": "y"}, nil))
	require.NoError(t, os.Remove(storage.keyToPath(advertised)))

	before := map[string]float64{}
	for _, reason := range []string{missNotFound, missAdvertised, missEvicted} {
		before[reason] = batchMissCount(reason)
	}
	manifest, _ := batchGetManifestFor(t, ts, batchGetRequest{Keys: []string{stored, unknown, advertised, evicted}})
	require.Len(t, manifest.Entries, 1)
	assert.Equal(t, stored, manifest.Entries[0].Key)

	assert.Equal(t, before[missNotFound]+1, batchMissCount(missNotFound), "a key the server never advertised")
	assert.Equal(t, before[missAdvertised]+1, batchMissCount(missAdvertised), "a key /_index advertises with no body")
	assert.Equal(t, before[missEvicted]+1, batchMissCount(missEvicted), "a key the sweeper removed")
}

// A key stored again after an eviction is a hit, never a capacity miss.
func TestEvictedMemoForgetsARestoredKey(t *testing.T) {
	storage := newTestStorage(t)
	key := cacheprogKey('5')
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "x"}, nil))
	_, err := storage.Evict(0, 1, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, storage.wasEvicted(key))
	assert.Equal(t, missEvicted, absentKeyReason(storage, key))

	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "x"}, nil))
	assert.False(t, storage.wasEvicted(key))
}

// The GET path reads the same reasons as the batch path.
func TestGetOutcome_EvictedKeyIsACapacityMiss(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, storage := testSetupWithStorage(t)
	key := cacheprogKey('6')
	require.NoError(t, storage.Put(key, []byte("body"), map[string]string{"outputid": "x"}, nil))
	_, err := storage.Evict(0, 1, time.Now().Add(time.Hour))
	require.NoError(t, err)

	evictedBefore := getOutcome("miss_evicted")
	notFoundBefore := getOutcome("miss_not_found")
	resp := doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
	assert.Equal(t, evictedBefore+1, getOutcome("miss_evicted"))
	assert.Equal(t, notFoundBefore, getOutcome("miss_not_found"))
}

func missRegistry(t *testing.T) (*prometheus.Registry, *prometheus.CounterVec, *prometheus.CounterVec, *prometheus.CounterVec) {
	t.Helper()
	reg := prometheus.NewRegistry()
	skips := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_client_skips_total"}, []string{"op", "reason"})
	batch := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_batch_misses_total"}, []string{"reason"})
	gets := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_get_requests_total"}, []string{"outcome"})
	reg.MustRegister(skips, batch, gets)
	return reg, skips, batch, gets
}

func reasonByKey(m dashboardMisses, key string) missReasonRow {
	for _, r := range m.Reasons {
		if r.Key == key {
			return r
		}
	}
	return missReasonRow{}
}

// The diagnosis adds the client's skips to the server's own misses, gives
// each cause its share, and names the biggest one.
func TestDiagnoseMissesSortsEveryMissIntoACause(t *testing.T) {
	reg, skips, batch, gets := missRegistry(t)
	skips.WithLabelValues("get", "not_in_index").Add(60)
	skips.WithLabelValues("get", "empty_index").Add(10)
	skips.WithLabelValues("put", "known").Add(500)
	batch.WithLabelValues(missEvicted).Add(20)
	batch.WithLabelValues(missNotFound).Add(5)
	gets.WithLabelValues("hit").Add(1000)
	gets.WithLabelValues("miss_evicted").Add(5)

	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{})

	assert.InDelta(t, 100, m.Total, 0, "every get miss, and no put skip and no hit")
	assert.Equal(t, "not_in_index", m.Dominant)
	assert.InDelta(t, 0.6, reasonByKey(m, "not_in_index").Share, 0.001)
	assert.InDelta(t, 25, reasonByKey(m, missEvicted).Count, 0, "the batch reason and the single-GET outcome add up")
	assert.InDelta(t, 0.25, reasonByKey(m, missEvicted).Share, 0.001)
	assert.Equal(t, missSeverityCapacity, reasonByKey(m, missEvicted).Severity)
	assert.Contains(t, m.Verdict, "60.0% of misses: never stored.")
	assert.Len(t, m.Reasons, len(missReasonSpecs), "every cause keeps its row, zero or not")
	for _, r := range m.Reasons {
		assert.NotEmpty(t, r.Explain, r.Key)
		assert.NotEmpty(t, r.Action, r.Key)
	}
}

// A bug is named in the verdict even when an expected cause has more misses:
// an expected miss needs nobody, a bug needs somebody.
func TestDiagnoseMissesNamesABugBesideTheDominantCause(t *testing.T) {
	reg, skips, batch, _ := missRegistry(t)
	skips.WithLabelValues("get", "not_in_index").Add(90)
	batch.WithLabelValues(missAdvertised).Add(3)

	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{})
	assert.Equal(t, "not_in_index", m.Dominant)
	assert.Contains(t, m.Verdict, "Also 3 advertised but unservable, which is a bug")
}

// An upload a client never completed explains a never-stored count that
// does not fall on repeat builds, so the verdict names it.
func TestDiagnoseMissesNamesTheUploadsClientsLost(t *testing.T) {
	reg, skips, _, _ := missRegistry(t)
	lost := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_client_uploads_lost_total"}, []string{"reason"})
	reg.MustRegister(lost)
	skips.WithLabelValues("get", "not_in_index").Add(50)
	lost.WithLabelValues("failed").Add(4)
	lost.WithLabelValues("abandoned").Add(2)
	lost.WithLabelValues("refused").Add(9)

	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{})
	assert.Contains(t, m.Verdict, "Clients reported 6 uploads that never reached the store (4 failed, 2 abandoned)")
	var failed, refused missSignal
	for _, s := range m.Signals {
		switch s.Key {
		case "uploads_failed":
			failed = s
		case "uploads_refused":
			refused = s
		}
	}
	assert.InDelta(t, 4, failed.Count, 0)
	assert.True(t, failed.Alert)
	assert.InDelta(t, 9, refused.Count, 0)
	assert.False(t, refused.Alert, "a refusal is the client doing its job")

	m = diagnoseMisses(map[string]metricValue{}, dashboardEvictionInfo{})
	assert.NotContains(t, m.Verdict, "Clients reported")
}

// A client that skips a key its index lacks cannot know whether the sweeper
// evicted it. The server classifies the keys the client sampled. When most
// of them were evicted, the never-stored row becomes a capacity miss and
// the verdict says to grow the cache.
func TestDiagnoseMissesReadsTheSkipSample(t *testing.T) {
	reg, skips, _, _ := missRegistry(t)
	samples := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_client_skip_samples_total"}, []string{"verdict"})
	reg.MustRegister(samples)
	skips.WithLabelValues("get", "not_in_index").Add(1000)
	samples.WithLabelValues("evicted").Add(60)
	samples.WithLabelValues("advertised").Add(10)
	samples.WithLabelValues("unknown").Add(30)

	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{})
	assert.InDelta(t, 100, m.Sample.Keys, 0)
	assert.InDelta(t, 0.6, m.Sample.EvictedShare, 0.001)
	assert.Contains(t, m.Verdict, "most of the never-stored misses are evictions")
	row := reasonByKey(m, "not_in_index")
	assert.Equal(t, missSeverityCapacity, row.Severity)
	assert.Contains(t, row.Explain, "Of 100 skipped keys clients sampled, 60.0% were keys the sweeper had evicted")

	m = diagnoseMisses(map[string]metricValue{}, dashboardEvictionInfo{})
	assert.Zero(t, m.Sample.Keys)
	assert.Equal(t, missSeverityExpected, reasonByKey(m, "not_in_index").Severity)
}

func TestDiagnoseMissesWithNothingRecorded(t *testing.T) {
	reg, _, _, _ := missRegistry(t)
	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{})
	assert.Zero(t, m.Total)
	assert.Empty(t, m.Dominant)
	assert.Contains(t, m.Verdict, "No miss recorded yet")
	for _, r := range m.Reasons {
		assert.Zero(t, r.Share)
	}
}

// The signals read the cache against its budget. A full cache is the
// explanation for a capacity miss.
func TestDiagnoseMissesSignalsCacheFill(t *testing.T) {
	reg := prometheus.NewRegistry()
	cacheBytesGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "s3_cache_bytes"})
	shed := prometheus.NewCounter(prometheus.CounterOpts{Name: "s3_http_rejected_total"})
	reg.MustRegister(cacheBytesGauge, shed)
	cacheBytesGauge.Set(95 << 20)
	shed.Add(2)

	stats, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	m := diagnoseMisses(stats, dashboardEvictionInfo{Enabled: true, MaxBytes: 100 << 20})
	var fill, rejected missSignal
	for _, s := range m.Signals {
		switch s.Key {
		case "cache_fill":
			fill = s
		case "shed":
			rejected = s
		}
	}
	assert.InDelta(t, 0.95, fill.Count, 0.001)
	assert.True(t, fill.Alert, "a cache over nine tenths full evicts on every sweep")
	assert.Contains(t, fill.Note, "95MiB of 100MiB")
	assert.InDelta(t, 2, rejected.Count, 0)
	assert.True(t, rejected.Alert)

	m = diagnoseMisses(stats, dashboardEvictionInfo{})
	for _, s := range m.Signals {
		if s.Key == "cache_fill" {
			assert.Zero(t, s.Count)
			assert.Contains(t, s.Note, "no size budget")
		}
	}
}

// The snapshot carries the diagnosis, so the page and "copy json" both have it.
func TestDashboardStatsCarryTheMissDiagnosis(t *testing.T) {
	reg, skips, _, _ := missRegistry(t)
	skips.WithLabelValues("get", "batch_backoff").Add(7)
	d := testDashboard(t, reg)
	rec := httptest.NewRecorder()
	d.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, dashboardStatsPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var got dashboardStats
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.InDelta(t, 7, got.Misses.Total, 0)
	assert.Equal(t, "batch_backoff", got.Misses.Dominant)
	assert.Contains(t, rec.Body.String(), `"verdict": "100.0% of misses: client gave up probing.`)
}

// The page draws every cause the server can report and the verdict it writes.
func TestDashboardDrawsTheMissPanel(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard.html")
	require.NoError(t, err)
	script, err := dashboardAssets.ReadFile("dashboard.js")
	require.NoError(t, err)
	assert.Contains(t, string(page), `<h2>WHY MISSES</h2>`)
	assert.Contains(t, string(script), `stats.misses`)
	assert.Contains(t, string(script), `m.verdict`)
	for _, id := range []string{"miss-verdict", "miss-reasons", "miss-explain", "miss-signals", "miss-label"} {
		assert.Contains(t, string(page), `id="`+id+`"`)
	}
}
