package main

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDashboard(t *testing.T, g prometheus.Gatherer) *dashboard {
	t.Helper()
	cfg := &Config{
		Listen:  "127.0.0.1:9000",
		Bucket:  "test-cache",
		DataDir: t.TempDir(),
	}
	d := newDashboard(&Server{}, cfg, time.Now().Add(-90*time.Second))
	if g != nil {
		d.gatherer = g
	}
	return d
}

func TestDashboardListenAddrDefaultsOn(t *testing.T) {
	var cfg Config
	assert.Equal(t, defaultDashboardListen, cfg.DashboardListenAddr())

	off := ""
	cfg.DashboardListen = &off
	assert.Empty(t, cfg.DashboardListenAddr(), "an explicit empty dashboard_listen turns the dashboard off")

	addr := "127.0.0.1:19999"
	cfg.DashboardListen = &addr
	assert.Equal(t, addr, cfg.DashboardListenAddr())
}

func TestDashboardServesPageAndAssets(t *testing.T) {
	h := testDashboard(t, nil).handler()
	for path, want := range map[string]string{
		"/":                    "text/html; charset=utf-8",
		"/dashboard.css":       "text/css; charset=utf-8",
		"/dashboard.js":        "text/javascript; charset=utf-8",
		"/icon.svg":            "image/svg+xml",
		"/icon-monochrome.svg": "image/svg+xml",
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, path)
		assert.Equal(t, want, rec.Header().Get("Content-Type"), path)
		assert.NotEmpty(t, rec.Body.Bytes(), path)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// dashboard.js reads `checked` off a scratch-toggle, which is a property the
// element only has a single time the browser has upgraded it. A classic script
// runs before the deferred module that defines the components, so it read
// undefined, took "live" for off, and polled exactly a single time. The page
// then sat on a single snapshot forever. A module script runs after that
// definition, in document order, which is what keeps the poll loop alive.
func TestDashboardLoadsItsScriptAsAModule(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard.html")
	require.NoError(t, err)
	assert.Contains(t, string(page), `<script type="module" src="/dashboard.js">`)
	assert.NotContains(t, string(page), `<script src="/dashboard.js">`)
}

// The graphs come from the org library at runtime, never vendored, so an
// upstream fix reaches this page with no change here.
func TestDashboardImportsTheGraphFromTheLibrarySite(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard.html")
	require.NoError(t, err)
	script, err := dashboardAssets.ReadFile("dashboard.js")
	require.NoError(t, err)
	assert.Contains(t, string(page), `<script type="module" src="https://sites.pazer.build/js-snippets/@library/ui/perf-graph.js">`)
	for name, body := range map[string][]byte{"dashboard.html": page, "dashboard.js": script} {
		assert.NotContains(t, string(body), "sites.pazer.build/js-snippets/branch/", name)
		assert.NotContains(t, string(body), "wow-look-at-my.github.io", name)
	}
}

// "copy json" copies the /api/stats body the page last drew, so the button and
// its handler must both exist, and the poll must keep the raw text.
func TestDashboardHasCopyJSONButton(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard.html")
	require.NoError(t, err)
	script, err := dashboardAssets.ReadFile("dashboard.js")
	require.NoError(t, err)
	assert.Contains(t, string(page), `<scratch-button id="copy-json"`)
	assert.Contains(t, string(script), `$("copy-json").addEventListener("click"`)
	assert.Contains(t, string(script), `state.raw = raw;`)
	assert.Contains(t, string(script), `await writeClipboard(state.raw);`)
}

// published through an access proxy, which is where identity is checked.
func TestDashboardStatsNeedNoCredentials(t *testing.T) {
	reg := prometheus.NewRegistry()
	gets := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_get_requests_total"}, []string{"outcome"})
	bytesGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "s3_cache_bytes"})
	reg.MustRegister(gets, bytesGauge)
	gets.WithLabelValues("hit").Add(7)
	gets.WithLabelValues("miss_not_found").Add(3)
	bytesGauge.Set(4096)

	d := testDashboard(t, reg)
	rec := httptest.NewRecorder()
	d.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, dashboardStatsPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var got dashboardStats
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "test-cache", got.Server.Bucket)
	assert.Greater(t, got.UptimeSeconds, 60.0)
	assert.InDelta(t, 4096, *got.Metrics["s3_cache_bytes"].Value, 0)
	outcomes := got.Metrics["s3_get_requests_total"].Series
	assert.InDelta(t, 7, seriesValue(t, outcomes, map[string]string{"outcome": "hit"}), 0)
	assert.InDelta(t, 3, seriesValue(t, outcomes, map[string]string{"outcome": "miss_not_found"}), 0)
}

// seriesValue returns the value of the single series whose labels are exactly want.
func seriesValue(t *testing.T, series []seriesPoint, want map[string]string) float64 {
	t.Helper()
	for _, p := range series {
		if maps.Equal(p.Labels, want) {
			return p.Value
		}
	}
	require.Failf(t, "no series with these labels", "want %v in %v", want, series)
	return 0
}

// A labelled counter reaches the page as a list of series, each carrying its
// labels as an object. The page reads a label by name and never parses a key.
func TestDashboardStatsCarryThePerProjectSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	objects := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "s3_project_objects_total"}, []string{"project", "kind"})
	reg.MustRegister(objects)
	objects.WithLabelValues("github.com/wow-look-at-my/go-toolchain", objKindHit).Add(9)
	objects.WithLabelValues("github.com/wow-look-at-my/go-toolchain", objKindMiss).Add(1)
	objects.WithLabelValues("github.com/wow-look-at-my/js-snippets", objKindPut).Add(4)

	d := testDashboard(t, reg)
	stats, err := d.snapshot()
	require.NoError(t, err)

	got := stats.Metrics["s3_project_objects_total"].Series
	require.NotNil(t, got, "the per-project counter reaches the snapshot as a series")
	assert.InDelta(t, 9, seriesValue(t, got, map[string]string{"kind": "hit", "project": "github.com/wow-look-at-my/go-toolchain"}), 0)
	assert.InDelta(t, 1, seriesValue(t, got, map[string]string{"kind": "miss", "project": "github.com/wow-look-at-my/go-toolchain"}), 0)
	assert.InDelta(t, 4, seriesValue(t, got, map[string]string{"kind": "put", "project": "github.com/wow-look-at-my/js-snippets"}), 0)

	// The wire shape itself: labels are an object, never a "k=v,k=v" key.
	body, err := json.Marshal(stats.Metrics["s3_project_objects_total"])
	require.NoError(t, err)
	assert.Contains(t, string(body), `{"labels":{"kind":"hit","project":"github.com/wow-look-at-my/go-toolchain"},"value":9}`)
	assert.NotContains(t, string(body), "kind=")
}

// The snapshot is built for a browser, so it must not carry a credential from
// the config it reports.
func TestDashboardStatsCarryNoCredentials(t *testing.T) {
	d := testDashboard(t, prometheus.NewRegistry())
	d.cfg.Credentials = []Credential{{
		Username: ConfigString{Value: "builder"},
		Password: ConfigString{Value: "hunter2-secret"},
	}}

	rec := httptest.NewRecorder()
	d.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, dashboardStatsPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "hunter2-secret")
	assert.NotContains(t, rec.Body.String(), "builder")
}

func TestDashboardReportsDraining(t *testing.T) {
	d := testDashboard(t, prometheus.NewRegistry())
	stats, err := d.snapshot()
	require.NoError(t, err)
	assert.False(t, stats.Server.Draining)

	d.srv.BeginShutdown()
	stats, err = d.snapshot()
	require.NoError(t, err)
	assert.True(t, stats.Server.Draining, "the page must show the server is draining")
}

// A histogram has no single value, so it contributes both numbers an
// average needs. The buckets stay in /metrics.
func TestGatherFlattensHistogramsAndMultiLabelSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cache_op_seconds"})
	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cache_http_requests_total"}, []string{"method", "route", "status"})
	reg.MustRegister(hist, reqs)
	hist.Observe(0.25)
	hist.Observe(0.75)
	reqs.WithLabelValues("GET", "GetObject", "200").Inc()

	got, err := gatherDashboardMetrics(reg)
	require.NoError(t, err)
	assert.InDelta(t, 2, *got["cache_op_seconds_count"].Value, 0)
	assert.InDelta(t, 1.0, *got["cache_op_seconds_sum"].Value, 0.001)
	assert.InDelta(t, 1, seriesValue(t, got["cache_http_requests_total"].Series, map[string]string{"method": "GET", "route": "GetObject", "status": "200"}), 0)
}
