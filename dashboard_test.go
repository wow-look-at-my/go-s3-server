package main

import (
	"encoding/json"
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
		"/":              "text/html; charset=utf-8",
		"/dashboard.css": "text/css; charset=utf-8",
		"/dashboard.js":  "text/javascript; charset=utf-8",
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

// The stats endpoint must answer with no credentials: the dashboard port is
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
	assert.InDelta(t, 7, got.Metrics["s3_get_requests_total"].Series["hit"], 0)
	assert.InDelta(t, 3, got.Metrics["s3_get_requests_total"].Series["miss_not_found"], 0)
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

// A histogram has no single value, so it contributes the two numbers an
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
	assert.InDelta(t, 1, got["cache_http_requests_total"].Series["method=GET,route=GetObject,status=200"], 0)
}
