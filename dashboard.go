package main

import (
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// The dashboard is a single page, its assets and its icon. They are embedded so
// the binary stays the whole deployment: no asset directory to mount, and no way
// for the page to disagree with the server that serves it.
//
//go:embed dashboard.html dashboard.css dashboard.js icon.svg icon-monochrome.svg
var dashboardAssets embed.FS

// It is a SEPARATE port from the cache API on purpose: an access proxy
// (Cloudflare empty Trust, or any other) fronts this port and leaves the cache
// protocol port alone, so operators reach the dashboard through their identity
// provider while build machines keep talking basic auth to the API.
const defaultDashboardListen = ":9002"

// dashboardStatsPath serves the snapshot the page polls. Under it, the numbers
// come from the same Prometheus registry /metrics serves, so the page and the
// scrape can never report different values for a single counter.
const dashboardStatsPath = "/api/stats"

// dashboardBandwidthPath serves the bandwidth time series the page's chart
// polls. It is beside dashboardStatsPath and answered without credentials for
// the same reason: the dashboard port is the one an access proxy fronts, and
// identity is checked there.
const dashboardBandwidthPath = "/api/bandwidth"

// dashboardBandwidthTopModules is how many modules the chart names on its own
// axis key. Everything past it is summed into one remainder band, so a fleet of
// hundreds of modules still reads as one stack instead of as a legend nobody
// can find anything in.
const dashboardBandwidthTopModules = 5

// metricValue is a metric with no labels (Value) or a labeled metric (Series).
type metricValue struct {
	Value  *float64      `json:"value,omitempty"`
	Series []seriesPoint `json:"series,omitempty"`
}

// The labels are an object, so a reader looks a label up by its name and
// never parses a key.
type seriesPoint struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// dashboardServerInfo is the configuration the page shows. It carries no
// credential: the config holds usernames and passwords, and this page is
// reachable by everyone the access proxy lets in.
type dashboardServerInfo struct {
	Bucket                string `json:"bucket"`
	DataDir               string `json:"data_dir"`
	Listen                string `json:"listen"`
	MetricsListen         string `json:"metrics_listen"`
	DashboardListen       string `json:"dashboard_listen"`
	AuthDisabled          bool   `json:"auth_disabled"`
	WriteOnceAction       string `json:"write_once_action"`
	WriteOnceNotification string `json:"write_once_notification"`
	MaxConcurrentRequests int    `json:"max_concurrent_requests"`
	MaxObjectBytes        int64  `json:"max_object_bytes"`
	Draining              bool   `json:"draining"`
}

type dashboardEvictionInfo struct {
	Enabled  bool   `json:"enabled"`
	MaxBytes int64  `json:"max_bytes"`
	MaxAge   string `json:"max_age"`
	Interval string `json:"interval"`
}

type dashboardStats struct {
	GeneratedAt   time.Time              `json:"generated_at"`
	UptimeSeconds float64                `json:"uptime_seconds"`
	Server        dashboardServerInfo    `json:"server"`
	Eviction      dashboardEvictionInfo  `json:"eviction"`
	Metrics       map[string]metricValue `json:"metrics"`
}

// dashboard answers the page, its assets, the stats snapshot, and the
// bandwidth series.
type dashboard struct {
	srv      *Server
	cfg      *Config
	started  time.Time
	gatherer prometheus.Gatherer
	// bandwidth is the served-byte history the chart draws. It is the server's
	// own store, so the page and the accounting cannot disagree about a byte.
	bandwidth *bandwidthStore
}

func newDashboard(srv *Server, cfg *Config, started time.Time) *dashboard {
	d := &dashboard{srv: srv, cfg: cfg, started: started, gatherer: prometheus.DefaultGatherer}
	if srv != nil {
		d.bandwidth = srv.bandwidth
	}
	return d
}

func (d *dashboard) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(dashboardStatsPath, d.serveStats)
	mux.HandleFunc(dashboardBandwidthPath, d.serveBandwidth)
	mux.HandleFunc(healthPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", d.servePage)
	return mux
}

// servePage answers the page at "/" and its assets by name. It does not serve
// the embedded directory as a tree: the page must be at the root, which is
// where an access proxy points its hostname.
func (d *dashboard) servePage(w http.ResponseWriter, r *http.Request) {
	name, contentType := "", ""
	switch r.URL.Path {
	case "/":
		name, contentType = "dashboard.html", "text/html; charset=utf-8"
	case "/dashboard.css":
		name, contentType = "dashboard.css", "text/css; charset=utf-8"
	case "/dashboard.js":
		name, contentType = "dashboard.js", "text/javascript; charset=utf-8"
	case "/icon.svg":
		name, contentType = "icon.svg", "image/svg+xml"
	case "/icon-monochrome.svg":
		name, contentType = "icon-monochrome.svg", "image/svg+xml"
	default:
		http.NotFound(w, r)
		return
	}
	body, err := dashboardAssets.ReadFile(name)
	if err != nil {
		http.Error(w, "dashboard asset missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(body)
}

func (d *dashboard) serveStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.snapshot()
	if err != nil {
		// A gather failure means the numbers below would be partial. Say so
		// instead of drawing a page of stale or missing values.
		http.Error(w, "collect metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	if err := enc.Encode(stats); err != nil {
		log.Printf("dashboard: write stats: %v", err)
	}
}

// dashboardBandwidth is the body of the bandwidth endpoint: the retained
// window, stamped with the moment it was read.
type dashboardBandwidth struct {
	GeneratedAt time.Time `json:"generated_at"`
	bandwidthWindow
}

// serveBandwidth answers the bandwidth chart's series.
//
// The retention window is FIXED and small: bandwidthRetentionSeconds (300) of
// bandwidthBucketSeconds-wide (1 second) buckets, so the answer is always
// bandwidthBucketCount (300) points, one per second of the last five minutes,
// whether the server served nothing in them or a fleet's whole day.
//
// That window is what keeps the accounting bounded, and bounded in process
// memory is where it has to live: /metrics holds counters and gauges with no
// time axis, and a per-module label on one would be unbounded cardinality --
// the modules a fleet builds into this cache are unbounded, and a series per
// module per second that is never forgotten is a leak with a graph on top of
// it. So the store keeps a ring of bandwidthBucketCount buckets, holding at
// most bandwidthModuleCap module names in each, which is 300 x 32 counters plus
// one index counter per bucket -- a constant, whatever the traffic. Nothing
// sweeps it: recording into the second a bucket already covers, or past it,
// overwrites what was there, so the oldest second is dropped by arithmetic and
// the store cannot grow.
//
// The window is split three ways: the total served (what the chart's axis is
// scaled to), the index fetches as a series of their own, and the module bands,
// of which the top dashboardBandwidthTopModules by bytes over the window are
// named and every other module is summed into one remainder. The bands and the
// index series add up to the total for every point.
func (d *dashboard) serveBandwidth(w http.ResponseWriter, r *http.Request) {
	body := dashboardBandwidth{
		GeneratedAt:     time.Now(),
		bandwidthWindow: d.bandwidth.window(dashboardBandwidthTopModules),
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	if err := enc.Encode(body); err != nil {
		log.Printf("dashboard: write bandwidth: %v", err)
	}
}

func (d *dashboard) snapshot() (*dashboardStats, error) {
	metrics, err := gatherDashboardMetrics(d.gatherer)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &dashboardStats{
		GeneratedAt:   now,
		UptimeSeconds: now.Sub(d.started).Seconds(),
		Server: dashboardServerInfo{
			Bucket:                d.cfg.Bucket,
			DataDir:               d.cfg.DataDir,
			Listen:                d.cfg.Listen,
			MetricsListen:         d.cfg.MetricsListen,
			DashboardListen:       d.cfg.DashboardListenAddr(),
			AuthDisabled:          d.cfg.DisableAuth,
			WriteOnceAction:       d.cfg.WriteOnce.Action,
			WriteOnceNotification: d.cfg.WriteOnce.Notification,
			MaxConcurrentRequests: d.cfg.MaxConcurrentRequests,
			MaxObjectBytes:        d.cfg.MaxObjectBytes,
			Draining:              d.srv != nil && d.srv.shuttingDown.Load(),
		},
		Eviction: dashboardEvictionInfo{
			Enabled:  d.cfg.Eviction.Enabled(),
			MaxBytes: d.cfg.Eviction.SizeLimit(),
			MaxAge:   d.cfg.Eviction.AgeLimit().String(),
			Interval: d.cfg.Eviction.Interval.Std().String(),
		},
		Metrics: metrics,
	}, nil
}

// "<name>_sum", which is what an average duration needs; the buckets stay in
// /metrics for a real time-series database to read.
func gatherDashboardMetrics(g prometheus.Gatherer) (map[string]metricValue, error) {
	families, err := g.Gather()
	if err != nil {
		return nil, err
	}
	out := make(map[string]metricValue, len(families))
	for _, fam := range families {
		switch fam.GetType() {
		case dto.MetricType_COUNTER, dto.MetricType_GAUGE:
			addFamily(out, fam.GetName(), fam.GetMetric(), scalarOf)
		case dto.MetricType_HISTOGRAM:
			addFamily(out, fam.GetName()+"_count", fam.GetMetric(), func(m *dto.Metric) float64 {
				return float64(m.GetHistogram().GetSampleCount())
			})
			addFamily(out, fam.GetName()+"_sum", fam.GetMetric(), func(m *dto.Metric) float64 {
				return m.GetHistogram().GetSampleSum()
			})
		}
	}
	return out, nil
}

func scalarOf(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	if gg := m.GetGauge(); gg != nil {
		return gg.GetValue()
	}
	return 0
}

func addFamily(out map[string]metricValue, name string, metrics []*dto.Metric, value func(*dto.Metric) float64) {
	for _, m := range metrics {
		if len(m.GetLabel()) == 0 {
			v := value(m)
			out[name] = metricValue{Value: &v}
			continue
		}
		labels := make(map[string]string, len(m.GetLabel()))
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		mv := out[name]
		mv.Series = append(mv.Series, seriesPoint{Labels: labels, Value: value(m)})
		out[name] = mv
	}
}

// startDashboardServer serves the dashboard on addr. A bind failure is logged
// and the cache keeps running WITHOUT the dashboard, for the same reason the
// metrics listener does: a busy dashboard port must not take down the data
// path. The failure is loud, so an operator who wanted the dashboard learns
// the port is taken instead of finding a page that never loads.
func startDashboardServer(addr string, d *dashboard) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           d.handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("dashboard unavailable on %s (continuing WITHOUT the dashboard): %v", addr, err)
	}
}
