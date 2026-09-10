package main

import (
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// The dashboard is one page, its two assets and its icon. They are embedded so the
// binary stays the whole deployment: no asset directory to mount, and no way
// for the page to disagree with the server that serves it.
//
//go:embed dashboard.html dashboard.css dashboard.js icon.svg icon-monochrome.svg
var dashboardAssets embed.FS

// defaultDashboardListen is the address the dashboard binds when the config
// does not name one. It is a SEPARATE port from the cache API on purpose: an
// access proxy (Cloudflare Zero Trust, or any other) fronts this port and
// leaves the cache protocol port alone, so operators reach the dashboard
// through their identity provider while build machines keep talking basic auth
// to the API.
const defaultDashboardListen = ":9002"

// dashboardStatsPath serves the snapshot the page polls. Under it, the numbers
// come from the same Prometheus registry /metrics serves, so the page and the
// scrape can never report different values for one counter.
const dashboardStatsPath = "/api/stats"

// metricValue is one metric family, flattened for the browser: Value for a
// metric with no labels, Series for a labeled one (label set -> number).
type metricValue struct {
	Value  *float64           `json:"value,omitempty"`
	Series map[string]float64 `json:"series,omitempty"`
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

// dashboard answers the page, its assets, and the stats snapshot.
type dashboard struct {
	srv      *Server
	cfg      *Config
	started  time.Time
	gatherer prometheus.Gatherer
}

func newDashboard(srv *Server, cfg *Config, started time.Time) *dashboard {
	return &dashboard{srv: srv, cfg: cfg, started: started, gatherer: prometheus.DefaultGatherer}
}

func (d *dashboard) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(dashboardStatsPath, d.serveStats)
	mux.HandleFunc(healthPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", d.servePage)
	return mux
}

// servePage answers the page at "/" and its two assets by name. It does not
// serve the embedded directory as a tree: the page must be at the root, which
// is where an access proxy points its hostname.
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

// gatherDashboardMetrics flattens the registry into name -> value. A counter or
// gauge with no labels becomes one number. A labeled one becomes a series keyed
// by its label set. A histogram contributes "<name>_count" and "<name>_sum",
// which is what an average duration needs; the buckets stay in /metrics for a
// real time-series database to read.
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
		mv := out[name]
		if mv.Series == nil {
			mv.Series = make(map[string]float64, len(metrics))
		}
		mv.Series[seriesKey(m.GetLabel())] += value(m)
		out[name] = mv
	}
}

// seriesKey names one labeled series. A single label reads as its bare value
// ("hit"), because every one-label metric here already says what the label
// means in its own name. More than one label reads as "k=v,k=v", sorted so the
// key is stable between polls.
func seriesKey(labels []*dto.LabelPair) string {
	if len(labels) == 1 {
		return labels[0].GetValue()
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, l.GetName()+"="+l.GetValue())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
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
