// It is a SEPARATE port from the cache API on purpose.

package main

import (
	"compress/gzip"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//go:embed dashboard.html dashboard.css dashboard.js icon.svg icon-monochrome.svg
var dashboardAssets embed.FS

// It is a SEPARATE port from the cache API on purpose.
const defaultDashboardListen = ":9002"

// dashboardStatsPath serves the snapshot the page polls.
const dashboardStatsPath = "/api/stats"

// dashboardBandwidthPath serves the bandwidth time series the page's chart polls.
const dashboardBandwidthPath = "/api/bandwidth"

// dashboardVocabPath serves the text the snapshot leaves out: the label and the prose of every cause.
const dashboardVocabPath = "/api/vocab"

// dashboardVocab is the body of the vocabulary endpoint.
type dashboardVocab struct {
	Reasons []missReasonText `json:"reasons"`
	Signals []missSignalText `json:"signals"`
	Causes  []churnCauseText `json:"causes"`
}

// vocabBody is the vocabulary encoded once, with the ETag of its bytes.
var vocabBody = sync.OnceValues(func() ([]byte, string) {
	body, err := json.Marshal(dashboardVocab{Reasons: missReasonVocab(), Signals: missSignalVocab, Causes: churnCauseVocab()})
	if err != nil {
		panic("dashboard: encode vocabulary: " + err.Error())
	}
	sum := sha256.Sum256(body)
	return body, `"` + hex.EncodeToString(sum[:8]) + `"`
})

// projectSeriesNames are the metrics the snapshot's projects map already carries, cell for cell.
var projectSeriesNames = []string{"s3_project_objects_total", "s3_project_bytes_total"}

// dashboardBandwidthTopModules is how many modules the chart names on its own axis key.
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

// dashboardIndexInfo is what a client downloads from GET /_index?v=2 right now.
type dashboardIndexInfo struct {
	// WireBytes is the body length before compression. Zero when the server has no index.
	WireBytes int64 `json:"wire_bytes"`
	// Keys is how many hashes the index advertises.
	Keys int64 `json:"keys"`
	// HashBits is the prefix width each key is sent as.
	HashBits int `json:"hash_bits"`
}

type dashboardStats struct {
	GeneratedAt   time.Time              `json:"generated_at"`
	UptimeSeconds float64                `json:"uptime_seconds"`
	Server        dashboardServerInfo    `json:"server"`
	Eviction      dashboardEvictionInfo  `json:"eviction"`
	Index         dashboardIndexInfo     `json:"index"`
	Metrics       map[string]metricValue `json:"metrics"`
	// Projects holds every project's exact totals.
	Projects map[string]projectRow `json:"projects"`
	// Misses is why the cache missed, from the same counters.
	Misses dashboardMisses `json:"misses"`
	// Churn is what changed between uploads of the same package.
	Churn dashboardChurn `json:"churn"`
}

// dashboard answers the page, its assets, the stats snapshot, and the
// bandwidth series.
type dashboard struct {
	srv      *Server
	cfg      *Config
	started  time.Time
	gatherer prometheus.Gatherer
	// bandwidth is the served-byte history the chart draws.
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
	mux.HandleFunc(dashboardVocabPath, serveVocab)
	mux.HandleFunc(dashboardBandwidthPath, d.serveBandwidth)
	mux.HandleFunc(healthPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", d.servePage)
	return mux
}

// servePage answers the page at "/" and its assets by name. It does not serve
// the embedded directory as a tree. The page must be at the root, which is
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
	// The page and its script must come from the same build. A cached page with a newer script breaks the page.
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(body)
}

func (d *dashboard) serveStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.snapshot()
	if err != nil {
		// A gather failure means the numbers below would be partial.
		http.Error(w, "collect metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	// The page polls this every second, so the body is compact: no indent, and gzip for a client that takes it.
	body := compressedWriter(w, r)
	defer body.Close()
	if err := json.NewEncoder(body).Encode(stats); err != nil {
		log.Printf("dashboard: write stats: %v", err)
	}
}

// serveVocab answers the vocabulary. The body changes only with the build,
// so it carries an ETag and a browser revalidates it instead of downloading
// it again.
func serveVocab(w http.ResponseWriter, r *http.Request) {
	body, etag := vocabBody()
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "no-cache")
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	out := compressedWriter(w, r)
	defer out.Close()
	_, _ = out.Write(body)
}

// compressedWriter wraps w in gzip when the request accepts it, and sets
// the headers that say so. The caller closes it, which flushes the stream.
func compressedWriter(w http.ResponseWriter, r *http.Request) io.WriteCloser {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		return nopCloser{w}
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Add("Vary", "Accept-Encoding")
	return gzip.NewWriter(w)
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// dashboardBandwidth is the body of the bandwidth endpoint: the retained
// window, stamped with the moment it was read.
type dashboardBandwidth struct {
	GeneratedAt time.Time `json:"generated_at"`
	bandwidthWindow
}

// serveBandwidth answers the bandwidth chart's series.
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

func (d *dashboard) indexInfo() dashboardIndexInfo {
	if d.srv == nil || d.srv.storage == nil || d.srv.storage.Index == nil {
		return dashboardIndexInfo{}
	}
	idx := d.srv.storage.Index
	return dashboardIndexInfo{WireBytes: idx.WireSize(), Keys: int64(idx.hashCount()), HashBits: idx.HashBits()}
}

func (d *dashboard) snapshot() (*dashboardStats, error) {
	metrics, err := gatherDashboardMetrics(d.gatherer)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	eviction := dashboardEvictionInfo{
		Enabled:  d.cfg.Eviction.Enabled(),
		MaxBytes: d.cfg.Eviction.SizeLimit(),
		MaxAge:   d.cfg.Eviction.AgeLimit().String(),
		Interval: d.cfg.Eviction.Interval.Std().String(),
	}
	misses := diagnoseMisses(metrics, eviction)
	for _, name := range projectSeriesNames {
		delete(metrics, name)
	}
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
		Eviction: eviction,
		Index:    d.indexInfo(),
		Metrics:  metrics,
		Projects: projectTotals.snapshot(),
		Misses:   misses,
		Churn:    packageChurn.snapshot(),
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

// startDashboardServer binds addr, logs the bound address and serves the
// dashboard in the background. A bind failure is logged and the cache keeps
// running WITHOUT the dashboard. A busy dashboard port must not take down the
// data path.
func startDashboardServer(addr string, d *dashboard) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("dashboard unavailable on %s (continuing WITHOUT the dashboard): %v", addr, err)
		return
	}
	log.Printf("dashboard listening on %s; it answers without credentials, so publish that port through an access proxy (e.g. Cloudflare Zero Trust) rather than directly", ln.Addr())
	srv := &http.Server{
		Handler:           d.handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("dashboard stopped on %s (continuing WITHOUT the dashboard): %v", ln.Addr(), err)
		}
	}()
}
