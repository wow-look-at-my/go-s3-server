package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP metrics
var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "route", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	httpRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_http_request_size_bytes",
		Help:    "HTTP request body size in bytes.",
		Buckets: prometheus.ExponentialBuckets(256, 4, 8),
	}, []string{"method", "route"})

	httpResponseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_http_response_size_bytes",
		Help:    "HTTP response body size in bytes.",
		Buckets: prometheus.ExponentialBuckets(256, 4, 8),
	}, []string{"method", "route"})

	httpInFlightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_http_in_flight_requests",
		Help: "Number of HTTP requests currently being served.",
	})

	// httpRejectedTotal counts requests shed by admission control (503 +
	// Retry-After) because the server was at MaxConcurrentRequests. A nonzero,
	// rising value is the direct signal that the server is saturated and load
	// should be reduced or capacity added — the observable backpressure metric.
	httpRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_http_rejected_total",
		Help: "Total number of requests rejected with 503 due to the concurrency limit.",
	})

	// deprecatedRequestsTotal counts requests that used a deprecated
	// S3-compatibility feature (e.g. X-Amz-Meta-* metadata headers). A nonzero
	// value means not-yet-upgraded clients are still relying on the S3 shims; it
	// should trend to zero once every client speaks the native protocol, at
	// which point the shims can be removed. (The metric name keeps the s3_
	// prefix for consistency with the others until the repository rename.)
	deprecatedRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_deprecated_requests_total",
		Help: "Total requests that used a deprecated S3-compatibility feature.",
	}, []string{"feature"})
)

// Storage metrics
var (
	storageOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_storage_operations_total",
		Help: "Total number of storage operations.",
	}, []string{"operation", "status"})

	storageOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_storage_operation_duration_seconds",
		Help:    "Storage operation duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
)

// Auth metrics
var (
	authFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_auth_failures_total",
		Help: "Total number of authentication failures.",
	})
)

// Eviction metrics
var (
	evictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_evictions_total",
		Help: "Total number of cache entries evicted (age- plus size-based).",
	})

	evictedBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_evicted_bytes_total",
		Help: "Total bytes reclaimed by cache eviction.",
	})

	// cacheBytes is the total size of stored objects as of the last eviction
	// sweep. Watch it against eviction.max_bytes to see headroom; a value that
	// keeps climbing with eviction disabled is the unbounded-growth warning.
	cacheBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_cache_bytes",
		Help: "Total size of stored cache objects in bytes, measured at the last eviction sweep.",
	})

	// selfHealEvictionsTotal counts objects evicted on read because they carried
	// no usable outputid metadata (see selfheal.go). These are leftovers from
	// earlier cache-data iterations, or objects whose xattrs were stripped by a
	// data-dir move; each one can never be a cache hit yet pins its key in
	// /_index, forcing a rebuild on every consumer. A nonzero value that trends
	// to zero is the cache healing itself; a persistently rising one means
	// something keeps writing outputid-less objects. (s3_ prefix kept for
	// consistency with the other metrics until the repository rename.)
	selfHealEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_self_heal_evictions_total",
		Help: "Total cache objects evicted on read for missing outputid metadata.",
	})
)

// statusRecorder wraps http.ResponseWriter to capture status code and bytes written.
type statusRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += int64(n)
	return n, err
}

func statusStr(code int) string {
	return strconv.Itoa(code)
}

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("metrics server: %v", err)
	}
}
