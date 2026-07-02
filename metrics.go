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

	// selfHealRepairsTotal counts objects whose missing outputid metadata was
	// reconstructed in place on read (see selfheal.go). These are leftovers from
	// earlier cache-data iterations, or objects whose xattrs were stripped by a
	// data-dir move; each one can never be a cache hit yet pins its key in
	// /_index, forcing a rebuild on every consumer. The outputID is recomputed
	// from the body (it IS sha256 of the decompressed body) and written back, so
	// the object keeps its bytes and audit trail and becomes a hit -- no eviction,
	// no re-upload. A nonzero value that trends to zero is the cache healing
	// itself as it is read; the repair is one-time per object. (s3_ prefix kept
	// for consistency with the other metrics until the repository rename.)
	selfHealRepairsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_self_heal_repairs_total",
		Help: "Total cache objects whose missing outputid metadata was reconstructed in place on read.",
	})

	// outputIDMismatchTotal counts repairs that found an outputid on the inode
	// DISAGREEING with the just-computed hash of that inode's body. The repair
	// only runs when the metadata read said no outputid was present, so a
	// differing value appearing by stamp time is the stale-stamp corruption
	// signature (the historical path-based setxattr race): an object whose
	// outputid != sha256(body) is discarded by every client yet never
	// re-uploaded — a permanent forced miss. Should be 0; the fd-based repair
	// both counts and corrects it.
	outputIDMismatchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_outputid_mismatch_total",
		Help: "Self-heal repairs that found an existing outputid disagreeing with the body hash (stale-stamp corruption, repaired in place).",
	})

	// getRequestsTotal counts single-object GETs by outcome, so the different
	// flavors of "404" are distinguishable in metrics instead of all collapsing
	// into s3_http_requests_total{status="404"}:
	//
	//   hit                        — 200, body served
	//   miss_not_found             — no object and the key is not advertised in
	//                                /_index: a genuinely-absent key (normal).
	//   miss_advertised_unservable — no object on disk YET the key's action hash
	//                                is currently advertised in /_index. This is
	//                                the index/store-divergence signature behind
	//                                the "GETs 404 on keys the index lists"
	//                                incidents: each such key is a forced miss on
	//                                every consumer (clients skip re-uploading
	//                                indexed keys), so this counter should be ~0
	//                                always and any sustained nonzero rate is an
	//                                actionable bug.
	//   miss_module_index_evicted  — a stored module-index blob was detected and
	//                                evicted by the read guard.
	//   miss_peek_error            — the read guard could not inspect/rewind the
	//                                body (I/O error); refused fail-safe, left on
	//                                disk.
	//   miss_selfheal_failed       — the object lacks an outputid and its body
	//                                could not be decompressed to reconstruct
	//                                one; unservable, left on disk for eviction.
	getRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_get_requests_total",
		Help: "Single-object GET requests by outcome (hit, miss_not_found, miss_advertised_unservable, miss_module_index_evicted, miss_peek_error, miss_selfheal_failed).",
	}, []string{"outcome"})

	// moduleIndexEvictionsTotal counts already-stored Go module-index blobs that
	// were detected and evicted on a read path (single GET, batch GET, or
	// prefetch scan) -- see modindex.go's evictModuleIndexOnRead. The PUT guard
	// only stops NEW indexes from being stored; this counts the lazy removal of
	// poison already on disk (e.g. uploaded before the PUT guard existed, when
	// the v3 startup purge had already run). Each poisoned key is evicted on its
	// first post-deploy fetch -- the client gets a clean miss and recomputes the
	// index locally -- so a nonzero value that trends to zero is the cache
	// shedding residual module-index poison as it is read. (s3_ prefix kept for
	// consistency with the other metrics until the repository rename.)
	moduleIndexEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_module_index_evictions_total",
		Help: "Total Go module-index blobs detected and evicted on a read path (GET, batch get, or prefetch).",
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
