package main

import (
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP metrics
var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "route", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	httpRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_http_request_size_bytes",
		Help:    "HTTP request body size in bytes.",
		Buckets: prometheus.ExponentialBuckets(256, 4, 8),
	}, []string{"method", "route"})

	httpResponseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_http_response_size_bytes",
		Help:    "HTTP response body size in bytes.",
		Buckets: prometheus.ExponentialBuckets(256, 4, 8),
	}, []string{"method", "route"})

	httpInFlightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_http_in_flight_requests",
		Help: "Number of HTTP requests currently being served.",
	})

	// httpAdmittedRequests is the admission-control slots held right now: the number that max_concurrent_requests bounds. It is below the in-flight count when requests are streaming a body
	// after handing their slot back.
	httpAdmittedRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_http_admitted_requests",
		Help: "Requests currently holding an admission-control slot (bounded by max_concurrent_requests).",
	})

	// A nonzero, rising value is the direct signal that the server is saturated
	// and load should be reduced or capacity added — the observable
	// backpressure metric.
	httpRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_http_rejected_total",
		Help: "Total number of requests rejected with 503 due to the concurrency limit.",
	})

	// deprecatedRequestsTotal counts requests that used a deprecated
	// S3-compatibility feature (e.g. X-Amz-Meta-* metadata headers).
	deprecatedRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_deprecated_requests_total",
		Help: "Total requests that used a deprecated S3-compatibility feature.",
	}, []string{"feature"})
)

// PUT-refusal metrics
var (
	// reason="module_index" is the PutObject guard dropping a Go module-index
	// blob.
	putRefusalsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_put_refusals_total",
		Help: "Uploads accepted on the wire but refused storage, by reason (e.g. module_index).",
	}, []string{"reason"})

	// stage="put" is an upload whose bytes disagreed with the digest its own
	// metadata claimed, so the wire corrupted it between client and disk.
	storedDigestMismatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_stored_digest_mismatch_total",
		Help: "Objects whose bytes disagreed with their stored sha256, by stage (put, get).",
	}, []string{"stage"})
)

// Batch metrics
var (
	// batchKeysTotal breaks down /_batch/get volume: requested (keys asked
	// client_held (prefetch candidates the REQUEST said the client already
	// holds), streamed (bodies written into the tar).
	batchKeysTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_batch_keys_total",
		Help: "Batch GET key counts by kind: requested, found, anchors (look-ahead keys, not asked for), prefetched, client_held, streamed.",
	}, []string{"kind"})

	// nearbyScanExhaustedTotal counts prefetch selections that ran out of scan
	// budget before filling their limit. This is because nearly every
	// candidate in the window had already been sent to that client.
	nearbyScanExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_prefetch_scan_exhausted_total",
		Help: "Prefetch selections that hit the scan budget before filling their limit (window mostly already sent).",
	})

	// batchRequestsTotal counts /_batch/get requests served.
	batchRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_batch_requests_total",
		Help: "Total /_batch/get requests processed.",
	})
)

// Index metrics
var (
	// Index size gauges, updated wherever the index mutates under its lock.
	indexEntriesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_index_entries",
		Help: "Current mtime entries in the in-memory index (including pending).",
	})
	indexHashesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_index_hashes",
		Help: "Current action-ID hashes in the index master list (excluding pending).",
	})
	indexPendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_index_pending_hashes",
		Help: "Action-ID hashes buffered since the last /_index serialization.",
	})

	// indexRebuildDuration times full filesystem-walk rebuilds (startup and
	// post-eviction). On a large cache these take seconds; a growing duration
	// is early warning that sweeps are getting expensive.
	indexRebuildDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "s3_index_rebuild_duration_seconds",
		Help:    "Duration of full index rebuilds (filesystem walk + swap).",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 8),
	})
)

// Storage metrics
var (
	storageOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_storage_operations_total",
		Help: "Total number of storage operations.",
	}, []string{"operation", "status"})

	storageOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_storage_operation_duration_seconds",
		Help:    "Storage operation duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	// metadataXattrsDroppedTotal counts OPTIONAL user-metadata xattrs dropped
	// because the filesystem ran out of extended-attribute space (E2BIG / ENOSPC
	// / EDQUOT) while storing an object.
	metadataXattrsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_metadata_xattrs_dropped_total",
		Help: "Optional user-metadata xattrs dropped due to xattr-space exhaustion (object stored without them).",
	})
)

// Auth metrics
var (
	authFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_auth_failures_total",
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
	// sweep.
	cacheBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_cache_bytes",
		Help: "Total size of stored cache objects in bytes, measured at the last eviction sweep.",
	})

	// selfHealRepairsTotal counts objects whose missing outputid metadata was
	// reconstructed in place on read (see selfheal.go).
	selfHealRepairsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_self_heal_repairs_total",
		Help: "Total cache objects whose missing outputid metadata was reconstructed in place on read.",
	})

	// selfHealFailuresTotal counts objects that could NOT be repaired (the body
	// does not decompress, so no outputid can be reconstructed).
	selfHealFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_self_heal_failures_total",
		Help: "Self-heal attempts that could not reconstruct an outputid (unservable body; key de-advertised).",
	})

	// outputIDMismatchTotal counts repairs that found an outputid on the inode
	// DISAGREEING with the just-computed hash of that inode's body.
	outputIDMismatchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_outputid_mismatch_total",
		Help: "Self-heal repairs that found an existing outputid disagreeing with the body hash (stale-stamp corruption, repaired in place).",
	})

	// getRequestsTotal counts single-object GETs by outcome. The different
	// flavors of "404" are distinguishable in metrics instead of all collapsing
	// into s3_http_requests_total{status="404"}.
	getRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_get_requests_total",
		Help: "Single-object GET requests by outcome (hit, miss_not_found, miss_advertised_unservable, miss_module_index_evicted, miss_peek_error, miss_selfheal_failed).",
	}, []string{"outcome"})

	// moduleIndexEvictionsTotal counts already-stored Go module-index blobs that
	// were detected and evicted on a read path (single GET, batch GET, or
	// prefetch scan) -- see modindex.go's evictModuleIndexOnRead.
	moduleIndexEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_module_index_evictions_total",
		Help: "Total Go module-index blobs detected and evicted on a read path (GET, batch get, or prefetch).",
	})

	// metaCacheHitsTotal / metaCacheMissesTotal track the per-key metadata cache
	// (metacache.go).
	metaCacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_meta_cache_hits_total",
		Help: "Total object-metadata reads served from the in-memory metadata cache.",
	})

	metaCacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_meta_cache_misses_total",
		Help: "Total object-metadata reads that had to read extended attributes from disk.",
	})

	// Memory accounting (memlimit.go, lrucache.go).
	memoryLimitBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_memory_limit_bytes",
		Help: "The process memory ceiling the caches are sized against (0 = none discovered).",
	})

	memoryInUseBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_memory_in_use_bytes",
		Help: "Memory the Go runtime counts against its limit (mapped, not released), sampled periodically.",
	})

	memoryShrinksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_memory_shrinks_total",
		Help: "Times the in-memory cache budgets were cut because memory in use crossed the shrink threshold.",
	})

	cacheMemoryBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3_cache_memory_bytes",
		Help: "Bytes currently held by each in-memory cache.",
	}, []string{"cache"})

	cacheBudgetBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3_cache_memory_budget_bytes",
		Help: "Byte budget currently allowed for each in-memory cache (falls as memory gets tight).",
	}, []string{"cache"})
)

// statusRecorder wraps http.ResponseWriter to capture status code and bytes written.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
	// Atomic because the stall guard samples them from a timer.
	bytesWritten atomic.Int64
	progress     atomic.Int64
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.progress.Add(1)
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten.Add(int64(n))
	r.progress.Add(int64(n))
	return n, err
}

// Unwrap gives http.ResponseController the real writer underneath, which is
// what carries the connection deadlines the stall guard moves.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// readFromChunk is the most a single forwarded ReadFrom call copies.
const readFromChunk = 1 << 20

// net/http's response writer implements ReadFrom with a sendfile fast path for
// *os.File sources. A wrapper that hides the interface silently downgrades
// every GET body copy to userspace read/write loops. When the wrapped writer is
// not a ReaderFrom (e.g. httptest recorders), fall back to a plain copy through
// r.Write (which already counts bytes — writerOnly hides this method so
// io.Copy cannot recurse into it).
func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	rf, ok := r.ResponseWriter.(io.ReaderFrom)
	if !ok {
		return io.Copy(writerOnly{r}, src)
	}
	inner, remaining := src, int64(math.MaxInt64)
	outer, limited := src.(*io.LimitedReader)
	if limited {
		inner, remaining = outer.R, outer.N
	}
	var total int64
	for remaining > 0 {
		chunk := min(remaining, readFromChunk)
		n, err := rf.ReadFrom(&io.LimitedReader{R: inner, N: chunk})
		total += n
		remaining -= n
		if limited {
			outer.N = remaining
		}
		r.bytesWritten.Add(n)
		r.progress.Add(n)
		// ReadFrom returns short only at the source's EOF or on an error.
		if err != nil || n < chunk {
			return total, err
		}
	}
	return total, nil
}

// writerOnly masks every method except Write.
type writerOnly struct{ io.Writer }

func statusStr(code int) string {
	return strconv.Itoa(code)
}

// startMetricsServer serves /metrics on addr.
func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics server unavailable (continuing WITHOUT metrics): %v", err)
	}
}
