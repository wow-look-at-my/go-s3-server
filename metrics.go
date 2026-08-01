package main

import (
	"io"
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

// PUT-refusal metrics
var (
	// putRefusalsTotal counts uploads the server accepted on the wire (200)
	// but deliberately did not store, by reason. reason="module_index" is the
	// PutObject guard dropping a Go module-index blob. This guard was
	// previously completely invisible — no metric, no log — which is exactly
	// the blind spot that let the 512-byte-peek bug go unnoticed for weeks: a
	// broken guard is indistinguishable from a quiet one. Clients build module
	// indexes constantly, so during CI activity this counter being
	// occasionally nonzero is PROOF the guard fires; a flat zero across busy
	// periods means the guard is broken again.
	putRefusalsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_put_refusals_total",
		Help: "Uploads accepted on the wire but refused storage, by reason (e.g. module_index).",
	}, []string{"reason"})
)

// Batch metrics
var (
	// batchKeysTotal breaks down /_batch/get volume: requested (keys asked
	// for), found (requested keys served), prefetched (extra entries included),
	// suppressed (prefetch candidates skipped as recently sent), streamed
	// (bodies actually written into the tar). A falling found/requested ratio
	// is the earliest "cache is fickle" indicator; previously these numbers
	// were log-only and required grepping server logs.
	batchKeysTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_batch_keys_total",
		Help: "Batch GET key counts by kind: requested, found, prefetched, suppressed, streamed.",
	}, []string{"kind"})

	// batchRequestsTotal counts /_batch/get requests served.
	batchRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_batch_requests_total",
		Help: "Total /_batch/get requests processed.",
	})
)

// Index metrics
var (
	// Index size gauges, updated wherever the index mutates under its lock.
	// A post-sweep dip in s3_index_hashes that PUT volume does not explain is
	// the signature of a rebuild dropping keys; pending depths show how much
	// is buffered between /_index serializations.
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
		Name: "s3_storage_operations_total",
		Help: "Total number of storage operations.",
	}, []string{"operation", "status"})

	storageOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_storage_operation_duration_seconds",
		Help:    "Storage operation duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	// metadataXattrsDroppedTotal counts OPTIONAL user-metadata xattrs dropped
	// because the filesystem ran out of extended-attribute space (E2BIG /
	// ENOSPC / EDQUOT) while storing an object. The object still stores and
	// serves; only the provenance field is lost. A rising rate usually means
	// the client is sending an oversized Src list or the data_dir filesystem
	// has a tight per-inode EA budget (ext4 without ea_inode: ~4 KiB shared).
	metadataXattrsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_metadata_xattrs_dropped_total",
		Help: "Optional user-metadata xattrs dropped due to xattr-space exhaustion (object stored without them).",
	})
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

	// selfHealFailuresTotal counts objects that could NOT be repaired (the
	// body does not decompress, so no outputid can be reconstructed). Each
	// such object is unservable; the read path de-advertises it so consumers
	// re-upload. Previously a failure was only a log line, so the recurring
	// forced-miss signature was invisible in metrics. A sustained nonzero
	// RATE (the same keys failing over and over) means corrupt bodies are
	// being re-advertised faster than they are healed — investigate.
	selfHealFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_self_heal_failures_total",
		Help: "Self-heal attempts that could not reconstruct an outputid (unservable body; key de-advertised).",
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

	// metaCacheHitsTotal / metaCacheMissesTotal track the per-key metadata cache
	// (metacache.go). A miss costs a listxattr plus a getxattr per attribute --
	// roughly a dozen syscalls -- so on a warm cache this ratio IS the read
	// path's CPU story: a ratio that collapses means keys are being rewritten
	// (mtime moves, entries invalidate) or the cache is thrashing its bound.
	metaCacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_meta_cache_hits_total",
		Help: "Total object-metadata reads served from the in-memory metadata cache.",
	})

	metaCacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_meta_cache_misses_total",
		Help: "Total object-metadata reads that had to read extended attributes from disk.",
	})

	// Memory accounting (memlimit.go, lrucache.go). The server's in-memory
	// caches are byte-bounded and evict; these say how big each one is allowed
	// to be, how big it actually is, and how many entries it has given up.
	// Requests are never refused for memory, so a cache shrinking is the ONLY
	// visible effect of pressure -- which is what these expose.
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

	cacheEvictionsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3_cache_memory_evictions",
		Help: "Entries evicted from each in-memory cache to stay inside its byte budget.",
	}, []string{"cache"})
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

// ReadFrom forwards to the wrapped ResponseWriter's io.ReaderFrom when it has
// one. net/http's response writer implements ReadFrom with a sendfile fast
// path for *os.File sources; a wrapper that hides the interface silently
// downgrades every GET body copy to userspace read/write loops. When the
// wrapped writer is not a ReaderFrom (e.g. httptest recorders), fall back to
// a plain copy through r.Write (which already counts bytes — writerOnly hides
// this method so io.Copy cannot recurse into it).
func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(src)
		r.bytesWritten += n
		return n, err
	}
	return io.Copy(writerOnly{r}, src)
}

// writerOnly masks every method except Write, so the ReadFrom fallback's
// io.Copy takes the plain-write path instead of recursing into ReadFrom.
type writerOnly struct{ io.Writer }

func statusStr(code int) string {
	return strconv.Itoa(code)
}

// startMetricsServer serves /metrics on addr. A failure (typically the port
// being in use) is logged and the cache keeps running WITHOUT metrics — the
// old log.Fatalf here killed the whole cache server at startup over a busy
// metrics port, taking down the data path to preserve the monitoring path.
func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics server unavailable (continuing WITHOUT metrics): %v", err)
	}
}
