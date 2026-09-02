package cacheclient

import (
	"github.com/wow-look-at-my/go-containers/set"

	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrLogged marks an error already reported to stderr; callers must not log it again.
var ErrLogged = errors.New("web: already logged")

// MaxConnsPerHost is the HTTP connection pool size for the remote cache.
const MaxConnsPerHost = 64

// WebConfig holds the configuration for a web cache backend.
type WebConfig struct {
	Bucket    string // Required. Empty bucket disables the backend.
	Endpoint  string // Endpoint URL (e.g. "https://cache.example.com"). Required.
	Prefix    string // Key prefix (defaults to "go-buildcache/").
	AccessKey string // Basic Auth username
	SecretKey string // Basic Auth password
	Version   string // go-toolchain version, stored as object metadata
	Module    string // main module path, stored as object metadata (provenance)
}

// WebBackend stores cache objects in a remote web server with LZ4 compression.
// GETs use the server's batch endpoint to fetch entries with prefetch support,
// proactively populating the local cache with related entries. PUTs are
// coalesced onto the server's /_batch/put endpoint (mirroring the batch GET
// coalescer), falling back to individual PUTs against a server that does not
// support it.
type WebBackend struct {
	client     *http.Client
	bucket     string
	prefix     string
	endpoint   string
	accessKey  string
	secretKey  string
	version    string // go-toolchain version for object metadata
	module     string // main module path for object metadata (provenance)
	Stats      CacheStats
	Pool       ConcurrencyTracker // HTTP connection pool usage (shared across all Servers)
	Latency    *LatencyStats      // optional; set by Server for sub-operation tracking
	keysMu     sync.RWMutex
	keys       set.Set[string] // known keys, from the startup index fetch + Put claims
	indexEmpty bool            // remote index was empty at startup: nothing to batch-probe for
	// indexAuthoritative marks a fresh, server-confirmed index: an absent key can then miss without a probe.
	indexAuthoritative bool
	// indexKeysAtStart is the key count from the startup index fetch, reported in WebSummary to flag a dead remote.
	indexKeysAtStart int
	missesMu         sync.RWMutex
	knownMiss        set.Set[string] // keys confirmed absent from remote this session

	// emptyBatchBackoffThreshold: after this many empty batches in a row, stop probing for the run (an unset value disables).
	emptyBatchBackoffThreshold int          // an unset value disables the backoff
	consecutiveEmptyBatches    atomic.Int64 // current run of empty batches
	batchProbingDisabled       atomic.Bool  // true after the backoff has tripped
	batchBackoffLogOnce        sync.Once    // logs the disable notice a single time

	// OnBatchEntries lets the caller populate the local cache from a batch GET's prefetch entries.
	OnBatchEntries func(entries []BatchEntry)

	// Miss reason counters for diagnostics.
	MissNotInIndex  AtomicCounter
	MissHTTP404     AtomicCounter
	MissHTTPError   AtomicCounter
	MissNoOutputID  AtomicCounter
	MissReadBody    AtomicCounter
	MissDecompress  AtomicCounter
	MissChecksum    AtomicCounter
	MissBuildID     AtomicCounter
	MissModuleIndex AtomicCounter // module-index blobs refused: unverifiable under a key
	MissNetwork     AtomicCounter

	// SkippedEmptyIndex counts clean misses skipped because the startup index was empty.
	SkippedEmptyIndex AtomicCounter

	// SkippedBatchBackoff counts clean misses skipped after the empty-batch backoff tripped.
	SkippedBatchBackoff AtomicCounter

	// SkippedNotInIndex counts clean misses skipped because the authoritative index omits the key.
	SkippedNotInIndex AtomicCounter

	// Reclaimed404 counts stale index claims dropped after the server reported the key absent.
	Reclaimed404 AtomicCounter

	// PUT-side skip/refusal counters: what Put(nil, no error) actually did instead of uploading.
	PutSkippedKnown    AtomicCounter // key already in the index or claimed by an in-flight upload
	PutRefusedBuildID  AtomicCounter // refused: build-id action mismatch (mis-keyed object)
	PutRefusedModIndex AtomicCounter // refused: Go module index (never published to the shared cache)

	// maxRetries bounds retries for a transient failure; past the budget the op falls back to a local miss.
	maxRetries int // bounded retries for transient failures

	errLog *httpErrLogger

	// batchReqCh funnels concurrent Get keys to a worker that ships them as a single /_batch/get request.
	batchReqCh  chan batchReq
	batchStop   chan struct{}
	batchDone   chan struct{}
	batchHTTPWG sync.WaitGroup

	// putBatchReqCh funnels prepped Put objects to a worker that ships them as a single /_batch/put tar.
	putBatchReqCh       chan putReq
	putBatchStop        chan struct{}
	putBatchDone        chan struct{}
	putBatchHTTPWG      sync.WaitGroup
	batchPutUnsupported atomic.Bool // sticky after the server refuses /_batch/put; Put then uses doRetryPUT
}

type batchReq struct {
	actionID string
	key      string
	resp     chan batchResp
}

type batchResp struct {
	outputID   string
	body       io.ReadCloser
	size       int64
	t          time.Time
	miss       bool
	executable bool
}

const (
	batchMaxKeys      = 128
	batchCoalesceWait = 10 * time.Millisecond
	batchReqChBuf     = 1024
)

// NewWebBackend creates a web backend from the given config.
// Returns nil if bucket is empty.
func NewWebBackend(cfg WebConfig) (*WebBackend, error) {
	if cfg.Bucket == "" {
		return nil, nil
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("web: endpoint is required")
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "go-buildcache/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	accessKey := cfg.AccessKey
	secretKey := cfg.SecretKey
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("web: access key and secret key are required")
	}

	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "https://") && !strings.HasPrefix(endpoint, "http://") {
		endpoint = "https://" + endpoint
	}

	// Tune the transport for high-throughput cache uploads. The default Go
	// transport keeps very few idle connections per host, which forces a new
	// TCP+TLS handshake for nearly every request. We allow many more
	// concurrent connections and keep them all alive in the idle pool.
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig:       &tls.Config{},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          MaxConnsPerHost,
		MaxIdleConnsPerHost:   MaxConnsPerHost,
		MaxConnsPerHost:       MaxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	b := &WebBackend{
		maxRetries:                 envInt("GO_TOOLCHAIN_CACHE_MAX_RETRIES", defaultMaxRetries),
		emptyBatchBackoffThreshold: envInt("GO_TOOLCHAIN_CACHE_EMPTY_BATCH_BACKOFF", defaultEmptyBatchBackoff),
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				// Preserve original method — Go changes PUT/POST to GET on a redirect.
				orig := via[0]
				req.Method = orig.Method
				// orig.Body was already consumed; resend via GetBody or the retry
				// ships an empty body and fails the ContentLength check.
				if orig.GetBody != nil {
					body, err := orig.GetBody()
					if err != nil {
						return err
					}
					req.Body = body
					req.GetBody = orig.GetBody
					req.ContentLength = orig.ContentLength
				}
				for key, vals := range orig.Header {
					req.Header[key] = vals
				}
				return nil
			},
		},
		bucket:    cfg.Bucket,
		prefix:    prefix,
		endpoint:  endpoint,
		accessKey: accessKey,
		secretKey: secretKey,
		version:   cfg.Version,
		module:    cfg.Module,
	}

	b.errLog = newHTTPErrLogger(loggerWriter{}, httpErrFlushInterval)
	b.batchReqCh = make(chan batchReq, batchReqChBuf)
	b.batchStop = make(chan struct{})
	b.batchDone = make(chan struct{})
	go b.batchCoalescer()
	b.putBatchReqCh = make(chan putReq, batchReqChBuf)
	b.putBatchStop = make(chan struct{})
	b.putBatchDone = make(chan struct{})
	go b.batchPutCoalescer()
	b.keys, b.indexAuthoritative = b.loadOrFetchIndex()
	b.indexEmpty = b.keys.Len() == 0
	b.indexKeysAtStart = b.keys.Len()
	b.knownMiss = set.New[string]()
	if b.indexAuthoritative {
		logging.Infof("cacheprog: web index: %d keys", b.keys.Len())
	} else {
		logging.Warnf("cacheprog: web index: fetch failed; using %d cached keys (batch probing enabled)", b.keys.Len())
	}
	return b, nil
}

// KeyPrefix returns what a cache key carries ahead of its action ID. The
// client owns the key grammar, so a consumer recovering an action ID from a
// BatchEntry asks for the prefix rather than rebuilding it.
func (b *WebBackend) KeyPrefix() string {
	return b.prefix + "v1"
}

func (b *WebBackend) key(actionID string) string {
	return b.prefix + "v1" + actionID
}

func (b *WebBackend) url(key string) string {
	return b.endpoint + "/" + b.bucket + "/" + key
}

// Get retrieves a cached object.
//
// Routing policy (the batch endpoint is the primary fetch path):
//
//   - Key in the index: fetch via the coalescing batch endpoint — a single round-trip
//     serves many callers and carries prefetch entries from the same build.
//     Servers without /_batch/get fall back to individual GETs (sendBatch).
//
//   - Key absent from an AUTHORITATIVE index (freshly fetched or revalidated this run): miss
//     cleanly with no network.
//
//   - Key absent but the index fetch FAILED: batch-probe the key (the recovery
//     path), bounded by the consecutive-empty-batch backoff.
func (b *WebBackend) Get(actionID string) (outputID string, body io.ReadCloser, size int64, t time.Time, miss bool, executable bool, err error) {
	key := b.key(actionID)
	if b.keyKnown(key) {
		return b.getBatch(actionID, key)
	}

	// Key not in index — check if we already know it's absent.
	b.missesMu.RLock()
	alreadyMissed := b.knownMiss.Contains(key)
	b.missesMu.RUnlock()
	if alreadyMissed {
		b.MissNotInIndex.Increment()
		return "", nil, 0, time.Time{}, true, false, nil
	}

	b.MissNotInIndex.Increment()
	if b.indexAuthoritative {
		// Authoritative index already says the key is absent: miss without a probe.
		if b.indexEmpty {
			b.SkippedEmptyIndex.Increment()
		} else {
			b.SkippedNotInIndex.Increment()
		}
		return "", nil, 0, time.Time{}, true, false, nil
	}
	if b.batchProbingOff() {
		// Backoff tripped: the remote has proven empty for this run. Miss without probing.
		b.SkippedBatchBackoff.Increment()
		return "", nil, 0, time.Time{}, true, false, nil
	}
	return b.getBatch(actionID, key)
}

// keyKnown reports whether key is in the known-keys set (the startup index
// plus optimistic Put claims).
func (b *WebBackend) keyKnown(key string) bool {
	b.keysMu.RLock()
	defer b.keysMu.RUnlock()
	return b.keys.Contains(key)
}

// reclaimAbsent records an authoritative absent answer (a not-found, or missing from a batch
// response) for key. It drops any stale index claim so Put re-uploads instead of
// skipping, and marks the key knownMiss so Gets stop re-asking this run.
func (b *WebBackend) reclaimAbsent(key string) bool {
	b.keysMu.Lock()
	removed := b.keys.Contains(key)
	if removed {
		b.keys.Remove(key)
	}
	b.keysMu.Unlock()
	if removed {
		b.Reclaimed404.Increment()
	}
	b.missesMu.Lock()
	b.knownMiss.Add(key)
	b.missesMu.Unlock()
	return removed
}

// ForgetStale drops the index claim for actionID so the next Put re-uploads
// instead of skipping as already known.
func (b *WebBackend) ForgetStale(actionID string) {
	b.removeClaimed(b.key(actionID))
}

// Close drains the batch coalescer and flushes the HTTP error logger.

func (b *WebBackend) Close() error {
	// Flush the PUT coalescer up front: an unflushed upload was claimed in the index but never stored.
	if b.putBatchStop != nil {
		close(b.putBatchStop)
		<-b.putBatchDone
	}
	if b.batchStop != nil {
		close(b.batchStop)
		<-b.batchDone
	}
	if b.errLog != nil {
		_ = b.errLog.Close()
	}
	return nil
}

func (b *WebBackend) GetStats() *CacheStats { return &b.Stats }

// signRequest authenticates an HTTP request using HTTP Basic Auth.
func (b *WebBackend) signRequest(req *http.Request) {
	req.SetBasicAuth(b.accessKey, b.secretKey)
}
