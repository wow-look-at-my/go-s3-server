package cacheclient

import (
	"github.com/wow-look-at-my/go-containers/set"

	"crypto/tls"
	"errors"
	"fmt"
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
	Target    string // GOOS/GOARCH this build is producing, sent as provenance
}

// WebBackend stores cache objects in a remote web server with LZ4 compression.
// GETs use the server's batch endpoint to fetch entries with prefetch support,
// proactively populating the local cache with related entries. PUTs are
// coalesced onto the server's /_batch/put endpoint (mirroring the batch GET
// coalescer), falling back to individual PUTs against a server that does not
// support it.
type WebBackend struct {
	client    *http.Client
	bucket    string
	prefix    string
	endpoint  string
	accessKey string
	secretKey string
	version   string // go-toolchain version for object metadata
	module    string // main module path for object metadata (provenance)
	target    string // GOOS/GOARCH this build produces, for request provenance
	// moduleLate carries a module path learned after the backend was built. A
	// consumer often knows its endpoint before it knows which module it is
	// building, and the requests in between still deserve an attribution.
	moduleLate atomic.Pointer[string]
	Stats     CacheStats
	Pool      ConcurrencyTracker // HTTP connection pool usage (shared across all Servers)
	Latency   *LatencyStats      // optional; set by Server for sub-operation tracking
	keysMu    sync.RWMutex
	// keys holds RAW ACTION HASHES, not cache-key strings. A key string is the
	// same 32-byte hash written as 64 hex characters behind a fixed prefix, so
	// a string set costs about three times the memory and charges a hex encode
	// and an allocation per entry to build. A large cache is hundreds of
	// thousands of entries, and that set is built at startup before the build
	// does anything at all.
	keys       set.Set[actionHash] // known keys, from the startup index fetch + Put claims
	indexEmpty bool                // remote index was empty at startup: nothing to batch-probe for
	// indexAuthoritative marks a fresh, server-confirmed index: an absent key can then miss without a probe.
	indexAuthoritative bool
	// indexKeysAtStart is the key count from the startup index fetch, reported in WebSummary to flag a dead remote.
	indexKeysAtStart int
	missesMu         sync.RWMutex
	knownMiss        set.Set[actionHash] // keys confirmed absent from remote this session

	// emptyBatchBackoffThreshold: after this many empty batches in a row, stop probing for the run (an unset value disables).
	emptyBatchBackoffThreshold int          // an unset value disables the backoff
	consecutiveEmptyBatches    atomic.Int64 // current run of empty batches
	batchProbingDisabled       atomic.Bool  // true after the backoff has tripped
	batchBackoffLogOnce        sync.Once    // logs the disable notice a single time

	// OnBatchEntries receives the objects the look-ahead pool fetched ahead of
	// the build. Leaving it nil turns look-ahead off entirely: with nowhere to
	// put an object nobody has asked for yet, fetching it is pure cost. That is
	// not hypothetical -- the pool's ancestor rode every batch response and its
	// entries went straight to the garbage collector, because the one consumer
	// never set this.
	//
	// Entries arrive on the pool's own goroutines, several at once, and carry
	// COMPRESSED bodies: a consumer that already holds an object locally drops
	// it without paying to decompress it. Verify anything kept with Verify.
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

	// RawBytes and CompressedBytes are what this process offered the store,
	// before and after lz4. Their ratio is what compression actually bought on
	// this build's objects, rather than on a benchmark corpus.
	RawBytes        AtomicCounter
	CompressedBytes AtomicCounter

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
	batchTiming batchTiming
	batchHTTPWG sync.WaitGroup

	// putBatchReqCh funnels prepped Put objects to a worker that ships them as a single /_batch/put tar.
	putBatchReqCh       chan putReq
	putBatchStop        chan struct{}
	putBatchDone        chan struct{}
	putBatchHTTPWG      sync.WaitGroup
	batchPutUnsupported atomic.Bool // sticky after the server refuses /_batch/put; Put then uses doRetryPUT

	// lookAhead fetches objects the build has not asked for yet, on its own
	// goroutines. See lookahead.go.
	lookAhead *lookAhead
	// prep runs a Put's compression off the build's goroutine. See webput.go.
	prep *prepPool
}

type batchReq struct {
	actionID string
	key      string
	hash     actionHash
	resp     chan batchResp
}

// batchResp carries a decompressed, fully verified body. It is a []byte rather
// than a reader because the client has the whole object in memory by the time
// it can answer at all: handing back a reader only bought the consumer another
// copy on its way to a file.
type batchResp struct {
	outputID string
	data     []byte
	t        time.Time
	miss     bool
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
		TLSClientConfig: &tls.Config{},
		// HTTP/1.1, deliberately. HTTP/2 multiplexes every request onto ONE TCP
		// connection, so MaxConnsPerHost below stops meaning anything: the pool
		// holds one connection with one congestion window, and throughput ramps
		// at whatever that single window opens at. This workload is many
		// independent blobs and wants many independent windows, which is what
		// the connection pool gives it once nothing collapses them.
		//
		// H2's advantages -- header compression, one handshake -- are worth
		// little here: the requests are few and large, and the bodies dwarf the
		// headers.
		ForceAttemptHTTP2:     false,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
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
		target:    cfg.Target,
	}

	b.errLog = newHTTPErrLogger(os.Stderr, httpErrFlushInterval)
	b.batchReqCh = make(chan batchReq, batchReqChBuf)
	b.batchStop = make(chan struct{})
	b.batchDone = make(chan struct{})
	go b.batchCoalescer()
	b.putBatchReqCh = make(chan putReq, batchReqChBuf)
	b.putBatchStop = make(chan struct{})
	b.putBatchDone = make(chan struct{})
	go b.batchPutCoalescer()
	b.prep = newPrepPool(b)
	b.lookAhead = newLookAhead(b)
	b.keys, b.indexAuthoritative = b.loadOrFetchIndex()
	b.indexEmpty = b.keys.Len() == 0
	b.indexKeysAtStart = b.keys.Len()
	b.knownMiss = set.New[actionHash]()
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
func (b *WebBackend) Get(actionID string) (outputID string, data []byte, t time.Time, miss bool) {
	h, ok := parseActionHash(actionID)
	if !ok {
		return "", nil, time.Time{}, true
	}
	if b.keyKnown(h) {
		r := b.getBatch(actionID, b.key(actionID), h)
		return r.outputID, r.data, r.t, r.miss
	}

	b.MissNotInIndex.Increment()

	// Already proven absent this run: no round trip can change that answer.
	b.missesMu.RLock()
	alreadyMissed := b.knownMiss.Contains(h)
	b.missesMu.RUnlock()
	if alreadyMissed {
		return "", nil, time.Time{}, true
	}

	if b.indexAuthoritative {
		// Authoritative index already says the key is absent: miss without a probe.
		if b.indexEmpty {
			b.SkippedEmptyIndex.Increment()
		} else {
			b.SkippedNotInIndex.Increment()
		}
		return "", nil, time.Time{}, true
	}
	if b.batchProbingOff() {
		// Backoff tripped: the remote has proven empty for this run. Miss without probing.
		b.SkippedBatchBackoff.Increment()
		return "", nil, time.Time{}, true
	}
	r := b.getBatch(actionID, b.key(actionID), h)
	return r.outputID, r.data, r.t, r.miss
}

// Verify decompresses and checks a stored body from a look-ahead entry, under
// exactly the gates a requested object passes. It answers the object's bytes,
// or false for a body no consumer may see.
func (b *WebBackend) Verify(actionID, outputID string, stored []byte) ([]byte, bool) {
	return b.verify("look-ahead", actionID, outputID, stored)
}

// ActionIDFromKey recovers the action ID a cache key names, so a consumer
// holding a look-ahead entry can find the action it belongs to without
// rebuilding the client's key grammar.
func (b *WebBackend) ActionIDFromKey(key string) (string, bool) {
	prefix := b.KeyPrefix()
	if len(key) != len(prefix)+2*hashSize || key[:len(prefix)] != prefix {
		return "", false
	}
	id := key[len(prefix):]
	if _, ok := parseActionHash(id); !ok {
		return "", false
	}
	return id, true
}

// keyKnown reports whether the hash is in the known-keys set (the startup
// index plus optimistic Put claims).
func (b *WebBackend) keyKnown(h actionHash) bool {
	b.keysMu.RLock()
	defer b.keysMu.RUnlock()
	return b.keys.Contains(h)
}

// reclaimAbsent records an authoritative absent answer (a not-found, or missing from a batch
// response) for a key. It drops any stale index claim so Put re-uploads instead of
// skipping, and marks the key knownMiss so Gets stop re-asking this run.
func (b *WebBackend) reclaimAbsent(h actionHash) bool {
	b.keysMu.Lock()
	removed := b.keys.Contains(h)
	if removed {
		b.keys.Remove(h)
	}
	b.keysMu.Unlock()
	if removed {
		b.Reclaimed404.Increment()
	}
	b.missesMu.Lock()
	b.knownMiss.Add(h)
	b.missesMu.Unlock()
	return removed
}

// ForgetStale drops the index claim for actionID so the next Put re-uploads
// instead of skipping as already known.
func (b *WebBackend) ForgetStale(actionID string) {
	if h, ok := parseActionHash(actionID); ok {
		b.removeClaimed(h)
	}
}

// Close drains the batch coalescer and flushes the HTTP error logger.

func (b *WebBackend) Close() error {
	// Look-ahead first: it is speculation, and nothing waits on it, so a
	// shutdown must not hold for a round trip nobody asked for.
	b.lookAhead.Close()
	// Then the prep pool, which still owes the coalescer every object it holds.
	b.prep.Close()
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

// signRequest authenticates an HTTP request and stamps its provenance.
//
// The provenance is what turns the server's log from a stream of hashes into
// something an operator can act on. A key says nothing about who wanted it; the
// module says which project's build is running, the target says which port it
// is building for, and the kind separates the requests a build is blocked on
// from the ones the look-ahead pool made on its own. Without that last one a
// server cannot tell a slow build from a busy one.
func (b *WebBackend) signRequest(req *http.Request) {
	req.SetBasicAuth(b.accessKey, b.secretKey)
	if module := b.moduleName(); module != "" {
		req.Header.Set(HeaderModule, module)
	}
	if b.version != "" {
		req.Header.Set(HeaderToolchain, b.version)
	}
	if b.target != "" {
		req.Header.Set(HeaderTarget, b.target)
	}
	req.Header.Set(HeaderClient, clientVersion)
}

// SetModule names the module whose build is running, for consumers that learn
// it after the backend exists.
func (b *WebBackend) SetModule(path string) {
	if path != "" {
		b.moduleLate.Store(&path)
	}
}

// moduleName is the configured module path, or one set later.
func (b *WebBackend) moduleName() string {
	if p := b.moduleLate.Load(); p != nil {
		return *p
	}
	return b.module
}

// The provenance headers a client stamps on every request.
const (
	HeaderModule    = "X-Cache-Module"    // the main module whose build is asking
	HeaderToolchain = "X-Cache-Toolchain" // the toolchain version that produced or wants the object
	HeaderTarget    = "X-Cache-Target"    // GOOS/GOARCH the build is producing
	HeaderClient    = "X-Cache-Client"    // this client's wire version
	HeaderKind      = "X-Cache-Kind"      // KindCritical or KindLookAhead
)

// Request kinds. A server that cannot tell these apart cannot tell a build
// that is waiting from one that is merely reading ahead.
const (
	KindCritical  = "critical"
	KindLookAhead = "look-ahead"
)

// clientVersion names the wire contract, so a server log can attribute a
// request to the client that made it.
const clientVersion = "cacheclient/2"
