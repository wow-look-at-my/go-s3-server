package cacheclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWebBackend_RemoteNeverDisabledAfterFailureBurst is the regression for the
// removed client-side remote-disable behavior. A burst of remote failures
// (a fault or a shed) must NOT permanently disable the remote tier: every failure
// degrades to a clean miss for that operation, but the very next GET must still
// attempt the network. The old behavior turned a transient blip into "no cache
// hits for the rest of the run". Here, after N failing GETs, the backend recovers
// and the next GET must be served as a HIT (proving the remote was never
// disabled).
func TestWebBackend_RemoteNeverDisabledAfterFailureBurst(t *testing.T) {
	hermeticOTel(t)
	t.Setenv("GO_TOOLCHAIN_CACHE_MAX_RETRIES", "0") // a single request per op: fast and deterministic

	const actionID = "deadbeef00000000"
	objectPath := "/testbucket/go-buildcache/v1" + actionID
	good := largePayload(2048)
	outputID := testOutputID(good)

	var failing atomic.Bool
	failing.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(http.StatusNotFound) // index etc.
			return
		}
		if failing.Load() {
			// Alternate the statuses to cover both a genuine fault and a shed.
			if time.Now().UnixNano()%2 == 0 {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(http.StatusOK)
		c, _ := Compress([]byte(good))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	// A long burst of failing GETs — far more than a handful.
	const burst = 40
	for i := 0; i < burst; i++ {
		_, _, _, _, miss, err := b.Get(actionID)
		require.NoError(t, err)
		require.True(t, miss, "a failing remote GET degrades to a clean miss")
	}

	// The backend recovers; the next GET must still attempt the remote and hit, proving the burst never disabled the tier.
	failing.Store(false)
	gotOutputID, body, size, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss, "after a failure burst the remote must still be attempted and hit, not permanently disabled")
	require.Equal(t, outputID, gotOutputID)
	require.Equal(t, int64(len(good)), size)
	require.NoError(t, body.Close())
	require.Equal(t, uint32(1), b.Stats.Hits.Load(), "exactly the post-recovery GET is a hit")
}

// TestWebBackend_RetriesTransientThenRecovers proves bounded retries: a single
// GET retries a transient gateway error up to maxRetries times, and if the backend
// recovers within that budget the GET succeeds rather than wastefully missing.
func TestWebBackend_RetriesTransientThenRecovers(t *testing.T) {
	t.Setenv("GO_TOOLCHAIN_CACHE_MAX_RETRIES", "3")

	const actionID = "aabbccdd11223344"
	good := largePayload(2048)
	outputID := testOutputID(good)
	objectPath := "/testbucket/go-buildcache/v1" + actionID

	var objectReqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(http.StatusNotFound) // index etc.
			return
		}
		// Fail the opening object attempts, then serve the real body.
		if objectReqs.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(http.StatusOK)
		c, _ := Compress([]byte(good))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	gotOutputID, body, size, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss, "a backend that recovers within the retry budget must yield a hit, not a miss")
	require.Equal(t, outputID, gotOutputID)
	require.Equal(t, int64(len(good)), size)
	defer body.Close()
	require.Equal(t, int64(3), objectReqs.Load(), "should have retried twice before the 3rd attempt succeeded")
	require.Equal(t, uint32(1), b.Stats.Hits.Load())
}

// never corrupt the build. The cache layer must degrade to clean misses, the
// local tier must keep serving exactly what was stored, and a GET for a
// never-stored key must miss — never a spurious hit or empty/garbage body.
func TestWebBackend_PutRetriesTransient503ThenSucceeds(t *testing.T) {
	hermeticOTel(t)
	t.Setenv("GO_TOOLCHAIN_CACHE_MAX_RETRIES", "3")

	const actionID = "aabbccdd11223344"
	objectPath := "/testbucket/go-buildcache/v1" + actionID

	var putAttempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" || r.URL.Path != objectPath {
			w.WriteHeader(http.StatusNotFound) // index etc.
			return
		}
		// Shed the opening PUTs like admission control does, with an
		// immediate Retry-After (fast test; jittered backoff still applies).
		if putAttempts.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	defer b.Close()
	// Force the single-PUT retry path so the shed-retry-then-store outcome is observable directly.
	b.batchPutUnsupported.Store(true)

	payload := largePayload(1024)
	outputID := testOutputID(payload)
	err = b.Put(actionID, outputID, strings.NewReader(payload), int64(len(payload)))
	require.NoError(t, err, "a 503-shed PUT must be retried and ultimately succeed, not silently dropped")
	require.Equal(t, int64(3), putAttempts.Load(), "should have retried twice before the 3rd PUT attempt was admitted")
	require.Equal(t, uint32(1), b.Stats.Puts.Load(), "the object must be recorded as stored")
}

// TestParseRetryAfter covers the Retry-After header parsing helper.
func TestParseRetryAfter(t *testing.T) {
	mk := func(v string) *http.Response {
		r := &http.Response{Header: http.Header{}}
		if v != "" {
			r.Header.Set("Retry-After", v)
		}
		return r
	}
	require.Equal(t, time.Duration(0), parseRetryAfter(nil), "nil response")
	require.Equal(t, time.Duration(0), parseRetryAfter(mk("")), "absent header")
	require.Equal(t, time.Duration(0), parseRetryAfter(mk("garbage")), "unparseable")
	require.Equal(t, time.Duration(0), parseRetryAfter(mk("0")), "zero seconds")
	require.Equal(t, 1*time.Second, parseRetryAfter(mk("1")), "delta-seconds")
	// Capped at retryMaxDelay.
	require.Equal(t, retryMaxDelay, parseRetryAfter(mk("3600")), "large delta capped at retryMaxDelay")
}

// sanity: the batch request shape is unchanged by the resilience wrapper.
func TestBatchGetRequest_JSONShape(t *testing.T) {
	data, err := json.Marshal(batchGetRequest{Keys: []string{"a", "b"}, Prefetch: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"keys":["a","b"],"prefetch":true}`, string(data))
}
