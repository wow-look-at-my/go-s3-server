package cacheclient

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// shrinkIndexBudgets temporarily replaces the index-fetch budgets and
// returns a restore func. Tests use it instead of poking the vars directly so
// no test can leave a shortened budget behind for the tests after it.
func shrinkIndexBudgets(header, stall, ceiling time.Duration) func() {
	oh, os, oc := indexHeaderBudget, indexStallTimeout, indexFetchCeiling
	indexHeaderBudget, indexStallTimeout, indexFetchCeiling = header, stall, ceiling
	return func() {
		indexHeaderBudget, indexStallTimeout, indexFetchCeiling = oh, os, oc
	}
}

// testIndexBlob builds a valid GBCI v1 blob advertising n synthetic keys.
func testIndexBlob(n int) []byte {
	keys := set.New[string]()
	for i := 0; i < n; i++ {
		h := make([]byte, gbciHashSize)
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		h[2] = byte(i >> 16)
		keys.Add(gbciKeyPrefix + hex.EncodeToString(h))
	}
	return marshalIndex(keys)
}

// TestLoadOrFetchIndex_SlowButSteadyBodySucceeds is the regression test for
// the flaw that made every large-index build lose its cache: the fetch used
// to be bounded by a SINGLE total deadline, so an index big enough to take longer
// than that deadline to stream down was thrown away even though the server
// was healthy and the bytes were arriving. The bound is now per-chunk, so a
// transfer that keeps making progress completes however long it takes — here
// the body is delivered in chunks spread over several stall windows and well
// past the total the old budget would have allowed.
func TestLoadOrFetchIndex_SlowButSteadyBodySucceeds(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	const (
		chunks   = 16
		chunkGap = 40 * time.Millisecond
	)
	blob := testIndexBlob(64)
	chunkSize := (len(blob) + chunks - 1) / chunks

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for off := 0; off < len(blob); off += chunkSize {
			end := min(off+chunkSize, len(blob))
			if _, err := w.Write(blob[off:end]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			// Each gap is well under the stall window, but the total transfer runs past it.
			time.Sleep(chunkGap)
		}
	}))
	defer srv.Close()

	// A single deadline over the whole transfer would kill it mid-stream; only a per-chunk bound finishes it.
	defer shrinkIndexBudgets(200*time.Millisecond, 200*time.Millisecond, 30*time.Second)()

	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	defer b.Close()

	require.True(t, b.indexAuthoritative,
		"a healthy server that keeps streaming must yield an AUTHORITATIVE index — "+
			"a non-authoritative set disables index routing and costs the run its remote hits")
	require.Equal(t, 64, b.keys.Len())
}

// TestLoadOrFetchIndex_StalledBodyAbandoned is the other half of the
// contract: progress-based does not mean unbounded. A server that sends
// headers and some bytes and then goes silent is abandoned around the stall
// window, and the backend proceeds non-authoritative so batch probing stays
// enabled.
func TestLoadOrFetchIndex_StalledBodyAbandoned(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	blob := testIndexBlob(64)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write(blob[:16])
			f.Flush()
		}
		// Then go silent until the client gives up (or the test ends).
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	// LIFO defers: release the handler BEFORE srv.Close waits on it.
	defer srv.Close()
	defer close(release)

	defer shrinkIndexBudgets(2*time.Second, 150*time.Millisecond, 10*time.Second)()

	start := time.Now()
	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer b.Close()

	require.Less(t, elapsed, 5*time.Second,
		"a stalled body must be abandoned on the stall window, not on the 30s client timeout")
	require.False(t, b.indexAuthoritative,
		"an abandoned index fetch must leave the key set non-authoritative so batch probing stays enabled")
	require.Equal(t, 0, b.keys.Len())
}
