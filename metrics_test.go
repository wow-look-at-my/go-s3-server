package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownProcessEnv names the test whose body the child process is meant to run.
const ownProcessEnv = "GO_S3_SERVER_ISOLATED_TEST"

// inOwnProcess reports whether the caller is the child process that runs this
// test alone. In the parent it re-executes the test binary for this a single
// test, waits, and reports false; the caller must then return without running
// the body. A failure in the child becomes a failure here.
//
// Every metric in this package is a process-global collector, so a before/after
// pair counts what a concurrent test does as well. A separate process starts
// with those counters at empty and nothing else writing them, which buys the
// isolation without making the rest of the suite wait.
func inOwnProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv(ownProcessEnv) == t.Name() {
		return true
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.v")
	cmd.Env = append(os.Environ(), ownProcessEnv+"="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "the isolated run of %s failed:\n%s", t.Name(), out)
	return false
}

func TestMetricsServer(t *testing.T) {
	// Waiting for another test to do it makes the assertion depend on which
	// tests ran and top-level tests here run in parallel: this failed with the
	// http vec still empty. Touching them is what makes the series exist.
	// Values go unasserted, and every test that measures a delta already runs
	// in its own process.
	httpRequestsTotal.WithLabelValues("GET", "metrics-endpoint-probe", "200").Add(0)
	storageOpsTotal.WithLabelValues("metrics-endpoint-probe", "ok").Add(0)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.Nil(t, err)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)
	t.Cleanup(func() { srv.Close() })

	resp, err := http.Get("http://" + listener.Addr().String() + "/metrics")
	require.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	assert.Contains(t, string(body), "cache_http_requests_total")
	assert.Contains(t, string(body), "cache_storage_operations_total")
	assert.Contains(t, string(body), "cache_auth_failures_total")
}

// TestPutRefusalCounted: the module-index PUT guard is no longer silent — a
// refused upload increments s3_put_refusals_total{reason="module_index"}.
// This counter existing (and moving during CI activity) is the liveness proof
// for the guard; its historical silence is what hid the 512-byte-peek bug.
func TestPutRefusalCounted(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts := testSetup(t)

	before := testutil.ToFloat64(putRefusalsTotal.WithLabelValues("module_index"))

	key := "/testbucket/go-buildcache/v1" + strings.Repeat("1", 64)
	resp := doRequest(t, ts, "PUT", key, lz4Compress(t, incompressibleIndexBody(t, 2048)),
		map[string]string{"X-Cache-Meta-Compression": "lz4"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	require.Equal(t, before+1, testutil.ToFloat64(putRefusalsTotal.WithLabelValues("module_index")))
}

// TestBatchCountersRecorded: batch volume lands in s3_batch_keys_total by kind
// instead of being log-only.
func TestBatchCountersRecorded(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts := testSetup(t)
	client := ts.Client()

	kind := func(k string) float64 { return testutil.ToFloat64(batchKeysTotal.WithLabelValues(k)) }
	requestedBefore, foundBefore, streamedBefore := kind("requested"), kind("found"), kind("streamed")
	reqsBefore := testutil.ToFloat64(batchRequestsTotal)

	present := "go-buildcache/v1" + strings.Repeat("2", 64)
	absent := "go-buildcache/v1" + strings.Repeat("3", 64)
	putObject(t, client, ts.URL, present, []byte("x"), map[string]string{"Outputid": "o"})

	reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{present, absent}})
	resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	require.Equal(t, requestedBefore+2, kind("requested"))
	require.Equal(t, foundBefore+1, kind("found"))
	require.Equal(t, streamedBefore+1, kind("streamed"))
	require.Equal(t, reqsBefore+1, testutil.ToFloat64(batchRequestsTotal))
}

// TestLookAheadAnchorsAreNotMisses: the client seeds a look-ahead request only
// with keys it just hit. With prefetch on or off, those keys must not count as
// requested or as project misses, or every hit also reads as a miss.
func TestLookAheadAnchorsAreNotMisses(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	for _, prefetch := range []bool{false, true} {
		ts := testSetupPrefetch(t, prefetch)
		client := ts.Client()

		kind := func(k string) float64 { return testutil.ToFloat64(batchKeysTotal.WithLabelValues(k)) }
		requestedBefore, foundBefore, anchorsBefore := kind("requested"), kind("found"), kind("anchors")
		missesBefore := testutil.ToFloat64(projectObjectsTotal.WithLabelValues(unknownProject, objKindMiss))

		anchor := "go-buildcache/v1" + strings.Repeat("4", 64)
		putObject(t, client, ts.URL, anchor, []byte("x"), map[string]string{"Outputid": "o"})

		reqBody, _ := json.Marshal(batchGetRequest{Keys: []string{anchor}, Prefetch: true, PrefetchOnly: true})
		resp, err := doBatchGet(client, ts.URL+"/testbucket/_batch/get", reqBody)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		assert.Equal(t, requestedBefore, kind("requested"), "prefetch=%v: an anchor is not a requested key", prefetch)
		assert.Equal(t, foundBefore, kind("found"), "prefetch=%v", prefetch)
		assert.Equal(t, anchorsBefore+1, kind("anchors"), "prefetch=%v: the anchor is counted as an anchor", prefetch)
		assert.Equal(t, missesBefore, testutil.ToFloat64(projectObjectsTotal.WithLabelValues(unknownProject, objKindMiss)),
			"prefetch=%v: an anchor is not a project miss", prefetch)
	}
}

// TestIndexGauges: the index size gauges track puts and serializations.
func TestIndexGauges(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	idx := &Index{}
	var h1, h2 [gbciHashSize]byte
	h1[0], h2[0] = 1, 2
	idx.Put(keyForHash(h1), 1)
	idx.Put(keyForHash(h2), 1)

	require.Equal(t, float64(2), testutil.ToFloat64(indexEntriesGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(indexHashesGauge), "hashes stay pending until Blob")
	require.Equal(t, float64(2), testutil.ToFloat64(indexPendingGauge))

	idx.Blob()
	require.Equal(t, float64(2), testutil.ToFloat64(indexHashesGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(indexPendingGauge))

	idx.RemoveKeys([]string{keyForHash(h1)})
	require.Equal(t, float64(1), testutil.ToFloat64(indexEntriesGauge))
	require.Equal(t, float64(1), testutil.ToFloat64(indexHashesGauge))
}
