package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forkEnv names the test whose body the child process is meant to run.
const forkEnv = "GO_S3_SERVER_FORKED_TEST"

// forkMetrics runs the calling test in a child process and reports whether this
// call is that child. Every metric in this package is a process-global
// collector, so a before/after pair here also counts what a concurrent test
// does. The child holds counters nobody else writes, which keeps these tests
// parallel with the rest of the suite. The parent reports the child's failure
// as its own.
//
// The caller returns when this is false: the parent must not run the body.
func forkMetrics(t *testing.T) bool {
	t.Helper()
	if os.Getenv(forkEnv) == t.Name() {
		return true
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.v")
	cmd.Env = append(os.Environ(), forkEnv+"="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "the forked run of %s failed:\n%s", t.Name(), out)
	return false
}

func TestMetricsServer(t *testing.T) {
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
	if !forkMetrics(t) {
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
	if !forkMetrics(t) {
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

// TestIndexGauges: the index size gauges track puts and serializations.
func TestIndexGauges(t *testing.T) {
	if !forkMetrics(t) {
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
