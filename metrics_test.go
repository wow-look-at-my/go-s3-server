package main

import (
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wow-look-at-my/testify/assert"
	"github.com/wow-look-at-my/testify/require"
)

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
	assert.Contains(t, string(body), "s3_http_requests_total")
	assert.Contains(t, string(body), "s3_storage_operations_total")
}
