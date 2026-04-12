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
