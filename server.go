package main

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	config  *Config
	storage *Storage
}

func NewServer(cfg *Config, storage *Storage) *Server {
	return &Server{config: cfg, storage: storage}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	httpInFlightRequests.Inc()
	defer httpInFlightRequests.Dec()

	rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
	route := "Other"
	defer func() {
		duration := time.Since(start)
		log.Printf("req method=%s path=%s remote=%s status=%d bytes=%d duration_ms=%d",
			r.Method, r.URL.Path, r.RemoteAddr,
			rec.statusCode, rec.bytesWritten, duration.Milliseconds())
		httpRequestsTotal.WithLabelValues(r.Method, route, statusStr(rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route).Observe(duration.Seconds())
		httpResponseSize.WithLabelValues(r.Method, route).Observe(float64(rec.bytesWritten))
		if r.ContentLength > 0 {
			httpRequestSize.WithLabelValues(r.Method, route).Observe(float64(r.ContentLength))
		}
	}()

	// Parse path: /{bucket}/{key...} or /{bucket}
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]

	if bucket != s.config.Bucket {
		route = "NoSuchBucket"
		writeS3Error(rec, 404, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	switch {
	case r.Method == "GET" && key == "" && r.URL.Query().Get("list-type") == "2":
		route = "ListObjectsV2"
		handleListObjectsV2(rec, r, s.storage, bucket)
	case r.Method == "GET" && key != "":
		route = "GetObject"
		handleGetObject(rec, r, s.storage, key)
	case r.Method == "PUT" && key != "":
		route = "PutObject"
		handlePutObject(rec, r, s.storage, key)
	default:
		writeS3Error(rec, 405, "MethodNotAllowed", "Method not allowed")
	}
}
