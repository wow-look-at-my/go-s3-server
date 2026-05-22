package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	config          *Config
	storage         *Storage
	prefetchTracker *prefetchTracker
}

func NewServer(cfg *Config, storage *Storage) *Server {
	return &Server{config: cfg, storage: storage, prefetchTracker: newPrefetchTracker()}
}

// auditInfo captures per-request context that is logged on every request and,
// on uploads, persisted as extended attributes alongside the stored object.
type auditInfo struct {
	Username  string
	ClientIP  string
	UserAgent string
	Timestamp time.Time
	Label     string // decoded object description (type, package, go version, target)
}

type auditKey struct{}

func auditFromContext(ctx context.Context) *auditInfo {
	if v, ok := ctx.Value(auditKey{}).(*auditInfo); ok {
		return v
	}
	return nil
}

// clientIP returns the originating client IP, preferring proxy-provided headers
// that are set by Cloudflare and standard reverse proxies. Falls back to the
// TCP peer address. These headers are trusted unconditionally because this
// server is expected to run behind a trusted proxy (e.g. Cloudflare Tunnel);
// if you expose it directly to the internet, clients can spoof these headers.
func clientIP(r *http.Request) string {
	if v := r.Header.Get("CF-Connecting-IP"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Real-IP"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			return strings.TrimSpace(v[:i])
		}
		return strings.TrimSpace(v)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	httpInFlightRequests.Inc()
	defer httpInFlightRequests.Dec()

	rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
	route := "Other"
	ip := clientIP(r)
	ua := r.UserAgent()
	username := anonymousUser
	defer func() {
		duration := time.Since(start)
		label := ""
		if a := auditFromContext(r.Context()); a != nil && a.Label != "" {
			label = " [" + a.Label + "]"
		}
		log.Printf("req method=%s path=%s%s client_ip=%s user=%s user_agent=%q status=%d bytes=%d duration_ms=%d",
			r.Method, r.URL.Path, label, ip, username, ua,
			rec.statusCode, rec.bytesWritten, duration.Milliseconds())
		httpRequestsTotal.WithLabelValues(r.Method, route, statusStr(rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route).Observe(duration.Seconds())
		httpResponseSize.WithLabelValues(r.Method, route).Observe(float64(rec.bytesWritten))
		if r.ContentLength > 0 {
			httpRequestSize.WithLabelValues(r.Method, route).Observe(float64(r.ContentLength))
		}
	}()

	user, err := authenticate(r, s.config)
	if err != nil {
		log.Printf("auth: client_ip=%s user_agent=%q err=%v", ip, ua, err)
		authFailuresTotal.Inc()
		route = "Auth"
		writeS3Error(rec, 403, "AccessDenied", "Access Denied")
		return
	}
	username = user

	audit := &auditInfo{
		Username:  user,
		ClientIP:  ip,
		UserAgent: ua,
		Timestamp: start,
	}
	r = r.WithContext(context.WithValue(r.Context(), auditKey{}, audit))

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
	case r.Method == "GET" && key == "_index":
		route = "Index"
		handleGetIndex(rec, r, s.storage.Index)
	case r.Method == "PUT" && key == "_index":
		route = "PutIndex"
		handlePutIndex(rec, r, s.storage.Index)
	case r.Method == "GET" && key == "_batch/get":
		route = "BatchGet"
		handleBatchGet(rec, r, s.storage, s.prefetchTracker)
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
