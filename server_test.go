package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// newHealthTestServer builds a Server with auth enabled so the health tests also
// prove the probe bypasses the auth gate. Storage is nil because the health path
// never touches it.
func newHealthTestServer() *Server {
	cfg := &Config{
		Bucket: "testbucket",
		Credentials: []Credential{
			{Username: ConfigString{Value: "u"}, Password: ConfigString{Value: "p"}},
		},
	}
	return NewServer(cfg, nil)
}

func TestHealthEndpointOK(t *testing.T) {
	s := newHealthTestServer()

	rec := httptest.NewRecorder()
	// No Authorization header: the probe must answer without credentials.
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, healthPath, nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok\n", rec.Body.String())
}

func TestHealthEndpointReportsDraining(t *testing.T) {
	s := newHealthTestServer()
	s.BeginShutdown()

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, healthPath, nil))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotEmpty(t, rec.Header().Get("Retry-After"))
}

func TestHealthEndpointBypassesAuthGate(t *testing.T) {
	s := newHealthTestServer()

	// A normal request without credentials is rejected with 403...
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/testbucket/some/key", nil))
	require.Equal(t, http.StatusForbidden, rec.Code)

	// ...but the health probe is answered before the auth gate runs.
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, healthPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHealthEndpointBypassesAdmissionControl(t *testing.T) {
	cfg := &Config{Bucket: "testbucket", DisableAuth: true, MaxConcurrentRequests: 1}
	s := NewServer(cfg, nil)
	// Saturate the concurrency limiter: no slots remain free, so a normal request
	// would be shed with 503 Overload. The probe is handled before admission
	// control, so it must still succeed.
	s.sem <- struct{}{}

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, healthPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
}

// docker-updater discovers these two paths on its own, reads only the status
// code, and carries no credential. They sit alongside /_health so they clear
// the same three gates: auth, admission control, and the access log.
func TestWellKnownUpdateChecksAnswerUnauthenticated(t *testing.T) {
	s := newHealthTestServer()

	for _, path := range []string{wellKnownHealthPath, wellKnownPreUpdatePath} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, path)
	}
}

// Both report the drain: a container already shutting down is neither healthy
// nor a safe thing to replace out from under its in-flight requests.
func TestWellKnownUpdateChecksReportDraining(t *testing.T) {
	s := newHealthTestServer()
	s.BeginShutdown()

	for _, path := range []string{wellKnownHealthPath, wellKnownPreUpdatePath} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, path)
		require.NotEmpty(t, rec.Header().Get("Retry-After"), path)
	}
}

// The probes must not consume a concurrency slot, or a saturated server would
// look dead to the updater and have its update rolled back.
func TestWellKnownUpdateChecksBypassAdmissionControl(t *testing.T) {
	cfg := &Config{Bucket: "testbucket", DisableAuth: true, MaxConcurrentRequests: 1}
	s := NewServer(cfg, nil)
	s.sem <- struct{}{}

	for _, path := range []string{wellKnownHealthPath, wellKnownPreUpdatePath} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, path)
	}
}
