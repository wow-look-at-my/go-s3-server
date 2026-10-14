package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The first request holds the others back until it has a connection, not
// until its answer comes. A slow answer to it must not stall the rest.
func TestFirstDialReleasesOnceItHasAConnection(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	gate := &firstDial{next: &http.Transport{}, ready: make(chan struct{})}
	client := &http.Client{Transport: gate}
	go func() {
		if resp, err := client.Get(srv.URL + "/slow"); err == nil {
			resp.Body.Close()
		}
	}()
	require.Eventually(t, gate.started.Load, time.Second, time.Millisecond)

	start := time.Now()
	resp, err := client.Get(srv.URL + "/fast")
	require.NoError(t, err)
	resp.Body.Close()
	require.Less(t, time.Since(start), 2*time.Second, "the second request waited for the first one's answer")
}
