package cacheclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// trickleBody sends a small chunk every gap until total has passed. Before
// each chunk it waits on hold, so a test can stop the flow.
type trickleBody struct {
	gap   time.Duration
	until time.Time
	hold  <-chan struct{}
}

func (t *trickleBody) Read(p []byte) (int, error) {
	if time.Now().After(t.until) {
		return 0, io.EOF
	}
	<-t.hold
	time.Sleep(t.gap)
	n := min(len(p), 64)
	for i := range n {
		p[i] = 'x'
	}
	return n, nil
}

func (*trickleBody) Close() error { return nil }

func uploadStallServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func trickleRequest(t *testing.T, url string, body *trickleBody) *http.Request {
	t.Helper()
	req, err := http.NewRequest("PUT", url+"/testbucket/_batch/put", body)
	require.NoError(t, err)
	req.ContentLength = -1
	return req
}

// An upload that keeps sending for longer than stallTimeout is healthy.
func TestUploadThatKeepsFlowingOutlastsTheStallTimeout(t *testing.T) {
	logs := newCaptureLogger(t) // also serializes: stallTimeout is package state
	oldStall := stallTimeout
	stallTimeout = 100 * time.Millisecond
	defer func() { stallTimeout = oldStall }()

	srv := uploadStallServer(t)
	b := testBackend(t, srv.URL)
	defer b.Close()

	open := make(chan struct{})
	close(open)
	body := &trickleBody{gap: 10 * time.Millisecond, until: time.Now().Add(4 * stallTimeout), hold: open}
	resp, err := b.doRetry(trickleRequest(t, srv.URL, body), 0)
	require.NoError(t, err, "a body that keeps flowing must not be cancelled")
	closeBody(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, b.Down(), "a slow upload is not an outage")
	require.NotContains(t, logs.String(), "not responding")
}

// An upload that stops sending for longer than stallTimeout is cancelled, and the server counts as down.
func TestUploadThatStopsFlowingIsCancelled(t *testing.T) {
	logs := newCaptureLogger(t) // also serializes: stallTimeout and downProbeEvery are package state
	oldStall, oldProbe := stallTimeout, downProbeEvery
	stallTimeout, downProbeEvery = 100*time.Millisecond, time.Hour
	defer func() { stallTimeout, downProbeEvery = oldStall, oldProbe }()

	srv := uploadStallServer(t)
	b := testBackend(t, srv.URL)
	defer b.Close()

	hold := make(chan struct{})
	defer close(hold)
	body := &trickleBody{gap: time.Millisecond, until: time.Now().Add(time.Hour), hold: hold}
	start := time.Now()
	_, err := b.doRetry(trickleRequest(t, srv.URL, body), 0)
	require.Error(t, err)
	require.Less(t, time.Since(start), 20*stallTimeout, "the watchdog cancels a silent upload")
	require.True(t, b.Down())
	require.Contains(t, logs.String(), "not responding")
}
