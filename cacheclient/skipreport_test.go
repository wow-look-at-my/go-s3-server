package cacheclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipServer answers /_skips with status and records each body it accepts.
func skipServer(t *testing.T, status *atomic.Int32, got *[]SkipReport, mu *sync.Mutex) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_skips" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		code := int(status.Load())
		if code == http.StatusNoContent {
			var rep SkipReport
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&rep))
			mu.Lock()
			*got = append(*got, rep)
			mu.Unlock()
		}
		w.WriteHeader(code)
	}))
}

func newSkipBackend(t *testing.T, url string) *WebBackend {
	t.Helper()
	b, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: url, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	return b
}

func TestSkipReportResendsAfterAFailure(t *testing.T) {
	var status atomic.Int32
	var got []SkipReport
	var mu sync.Mutex
	srv := skipServer(t, &status, &got, &mu)
	defer srv.Close()
	b := newSkipBackend(t, srv.URL)

	b.SkippedNotInIndex.Add(4)
	b.PutSkippedKnown.Add(1)
	status.Store(http.StatusInternalServerError)
	require.False(t, b.FlushSkips(), "a refused report is not delivered")

	b.SkippedKnownMiss.Add(2)
	status.Store(http.StatusNoContent)
	require.True(t, b.FlushSkips())
	require.True(t, b.FlushSkips(), "nothing new to send is still fully delivered")

	b.SkippedEmptyIndex.Add(1)
	require.NoError(t, b.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []SkipReport{
		{GetNotInIndex: 4, GetKnownMiss: 2, PutKnown: 1},
		{GetEmptyIndex: 1},
	}, got, "a failed report must ride the next one, and each count must arrive exactly once")
}

func TestSkipReportStopsAgainstAnOlderServer(t *testing.T) {
	var status atomic.Int32
	var got []SkipReport
	var mu sync.Mutex
	srv := skipServer(t, &status, &got, &mu)
	defer srv.Close()
	b := newSkipBackend(t, srv.URL)
	defer b.Close()

	status.Store(http.StatusMethodNotAllowed)
	b.SkippedNotInIndex.Add(1)
	require.False(t, b.FlushSkips())
	require.True(t, b.skips.disabled.Load(), "a server with no skip endpoint is not asked again")
}
