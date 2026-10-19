package cacheclient

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// windowServer answers each look-ahead request with a window of size new
// entries.
type windowServer struct {
	srv      *httptest.Server
	requests atomic.Int64
	written  atomic.Int64
	next     atomic.Int64
}

func newWindowServer(t *testing.T, size int, pace time.Duration) *windowServer {
	t.Helper()
	ws := &windowServer{}
	body, err := Compress([]byte("a body in the window"))
	require.NoError(t, err)
	ws.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_batch/get") {
			w.WriteHeader(404)
			return
		}
		var req batchGetRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		count := 0
		if req.PrefetchOnly {
			ws.requests.Add(1)
			count = size
		}
		var entries []batchGetManifestEntry
		for range count {
			key := fmt.Sprintf("go-buildcache/v1%064x", ws.next.Add(1))
			entries = append(entries, batchGetManifestEntry{
				Key:      key,
				Size:     int64(len(body)),
				Metadata: map[string]string{"outputid": testOutputID("a body in the window"), "body-size": strconv.Itoa(len("a body in the window"))},
				Prefetch: true,
			})
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(200)
		tw := tar.NewWriter(w)
		mdata, _ := json.Marshal(batchGetManifest{Entries: entries})
		_ = tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0o644})
		_, _ = tw.Write(mdata)
		for _, e := range entries {
			if r.Context().Err() != nil {
				return
			}
			_ = tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(body)), Mode: 0o644})
			_, _ = tw.Write(body)
			_ = tw.Flush()
			w.(http.Flusher).Flush()
			ws.written.Add(1)
			time.Sleep(pace)
		}
		_ = tw.Close()
	}))
	t.Cleanup(ws.srv.Close)
	return ws
}

// judgePool returns a backend whose pool judges after sample entries, and the keys its windows brought.
func judgePool(t *testing.T, ws *windowServer, sample int64) (*WebBackend, func() []string) {
	t.Helper()
	old := lookAheadJudgeAfter
	lookAheadJudgeAfter = sample
	t.Cleanup(func() { lookAheadJudgeAfter = old })
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "2")
	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: ws.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	var mu sync.Mutex
	var got []string
	b.OnBatchEntries = func(entries []BatchEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			got = append(got, e.Key)
		}
	}
	return b, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

// seedMany offers count distinct seeds from first on, one at a time, as a build's batches do.
func seedMany(b *WebBackend, first, count int) {
	for idx := range count {
		b.lookAhead.Seed([]string{fmt.Sprintf("go-buildcache/v1%064x", 1<<40+first+idx)})
	}
}

// A full sample pauses the pool. The windows in flight stop, and no new window
// goes out while the build has not used the sample.
func TestLookAheadSampleStopsTheWindows(t *testing.T) {
	ws := newWindowServer(t, 200, 2*time.Millisecond)
	b, _ := judgePool(t, ws, 32)
	la := b.lookAhead
	seedMany(b, 0, 8)
	require.Eventually(t, la.paused.Load, 5*time.Second, time.Millisecond, "the pool pauses at the sample")
	// Seeds that come after the pause send nothing.
	requests := ws.requests.Load()
	seedMany(b, 8, 8)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, requests, ws.requests.Load(), "a paused pool sends no new window")
	written := ws.written.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, written, ws.written.Load(), "the windows in flight stopped at the pause")
	require.Less(t, written, int64(2*200), "no window streamed to its end")
	require.Zero(t, la.Failed.Load(), "a window the pool cancelled is not a failure")

	// The build makes its lookups and uses none of the sample: the pool stops for good.
	// judge asks for half the sample, rounded up. A window the pause cut off can leave the sample odd.
	for range (la.sampled.Load() + 1) / 2 {
		b.Get(strings.Repeat("ab", 32))
	}
	require.True(t, la.wasteful.Load())
	require.NoError(t, b.Close())
}

// A build that uses the sample lets the pool go on.
func TestLookAheadUsedSampleGoesOn(t *testing.T) {
	ws := newWindowServer(t, 20, 0)
	b, got := judgePool(t, ws, 32)
	la := b.lookAhead
	seedMany(b, 0, 8)
	require.Eventually(t, la.paused.Load, 5*time.Second, time.Millisecond, "the pool pauses at the sample")
	// Every brought key the populator received is also in the brought set by now.
	require.Eventually(t, func() bool { return int64(len(got())) == la.sampled.Load() }, 5*time.Second, time.Millisecond)
	for _, key := range got() {
		id, ok := b.ActionIDFromKey(key)
		require.True(t, ok)
		h, ok := parseActionHash(id)
		require.True(t, ok)
		b.NoteUsed(h)
	}
	require.False(t, la.off(), "a used sample lets the pool go on")
	requests := ws.requests.Load()
	seedMany(b, 8, 1)
	require.Eventually(t, func() bool { return ws.requests.Load() > requests }, 5*time.Second, time.Millisecond, "a new window goes out")
	require.False(t, la.wasteful.Load())
	require.NoError(t, b.Close())
}
