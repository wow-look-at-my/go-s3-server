package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// shrinkStallWindow shortens the inactivity window and returns a restore func,
// so no test leaves a shortened window behind for the tests after it.
func shrinkStallWindow(d time.Duration) func() {
	old := stallWindow
	stallWindow = d
	return func() { stallWindow = old }
}

// guarded serves fn under the same wrapping ServeHTTP applies: the byte
// recorder, and the inactivity watchdog over it.
func guarded(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, statusCode: 200}
		defer guardStall(w, r, rec)()
		fn(rec, r)
	}
}

// TestStallGuard_SlowButSteadyResponseCompletes is the regression test for the
// deadline this guard replaced. A whole-request write deadline threw away a
// response for taking too long in total, however healthy it was. The cache's
// biggest responses -- a batch tar, the key index -- are exactly the ones that
// take longest, so the cap fell hardest on the transfers worth keeping. Here
// the body is delivered in chunks spread well past the window, and it must
// arrive whole.
func TestStallGuard_SlowButSteadyResponseCompletes(t *testing.T) {
	t.Serial() // the window is package state

	const (
		chunks   = 10
		chunkGap = 30 * time.Millisecond
	)
	defer shrinkStallWindow(100 * time.Millisecond)()

	srv := httptest.NewServer(guarded(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		for i := 0; i < chunks; i++ {
			if _, err := w.Write([]byte("chunk")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			// Each gap is under the window; their sum is several times it.
			time.Sleep(chunkGap)
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "a response that keeps delivering bytes must not be cut off")
	require.Len(t, body, chunks*len("chunk"))
	require.Greater(t, chunks*chunkGap, stallWindow,
		"the test proves nothing unless the transfer outlasts the window")
}

// TestStallGuard_BlockedWriteIsCutOff is the other half: progress-based does
// not mean unbounded. A client that asks for a body and then stops reading
// fills the socket buffers and wedges the handler inside a write. Without a
// bound that write waits forever, holding a concurrency slot and a file
// handle. The guard fails it on the window instead.
func TestStallGuard_BlockedWriteIsCutOff(t *testing.T) {
	t.Serial() // the window is package state

	defer shrinkStallWindow(200 * time.Millisecond)()

	writeErr := make(chan error, 1)
	srv := httptest.NewServer(guarded(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 1<<16)
		// Far past any socket buffer, so the writes block long before the loop ends.
		for i := 0; i < 4096; i++ {
			if _, err := w.Write(chunk); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}))
	defer srv.Close()

	// A raw connection, because the point is a client that never reads the body.
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: cache\r\n\r\n")
	require.NoError(t, err)

	select {
	case err := <-writeErr:
		require.Error(t, err, "a write that stops making progress must fail rather than wait")
	case <-time.After(15 * time.Second):
		t.Fatal("the guard never cut off a response that stopped making progress")
	}
}

// TestStallGuard_SlowUploadCounts pins that a request BODY arriving counts as
// progress. Without it an upload that writes nothing back -- every PUT -- would
// look silent and be cut off at the window however fast it was uploading.
func TestStallGuard_SlowUploadCounts(t *testing.T) {
	t.Serial() // the window is package state

	const (
		chunks   = 10
		chunkGap = 30 * time.Millisecond
	)
	defer shrinkStallWindow(100 * time.Millisecond)()

	var got atomic.Int64
	srv := httptest.NewServer(guarded(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		got.Store(n)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < chunks; i++ {
			if _, err := pw.Write([]byte("chunk")); err != nil {
				return
			}
			time.Sleep(chunkGap)
		}
		pw.Close()
	}()

	resp, err := http.Post(srv.URL+"/bk/key", "application/octet-stream", pr)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode, "a steady upload must not be cut off")
	require.Equal(t, int64(chunks*len("chunk")), got.Load())
}
