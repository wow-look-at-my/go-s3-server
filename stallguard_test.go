package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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

// readSteadily downloads url the way a CI runner on a thin link does: a fixed
// slice at a time with a pause between, never stopping. It returns the body
// length and how long the transfer took.
func readSteadily(t *testing.T, url string) (int64, time.Duration, error) {
	t.Helper()
	start := time.Now()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := make([]byte, 64<<10)
	var total int64
	for {
		n, err := io.ReadFull(resp.Body, buf)
		total += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF && total == resp.ContentLength {
			return total, time.Since(start), nil
		}
		if err != nil {
			return total, time.Since(start), err
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// steadyBodyBytes is far past loopback socket buffers, so the server is still
// writing long after the stall window has elapsed several times over.
const steadyBodyBytes = 32 << 20

// TestStallGuard_LongServeContentCompletes is the regression for truncated
// /_index downloads. handleGetIndex serves the blob through http.ServeContent,
// whose copy lands in statusRecorder.ReadFrom as ONE call for the whole body.
// Progress was counted only when that call returned, so a download that kept
// flowing looked silent to the guard and was cut off after two windows, and
// the client read "unexpected EOF". Progress must count as the body moves.
func TestStallGuard_LongServeContentCompletes(t *testing.T) {
	t.Serial() // the window is package state

	defer shrinkStallWindow(150 * time.Millisecond)()

	blob := bytes.Repeat([]byte("i"), steadyBodyBytes)
	srv := httptest.NewServer(guarded(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(blob))
	}))
	defer srv.Close()

	n, took, err := readSteadily(t, srv.URL)
	require.NoError(t, err, "a download that keeps flowing must not be cut off")
	require.Equal(t, int64(len(blob)), n)
	require.Greater(t, took, 3*stallWindow,
		"the test proves nothing unless the transfer outlasts the window")
}

// TestStallGuard_LongFileCopyCompletes is the same regression on the object
// GET path: handleGetObject io.Copies an open file, which also reaches the
// forwarded ReadFrom as one call.
func TestStallGuard_LongFileCopyCompletes(t *testing.T) {
	t.Serial() // the window is package state

	defer shrinkStallWindow(150 * time.Millisecond)()

	path := filepath.Join(t.TempDir(), "object")
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("o"), steadyBodyBytes), 0o644))
	srv := httptest.NewServer(guarded(func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Length", strconv.Itoa(steadyBodyBytes))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, f)
	}))
	defer srv.Close()

	n, took, err := readSteadily(t, srv.URL)
	require.NoError(t, err, "a download that keeps flowing must not be cut off")
	require.Equal(t, int64(steadyBodyBytes), n)
	require.Greater(t, took, 3*stallWindow,
		"the test proves nothing unless the transfer outlasts the window")
}

// TestStatusRecorderReadFromHonorsLimit pins the chunked forward against the
// io.CopyN that http.ServeContent performs: it must stop at the limit, report
// exactly what it copied, and leave the caller's LimitedReader drained.
func TestStatusRecorderReadFromHonorsLimit(t *testing.T) {
	src := bytes.Repeat([]byte("L"), 3*readFromChunk+17)
	rf := &countingReaderFrom{}
	rec := &statusRecorder{ResponseWriter: rf, statusCode: 200}

	limit := int64(2*readFromChunk + 5)
	lr := &io.LimitedReader{R: bytes.NewReader(src), N: limit}
	n, err := rec.ReadFrom(lr)
	require.NoError(t, err)
	require.Equal(t, limit, n)
	require.Equal(t, int64(0), lr.N, "the caller's limit is consumed as the copy goes")
	require.Equal(t, limit, int64(rf.buf.Len()))
	require.Equal(t, limit, rec.progress.Load(), "every copied byte counts as progress")

	// An unlimited source runs to EOF across several chunks.
	rf2 := &countingReaderFrom{}
	rec2 := &statusRecorder{ResponseWriter: rf2, statusCode: 200}
	n, err = rec2.ReadFrom(bytes.NewReader(src))
	require.NoError(t, err)
	require.Equal(t, int64(len(src)), n)
	require.Equal(t, src, rf2.buf.Bytes())
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
