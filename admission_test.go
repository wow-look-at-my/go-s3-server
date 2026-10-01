package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Here the server has a single slot, a client starts a download and stops
// reading it, and a PUT must still be admitted while that download is blocked
// mid-body.
func TestAdmission_BodyTransferHoldsNoSlot(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, st := newLoadTestServer(t, 1)
	// Both bodies are far past loopback socket buffers, so a client that stops
	// reading leaves the handler blocked in the middle of the transfer.
	for n := 0; n < 1<<20; n++ {
		st.Index.Put(loadTestKey(n), 1)
	}
	require.NoError(t, st.Put("plain/v1big0000000000000001", bytes.Repeat([]byte("x"), 32<<20), nil, nil))

	for i, path := range []string{"/testbucket/_index", "/testbucket/plain/v1big0000000000000001"} {
		conn, err := net.Dial("tcp", ts.Listener.Addr().String())
		require.NoError(t, err)
		_, err = io.WriteString(conn, "GET "+path+" HTTP/1.1\r\nHost: cache\r\n\r\n")
		require.NoError(t, err)
		status, err := bufio.NewReader(conn).ReadString('\n')
		require.NoError(t, err)
		require.Contains(t, status, " 200 ", path)

		require.Eventually(t, func() bool {
			return testutil.ToFloat64(httpInFlightRequests) == 1 && testutil.ToFloat64(httpAdmittedRequests) == 0
		}, 10*time.Second, 5*time.Millisecond, "%s: the stalled download must be in flight without a slot", path)

		resp := doRequest(t, ts, "PUT", "/testbucket/plain/v1small000000000000"+strconv.Itoa(i), []byte("small"), nil)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: a stalled download must not shed other requests", path)
		require.Equal(t, float64(1), testutil.ToFloat64(httpInFlightRequests),
			"%s: the download must still be blocked when the PUT got in, or this proves nothing", path)

		require.NoError(t, conn.Close())
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(httpInFlightRequests) == 0
		}, 10*time.Second, 5*time.Millisecond, "%s: closing the client ends the download", path)
	}
}

// TestAdmission_WorkStillHoldsTheSlot is the other half: handing the slot back
// early is for body transfers only.
func TestAdmission_WorkStillHoldsTheSlot(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, _ := newLoadTestServer(t, 1)

	// A PUT whose body never finishes is work in progress: it holds the slot.
	pr, pw := io.Pipe()
	putDone := make(chan struct{})
	go func() {
		defer close(putDone)
		req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/plain/v1pending00000000001", pr)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	_, _ = pw.Write([]byte("partial"))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpAdmittedRequests) == 1
	}, 5*time.Second, 5*time.Millisecond, "the unfinished PUT holds the only slot")

	resp, err := http.Get(ts.URL + "/testbucket/_index")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "admission still applies before any work starts")

	require.NoError(t, pw.Close())
	select {
	case <-putDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the PUT did not finish after its body was closed")
	}
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpAdmittedRequests) == 0
	}, 5*time.Second, 5*time.Millisecond, "the slot is returned when the request ends")
}
