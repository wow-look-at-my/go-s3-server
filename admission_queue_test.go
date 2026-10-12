package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newAdmissionTestServer is newLoadTestServer with the wait for a slot set by
// the test, and the Server handed back.
func newAdmissionTestServer(t *testing.T, slots int, wait time.Duration) (*httptest.Server, *Storage, *Server) {
	t.Helper()
	dir := t.TempDir()
	st, err := NewStorage(dir, WriteOnceConfig{Action: "allow", Notification: "never"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	srv := NewServer(&Config{Bucket: "testbucket", DataDir: dir, DisableAuth: true, MaxConcurrentRequests: slots}, st)
	srv.admitWait = wait
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, st, srv
}

// holdSlot starts a PUT whose body stays open, so it holds a slot until the
// returned function closes the body and waits for the answer.
func holdSlot(t *testing.T, ts *httptest.Server, key string) func() {
	t.Helper()
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/"+key, pr)
		resp, err := ts.Client().Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	_, err := pw.Write([]byte("partial"))
	require.NoError(t, err)
	return func() {
		require.NoError(t, pw.Close())
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the held PUT did not finish after its body was closed")
		}
	}
}

// A request that arrives while every slot is busy waits for one, and is
// served when it frees, instead of being shed.
func TestAdmission_BusyServerQueuesInsteadOfShedding(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, _, _ := newAdmissionTestServer(t, 1, 30*time.Second)
	release := holdSlot(t, ts, "plain/v1held0000000000000001")
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpAdmittedRequests) == 1
	}, 5*time.Second, 5*time.Millisecond, "the unfinished PUT holds the only slot")

	status := make(chan int, 1)
	go func() {
		resp, err := ts.Client().Get(ts.URL + "/testbucket/_index")
		if err != nil {
			status <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpQueuedRequests) == 1
	}, 5*time.Second, 5*time.Millisecond, "the GET waits for the slot")

	rejected := testutil.ToFloat64(httpRejectedTotal)
	release()
	select {
	case code := <-status:
		require.Equal(t, http.StatusOK, code, "the waiter is served once the slot frees")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting GET was not served after the slot freed")
	}
	require.Equal(t, rejected, testutil.ToFloat64(httpRejectedTotal), "nothing was shed")
	require.Equal(t, float64(0), testutil.ToFloat64(httpQueuedRequests))
}

// The wait itself is bounded: past maxQueued waiters a request is shed at
// once, with the Retry-After the client honors.
func TestAdmission_ShedsPastTheWaitBound(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, _, srv := newAdmissionTestServer(t, 1, 30*time.Second)
	srv.maxQueued = 1
	release := holdSlot(t, ts, "plain/v1held0000000000000002")
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpAdmittedRequests) == 1
	}, 5*time.Second, 5*time.Millisecond)

	waiter := make(chan int, 1)
	go func() {
		resp, err := ts.Client().Get(ts.URL + "/testbucket/_index")
		if err != nil {
			waiter <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		waiter <- resp.StatusCode
	}()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpQueuedRequests) == 1
	}, 5*time.Second, 5*time.Millisecond)

	start := time.Now()
	resp, err := ts.Client().Get(ts.URL + "/testbucket/_index")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "the waiting room is full")
	require.Equal(t, "2", resp.Header.Get("Retry-After"))
	require.Less(t, time.Since(start), 5*time.Second, "a full waiting room sheds without waiting")

	release()
	select {
	case code := <-waiter:
		require.Equal(t, http.StatusOK, code, "the request already waiting is still served")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting GET was not served after the slot freed")
	}
}

// A batch GET's tar stream is wire time. A runner that stops reading mid-tar
// must not keep the slot from everyone else.
func TestAdmission_BatchStreamHoldsNoSlot(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}

	ts, st, _ := newAdmissionTestServer(t, 1, 200*time.Millisecond)
	var keys []string
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("plain/v1batch%014d", i)
		require.NoError(t, st.Put(key, bytes.Repeat([]byte{byte('a' + i)}, 8<<20), nil, nil))
		keys = append(keys, key)
	}
	reqBody, err := json.Marshal(batchGetRequest{Keys: keys})
	require.NoError(t, err)

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "POST /testbucket/_batch/get HTTP/1.1\r\nHost: cache\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(reqBody), reqBody)
	require.NoError(t, err)
	status, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, status, " 200 ")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpInFlightRequests) == 1 && testutil.ToFloat64(httpAdmittedRequests) == 0
	}, 10*time.Second, 5*time.Millisecond, "the stalled batch stream must be in flight without a slot")

	resp := doRequest(t, ts, "PUT", "/testbucket/plain/v1small0000000000000", []byte("small"), nil)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "a stalled batch stream must not shed other requests")
	require.Equal(t, float64(1), testutil.ToFloat64(httpInFlightRequests),
		"the batch stream must still be blocked when the PUT got in, or this proves nothing")
}

// TestAdmission_BurstPastTheLimitIsServed is the CI burst: far more clients
// than slots, each asking for objects and storing them. The admission shed
// every request that found the slots busy; every one of these must be served.
func TestAdmission_BurstPastTheLimitIsServed(t *testing.T) {
	ts, st, _ := newAdmissionTestServer(t, 2, admissionWait)
	const (
		clients = 64
		rounds  = 10
		stored  = 32
	)
	var keys []string
	for i := 0; i < stored; i++ {
		key := fmt.Sprintf("plain/v1burstsrc%012d", i)
		require.NoError(t, st.Put(key, bytes.Repeat([]byte{byte(i)}, 64<<10), nil, nil))
		keys = append(keys, key)
	}
	reqBody, err := json.Marshal(batchGetRequest{Keys: keys})
	require.NoError(t, err)

	var served, shed, failed atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 4}}
			defer client.CloseIdleConnections()
			for r := 0; r < rounds; r++ {
				var req *http.Request
				if r%2 == 0 {
					req, _ = http.NewRequest("POST", ts.URL+"/testbucket/_batch/get", bytes.NewReader(reqBody))
					req.Header.Set("Content-Type", "application/json")
				} else {
					req, _ = http.NewRequest("PUT", fmt.Sprintf("%s/testbucket/plain/v1burst%04d%04d", ts.URL, c, r), bytes.NewReader(bytes.Repeat([]byte("p"), 16<<10)))
				}
				resp, err := client.Do(req)
				if err != nil {
					failed.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					served.Add(1)
				case http.StatusServiceUnavailable:
					shed.Add(1)
				default:
					failed.Add(1)
				}
			}
		}(c)
	}
	wg.Wait()

	require.Zero(t, failed.Load())
	require.Zero(t, shed.Load(), "a burst past the slot count waits for slots rather than being shed")
	require.Equal(t, int64(clients*rounds), served.Load())
}
