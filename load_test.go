package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// loadTestKey returns a deterministic cacheprog-style key (go-buildcache/v1 +
// 64 hex) for index n.
func loadTestKey(n int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("load-key-%d", n)))
	return "go-buildcache/v1" + hex.EncodeToString(h[:])
}

// newLoadTestServer starts an httptest server backed by a fresh data dir with
// auth disabled and the given concurrency limit.
func newLoadTestServer(t *testing.T, maxConcurrent int) (*httptest.Server, *Storage) {
	t.Helper()
	dir := t.TempDir()
	st, err := NewStorage(dir, WriteOnceConfig{Action: "allow", Notification: "never"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	cfg := &Config{Bucket: "testbucket", DataDir: dir, DisableAuth: true, MaxConcurrentRequests: maxConcurrent}
	ts := httptest.NewServer(NewServer(cfg, st))
	t.Cleanup(ts.Close)
	return ts, st
}

// consumeTar reads a batch-get tar response one entry at a time, discarding each
// body, and returns the total bytes seen. Reading entry-by-entry both validates
// that the streamed tar is well-formed under load and keeps the client's own
// memory bounded, so the process-heap assertion reflects the server.
func consumeTar(r io.Reader) (int64, int, error) {
	tr := tar.NewReader(r)
	var total int64
	var entries int
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return total, entries, nil
		}
		if err != nil {
			return total, entries, err
		}
		n, err := io.Copy(io.Discard, tr)
		if err != nil {
			return total, entries, err
		}
		total += n
		if strings.HasPrefix(hdr.Name, "data/") {
			entries++
		}
	}
}

// TestLoad_ConcurrentMatrixStreamsWithBoundedMemory emulates the CI matrix that
// OOM-killed the server: many concurrent clients each batch-fetching hundreds of
// sizable objects plus interleaved PUTs. The server must (1) never return a 5xx,
// and (2) keep its heap bounded far below the total bytes served — proof that
// bodies are streamed, not all buffered. Pre-fix (every batch body buffered into
// one slice) the live heap would track concurrent-batches × batch-bytes (~1.5
// GiB here); post-fix it stays flat.
func TestLoad_ConcurrentMatrixStreamsWithBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in -short mode")
	}
	ts, st := newLoadTestServer(t, 128)

	const (
		numObjects = 600
		objSize    = 256 * 1024 // big enough that buffering a batch would dwarf the heap cap
		clients    = 16
		batchSize  = 400
		putsPerCl  = 10
	)
	// If every batch body were buffered (the pre-fix behavior), the server would
	// hold clients*batchSize*objSize of live bodies at the synchronized peak:
	//   16 * 400 * 256 KiB = 1.6 GiB — far above heapCap below.

	// Pre-populate the cache. st.Put streams to disk, so this does not buffer
	// 600 objects in memory.
	body := bytes.Repeat([]byte{'x'}, objSize)
	keys := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		keys[i] = loadTestKey(i)
		require.NoError(t, st.Put(keys[i], body, map[string]string{"outputid": fmt.Sprintf("o%d", i)}, nil))
	}

	// Sample peak heap-in-use throughout the load.
	var peakHeap uint64
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		var ms runtime.MemStats
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-tk.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > atomic.LoadUint64(&peakHeap) {
					atomic.StoreUint64(&peakHeap, ms.HeapInuse)
				}
			}
		}
	}()

	var maxStatus int64
	var totalServed int64
	var (
		errMu    sync.Mutex
		firstErr error
	)
	recordErr := func(e error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		errMu.Unlock()
	}
	recordStatus := func(code int) {
		for {
			old := atomic.LoadInt64(&maxStatus)
			if int64(code) <= old || atomic.CompareAndSwapInt64(&maxStatus, old, int64(code)) {
				return
			}
		}
	}

	// Barrier so all clients hit the server at once, maximizing the concurrent
	// peak the sampler must catch (and that buffering would balloon).
	start := make(chan struct{})
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 120 * time.Second}
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			<-start
			bk := make([]string, batchSize)
			for i := range bk {
				bk[i] = keys[(c*7+i*3)%numObjects]
			}
			reqBody, _ := json.Marshal(batchGetRequest{Keys: bk, Prefetch: true})
			req, _ := http.NewRequest("GET", ts.URL+"/testbucket/_batch/get", bytes.NewReader(reqBody))
			resp, err := client.Do(req)
			if err != nil {
				recordErr(err)
				return
			}
			recordStatus(resp.StatusCode)
			n, _, terr := consumeTar(resp.Body)
			resp.Body.Close()
			if terr != nil {
				recordErr(fmt.Errorf("tar parse: %w", terr))
			}
			atomic.AddInt64(&totalServed, n)

			// Interleave PUTs of fresh keys.
			for p := 0; p < putsPerCl; p++ {
				pk := loadTestKey(numObjects + c*10000 + p)
				preq, _ := http.NewRequest("PUT", ts.URL+"/testbucket/"+pk, bytes.NewReader(body))
				presp, err := client.Do(preq)
				if err != nil {
					recordErr(err)
					continue
				}
				recordStatus(presp.StatusCode)
				io.Copy(io.Discard, presp.Body)
				presp.Body.Close()
			}
		}(c)
	}
	close(start)
	wg.Wait()
	close(stopSampler)
	samplerWG.Wait()

	require.NoError(t, firstErr)

	require.Less(t, int(atomic.LoadInt64(&maxStatus)), 500,
		"server must never return a 5xx under sustained concurrent load (got max status %d)", maxStatus)
	require.Greater(t, atomic.LoadInt64(&totalServed), int64(1)<<30,
		"sanity: the test should have streamed well over 1 GiB of bodies")

	peak := atomic.LoadUint64(&peakHeap)
	const heapCap = 512 * 1024 * 1024
	t.Logf("served %d MiB; peak heap %d MiB; max status %d",
		atomic.LoadInt64(&totalServed)/(1024*1024), peak/(1024*1024), atomic.LoadInt64(&maxStatus))
	if raceDetectorEnabled {
		// The race detector's allocation tracking inflates the heap, so the
		// streaming-vs-buffering memory bound is only meaningful without it.
		// The no-5xx and tar-validity checks above still exercise the streaming
		// path under -race.
		t.Log("skipping heap-bound assertion under -race")
		return
	}
	require.Less(t, peak, uint64(heapCap),
		"peak heap %d MiB exceeded %d MiB while serving %d MiB — bodies are being buffered, not streamed",
		peak/(1024*1024), heapCap/(1024*1024), atomic.LoadInt64(&totalServed)/(1024*1024))
}

// TestLoad_OverloadShedsWith503 proves the backpressure path: when the server is
// at its concurrency limit, excess requests are shed with 503 + Retry-After
// (the signal clients back off on) instead of being queued until the process
// OOMs — the failure a fronting proxy would otherwise surface as a 502.
func TestLoad_OverloadShedsWith503(t *testing.T) {
	ts, _ := newLoadTestServer(t, 1) // single slot: trivially saturable

	rejectedBefore := testutil.ToFloat64(httpRejectedTotal)

	// Occupy the only slot with a PUT whose body never completes (a pipe we
	// don't close), so the server blocks in storage streaming while holding it.
	pr, pw := io.Pipe()
	putDone := make(chan struct{})
	go func() {
		defer close(putDone)
		req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/go-buildcache/v1"+strings.Repeat("a", 64), pr)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	_, _ = pw.Write([]byte("partial")) // ensure the request is in flight
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(httpInFlightRequests) >= 1
	}, 5*time.Second, 5*time.Millisecond, "the blocking PUT should hold the only slot")

	// A second request must be shed immediately with 503 + Retry-After.
	resp, err := http.Get(ts.URL + "/testbucket/go-buildcache/v1" + strings.Repeat("b", 64))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 503, resp.StatusCode, "an over-capacity request must be shed with 503, never 502 or a dropped connection")
	require.NotEmpty(t, resp.Header.Get("Retry-After"), "503 must carry Retry-After so clients back off")

	// Release the blocked PUT and confirm it drains.
	pw.Close()
	select {
	case <-putDone:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked PUT did not finish after its body was closed")
	}

	require.Greater(t, testutil.ToFloat64(httpRejectedTotal), rejectedBefore,
		"the overload rejection must be observable via s3_http_rejected_total")
}

// TestLoad_MemoryPressureShedsInsteadOfGrowing is the load-side statement of the
// requirement: under memory pressure the server refuses work visibly instead of
// growing until something kills it. Every client here is doing exactly what a CI
// matrix does -- batch fetches and interleaved uploads -- while the watcher
// reports the process past its budget for the middle of the run.
//
// The pressure signal is faked because the alternative is not testable: making a
// test process genuinely exceed a real GOMEMLIMIT means allocating gigabytes and
// then racing the GC. What is real here is everything downstream of the signal:
// the trimmers run against live caches, the HTTP path sheds, clients see 503 +
// Retry-After, and service resumes when the pressure lifts.
func TestLoad_MemoryPressureShedsInsteadOfGrowing(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in -short mode")
	}
	dir := t.TempDir()
	st, err := NewStorage(dir, WriteOnceConfig{Action: "allow", Notification: "never"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	cfg := &Config{Bucket: "testbucket", DataDir: dir, DisableAuth: true, MaxConcurrentRequests: 128}
	srv := NewServer(cfg, st)

	// A watcher with a budget and a sampler the test drives. The trimmers
	// registered by NewServer (the storage caches, the prefetch tracker) are
	// carried over, so a trim here drops the real thing.
	const budget = 1000
	var inUse atomic.Int64
	trimmers := srv.mem.trimmers
	srv.mem = newMemWatcher(budget)
	srv.mem.sample = func() (int64, float64) { return inUse.Load(), 0 }
	srv.mem.trimNow = nil
	for _, f := range trimmers {
		srv.mem.AddTrimmer(f)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	const (
		numObjects = 200
		objSize    = 64 * 1024
		clients    = 16
		reqPerCl   = 20
	)
	body := bytes.Repeat([]byte{'y'}, objSize)
	keys := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		keys[i] = loadTestKey(i)
		require.NoError(t, st.Put(keys[i], body, map[string]string{"outputid": fmt.Sprintf("o%d", i)}, nil))
	}

	shedBefore := testutil.ToFloat64(memoryShedTotal)
	trimsBefore := testutil.ToFloat64(memoryTrimsTotal)

	// Pressure rises for the middle of the run and then clears, so the test
	// observes both the shed and the recovery.
	stopPressure := make(chan struct{})
	var pressureWG sync.WaitGroup
	pressureWG.Add(1)
	go func() {
		defer pressureWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stopPressure:
				inUse.Store(100)
				srv.mem.poll()
				return
			default:
			}
			if i%2 == 0 {
				inUse.Store(990) // above the shed threshold
			} else {
				inUse.Store(100)
			}
			srv.mem.poll()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var ok, shed, bad atomic.Int64
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 30 * time.Second}
	start := make(chan struct{})
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			<-start
			for i := 0; i < reqPerCl; i++ {
				var resp *http.Response
				var err error
				if i%3 == 0 {
					bk := []string{keys[(c*3+i)%numObjects], keys[(c*5+i)%numObjects]}
					reqBody, _ := json.Marshal(batchGetRequest{Keys: bk, Prefetch: true})
					resp, err = client.Post(ts.URL+"/testbucket/_batch/get", "application/json", bytes.NewReader(reqBody))
				} else if i%3 == 1 {
					pk := loadTestKey(numObjects + c*1000 + i)
					req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/"+pk, bytes.NewReader(body))
					resp, err = client.Do(req)
				} else {
					resp, err = client.Get(ts.URL + "/testbucket/" + keys[(c+i)%numObjects])
				}
				if err != nil {
					bad.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch {
				case resp.StatusCode == 503:
					require.NotEmpty(t, resp.Header.Get("Retry-After"))
					shed.Add(1)
				case resp.StatusCode < 400:
					ok.Add(1)
				default:
					bad.Add(1)
				}
			}
		}(c)
	}
	close(start)
	wg.Wait()
	close(stopPressure)
	pressureWG.Wait()

	t.Logf("ok=%d shed=%d bad=%d trims=%v", ok.Load(), shed.Load(), bad.Load(),
		testutil.ToFloat64(memoryTrimsTotal)-trimsBefore)

	require.Zero(t, bad.Load(), "under memory pressure every request must be answered or shed with 503 -- never a dropped connection or another error")
	require.Positive(t, shed.Load(), "the load must actually have been shed, or this test proves nothing")
	require.Positive(t, ok.Load(), "shedding must not mean refusing everything")
	require.Greater(t, testutil.ToFloat64(memoryShedTotal), shedBefore, "shed requests must be observable in s3_memory_shed_total")

	// Pressure has cleared: the server serves again, with correct bodies, and
	// without a restart.
	resp, err := client.Get(ts.URL + "/testbucket/" + keys[0])
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "service must resume once pressure clears")
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, got, "a trim must not corrupt what the server serves")
}
