package cacheclient

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A build asks for keys the way a dependency graph lets it: a level at a time,
// -p wide, and it cannot name the next level until this one answers. These
// benchmarks reproduce that shape against a real HTTP server, because the cost
// this client was paying was never visible in a per-call microbenchmark -- it
// was in what each round trip dragged along with it.

const (
	benchLevels   = 12 // dependency depth: one round trip's worth of latency each
	benchPerLevel = 4  // the build's -p: how many keys can be outstanding at once
)

// buildShapeServer answers /_batch/get with the requested bodies plus
// prefetchPer speculative ones, after latency. The latency stands in for a WAN
// round trip, which is where the difference shows: on loopback a wasted
// megabyte is nearly free, and on a real link it is the whole cost.
func buildShapeServer(tb testing.TB, body []byte, prefetchPer int, latency time.Duration) *httptest.Server {
	tb.Helper()
	compressed, err := Compress(body)
	if err != nil {
		tb.Fatalf("compress: %v", err)
	}
	outputID := testOutputID(string(body))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_batch/get" {
			w.WriteHeader(404)
			return
		}
		var req batchGetRequest
		json.NewDecoder(r.Body).Decode(&req)
		if latency > 0 {
			time.Sleep(latency)
		}

		var entries []batchGetManifestEntry
		if !req.PrefetchOnly {
			for _, k := range req.Keys {
				entries = append(entries, batchGetManifestEntry{
					Key: k, Size: int64(len(compressed)),
					Metadata: map[string]string{"outputid": outputID},
				})
			}
		}
		// The speculative half. A real server picks these by store locality; what
		// matters here is only that they are bodies nobody asked for.
		if req.Prefetch {
			for i := range prefetchPer {
				entries = append(entries, batchGetManifestEntry{
					Key: fmt.Sprintf("go-buildcache/v1%064x", 1<<40|i), Size: int64(len(compressed)),
					Metadata: map[string]string{"outputid": outputID}, Prefetch: true,
				})
			}
		}

		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(200)
		tw := tar.NewWriter(w)
		manifest, _ := json.Marshal(batchGetManifest{Entries: entries})
		tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(manifest)), Mode: 0644})
		tw.Write(manifest)
		for _, e := range entries {
			tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(compressed)), Mode: 0644})
			tw.Write(compressed)
		}
		tw.Close()
	}))
	tb.Cleanup(srv.Close)
	return srv
}

// runBuildShape walks the levels, waiting for each before starting the next,
// and answers with the wall time the whole walk took.
func runBuildShape(tb testing.TB, b *WebBackend) {
	tb.Helper()
	for level := range benchLevels {
		var wg sync.WaitGroup
		for i := range benchPerLevel {
			wg.Add(1)
			go func(level, i int) {
				defer wg.Done()
				id := fmt.Sprintf("%064x", level*benchPerLevel+i)
				b.MarkPresent(id)
				b.Get(id)
			}(level, i)
		}
		wg.Wait()
	}
}

// BenchmarkBuildShape measures what a build's own access pattern costs.
//
// The prefetch=N cases are the shape this client used to have: every request
// the build was blocked on came back carrying N extra bodies. Compare them
// against prefetch=0, which is what the critical path asks for now -- the
// speculative fetching moved to the look-ahead pool, off this path.
func BenchmarkBuildShape(bench *testing.B) {
	body := make([]byte, 64<<10)
	for i := range body {
		body[i] = byte(i * 7)
	}

	for _, tc := range []struct {
		name        string
		prefetchPer int
		latency     time.Duration
	}{
		{"lan/prefetch=0", 0, 0},
		{"lan/prefetch=32", 32, 0},
		{"wan/prefetch=0", 0, 5 * time.Millisecond},
		{"wan/prefetch=32", 32, 5 * time.Millisecond},
	} {
		bench.Run(tc.name, func(bench *testing.B) {
			srv := buildShapeServer(bench, body, tc.prefetchPer, tc.latency)
			for range bench.N {
				b, err := NewWebBackend(WebConfig{
					Bucket: "testbucket", Endpoint: srv.URL,
					AccessKey: "k", SecretKey: "s",
				})
				if err != nil || b == nil {
					bench.Fatalf("backend: %v", err)
				}
				runBuildShape(bench, b)
				b.Close()
			}
		})
	}
}
