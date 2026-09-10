package cacheclient

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// benchKey names the object a build asks for at one position in the walk.
// runBuildShape and the fake server must agree on it, because a speculative
// body for a key the build never asks for cannot model a prefetch that pays
// off: it is pure cost by construction, whatever the client does with it.
func benchKey(n int) string { return fmt.Sprintf("%064x", n) }

// benchWireKey is the same object as it travels: Get takes a bare action ID
// and the client prefixes it, so a manifest and a tier are keyed by this and
// never by benchKey.
func benchWireKey(n int) string { return gbciKeyPrefix + benchKey(n) }

// benchKeyIndex reverses benchWireKey, and reports whether the key is one.
func benchKeyIndex(key string) (int, bool) {
	rest, ok := strings.CutPrefix(key, gbciKeyPrefix)
	if !ok {
		return 0, false
	}
	var n int
	if _, err := fmt.Sscanf(rest, "%064x", &n); err != nil {
		return 0, false
	}
	return n, true
}

// localTier stands in for the disk cache cmd/go puts in front of this client,
// which is the only place a look-ahead fetch can land.
//
// A benchmark without one measures a client whose look-ahead is switched OFF:
// expand returns at once when OnBatchEntries is nil, so the pool issues no
// request at all. Nothing ships that way. cmd/go always installs
// SharedCache.populate, and the build's next Get then reads what the pool
// already put on disk instead of reaching the network.
type localTier struct {
	mu   sync.Mutex
	objs map[string]struct{}
	Hits int
}

func newLocalTier() *localTier { return &localTier{objs: map[string]struct{}{}} }

func (t *localTier) store(entries []BatchEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range entries {
		t.objs[e.Key] = struct{}{}
	}
}

// take reports whether the object is already local, counting the hit.
func (t *localTier) take(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.objs[key]; !ok {
		return false
	}
	t.Hits++
	return true
}

// buildShapeServer answers /_batch/get after latency.
//
// The speculative bodies it attaches are the keys the build asks for at the
// NEXT levels of the walk, which is what a real server's store locality
// approximates. Where they ride decides which shape is under test.
//
// onBlocking is the old wire shape: the client set prefetch on every
// critical-path batch, so a request four keys wide came back carrying dozens
// of bodies nobody was waiting for yet. Reproducing it here rather than
// reintroducing the flag is what makes the two shapes comparable.
//
// A PrefetchOnly request is the look-ahead pool asking for the window around a
// seed, off the critical path. Answering it with nothing, as an earlier
// version of this server did, leaves the pool unable to fetch anything and
// makes every arm a no-prefetch arm.
//
// The latency stands in for a WAN round trip, which is where the difference
// shows: on loopback a wasted megabyte is nearly free, and on a real link it
// is the whole cost.
func buildShapeServer(tb testing.TB, body []byte, carried int, onBlocking bool, latency time.Duration) *httptest.Server {
	tb.Helper()
	compressed, err := Compress(body)
	if err != nil {
		tb.Fatalf("compress: %v", err)
	}
	outputID := testOutputID(string(body))

	// window names the keys stored around the requested ones: the next levels
	// of the walk, in order, capped at carried.
	window := func(keys []string) []string {
		var out []string
		for step := 1; len(out) < carried; step++ {
			for _, k := range keys {
				n, ok := benchKeyIndex(k)
				if !ok {
					continue
				}
				next := n + step*benchPerLevel
				if next >= benchLevels*benchPerLevel || len(out) >= carried {
					continue
				}
				out = append(out, benchWireKey(next))
			}
			if step > benchLevels {
				break
			}
		}
		return out
	}

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
		add := func(key string, prefetch bool) {
			entries = append(entries, batchGetManifestEntry{
				Key: key, Size: int64(len(compressed)), Prefetch: prefetch,
				Metadata: map[string]string{"outputid": outputID},
			})
		}
		if !req.PrefetchOnly {
			for _, k := range req.Keys {
				add(k, false)
			}
		}
		if req.PrefetchOnly || onBlocking {
			for _, k := range window(req.Keys) {
				add(k, true)
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

// runBuildShape walks the levels, waiting for each before starting the next.
//
// A key already in the local tier costs nothing: the build reads it and never
// reaches the network. That is what a look-ahead fetch buys, and a run without
// the tier cannot show it.
func runBuildShape(tb testing.TB, b *WebBackend, tier *localTier) {
	tb.Helper()
	for level := range benchLevels {
		var wg sync.WaitGroup
		for i := range benchPerLevel {
			wg.Add(1)
			go func(level, i int) {
				defer wg.Done()
				n := level*benchPerLevel + i
				if tier.take(benchWireKey(n)) {
					return
				}
				id := benchKey(n)
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

	// Three shapes. none is the critical path alone, with nothing fetched
	// ahead of it: the floor a cache must beat. blocking is the old wire
	// shape, where the speculative bodies ride the request the build is
	// waiting on. lookahead is what ships: the pool fetches the same window
	// off the critical path, into the tier the build reads next.
	for _, tc := range []struct {
		name       string
		carried    int
		onBlocking bool
		lookAhead  bool
		latency    time.Duration
	}{
		{"lan/none", 0, false, false, 0},
		{"lan/blocking", 32, true, false, 0},
		{"lan/lookahead", 32, false, true, 0},
		{"wan/none", 0, false, false, 5 * time.Millisecond},
		{"wan/blocking", 32, true, false, 5 * time.Millisecond},
		{"wan/lookahead", 32, false, true, 5 * time.Millisecond},
	} {
		bench.Run(tc.name, func(bench *testing.B) {
			srv := buildShapeServer(bench, body, tc.carried, tc.onBlocking, tc.latency)
			var hits int
			for range bench.N {
				b, err := NewWebBackend(WebConfig{
					Bucket: "testbucket", Endpoint: srv.URL,
					AccessKey: "k", SecretKey: "s",
				})
				if err != nil || b == nil {
					bench.Fatalf("backend: %v", err)
				}
				tier := newLocalTier()
				if tc.lookAhead {
					b.OnBatchEntries = tier.store
				}
				runBuildShape(bench, b, tier)
				b.Close()
				hits += tier.Hits
			}
			// A look-ahead arm that never hit the tier measured the same thing
			// as none, and would read as a speedup that is really a no-op.
			if tc.lookAhead && hits == 0 {
				bench.Fatal("look-ahead arm served no key from the local tier")
			}
		})
	}
}
