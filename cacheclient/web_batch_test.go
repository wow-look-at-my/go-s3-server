package cacheclient

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeBatchServer returns an httptest.Server that mimics the /_batch/get
// endpoint. store maps cache keys → compressed data. meta maps keys → metadata.
func fakeBatchServer(t *testing.T, store map[string][]byte, meta map[string]map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Individual PUT: store the object.
		if r.Method == "PUT" {
			key := r.URL.Path[len("/testbucket/"):]
			body, _ := io.ReadAll(r.Body)
			store[key] = body
			m := make(map[string]string)
			for k, v := range r.Header {
				lk := strings.ToLower(k)
				switch {
				case strings.HasPrefix(lk, "x-cache-meta-"):
					m[strings.TrimPrefix(lk, "x-cache-meta-")] = v[0]
				case strings.HasPrefix(lk, "x-amz-meta-"):
					// Deprecated alias; native wins if both present.
					mk := strings.TrimPrefix(lk, "x-amz-meta-")
					if _, ok := m[mk]; !ok {
						m[mk] = v[0]
					}
				}
			}
			meta[key] = m
			w.WriteHeader(200)
			return
		}

		// Batch GET endpoint (the real server accepts GET and POST; the
		// client sends POST).
		if (r.Method == "GET" || r.Method == "POST") && r.URL.Path == "/testbucket/_batch/get" {
			var req batchGetRequest
			json.NewDecoder(r.Body).Decode(&req)

			var entries []batchGetManifestEntry
			dataMap := map[string][]byte{}
			for _, key := range req.Keys {
				d, ok := store[key]
				if !ok {
					continue
				}
				entries = append(entries, batchGetManifestEntry{
					Key:      key,
					Size:     int64(len(d)),
					Metadata: meta[key],
				})
				dataMap[key] = d
			}
			// Add a prefetch entry if requested and there are extra entries.
			if req.Prefetch {
				for key, d := range store {
					alreadyIncluded := false
					for _, e := range entries {
						if e.Key == key {
							alreadyIncluded = true
							break
						}
					}
					if !alreadyIncluded {
						entries = append(entries, batchGetManifestEntry{
							Key:      key,
							Size:     int64(len(d)),
							Metadata: meta[key],
							Prefetch: true,
						})
						dataMap[key] = d
					}
				}
			}

			manifest := batchGetManifest{Entries: entries}
			w.Header().Set("Content-Type", "application/x-tar")
			w.WriteHeader(200)

			tw := tar.NewWriter(w)
			mdata, _ := json.Marshal(manifest)
			tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
			tw.Write(mdata)
			for _, e := range entries {
				d := dataMap[e.Key]
				tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(d)), Mode: 0644})
				tw.Write(d)
			}
			tw.Close()
			return
		}

		// Individual GET (for index listing).
		if r.Method == "GET" {
			w.WriteHeader(404)
			return
		}

		w.WriteHeader(405)
	}))
}

func TestGetBatch_ReturnsRequestedEntry(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	// Manually store a compressed entry.
	compressed, _ := Compress([]byte("hello world"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{"outputid": testOutputID("hello world")}

	// getBatch should find it via the batch endpoint.
	outputID, body, size, _, miss, err := b.getBatch("aabbccdd11223344", "go-buildcache/v1aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, testOutputID("hello world"), outputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, "hello world", string(data))
	require.Equal(t, int64(11), size)
}

// TestGetBatch_RejectsCorruptEntry verifies the batch serve path applies the
// same end-to-end integrity check as individual GETs: a batched entry whose
// body does not hash to its advertised outputID is refused (miss), never served.
func TestGetBatch_RejectsCorruptEntry(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	// Advertise the hash of a body but store different content under it.
	compressed, _ := Compress([]byte("a totally different body"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{"outputid": testOutputID("the correct body")}

	_, _, _, _, miss, err := b.getBatch("aabbccdd11223344", "go-buildcache/v1aabbccdd11223344")
	require.NoError(t, err)
	require.True(t, miss, "a batched entry failing the checksum must be a miss")
	require.Equal(t, uint32(1), b.MissChecksum.Load())
	require.Equal(t, uint32(0), b.Stats.Hits.Load())
}

// TestGetBatch_MissingOutputIDNotCorrupt verifies that a batched entry with no
// outputid metadata is counted as a no-outputid miss (a metadata gap), not as a
// checksum mismatch / corruption — mirroring getIndividual.
func TestGetBatch_MissingOutputIDNotCorrupt(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	compressed, _ := Compress([]byte("body with no advertised outputid"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{} // no outputid

	_, _, _, _, miss, err := b.getBatch("aabbccdd11223344", "go-buildcache/v1aabbccdd11223344")
	require.NoError(t, err)
	require.True(t, miss)
	require.Equal(t, uint32(1), b.MissNoOutputID.Load())
	require.Equal(t, uint32(0), b.MissChecksum.Load(), "missing outputid must not count as a checksum mismatch")
	require.Equal(t, uint32(0), b.Stats.Corrupt.Load(), "missing outputid must not mark the entry corrupt")
}

func TestGetBatch_PrefetchCallsOnBatchEntries(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	// Store the requested entry and an extra entry (will be prefetched).
	compressed1, _ := Compress([]byte("entry one"))
	compressed2, _ := Compress([]byte("entry two"))
	store["go-buildcache/v1aaaa000000000001"] = compressed1
	meta["go-buildcache/v1aaaa000000000001"] = map[string]string{"outputid": testOutputID("entry one")}
	store["go-buildcache/v1aaaa000000000002"] = compressed2
	meta["go-buildcache/v1aaaa000000000002"] = map[string]string{"outputid": testOutputID("entry two")}

	var callbackEntries []BatchEntry
	b.OnBatchEntries = func(entries []BatchEntry) {
		callbackEntries = append(callbackEntries, entries...)
	}

	// Request a single entry — server should also return the other as prefetch.
	outputID, body, _, _, miss, err := b.getBatch("aaaa000000000001", "go-buildcache/v1aaaa000000000001")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, testOutputID("entry one"), outputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, "entry one", string(data))

	// Callback gets only the prefetch entry; Close drains it race-free.
	require.NoError(t, b.Close())
	require.Len(t, callbackEntries, 1)
	require.Equal(t, "go-buildcache/v1aaaa000000000002", callbackEntries[0].Key)
	require.True(t, callbackEntries[0].Prefetch)
}

func TestGetBatch_FallbackToIndividual(t *testing.T) {
	// Server that doesn't support /_batch/get (answers not-found).
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			body, _ := io.ReadAll(r.Body)
			key := r.URL.Path[len("/testbucket/"):]
			store[key] = body
			m := make(map[string]string)
			for k, v := range r.Header {
				lk := strings.ToLower(k)
				switch {
				case strings.HasPrefix(lk, "x-cache-meta-"):
					m[strings.TrimPrefix(lk, "x-cache-meta-")] = v[0]
				case strings.HasPrefix(lk, "x-amz-meta-"):
					// Deprecated alias; native wins if both present.
					mk := strings.TrimPrefix(lk, "x-amz-meta-")
					if _, ok := m[mk]; !ok {
						m[mk] = v[0]
					}
				}
			}
			meta[key] = m
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/testbucket/_batch/get" {
			w.WriteHeader(404) // batch not supported (any method)
			return
		}
		if r.Method == "GET" {
			key := r.URL.Path[len("/testbucket/"):]
			d, ok := store[key]
			if !ok {
				w.WriteHeader(404)
				return
			}
			if m, ok := meta[key]; ok {
				for k, v := range m {
					w.Header().Set("X-Cache-Meta-"+k, v)
				}
			}
			w.WriteHeader(200)
			w.Write(d)
			return
		}
		w.WriteHeader(405)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	// Store a compressed entry and add its key to the index for getIndividual.
	compressed, _ := Compress([]byte("fallback data"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{"outputid": testOutputID("fallback data")}

	// Add to index so getIndividual works.
	b.keysMu.Lock()
	b.keys.Add("go-buildcache/v1aabbccdd11223344")
	b.keysMu.Unlock()

	// getBatch should fall back to getIndividual when batch is not found.
	outputID, body, _, _, miss, err := b.getBatch("aabbccdd11223344", "go-buildcache/v1aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, testOutputID("fallback data"), outputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, "fallback data", string(data))
}

func TestGetBatch_Miss(t *testing.T) {
	// Server with empty store.
	srv := fakeBatchServer(t, make(map[string][]byte), make(map[string]map[string]string))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	_, _, _, _, miss, _ := b.getBatch("deadbeef00000000", "go-buildcache/v1deadbeef00000000")
	require.True(t, miss)
}

// TestGet_UsessBatchForUnknownKeys verifies that Get() routes through getBatch
// when the key is not in the local index.
func TestGet_UsesBatchForUnknownKeys(t *testing.T) {
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	// Index fetch 404s, so unknown keys must be batch-probed, not fast-missed.
	require.False(t, b.indexAuthoritative)

	// Key is NOT in b.keys index (simulating a fresh cache).
	compressed, _ := Compress([]byte("batch hit"))
	store["go-buildcache/v1aabbccdd11223344"] = compressed
	meta["go-buildcache/v1aabbccdd11223344"] = map[string]string{"outputid": testOutputID("batch hit")}

	outputID, body, _, _, miss, err := b.Get("aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, testOutputID("batch hit"), outputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, "batch hit", string(data))
}

// TestGet_CoalescesConcurrentRequestsIntoOneHTTPRequest is the headline test
// for client-side batching: many concurrent Get callers must funnel through
// a single /_batch/get HTTP request rather than producing a request each.
func TestGet_CoalescesConcurrentRequestsIntoOneHTTPRequest(t *testing.T) {
	const N = 200

	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)

	var batchHTTPCalls int32
	var maxKeysInOneRequest int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method != "GET" && r.Method != "POST") || r.URL.Path != "/testbucket/_batch/get" {
			w.WriteHeader(404)
			return
		}
		var req batchGetRequest
		json.NewDecoder(r.Body).Decode(&req)
		atomic.AddInt32(&batchHTTPCalls, 1)
		if int32(len(req.Keys)) > atomic.LoadInt32(&maxKeysInOneRequest) {
			atomic.StoreInt32(&maxKeysInOneRequest, int32(len(req.Keys)))
		}

		var entries []batchGetManifestEntry
		dataMap := map[string][]byte{}
		for _, key := range req.Keys {
			d, ok := store[key]
			if !ok {
				continue
			}
			entries = append(entries, batchGetManifestEntry{
				Key:      key,
				Size:     int64(len(d)),
				Metadata: meta[key],
			})
			dataMap[key] = d
		}
		manifest := batchGetManifest{Entries: entries}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(200)
		tw := tar.NewWriter(w)
		mdata, _ := json.Marshal(manifest)
		tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
		tw.Write(mdata)
		for _, e := range entries {
			d := dataMap[e.Key]
			tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(d)), Mode: 0644})
			tw.Write(d)
		}
		tw.Close()
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)

	// Index 404s, so unknown keys take the batch-probe path this test exercises.
	require.False(t, b.indexAuthoritative)

	// Fire N parallel Get calls for unknown keys.
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("%016x", i)
			_, _, _, _, _, _ = b.Get(id)
		}(i)
	}
	wg.Wait()

	// batchMaxKeys plus coalescing should fit every caller into a couple of requests.
	calls := atomic.LoadInt32(&batchHTTPCalls)
	require.LessOrEqual(t, calls, int32(3),
		"expected ≤3 HTTP requests for %d parallel Gets, got %d (no client-side batching)", N, calls)
	require.Greater(t, atomic.LoadInt32(&maxKeysInOneRequest), int32(1),
		"expected at least one HTTP request to carry multiple keys; max was %d", atomic.LoadInt32(&maxKeysInOneRequest))
}
