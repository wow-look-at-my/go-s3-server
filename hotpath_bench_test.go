package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// benchBody builds a body shaped like a real compiled Go object: an ar archive
// header, then a mix of compressible and incompressible bytes, so lz4 produces
// several blocks' worth of realistic work rather than one degenerate run.
func benchBody(tb testing.TB, size int) []byte {
	raw := make([]byte, size)
	_, err := rand.Read(raw[size/2:])
	require.NoError(tb, err)
	for i := size / 2; i < size; i += 64 {
		// Repeat a slice of the random half so the compressor finds matches.
		end := i + 32
		if end > size {
			end = size
		}
		copy(raw[i-size/2:], raw[i:end])
	}
	copy(raw, "!<arch>\ndebug/deadcode")
	return raw
}

func benchMeta(body []byte) map[string]string {
	sum := sha256.Sum256(body)
	return map[string]string{
		"outputid":          hex.EncodeToString(sum[:]),
		"compression":       "lz4",
		"object-type":       "go-archive",
		"pkg":               "github.com/wow-look-at-my/go-s3-server/internal/bench",
		"src":               "a.go b.go c.go d.go e.go",
		"module":            "github.com/wow-look-at-my/go-s3-server",
		"go-version":        "go1.24.7",
		"target":            "linux/amd64",
		"toolchain-version": "v576",
		"created":           "2026-08-01T00:00:00Z",
	}
}

// BenchmarkPutObjectBySize measures the single-PUT store path across the object
// sizes a real build produces. The module-index guard has to decide whether the
// upload is a Go module index, and how it does that is the size-sensitive part:
// decoding the whole first lz4 block scales with the object, reading the frame
// header and first literal run does not.
func BenchmarkPutObjectBySize(b *testing.B) {
	for _, size := range []int{8 << 10, 256 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			dir := b.TempDir()
			storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
			require.NoError(b, err)
			b.Cleanup(func() { storage.Close() })

			payload := lz4Compress(b, benchBody(b, size))
			hdr := http.Header{}
			hdr.Set("X-Cache-Meta-Outputid", "benchoutputid")
			hdr.Set("X-Cache-Meta-Compression", "lz4")

			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("go-buildcache/v1%064x", i%4)
				req := httptest.NewRequest("PUT", "/testbucket/"+key, bytes.NewReader(payload))
				req.Header = hdr
				req.ContentLength = int64(len(payload))
				rec := httptest.NewRecorder()
				handlePutObject(rec, req, storage, key, defaultMaxObjectBytes)
				require.Equal(b, 200, rec.Code)
			}
		})
	}
}

// BenchmarkBatchGetWarm is the server's busiest path: the client coalesces up
// to 128 keys into one /_batch/get, and a warm CI cache answers nearly all of
// them. Every key costs a stat, a metadata read, the guard, and a body stream.
func BenchmarkBatchGetWarm(b *testing.B) {
	const nKeys = 128
	dir := b.TempDir()
	storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(b, err)
	b.Cleanup(func() { storage.Close() })

	body := benchBody(b, 16<<10)
	payload := lz4Compress(b, body)
	meta := benchMeta(body)

	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("go-buildcache/v1%064x", i)
		require.NoError(b, storage.PutStream(keys[i], bytes.NewReader(payload), meta, nil))
	}
	reqBody, err := json.Marshal(batchGetRequest{Keys: keys})
	require.NoError(b, err)
	tracker := newPrefetchTracker()

	// Warm the known-clean memo exactly as steady-state traffic would.
	rec := httptest.NewRecorder()
	handleBatchGet(rec, httptest.NewRequest("POST", "/testbucket/_batch/get", bytes.NewReader(reqBody)), storage, tracker)
	require.Equal(b, 200, rec.Code)

	b.SetBytes(int64(len(payload)) * nKeys)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/testbucket/_batch/get", bytes.NewReader(reqBody))
		rec := httptest.NewRecorder()
		handleBatchGet(rec, req, storage, tracker)
		require.Equal(b, 200, rec.Code)
		require.Greater(b, rec.Body.Len(), nKeys*len(payload))
	}
}

// BenchmarkStatMetadata isolates the per-key metadata read every served key
// pays (twice, before the batch path stopped re-reading it): one listxattr plus
// a read per attribute.
func BenchmarkStatMetadata(b *testing.B) {
	dir := b.TempDir()
	storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(b, err)
	b.Cleanup(func() { storage.Close() })

	key := "go-buildcache/v1" + strings.Repeat("7", 64)
	body := benchBody(b, 8<<10)
	require.NoError(b, storage.PutStream(key, bytes.NewReader(lz4Compress(b, body)), benchMeta(body), nil))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta, err := storage.Stat(key)
		require.NoError(b, err)
		require.NotEmpty(b, meta.Metadata["outputid"])
	}
}

// BenchmarkModuleIndexPeek measures the guard's verdict alone, on the exact
// input each path hands it: an in-memory prefix (PUT) and a stored body (read).
func BenchmarkModuleIndexPeek(b *testing.B) {
	payload := lz4Compress(b, benchBody(b, 256<<10))

	b.Run("put", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			require.False(b, looksLikeGoModuleIndex(payload, "lz4"))
		}
	})
	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			isIndex, err := readIsModuleIndex(bytes.NewReader(payload), "lz4")
			require.False(b, err != nil || isIndex)
		}
	})
	// An actual module index must still be recognized at the same cost class.
	idx := lz4Compress(b, append([]byte("go index v2\n"), bytes.Repeat([]byte("m"), 4096)...))
	b.Run("index", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			require.True(b, looksLikeGoModuleIndex(idx, "lz4"))
		}
	})
}
