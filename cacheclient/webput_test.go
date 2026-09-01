package cacheclient

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWebBackend_PutAndGet(t *testing.T) {
	// Fake server that stores objects in memory.
	store := map[string][]byte{}
	headers := map[string]http.Header{} // capture all headers per path

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No batch endpoint on this fake (any method): the client falls back
		// to individual GETs, the path this test exercises.
		if r.URL.Path == "/testbucket/_batch/get" {
			w.WriteHeader(404)
			return
		}
		switch r.Method {
		case "PUT":
			body, _ := io.ReadAll(r.Body)
			store[r.URL.Path] = body
			headers[r.URL.Path] = r.Header.Clone()
			w.WriteHeader(200)
		case "HEAD":
			if _, ok := store[r.URL.Path]; !ok {
				w.WriteHeader(404)
				return
			}
			w.WriteHeader(200)
		case "GET":
			data, ok := store[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			h := headers[r.URL.Path]
			w.Header().Set("X-Cache-Meta-Outputid", h.Get("X-Cache-Meta-Outputid"))
			w.WriteHeader(200)
			w.Write(data)
		}
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket:    "testbucket",
		Endpoint:  srv.URL,
		AccessKey: "testkey",
		SecretKey: "testsecret",
		Version:   "v1.2.3",
	})
	require.NoError(t, err)
	// Force the synchronous single-PUT path (batch-unsupported fallback); asserts the X-Cache-Meta-* headers per PUT.
	b.batchPutUnsupported.Store(true)

	// Use a payload >= batchSizeThreshold so it's uploaded individually.
	payload := largePayload(1024)
	outputID := testOutputID(payload)

	// Put.
	err = b.Put("aabbccdd11223344", outputID, nopReader(payload), int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, uint32(1), b.Stats.Puts.Load())

	// Verify metadata headers were sent.
	h := headers["/testbucket/go-buildcache/v1aabbccdd11223344"]
	require.Equal(t, outputID, h.Get("X-Cache-Meta-Outputid"))
	require.Equal(t, "unknown", h.Get("X-Cache-Meta-Object-Type"))
	require.Equal(t, strconv.Itoa(len(payload)), h.Get("X-Cache-Meta-Body-Size"))
	require.Equal(t, "lz4", h.Get("X-Cache-Meta-Compression"))
	require.NotEmpty(t, h.Get("X-Cache-Meta-Created"))
	require.Equal(t, "v1.2.3", h.Get("X-Cache-Meta-Toolchain-Version"))
	// Plain text body has no go object header, so these should be absent.
	require.Empty(t, h.Get("X-Cache-Meta-Go-Version"))
	require.Empty(t, h.Get("X-Cache-Meta-Target"))

	// Get.
	gotOutputID, body, size, _, miss, _, err := b.Get("aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, outputID, gotOutputID)
	data, _ := io.ReadAll(body)
	require.Equal(t, payload, string(data))
	require.Equal(t, int64(len(payload)), size)
	require.Equal(t, uint32(1), b.Stats.Hits.Load())
}

func TestWebBackend_PutArchiveMetadata(t *testing.T) {
	// Verify that Go archive bodies get Go-Version and Target metadata.
	headers := map[string]http.Header{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			io.ReadAll(r.Body)
			headers[r.URL.Path] = r.Header.Clone()
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	b.batchPutUnsupported.Store(true) // assert single-PUT headers synchronously

	// Simulate a Go archive body with __.PKGDEF containing a go object header; padded past batchSizeThreshold.
	archiveBody := "!<arch>\n__.PKGDEF       0           0     0     644     100       `\ngo object linux amd64 go1.24.7 X:regabiwrappers\nsome export data here\n"
	archiveBody += largePayload(1024)
	err = b.Put("1111111122222222", "3333333344444444", nopReader(archiveBody), int64(len(archiveBody)))
	require.NoError(t, err)

	h := headers["/testbucket/go-buildcache/v11111111122222222"]
	require.Equal(t, "go-archive", h.Get("X-Cache-Meta-Object-Type"))
	require.Equal(t, "go1.24.7", h.Get("X-Cache-Meta-Go-Version"))
	require.Equal(t, "linux/amd64", h.Get("X-Cache-Meta-Target"))
}

func TestWebBackend_PutNoVersionWhenEmpty(t *testing.T) {
	headers := map[string]http.Header{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			io.ReadAll(r.Body)
			headers[r.URL.Path] = r.Header.Clone()
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
		// Version intentionally empty.
	})
	require.NoError(t, err)
	b.batchPutUnsupported.Store(true) // assert single-PUT headers synchronously

	payload := largePayload(1024)
	err = b.Put("aaaa000011112222", "bbbb333344445555", nopReader(payload), int64(len(payload)))
	require.NoError(t, err)

	h := headers["/testbucket/go-buildcache/v1aaaa000011112222"]
	require.Empty(t, h.Get("X-Cache-Meta-Toolchain-Version"))
}

func TestWebBackend_PutServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	// Force the synchronous single-PUT path so the HTTP error (wrapped in ErrLogged) returns from Put directly.
	b.batchPutUnsupported.Store(true)

	payload := largePayload(1024)
	err = b.Put("aabbccdd11223344", "eeff0011aabbccdd", nopReader(payload), int64(len(payload)))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrLogged), "PUT HTTP error must wrap ErrLogged so cache.go suppresses the duplicate log")
	require.Equal(t, uint32(0), b.Stats.Puts.Load())
}

func TestWebBackend_PutServerError_Coalesced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(502)
		w.Write([]byte("error code: 502"))
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	// Exercises per-object PUT error coalescing in the error logger via the synchronous single-PUT path.
	b.batchPutUnsupported.Store(true)

	// Swap the auto-init logger for a logger bound to a buffer with a long interval; flushing happens on Close.
	_ = b.errLog.Close()
	var buf bytes.Buffer
	b.errLog = newHTTPErrLogger(&buf, time.Hour)

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			actionID := strings.Repeat("0", 14) + strconv.FormatInt(int64(i), 16) + "0"
			outputID := "eeff0011aabbccdd"
			payload := largePayload(64)
			_ = b.Put(actionID, outputID, nopReader(payload), int64(len(payload)))
		}(i)
	}
	wg.Wait()
	require.NoError(t, b.Close())

	out := buf.String()
	require.Equal(t, 1, strings.Count(out, "\n"), "expected exactly one aggregated line, got: %q", out)
	require.Contains(t, out, "cacheprog: web put ")
	require.Contains(t, out, "HTTP 502: error code: 502")
}

func TestWebBackend_PutPreservesMethodOnRedirect(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{"301", http.StatusMovedPermanently},
		{"302", http.StatusFound},
		{"307", http.StatusTemporaryRedirect},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			var gotBody []byte
			var gotAuth string

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/testbucket/go-buildcache/v1aabbccdd11223344" {
					// The opening request: redirect to /final.
					http.Redirect(w, r, "/final", tt.status)
					return
				}
				if r.URL.Path == "/final" {
					gotMethod = r.Method
					gotBody, _ = io.ReadAll(r.Body)
					gotAuth = r.Header.Get("Authorization")
					w.WriteHeader(200)
					return
				}
				w.WriteHeader(404)
			}))
			defer srv.Close()

			b, err := NewWebBackend(WebConfig{
				Bucket: "testbucket", Endpoint: srv.URL,
				AccessKey: "testkey", SecretKey: "testsecret",
			})
			require.NoError(t, err)
			b.batchPutUnsupported.Store(true) // single PUT to the object path under test

			payload := largePayload(1024)
			err = b.Put("aabbccdd11223344", "eeff0011aabbccdd", nopReader(payload), int64(len(payload)))
			require.NoError(t, err)
			require.Equal(t, "PUT", gotMethod, "redirect should preserve PUT method")
			require.NotEmpty(t, gotBody, "redirect should preserve request body")
			require.NotEmpty(t, gotAuth, "redirect should preserve Authorization header")
			require.Equal(t, uint32(1), b.Stats.Puts.Load())
		})
	}
}

// TestWebBackend_PutExecutableRoundTrip pins the wire contract for the
// executable bit: PutExecutable is the ONLY thing that sets the metadata,
// Put never does, and Get reports back exactly what was stored — never a
// guess, never a default of true.
func TestWebBackend_PutExecutableRoundTrip(t *testing.T) {
	store := map[string][]byte{}
	headers := map[string]http.Header{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/get" {
			w.WriteHeader(404) // force the individual-GET fallback this test exercises
			return
		}
		switch r.Method {
		case "PUT":
			body, _ := io.ReadAll(r.Body)
			store[r.URL.Path] = body
			headers[r.URL.Path] = r.Header.Clone()
			w.WriteHeader(200)
		case "GET":
			data, ok := store[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			h := headers[r.URL.Path]
			for name, vals := range h {
				if strings.HasPrefix(strings.ToLower(name), "x-cache-meta-") {
					w.Header().Set(name, vals[0])
				}
			}
			w.WriteHeader(200)
			w.Write(data)
		}
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	b.batchPutUnsupported.Store(true) // synchronous single-PUT path

	exePayload := largePayload(1024)
	exeOutputID := testOutputID(exePayload)
	require.NoError(t, b.PutExecutable("aabbccdd11223344", exeOutputID, nopReader(exePayload), int64(len(exePayload))))

	plainPayload := largePayload(1024) + "x" // distinct body
	plainOutputID := testOutputID(plainPayload)
	require.NoError(t, b.Put("eeff00112233ffff", plainOutputID, nopReader(plainPayload), int64(len(plainPayload))))

	exeHeader := headers["/testbucket/go-buildcache/v1aabbccdd11223344"]
	require.Equal(t, "1", exeHeader.Get("X-Cache-Meta-Executable"), "PutExecutable must mark the object executable")
	plainHeader := headers["/testbucket/go-buildcache/v1eeff00112233ffff"]
	require.Empty(t, plainHeader.Get("X-Cache-Meta-Executable"), "an ordinary Put must never mark the object executable")

	gotOutputID, body, _, _, miss, executable, err := b.Get("aabbccdd11223344")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, exeOutputID, gotOutputID)
	require.True(t, executable, "a PutExecutable object must round-trip as executable")
	body.Close()

	gotOutputID, body, _, _, miss, executable, err = b.Get("eeff00112233ffff")
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, plainOutputID, gotOutputID)
	require.False(t, executable, "an ordinary Put object must never round-trip as executable")
	body.Close()
}
