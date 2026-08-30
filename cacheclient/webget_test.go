package cacheclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebBackend_GetMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)

	_, _, _, _, miss, err := b.Get("deadbeef00000000")
	require.NoError(t, err)
	require.True(t, miss)
}

func TestWebBackend_GetMissingMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve the body but no outputid metadata.
		w.WriteHeader(200)
		w.Write([]byte("data"))
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)

	_, _, _, _, miss, _ := b.Get("deadbeef00000000")
	require.True(t, miss)
}

// primeIndex test-only: forces a key into the index so Gets take getIndividual instead of batch GET.
func primeIndex(b *WebBackend, actionID string) {
	b.keysMu.Lock()
	b.keys.Add(b.key(actionID))
	b.keysMu.Unlock()
}

// TestWebBackend_GetIndividualMissPaths drives each error branch in
// getIndividual by routing Gets past the batch fallback. The branches
// are the same ones that emit miss-reason spans, so this also guards
// against span-tagging regressions.
func TestWebBackend_GetIndividualMissPaths(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"404", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }},
		{"http_error", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte("boom"))
		}},
		{"no_outputid", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			w.Write([]byte("data-without-outputid-header"))
		}},
		{"decompress", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Cache-Meta-Outputid", "aabbccdd")
			w.WriteHeader(200)
			// Not LZ4 — will fail decompress.
			w.Write([]byte("not lz4 framed data"))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			b, err := NewWebBackend(WebConfig{
				Bucket: "testbucket", Endpoint: srv.URL,
				AccessKey: "testkey", SecretKey: "testsecret",
			})
			require.NoError(t, err)
			primeIndex(b, "deadbeef00000000")

			_, _, _, _, miss, _ := b.Get("deadbeef00000000")
			require.True(t, miss, "expected miss for %s path", tc.name)
		})
	}
}

// TestWebBackend_GetRejectsCorruptBody is the regression test for the "corrupt
// index" failure. A remote object whose body does not hash to its advertised
// outputID is corrupt (truncated, badly decoded, or poisoned/rotted remotely) and
// must be refused — treated as a miss, never served — so the go command never
// consumes a damaged object. The key is also evicted from the in-memory index
// so a later recompute re-uploads (overwrites) it clean instead of skipping the
// Put as already-present.
func TestWebBackend_GetRejectsCorruptBody(t *testing.T) {
	const actionID = "aabbccdd11223344"
	// outputID advertises the CORRECT body's hash, but the server serves a corrupt body of the same length.
	good := largePayload(2048)
	corrupt := good[:len(good)-10] + "XXXXXXXXXX"
	outputID := testOutputID(good)
	require.NotEqual(t, outputID, testOutputID(corrupt))

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404) // index fetch etc.
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(corrupt))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	contains := func() bool {
		b.keysMu.RLock()
		defer b.keysMu.RUnlock()
		return b.keys.Contains(b.key(actionID))
	}
	require.True(t, contains(), "precondition: key is in the index")

	_, _, _, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.True(t, miss, "a corrupt body must be treated as a miss, never served")
	require.Equal(t, uint32(1), b.MissChecksum.Load())
	require.Equal(t, uint32(1), b.Stats.Corrupt.Load())
	require.Equal(t, uint32(0), b.Stats.Hits.Load())
	require.False(t, contains(), "corrupt key must be evicted from the index so a recompute re-uploads it clean")
}

// TestWebBackend_GetServesCorrectBody is the positive control for the integrity
// check: a body that hashes to its advertised outputID is served as a hit.
func TestWebBackend_GetServesCorrectBody(t *testing.T) {
	const actionID = "aabbccdd11223344"
	good := largePayload(2048)
	outputID := testOutputID(good)

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(good))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	gotOutputID, body, size, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, outputID, gotOutputID)
	require.Equal(t, uint32(0), b.MissChecksum.Load())
	require.Equal(t, uint32(1), b.Stats.Hits.Load())
	data, _ := io.ReadAll(body)
	require.Equal(t, good, string(data))
	require.Equal(t, int64(len(good)), size)
}

// TestWebBackend_GetLegacyAmzMetaFallback verifies the deprecated read path: a
// new client still reads the outputid from an older cache server that emits only
// the legacy S3-style X-Amz-Meta-Outputid header (no native X-Cache-Meta-*).
func TestWebBackend_GetLegacyAmzMetaFallback(t *testing.T) {
	const actionID = "aabbccdd11223344"
	good := largePayload(2048)
	outputID := testOutputID(good)

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404)
			return
		}
		// Only the deprecated header — simulates a not-yet-upgraded server.
		w.Header().Set("X-Amz-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(good))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	gotOutputID, body, _, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss, "client must fall back to X-Amz-Meta-Outputid when the native header is absent")
	require.Equal(t, outputID, gotOutputID)
	require.Equal(t, uint32(1), b.Stats.Hits.Load())
	data, _ := io.ReadAll(body)
	require.Equal(t, good, string(data))
}
