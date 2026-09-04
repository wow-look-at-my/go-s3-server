package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestHeadObject: HEAD serves the exact header surface of a GET (both
// metadata prefixes, Last-Modified, Content-Length) with no body, no guard
// probes, and no access-record side effects — the cheap "inspect a key"
// endpoint.
func TestHeadObject(t *testing.T) {
	ts, storage := testSetupWithStorage(t)
	storage.EnableAccessTracking()

	key := "go-buildcache/v1" + strings.Repeat("5", 64)
	body := []byte("head-test-body")
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, body, map[string]string{
		"X-Cache-Meta-Outputid":    "headoid",
		"X-Cache-Meta-Object-Type": "go-archive",
	})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "HEAD", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Empty(t, got, "HEAD must not carry a body")
	require.Equal(t, "headoid", resp.Header.Get("X-Cache-Meta-Outputid"))
	require.Equal(t, "headoid", resp.Header.Get("X-Amz-Meta-Outputid"), "legacy prefix emitted too")
	require.Equal(t, "go-archive", resp.Header.Get("X-Cache-Meta-Object-Type"))
	require.Equal(t, int64(len(body)), resp.ContentLength)
	require.NotEmpty(t, resp.Header.Get("Last-Modified"))

	// Inspection must not extend the object's LRU lifetime.
	_, tracked := storage.lastAccess(key)
	require.False(t, tracked, "HEAD must not record last-access")

	// Absent key: a clean 404.
	resp = doRequest(t, ts, "HEAD", "/testbucket/go-buildcache/v1"+strings.Repeat("6", 64), nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()
}

// TestStatusRecorderReadFrom: the metrics wrapper forwards io.ReaderFrom (the
// sendfile fast path for GET body copies) instead of hiding it, and still
// counts bytes; a wrapped writer WITHOUT ReaderFrom falls back to plain
// copying without double-counting.
func TestStatusRecorderReadFrom(t *testing.T) {
	payload := strings.Repeat("z", 4096)
	// Strip WriterTo from the source: io.Copy prefers src.WriteTo over
	// dst.ReadFrom, and strings.Reader has one — the real GET source
	// (*os.File) does not, so this matches production dispatch.
	source := func() io.Reader { return struct{ io.Reader }{strings.NewReader(payload)} }

	// Fallback: httptest.ResponseRecorder has no ReadFrom.
	plain := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: plain, statusCode: 200}
	n, err := io.Copy(rec, source())
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.Equal(t, int64(len(payload)), rec.bytesWritten, "fallback path must count bytes exactly once")
	require.Equal(t, payload, plain.Body.String())

	// Passthrough: a ReaderFrom-capable writer receives the call directly.
	rf := &countingReaderFrom{}
	rec2 := &statusRecorder{ResponseWriter: rf, statusCode: 200}
	n, err = io.Copy(rec2, source())
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.True(t, rf.readFromCalled, "the wrapped writer's ReadFrom must be used")
	require.Equal(t, int64(len(payload)), rec2.bytesWritten, "passthrough path must count bytes")
}

// countingReaderFrom is a minimal ResponseWriter with a ReadFrom fast path.
type countingReaderFrom struct {
	buf            bytes.Buffer
	readFromCalled bool
}

func (c *countingReaderFrom) Header() http.Header         { return http.Header{} }
func (c *countingReaderFrom) WriteHeader(int)             {}
func (c *countingReaderFrom) Write(b []byte) (int, error) { return c.buf.Write(b) }
func (c *countingReaderFrom) ReadFrom(src io.Reader) (int64, error) {
	c.readFromCalled = true
	return io.Copy(&c.buf, src)
}

// TestMetadataOverflowDropsOptionalKey: an optional metadata value too large
// for the xattr budget (>64 KiB triggers the VFS E2BIG cap portably) is
// dropped — counted and logged — while the object, its body, and its
// protected keys store and serve normally. A protected key hitting the same
// limit still fails the PUT.
func TestMetadataOverflowDropsOptionalKey(t *testing.T) {
	if !forkMetrics(t) {
		return
	}

	_, storage := testSetupWithStorage(t)

	huge := strings.Repeat("s", 70_000) // > XATTR_SIZE_MAX (64 KiB) => E2BIG
	key := "go-buildcache/v1" + strings.Repeat("7", 64)

	droppedBefore := testutil.ToFloat64(metadataXattrsDroppedTotal)
	err := storage.Put(key, []byte("body"), map[string]string{
		"outputid": "oid123",
		"src":      huge,
	}, nil)
	require.NoError(t, err, "an oversized OPTIONAL key must not fail the PUT")
	require.Equal(t, droppedBefore+1, testutil.ToFloat64(metadataXattrsDroppedTotal))

	meta, err := storage.Stat(key)
	require.NoError(t, err)
	require.Equal(t, "oid123", meta.Metadata["outputid"], "protected keys must persist")
	_, hasSrc := meta.Metadata["src"]
	require.False(t, hasSrc, "the oversized optional key must be dropped")

	// A protected key over the limit still fails the PUT (better no object
	// than an unusable one).
	err = storage.Put("go-buildcache/v1"+strings.Repeat("a", 63)+"b", []byte("body"),
		map[string]string{"outputid": huge}, nil)
	require.Error(t, err, "an oversized PROTECTED key must fail the PUT")
}
