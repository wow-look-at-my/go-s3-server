package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func lz4Compress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestLooksLikeGoModuleIndex(t *testing.T) {
	index := []byte("go index v2\n" + string(make([]byte, 4096))) // realistic-ish size
	plain := []byte("!<arch>\n__.PKGDEF compiled package, not an index")

	// Uncompressed.
	require.True(t, looksLikeGoModuleIndex(index, ""))
	require.False(t, looksLikeGoModuleIndex(plain, ""))
	require.False(t, looksLikeGoModuleIndex([]byte("go index"), "")) // no version letter
	require.False(t, looksLikeGoModuleIndex(nil, ""))

	// lz4-compressed (the wire format), peeking only the leading bytes.
	cIndex := lz4Compress(t, index)
	cPlain := lz4Compress(t, plain)
	peek := func(b []byte) []byte {
		if len(b) > indexPeekBytes {
			return b[:indexPeekBytes]
		}
		return b
	}
	require.True(t, looksLikeGoModuleIndex(peek(cIndex), "lz4"))
	require.False(t, looksLikeGoModuleIndex(peek(cPlain), "lz4"))

	// A version-1 index (format-bump robustness).
	require.True(t, looksLikeGoModuleIndex(lz4Compress(t, []byte("go index v1\nlegacy")), "lz4"))
}

// TestPutObject_RefusesModuleIndex is the server half of the poison fix: a
// module-index upload is accepted on the wire (200) but never stored, so a
// later GET misses and the client recomputes the index locally. A normal
// object alongside it stores and serves as usual, proving the drop is specific.
func TestPutObject_RefusesModuleIndex(t *testing.T) {
	ts := testSetup(t)

	idxKey := "/testbucket/go-buildcache/v1aaaaaaaaaaaaaaaa"
	index := []byte("go index v2\n" + string(bytes.Repeat([]byte("x"), 2048)))
	hdr := map[string]string{
		"X-Amz-Meta-Outputid":    "deadbeef",
		"X-Amz-Meta-Compression": "lz4",
		"X-Amz-Meta-Object-Type": "unknown", // what old clients tag an index as
	}
	resp := doRequest(t, ts, "PUT", idxKey, lz4Compress(t, index), hdr)
	require.Equal(t, 200, resp.StatusCode, "an index PUT is accepted on the wire")
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", idxKey, nil, nil)
	require.Equal(t, 404, resp.StatusCode, "the index must not have been stored")
	resp.Body.Close()

	// Control: a normal (non-index) object is stored and served unchanged.
	objKey := "/testbucket/go-buildcache/v1bbbbbbbbbbbbbbbb"
	payload := lz4Compress(t, []byte("!<arch>\nnormal compiled object body"))
	resp = doRequest(t, ts, "PUT", objKey, payload, map[string]string{"X-Amz-Meta-Compression": "lz4"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", objKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode, "a normal object must still be stored")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, payload, got, "a stored object must round-trip byte-for-byte")
}
