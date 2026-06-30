package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func lz4Compress(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// incompressibleIndexBody builds a module-index payload (magic + random bytes)
// whose lz4-compressed first block exceeds the old fixed 512-byte peek -- i.e. a
// REALISTIC index, the shape that broke the original guard (and the shape of the
// real production poison blobs, which compress to 600 B - ~36 KB). The earlier
// test used 2 KiB of zeros, which lz4 shrank to ~61 bytes (under 512), masking the
// bug; random bytes do not compress, so the compressed body is several KB.
func incompressibleIndexBody(t *testing.T, randLen int) []byte {
	t.Helper()
	body := make([]byte, len(goModuleIndexMagic)+2+randLen) // magic + "2\n" + entropy
	copy(body, goModuleIndexMagic)
	copy(body[len(goModuleIndexMagic):], "2\n")
	_, err := rand.Read(body[len(goModuleIndexMagic)+2:])
	require.NoError(t, err)
	return body
}

func TestLooksLikeGoModuleIndex(t *testing.T) {
	index := []byte("go index v2\n" + string(make([]byte, 4096))) // realistic-ish size
	plain := []byte("!<arch>\n__.PKGDEF compiled package, not an index")

	// Uncompressed.
	require.True(t, looksLikeGoModuleIndex(index, ""))
	require.False(t, looksLikeGoModuleIndex(plain, ""))
	require.False(t, looksLikeGoModuleIndex([]byte("go index"), "")) // no version letter
	require.False(t, looksLikeGoModuleIndex(nil, ""))

	// lz4-compressed (the wire format). The detector is now handed the WHOLE
	// compressed body (its contract: it needs the full first block), the same as
	// the read path streams off the file and the PUT path peeks block-sized.
	cIndex := lz4Compress(t, index)
	cPlain := lz4Compress(t, plain)
	require.True(t, looksLikeGoModuleIndex(cIndex, "lz4"))
	require.False(t, looksLikeGoModuleIndex(cPlain, "lz4"))

	// A version-1 index (format-bump robustness).
	require.True(t, looksLikeGoModuleIndex(lz4Compress(t, []byte("go index v1\nlegacy")), "lz4"))

	// Regression: a REALISTIC, incompressible index whose compressed first block
	// is far larger than the old 512-byte peek. The original guard truncated at
	// 512 and returned false here (poison served); the full-block detector
	// returns true. This is the exact shape of the real production blobs.
	cReal := lz4Compress(t, incompressibleIndexBody(t, 8192))
	require.Greater(t, len(cReal), 512, "the compressed index must exceed the old 512-byte peek to exercise the bug")
	require.True(t, looksLikeGoModuleIndex(cReal, "lz4"),
		"a realistic >512-byte single-block index must still be detected")
}

// TestReadIsModuleIndex_FullBlock exercises the read-path detector against a
// realistic, incompressible index whose compressed body far exceeds the old
// 512-byte peek. readIsModuleIndex streams an lz4.Reader over the source and
// pulls exactly the first block, so the magic is recovered however large that
// block is -- the property the fixed-peek code lacked.
func TestReadIsModuleIndex_FullBlock(t *testing.T) {
	cIndex := lz4Compress(t, incompressibleIndexBody(t, 16384))
	require.Greater(t, len(cIndex), 512, "compressed index must exceed the old peek window")

	isIndex, err := readIsModuleIndex(bytes.NewReader(cIndex), "lz4")
	require.NoError(t, err)
	require.True(t, isIndex, "a realistic >512-byte single-block index must be detected on read")

	// A non-index lz4 body of similar size is not flagged.
	notIndex := make([]byte, 16384)
	_, err = rand.Read(notIndex)
	require.NoError(t, err)
	copy(notIndex, "!<arch>\n") // an ar archive header, not an index
	cNot := lz4Compress(t, notIndex)
	isIndex, err = readIsModuleIndex(bytes.NewReader(cNot), "lz4")
	require.NoError(t, err)
	require.False(t, isIndex, "a non-index body must not be flagged")

	// Uncompressed paths still work.
	isIndex, err = readIsModuleIndex(bytes.NewReader([]byte("go index v2\nraw")), "")
	require.NoError(t, err)
	require.True(t, isIndex)
	isIndex, err = readIsModuleIndex(bytes.NewReader([]byte("!<arch>\nraw")), "")
	require.NoError(t, err)
	require.False(t, isIndex)

	// A garbled "lz4" body that cannot be decompressed is treated as not-an-index
	// (fail-open), never an error that would fail the serve path.
	isIndex, err = readIsModuleIndex(bytes.NewReader([]byte("not a valid lz4 frame at all")), "lz4")
	require.NoError(t, err)
	require.False(t, isIndex)
}

// BenchmarkPutObjectPeek exercises the PUT-path module-index guard for a typical
// small (8 KiB) non-index object and reports -benchmem. It is the proof for the
// allocation fix: the guard's prefix read must self-size to the body (~tens of
// KiB), NOT pre-allocate the full indexPutPeekBytes (1 MiB) cap on every PUT.
//
// Before the fix (prefix := make([]byte, indexPutPeekBytes); io.ReadFull), this
// reported ~1.09 MB/op. After (io.ReadAll(io.LimitReader(body, cap))), it drops
// to roughly the body size. Driving handlePutObject directly (not over HTTP)
// keeps the measurement on the body-read + store path where the regression lived.
func BenchmarkPutObjectPeek(b *testing.B) {
	dir := b.TempDir()
	storage, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(b, err)
	b.Cleanup(func() { storage.Close() })

	// A realistic ~8 KiB compiled-object body (non-index), lz4-compressed as the
	// client sends it. Incompressible so the stored body is genuinely ~8 KiB.
	raw := make([]byte, 8192)
	_, err = rand.Read(raw)
	require.NoError(b, err)
	copy(raw, "!<arch>\n") // an ar archive header, not an index
	payload := lz4Compress(b, raw)

	hdr := http.Header{}
	hdr.Set("X-Cache-Meta-Outputid", "benchoutputid")
	hdr.Set("X-Cache-Meta-Compression", "lz4")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// write_once action=allow lets each iteration overwrite the same key, so
		// the storage layer stays bounded while the peek path runs every time.
		key := fmt.Sprintf("go-buildcache/v1%064x", i%4)
		req := httptest.NewRequest("PUT", "/testbucket/"+key, bytes.NewReader(payload))
		req.Header = hdr
		req.ContentLength = int64(len(payload))
		rec := httptest.NewRecorder()
		handlePutObject(rec, req, storage, key, defaultMaxObjectBytes)
		require.Equal(b, 200, rec.Code)

	}
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

// TestPutObject_RefusesRealisticModuleIndex is the regression guard at the PUT
// level: a REALISTIC, incompressible index whose single lz4 block is far larger
// than the old 512-byte peek (the exact shape of the real production poison)
// must be refused and stored nothing. The old fixed-512 peek truncated this
// body, never decoded the magic, and STORED the poison. The control normal
// object alongside it (also multi-KB so it can't be confused with the peek
// window) still stores and round-trips.
func TestPutObject_RefusesRealisticModuleIndex(t *testing.T) {
	ts, storage := testSetupWithStorage(t)

	idxKey := "go-buildcache/v1" + strings.Repeat("a", 64)
	index := lz4Compress(t, incompressibleIndexBody(t, 16384))
	require.Greater(t, len(index), 512, "the test index must exceed the old 512-byte peek to exercise the bug")
	hdr := map[string]string{
		"X-Cache-Meta-Outputid":    "deadbeef",
		"X-Cache-Meta-Compression": "lz4",
		"X-Cache-Meta-Object-Type": "unknown",
	}
	resp := doRequest(t, ts, "PUT", "/testbucket/"+idxKey, index, hdr)
	require.Equal(t, 200, resp.StatusCode, "a realistic index PUT is accepted on the wire")
	resp.Body.Close()

	// It must NOT have been stored -- the storage layer is the source of truth
	// (the GET path would also evict it, so check storage directly).
	_, err := storage.Stat(idxKey)
	require.ErrorIs(t, err, ErrNotFound, "a realistic module index must never be stored on PUT")

	// Control: a realistic incompressible NON-index object stores and serves.
	objKey := "go-buildcache/v1" + strings.Repeat("b", 64)
	objRaw := make([]byte, 16384)
	_, err = rand.Read(objRaw)
	require.NoError(t, err)
	copy(objRaw, "!<arch>\n") // a compiled-package archive, not an index
	objPayload := lz4Compress(t, objRaw)
	require.Greater(t, len(objPayload), 512)
	resp = doRequest(t, ts, "PUT", "/testbucket/"+objKey, objPayload,
		map[string]string{"X-Cache-Meta-Outputid": "abc", "X-Cache-Meta-Compression": "lz4"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+objKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode, "a realistic non-index object must be stored and served")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, objPayload, got, "a stored object must round-trip byte-for-byte")
}
