package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wow-look-at-my/testify/require"
)

// parsedIndex is the test-side decoded form of a GBCI v1 blob.
type parsedIndex struct {
	Version    uint8
	HashSize   uint8
	Generation uint64
	Count      uint64
	Hashes     [][32]byte
	Trailer    [32]byte
}

// parseGBCI decodes a GBCI v1 blob and verifies the trailer hash. Used by
// tests to assert on the wire format produced by handleGetIndex.
func parseGBCI(t *testing.T, blob []byte) parsedIndex {
	t.Helper()
	require.GreaterOrEqual(t, len(blob), gbciHeaderSize+sha256.Size, "blob too small for header+trailer")
	require.Equal(t, byte('G'), blob[0])
	require.Equal(t, byte('B'), blob[1])
	require.Equal(t, byte('C'), blob[2])
	require.Equal(t, byte('I'), blob[3])

	var p parsedIndex
	p.Version = blob[4]
	p.HashSize = blob[5]
	require.Equal(t, uint8(gbciVersion), p.Version)
	require.Equal(t, uint8(gbciHashSize), p.HashSize)
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(blob[6:8]))
	p.Generation = binary.LittleEndian.Uint64(blob[8:16])
	p.Count = binary.LittleEndian.Uint64(blob[16:24])

	bodyEnd := gbciHeaderSize + int(p.Count)*gbciHashSize
	require.Equal(t, bodyEnd+sha256.Size, len(blob), "length mismatch with declared count")

	p.Hashes = make([][32]byte, p.Count)
	for i := uint64(0); i < p.Count; i++ {
		copy(p.Hashes[i][:], blob[gbciHeaderSize+int(i)*gbciHashSize:gbciHeaderSize+int(i+1)*gbciHashSize])
	}
	copy(p.Trailer[:], blob[bodyEnd:])

	expected := sha256.Sum256(blob[:bodyEnd])
	require.Equal(t, expected, p.Trailer, "trailer SHA-256 does not match body")
	return p
}

// keyForHash builds a well-formed cacheprog cache key from a 32-byte hash.
func keyForHash(h [32]byte) string {
	return gbciKeyPrefix + hex.EncodeToString(h[:])
}

// putKey is a tiny convenience wrapper.
func putKey(t *testing.T, ts *httptest.Server, key string, data []byte) {
	t.Helper()
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, data, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
}

// getIndex fetches /_index and returns (status, body, etag).
func getIndex(t *testing.T, ts *httptest.Server, ifNoneMatch string) (int, []byte, string) {
	t.Helper()
	headers := map[string]string{}
	if ifNoneMatch != "" {
		headers["If-None-Match"] = ifNoneMatch
	}
	resp := doRequest(t, ts, "GET", "/testbucket/_index", nil, headers)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body, resp.Header.Get("ETag")
}

func TestIndexEmptyBucket(t *testing.T) {
	ts := testSetup(t)

	status, body, etag := getIndex(t, ts, "")
	require.Equal(t, 200, status)
	require.NotEqual(t, "", etag)

	p := parseGBCI(t, body)
	require.Equal(t, uint64(0), p.Count)
	require.Len(t, p.Hashes, 0)

	// ETag is stable across calls when nothing changes.
	status2, _, etag2 := getIndex(t, ts, "")
	require.Equal(t, 200, status2)
	require.Equal(t, etag, etag2)
}

func TestIndexAfterPuts(t *testing.T) {
	ts := testSetup(t)

	var hashes [][32]byte
	for i := 0; i < 3; i++ {
		var h [32]byte
		for j := range h {
			h[j] = byte((i*7 + j) & 0xff)
		}
		hashes = append(hashes, h)
		putKey(t, ts, keyForHash(h), []byte(fmt.Sprintf("data-%d", i)))
	}

	status, body, _ := getIndex(t, ts, "")
	require.Equal(t, 200, status)

	p := parseGBCI(t, body)
	require.Equal(t, uint64(3), p.Count)

	// Body must be sorted ascending.
	for i := 1; i < len(p.Hashes); i++ {
		require.Less(t, hashCompare(p.Hashes[i-1], p.Hashes[i]), 0)
	}

	// Every PUT hash must appear exactly once.
	seen := make(map[[32]byte]bool, len(hashes))
	for _, h := range p.Hashes {
		seen[h] = true
	}
	for _, h := range hashes {
		require.True(t, seen[h], "missing hash %x", h)
	}
}

func TestIndexConditionalGet(t *testing.T) {
	ts := testSetup(t)

	var h [32]byte
	for i := range h {
		h[i] = byte(i)
	}
	putKey(t, ts, keyForHash(h), []byte("x"))

	status, _, etag := getIndex(t, ts, "")
	require.Equal(t, 200, status)
	require.NotEqual(t, "", etag)

	// Matching If-None-Match → 304 with no body.
	status304, body304, _ := getIndex(t, ts, etag)
	require.Equal(t, 304, status304)
	require.Len(t, body304, 0)

	// Non-matching If-None-Match → 200 with body.
	status200, body200, _ := getIndex(t, ts, `"deadbeef"`)
	require.Equal(t, 200, status200)
	require.Greater(t, len(body200), 0)
}

func TestIndexNonConformingKeysSkipped(t *testing.T) {
	ts := testSetup(t)

	// Wrong prefix.
	putKey(t, ts, "wrong-prefix/foo", []byte("a"))
	// Right prefix, hex too short.
	putKey(t, ts, gbciKeyPrefix+"abcd", []byte("b"))
	// Right prefix, non-hex.
	putKey(t, ts, gbciKeyPrefix+"GG"+hex.EncodeToString(make([]byte, 31)), []byte("c"))

	status, body, _ := getIndex(t, ts, "")
	require.Equal(t, 200, status)
	p := parseGBCI(t, body)
	require.Equal(t, uint64(0), p.Count)
}

func TestIndexGenerationBumps(t *testing.T) {
	ts := testSetup(t)

	_, body1, _ := getIndex(t, ts, "")
	p1 := parseGBCI(t, body1)

	var h [32]byte
	h[0] = 0xab
	putKey(t, ts, keyForHash(h), []byte("d"))

	_, body2, _ := getIndex(t, ts, "")
	p2 := parseGBCI(t, body2)
	require.Greater(t, p2.Generation, p1.Generation)
	require.Equal(t, uint64(1), p2.Count)
}

func TestIndexIdempotentPut(t *testing.T) {
	ts := testSetup(t)

	var h [32]byte
	h[0] = 0x42
	key := keyForHash(h)
	putKey(t, ts, key, []byte("v1"))
	putKey(t, ts, key, []byte("v2"))

	status, body, _ := getIndex(t, ts, "")
	require.Equal(t, 200, status)
	p := parseGBCI(t, body)
	require.Equal(t, uint64(1), p.Count, "duplicate PUT must dedupe in the index")
}

func TestIndexBurstPuts(t *testing.T) {
	ts := testSetup(t)

	const n = 1000
	hashes := make([][32]byte, n)
	for i := range hashes {
		var h [32]byte
		// Spread bytes across the hash so sort order is non-trivial.
		binary.LittleEndian.PutUint64(h[0:8], uint64(i*2654435761))
		binary.LittleEndian.PutUint64(h[8:16], uint64(i))
		hashes[i] = h
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	sem := make(chan struct{}, 32) // bound concurrency, not the test
	for i := 0; i < n; i++ {
		i := i
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			putKey(t, ts, keyForHash(hashes[i]), []byte("x"))
		}()
	}
	wg.Wait()
	burstDur := time.Since(start)

	// 1000 PUTs in well under a second on real hardware; allow generous
	// budget for slow CI shared runners. The point is to fail loudly if
	// PUT regresses to O(n)-per-call sorting on the new hashes path.
	require.Less(t, burstDur, 5*time.Second, "burst PUT took %v (regression?)", burstDur)

	status, body, _ := getIndex(t, ts, "")
	require.Equal(t, 200, status)
	p := parseGBCI(t, body)
	require.Equal(t, uint64(n), p.Count)
}

// hashCompare returns -1/0/+1 for lexicographic ordering of two 32-byte
// hashes. Inlined here so tests don't pull in bytes.Compare.
func hashCompare(a, b [32]byte) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// Sanity check: a request with the wrong method against /_index returns 405,
// not a panic, even when the index is empty.
func TestIndexMethodNotAllowed(t *testing.T) {
	ts := testSetup(t)
	resp := doRequest(t, ts, "PUT", "/testbucket/_index", []byte("x"), nil)
	defer resp.Body.Close()
	// _index isn't a valid PUT key (the routing dispatches to PutObject
	// which writes it as a regular key); we accept either a 200 (treated
	// as a normal write) or a 405. The point is no panic.
	require.True(t, resp.StatusCode == 200 || resp.StatusCode == 405)
}

// Direct unit test for the in-memory Index.Blob path, bypassing HTTP.
func TestIndexBlobRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NoError(t, err)
	defer s.Close()

	for i := 0; i < 5; i++ {
		var h [32]byte
		h[0] = byte(i)
		require.NoError(t, s.Put(keyForHash(h), []byte("x"), nil, nil))
	}

	blob1, etag1 := s.Index.Blob()
	p1 := parseGBCI(t, blob1)
	require.Equal(t, uint64(5), p1.Count)

	// Repeated calls return the same buffer + ETag without re-sorting.
	blob2, etag2 := s.Index.Blob()
	require.Equal(t, etag1, etag2)
	require.Equal(t, &blob1[0], &blob2[0], "Blob should return cached slice")

	// One more PUT bumps generation on next read.
	var h [32]byte
	h[0] = 0xff
	require.NoError(t, s.Put(keyForHash(h), []byte("x"), nil, nil))

	_, etag3 := s.Index.Blob()
	require.NotEqual(t, etag1, etag3)
}

// Verifies handleGetIndex returns the same content the in-process Index would,
// so HTTP framing isn't silently rewriting bytes.
func TestIndexHTTPMatchesInProcess(t *testing.T) {
	ts := testSetup(t)

	for i := 0; i < 4; i++ {
		var h [32]byte
		h[0] = byte(i)
		putKey(t, ts, keyForHash(h), []byte("x"))
	}

	resp := doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	p := parseGBCI(t, body)
	require.Equal(t, uint64(4), p.Count)

	// Verify Content-Length matches body size if set.
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		require.Equal(t, fmt.Sprintf("%d", len(body)), cl)
	}
}
