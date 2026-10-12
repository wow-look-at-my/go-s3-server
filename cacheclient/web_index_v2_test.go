package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// marshalIndexV1 encodes whole action hashes as a version 1 blob, the format
// an older server sends and an older client left on disk.
func marshalIndexV1(hashes []actionHash) []byte {
	sorted := slices.SortedFunc(slices.Values(hashes), compareHash)
	blob := make([]byte, gbciHeaderSize+len(sorted)*gbciHashSize+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersionFull
	blob[5] = gbciHashSize
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(sorted)))
	off := gbciHeaderSize
	for i := range sorted {
		copy(blob[off:off+gbciHashSize], sorted[i][:])
		off += gbciHashSize
	}
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])
	return blob
}

// fullHash is an action hash whose leading byte is lead and whose last byte
// is tail.
func fullHash(lead, tail byte) actionHash {
	var h actionHash
	h[0], h[hashSize-1] = lead, tail
	return h
}

// Membership is by indexKey: a hash that shares one with a member is a
// member, and a hash that does not is absent.
func TestKeySetMatchesByIndexKey(t *testing.T) {
	s := newKeySet(0)
	member := fullHash(1, 1)
	s.Add(member)
	assert.True(t, s.Contains(member))
	assert.True(t, s.Contains(fullHash(1, 2)), "the same leading bytes are the same key")
	assert.False(t, s.Contains(fullHash(2, 1)))
	assert.Equal(t, 1, s.Len())

	s.Add(fullHash(1, 3))
	assert.Equal(t, 1, s.Len(), "a second hash with the same indexKey adds nothing")

	s.Remove(fullHash(1, 4))
	assert.False(t, s.Contains(member), "a confirmed absence drops the indexKey")
	assert.Equal(t, 0, s.Len())
}

func TestMarshalIndexWritesVersion2(t *testing.T) {
	s := newKeySet(0)
	for lead := range byte(5) {
		s.Add(fullHash(lead, 7))
	}
	blob := marshalIndex(s)
	assert.Equal(t, byte(gbciVersionPrefix), blob[4])
	assert.Equal(t, byte(indexKeySize), blob[5])
	assert.Len(t, blob, gbciHeaderSize+5*indexKeySize+sha256.Size)
}

func TestParseIndexBlobReadsVersion1(t *testing.T) {
	hashes := []actionHash{fullHash(3, 1), fullHash(1, 2), fullHash(2, 3)}
	keys, etag, err := parseIndexBlob(marshalIndexV1(hashes))
	require.NoError(t, err)
	require.NotEmpty(t, etag)
	require.Equal(t, len(hashes), keys.Len())
	for _, h := range hashes {
		assert.True(t, keys.Contains(h))
	}
	assert.False(t, keys.Contains(fullHash(4, 1)))
}

func TestParseIndexBlobFoldsVersion1EntriesThatShareAKey(t *testing.T) {
	keys, _, err := parseIndexBlob(marshalIndexV1([]actionHash{fullHash(1, 1), fullHash(1, 2), fullHash(2, 1)}))
	require.NoError(t, err)
	assert.Equal(t, 2, keys.Len())
}

func TestParseIndexBlobRefusesASizeTheVersionDoesNotCarry(t *testing.T) {
	v1 := marshalIndexV1(nil)
	v1[5] = indexKeySize
	_, _, err := parseIndexBlob(v1)
	require.Error(t, err)

	v2 := marshalIndex(newKeySet(0))
	v2[5] = gbciHashSize
	_, _, err = parseIndexBlob(v2)
	require.Error(t, err)
}

func TestReadDiskIndexReadsAVersion1Copy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "copy.bin")
	hashes := []actionHash{fullHash(9, 1), fullHash(8, 2)}
	blob := marshalIndexV1(hashes)
	require.NoError(t, os.WriteFile(path, blob, 0644))

	got := (&WebBackend{}).readDiskIndex(path)
	require.Equal(t, blob, got.blob)
	require.Equal(t, 2, got.count)
	for _, h := range hashes {
		assert.True(t, got.keys.Contains(h))
	}
}

// versionedIndexServer answers /bk/_index with v2 when the request asks for
// it and v2 is set, and with v1 otherwise, recording every query it saw.
type versionedIndexServer struct {
	srv     *httptest.Server
	mu      sync.Mutex
	queries []string
}

func newVersionedIndexServer(t *testing.T, v1, v2 []byte) *versionedIndexServer {
	s := &versionedIndexServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.mu.Lock()
		s.queries = append(s.queries, r.URL.RawQuery)
		s.mu.Unlock()
		blob := v1
		if v2 != nil && r.URL.Query().Get("v") == "2" {
			blob = v2
		}
		etag := indexETag(blob)
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(blob)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *versionedIndexServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.queries)
}

// A server that predates version 2 ignores the request for it and answers
// version 1, which the client parses and keeps as its disk copy.
func TestIndexFetchAsksForVersion2AndTakesVersion1(t *testing.T) {
	dir := t.TempDir()
	hashes := []actionHash{fullHash(1, 1), fullHash(2, 2), fullHash(3, 3)}
	old := newVersionedIndexServer(t, marshalIndexV1(hashes), nil)

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: old.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	b.awaitIndex()

	require.Equal(t, []string{indexVersionQuery}, old.seen())
	require.True(t, b.indexAuthoritative)
	require.Equal(t, len(hashes), b.keys.Len())
	for _, h := range hashes {
		assert.True(t, b.keys.Contains(h))
	}
	onDisk, err := os.ReadFile(b.indexCachePath())
	require.NoError(t, err)
	assert.Equal(t, byte(gbciVersionFull), onDisk[4], "the copy is the answer as it came")
}

// A version 1 copy left by an older client revalidates against a version 2
// server as a changed index, and the copy becomes version 2.
func TestVersion1CopyIsReplacedByVersion2(t *testing.T) {
	dir := t.TempDir()
	hashes := []actionHash{fullHash(1, 1), fullHash(2, 2)}
	keys := newKeySet(0)
	for _, h := range hashes {
		keys.Add(h)
	}
	v2 := marshalIndex(keys)
	srv := newVersionedIndexServer(t, marshalIndexV1(hashes), v2)

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: -1}
	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	require.NoError(t, os.WriteFile(b.indexCachePath(), marshalIndexV1(hashes), 0644))
	b.awaitIndex()

	require.True(t, b.indexAuthoritative)
	require.Equal(t, len(hashes), b.keys.Len())
	onDisk, err := os.ReadFile(b.indexCachePath())
	require.NoError(t, err)
	assert.Equal(t, v2, onDisk)
}
