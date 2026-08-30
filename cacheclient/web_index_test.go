package cacheclient

import (
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

func TestParseIndexBlob_RoundTrip(t *testing.T) {
	keys := set.New[string]()
	for i := 0; i < 5; i++ {
		var h [gbciHashSize]byte
		h[0] = byte(i)
		keys.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	}

	blob := marshalIndex(keys)
	got, etag, err := parseIndexBlob(blob)
	require.NoError(t, err)
	require.Equal(t, keys.Len(), got.Len())
	require.True(t, strings.HasPrefix(etag, `"`) && strings.HasSuffix(etag, `"`))
	require.Equal(t, 1+sha256HexLen+1, len(etag))
	for k := range keys.All() {
		require.True(t, got.Contains(k))
	}
}

const sha256HexLen = 64

func TestParseIndexBlob_Empty(t *testing.T) {
	blob := marshalIndex(set.New[string]())
	got, etag, err := parseIndexBlob(blob)
	require.NoError(t, err)
	require.Equal(t, 0, got.Len())
	require.NotEqual(t, "", etag)
}

func TestParseIndexBlob_BadMagic(t *testing.T) {
	blob := marshalIndex(set.New[string]())
	blob[0] = 'X'
	_, _, err := parseIndexBlob(blob)
	require.Error(t, err)
}

func TestParseIndexBlob_TrailerMismatch(t *testing.T) {
	keys := set.New[string]()
	var h [gbciHashSize]byte
	h[0] = 1
	keys.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	blob := marshalIndex(keys)
	// Flip a byte in the body so the trailer no longer matches.
	blob[gbciHeaderSize] ^= 0xff
	_, _, err := parseIndexBlob(blob)
	require.Error(t, err)
}

func TestParseIndexBlob_TooSmall(t *testing.T) {
	_, _, err := parseIndexBlob([]byte("nope"))
	require.Error(t, err)
}

func TestParseIndexBlob_BadVersion(t *testing.T) {
	blob := marshalIndex(set.New[string]())
	blob[4] = 99
	_, _, err := parseIndexBlob(blob)
	require.Error(t, err)
}

func TestParseIndexBlob_BadHashSize(t *testing.T) {
	blob := marshalIndex(set.New[string]())
	blob[5] = 16
	_, _, err := parseIndexBlob(blob)
	require.Error(t, err)
}

func TestParseIndexBlob_LengthMismatch(t *testing.T) {
	keys := set.New[string]()
	var h [gbciHashSize]byte
	h[0] = 1
	keys.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	blob := marshalIndex(keys)
	// Lie about the count.
	binary.LittleEndian.PutUint64(blob[16:24], 99)
	_, _, err := parseIndexBlob(blob)
	require.Error(t, err)
}

func TestDecodeActionHash_Bad(t *testing.T) {
	cases := []string{
		"",
		"wrong-prefix/abcd",
		gbciKeyPrefix + "tooShort",
		gbciKeyPrefix + strings.Repeat("ZZ", gbciHashSize), // not hex
	}
	for _, c := range cases {
		_, ok := decodeActionHash(c)
		require.False(t, ok, "expected reject: %q", c)
	}
}

// indexFixture serves /<bucket>/_index from an in-memory blob with proper
// ETag revalidation semantics; PUT/GET on object keys are no-ops or misses.
type indexFixture struct {
	t       *testing.T
	bucket  string
	blob    atomic.Pointer[[]byte]
	hits200 atomic.Int32
	hits304 atomic.Int32
	hitsAny atomic.Int32
	srv     *httptest.Server
}

func newIndexFixture(t *testing.T, bucket string, initial set.Set[string]) *indexFixture {
	f := &indexFixture{t: t, bucket: bucket}
	b := marshalIndex(initial)
	f.blob.Store(&b)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hitsAny.Add(1)
		if r.Method == http.MethodGet && r.URL.Path == "/"+bucket+"/_index" {
			cur := *f.blob.Load()
			etag := indexETag(cur)
			if r.Header.Get("If-None-Match") == etag {
				f.hits304.Add(1)
				w.Header().Set("ETag", etag)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			f.hits200.Add(1)
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.Write(cur)
			return
		}
		// Default: not-found for everything else (object Get/Put paths not exercised here).
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func indexETag(blob []byte) string {
	bodyEnd := len(blob) - 32
	return `"` + hex.EncodeToString(blob[bodyEnd:]) + `"`
}

func TestLoadOrFetchIndex_ColdStart(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	want := set.New[string]()
	for i := 0; i < 7; i++ {
		var h [gbciHashSize]byte
		h[0] = byte(i + 100)
		want.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	}

	f := newIndexFixture(t, "bk", want)

	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: f.srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)

	require.Equal(t, want.Len(), b.keys.Len())
	for k := range want.All() {
		require.True(t, b.keys.Contains(k), "missing %q", k)
	}
	require.Equal(t, int32(1), f.hits200.Load(), "expected exactly one 200 fetch")
	require.Equal(t, int32(0), f.hits304.Load())
}

func TestLoadOrFetchIndex_WarmCache304(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	want := set.New[string]()
	var h [gbciHashSize]byte
	h[0] = 0xab
	want.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))

	f := newIndexFixture(t, "bk", want)

	// Cold start: writes the disk blob.
	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: f.srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	require.Equal(t, 1, b.keys.Len())
	require.Equal(t, int32(1), f.hits200.Load())

	// Confirm the disk blob was persisted.
	files, _ := filepath.Glob(filepath.Join(tmp, "gocache-web-index-*.bin"))
	require.NotEmpty(t, files)

	// A fresh backend instance: should send If-None-Match and get a not-modified answer.
	b2, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: f.srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	require.Equal(t, 1, b2.keys.Len())
	require.True(t, b2.keys.Contains(gbciKeyPrefix+hex.EncodeToString(h[:])))
	require.Equal(t, int32(1), f.hits304.Load(), "expected one 304 on warm restart")
	require.Equal(t, int32(1), f.hits200.Load(), "200 count should not have grown")
}

// TestLoadOrFetchIndex_SlowServerBounded pins the startup budget: an index
// endpoint that never answers must not hold NewWebBackend (and the daemon
// start) hostage — the fetch is abandoned within indexHeaderBudget and the
// backend proceeds with a non-authoritative (probing-enabled) key set.
func TestLoadOrFetchIndex_SlowServerBounded(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until released (slower than any sane budget), but also honor
		// the client abandoning the request so the handler can exit.
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	// LIFO defers: release the handler BEFORE srv.Close waits on it.
	defer srv.Close()
	defer close(release)

	defer shrinkIndexBudgets(150*time.Millisecond, 150*time.Millisecond, 5*time.Second)()

	start := time.Now()
	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer b.Close()

	require.Less(t, elapsed, 2*time.Second,
		"the index fetch must be abandoned within its budget, not the 30s client timeout")
	require.Equal(t, 0, b.keys.Len())
	require.False(t, b.indexAuthoritative,
		"an abandoned index fetch must leave the key set non-authoritative so batch probing stays enabled")
}

func TestLoadOrFetchIndex_ServerError(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	require.Equal(t, 0, b.keys.Len(), "fetch failure should yield empty index")
}

func TestLoadOrFetchIndex_GarbageBody(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_index") {
			w.Header().Set("ETag", `"deadbeef"`)
			w.WriteHeader(200)
			w.Write([]byte("not a GBCI blob"))
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	require.Equal(t, 0, b.keys.Len(), "garbage body should yield empty index")
}

func TestLoadOrFetchIndex_DiskBlobBeatsServerError(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	want := set.New[string]()
	var h [gbciHashSize]byte
	h[0] = 0x77
	want.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))

	var broken atomic.Bool
	blob := marshalIndex(want)
	etag := indexETag(blob)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() {
			w.WriteHeader(500)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/_index") {
			w.Header().Set("ETag", etag)
			w.WriteHeader(200)
			w.Write(blob)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	// Cold start: server is healthy, disk cache gets populated.
	_, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)

	// Break the server.
	broken.Store(true)

	// Same endpoint URL → same disk path. Server now 500s, but the disk
	// blob from the earlier run must still populate b.keys.
	b2, err := NewWebBackend(WebConfig{
		Bucket: "bk", Endpoint: srv.URL,
		AccessKey: "k", SecretKey: "s",
	})
	require.NoError(t, err)
	require.Equal(t, 1, b2.keys.Len(), "fallback should populate from disk blob")
}

// TestIndexCachePathSuffix locks in the rename from .txt to .bin so a stray
// upgrade doesn't clobber the on-disk format silently.
func TestIndexCachePathSuffix(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	b := &WebBackend{endpoint: "https://example.com", bucket: "b", prefix: "go-buildcache/"}
	require.True(t, strings.HasSuffix(b.indexCachePath(), ".bin"))
}

// Sanity test for the helper: read a blob written via writeIndexBlob.
func TestWriteAndReadIndexBlob(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	b := &WebBackend{}
	keys := set.New[string]()
	var h [gbciHashSize]byte
	h[0] = 0x42
	keys.Add(gbciKeyPrefix + hex.EncodeToString(h[:]))
	blob := marshalIndex(keys)
	b.writeIndexBlob(path, blob)

	got, gotKeys, etag := b.readDiskIndex(path)
	require.Equal(t, blob, got)
	require.Equal(t, 1, gotKeys.Len())
	require.NotEqual(t, "", etag)
}
