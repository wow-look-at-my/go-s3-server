package cacheclient

import (
	"github.com/wow-look-at-my/go-containers/set"

	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Progress-bounded via header/stall/ceiling budgets. Exhaustion falls back
// to a non-authoritative key set; batch-probing stays on.
var (
	indexHeaderBudget = 10 * time.Second
	indexStallTimeout = 10 * time.Second
	indexFetchCeiling = 60 * time.Second
)

const indexFetchRetries = 1

// gbciKeyPrefix leads every cacheprog key; the wire format carries only the raw hash after it.
const gbciKeyPrefix = "go-buildcache/v1"

// gbciHashSize is the number of bytes per entry in the index body.
const gbciHashSize = 32

// gbciHeaderSize is the fixed header size in bytes.
const gbciHeaderSize = 24

// gbciVersion is the wire-format version stored in the header.
const gbciVersion = 1

// gbciMagic is the file-format identifier "GBCI".
var gbciMagic = [4]byte{'G', 'B', 'C', 'I'}

// indexCachePath hashes (endpoint, bucket, prefix), so daemons on the same machine with different caches don't collide.
func (b *WebBackend) indexCachePath() string {
	h := sha256.Sum256([]byte(b.endpoint + "/" + b.bucket + "/" + b.prefix))
	name := "gocache-web-index-" + hex.EncodeToString(h[:8]) + ".bin"
	return filepath.Join(os.TempDir(), name)
}

// loadOrFetchIndex returns the set of known cache keys for this backend and
// whether that set is AUTHORITATIVE — i.e. server-confirmed fresh this run
// (a parsed blob, or a not-modified answer validating our disk copy).
//
// It reads any previously cached blob from disk, then issues a conditional
// GET /<bucket>/_index against the server. On not-modified we keep the disk
// blob; on a fresh body we adopt it and persist it. Any failure produces a
// NON-authoritative set (the stale disk copy, or empty): Get/Put still work,
// and because absences from a non-authoritative set prove nothing, cold keys
// are batch-probed instead of fast-missed (see WebBackend.Get).
func (b *WebBackend) loadOrFetchIndex() (set.Set[string], bool) {
	path := b.indexCachePath()
	diskBlob, diskKeys, diskETag := b.readDiskIndex(path)

	// The absolute ceiling covers the whole load; each fetch also enforces the header and stall budgets above.
	ctx, cancel := context.WithTimeout(context.Background(), indexFetchCeiling)
	defer cancel()

	blob, status, err := b.fetchIndexBlob(ctx, diskETag)
	if err != nil {
		logging.Warnf("cacheprog: web index fetch: %v", err)
		if diskBlob != nil {
			return diskKeys, false
		}
		return set.New[string](), false
	}
	if status == http.StatusNotModified {
		if diskBlob != nil {
			return diskKeys, true // server confirmed our disk copy is current
		}
		// No disk copy despite a not-modified answer (likely a cleared /tmp); refetch unconditionally.
		blob, _, err = b.fetchIndexBlob(ctx, "")
		if err != nil {
			logging.Warnf("cacheprog: web index refetch: %v", err)
			return set.New[string](), false
		}
	}
	keys, _, err := parseIndexBlob(blob)
	if err != nil {
		logging.Warnf("cacheprog: web index parse: %v", err)
		if diskBlob != nil {
			return diskKeys, false
		}
		return set.New[string](), false
	}
	b.writeIndexBlob(path, blob)
	return keys, true
}

// readDiskIndex returns (raw, parsed, etag) or (nil, empty, "") if the file
// is missing or invalid.
func (b *WebBackend) readDiskIndex(path string) ([]byte, set.Set[string], string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, set.New[string](), ""
	}
	keys, etag, err := parseIndexBlob(data)
	if err != nil {
		return nil, set.New[string](), ""
	}
	return data, keys, etag
}

// fetchIndexBlob does a conditional GET <endpoint>/<bucket>/_index within
// ctx's deadline, with at most indexFetchRetries retries (further capped by
// the configured retry policy). Returns:
//
//	body, http.StatusOK, nil          for a served blob
//	nil,  http.StatusNotModified, nil for a validated disk copy
//	nil,  <statusCode>, err           for a bad HTTP status (status preserved
//	                                  so the caller can classify it)
//	nil,  no status, err              for any transport failure
func (b *WebBackend) fetchIndexBlob(ctx context.Context, ifNoneMatch string) ([]byte, int, error) {
	// Watchdog re-arms per body read: bytes keep it alive, silence fires it and cancels the request.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var timedOut atomic.Bool
	watchdog := time.AfterFunc(indexHeaderBudget, func() {
		timedOut.Store(true)
		cancel()
	})
	defer watchdog.Stop()

	// wrapErr labels an error the watchdog caused, so the log line says the
	// fetch made no progress rather than the bare "context canceled".
	wrapErr := func(err error) error {
		if timedOut.Load() {
			return fmt.Errorf("abandoned: no progress within the index fetch budget (headers %v, stall %v): %w",
				indexHeaderBudget, indexStallTimeout, err)
		}
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "GET", b.endpoint+"/"+b.bucket+"/_index", nil)
	if err != nil {
		return nil, 0, err
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	b.signRequest(req)
	retries := indexFetchRetries
	if b.maxRetries < retries {
		retries = b.maxRetries
	}
	resp, err := b.doRetryGETN(req, retries)
	if err != nil {
		return nil, 0, wrapErr(err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(&stallGuardedReader{r: resp.Body, watchdog: watchdog, window: indexStallTimeout})
		if err != nil {
			return nil, 0, wrapErr(err)
		}
		return body, http.StatusOK, nil
	case http.StatusNotModified:
		return nil, http.StatusNotModified, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
}

// stallGuardedReader re-arms its watchdog every read: progress keeps it alive, silence fires it
// and cancels the request.
type stallGuardedReader struct {
	r        io.Reader
	watchdog *time.Timer
	window   time.Duration
}

func (s *stallGuardedReader) Read(p []byte) (int, error) {
	s.watchdog.Reset(s.window)
	return s.r.Read(p)
}

// writeIndexBlob persists a GBCI v1 blob via tmp file + atomic rename. Best-effort: a stale
// on-disk cache only forces a fresh GET next time.
func (b *WebBackend) writeIndexBlob(path string, blob []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, blob, 0644); err != nil {
		return
	}
	os.Rename(tmp, path)
}

// parseIndexBlob validates a GBCI v1 blob and returns:
//
//   - the reconstructed key set (full cache key strings)
//   - the strong ETag (hex-encoded sha256 trailer, quoted per the HTTP spec)
//   - or an error if magic, version, length, or trailer hash don't validate.
func parseIndexBlob(blob []byte) (set.Set[string], string, error) {
	if len(blob) < gbciHeaderSize+sha256.Size {
		return set.Set[string]{}, "", fmt.Errorf("blob too small (%d bytes)", len(blob))
	}
	if !bytes.Equal(blob[0:4], gbciMagic[:]) {
		return set.Set[string]{}, "", fmt.Errorf("bad magic")
	}
	if blob[4] != gbciVersion {
		return set.Set[string]{}, "", fmt.Errorf("unsupported version %d", blob[4])
	}
	if blob[5] != gbciHashSize {
		return set.Set[string]{}, "", fmt.Errorf("unsupported hash size %d", blob[5])
	}
	count := binary.LittleEndian.Uint64(blob[16:24])
	bodyEnd := gbciHeaderSize + int(count)*gbciHashSize
	if bodyEnd+sha256.Size != len(blob) {
		return set.Set[string]{}, "", fmt.Errorf("length %d != header+%d*%d+trailer", len(blob), count, gbciHashSize)
	}
	expected := sha256.Sum256(blob[:bodyEnd])
	if !bytes.Equal(expected[:], blob[bodyEnd:]) {
		return set.Set[string]{}, "", fmt.Errorf("trailer hash mismatch")
	}
	keys := set.New[string]()
	hashHex := make([]byte, gbciHashSize*2)
	for i := uint64(0); i < count; i++ {
		off := gbciHeaderSize + int(i)*gbciHashSize
		hex.Encode(hashHex, blob[off:off+gbciHashSize])
		keys.Add(gbciKeyPrefix + string(hashHex))
	}
	etag := `"` + hex.EncodeToString(blob[bodyEnd:]) + `"`
	return keys, etag, nil
}

// marshalIndex encodes the given key set as a GBCI v1 blob. Keys not matching
// the cacheprog pattern are skipped. Used by tests.
func marshalIndex(keys set.Set[string]) []byte {
	hashes := make([][gbciHashSize]byte, 0, keys.Len())
	for k := range keys.All() {
		if h, ok := decodeActionHash(k); ok {
			hashes = append(hashes, h)
		}
	}
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
	blob := make([]byte, gbciHeaderSize+len(hashes)*gbciHashSize+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = gbciHashSize
	binary.LittleEndian.PutUint16(blob[6:8], 0)
	binary.LittleEndian.PutUint64(blob[8:16], 0)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(hashes)))
	off := gbciHeaderSize
	for i := range hashes {
		copy(blob[off:off+gbciHashSize], hashes[i][:])
		off += gbciHashSize
	}
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])
	return blob
}

// decodeActionHash extracts the raw action ID from a cacheprog cache key.
func decodeActionHash(key string) ([gbciHashSize]byte, bool) {
	var zero [gbciHashSize]byte
	if !strings.HasPrefix(key, gbciKeyPrefix) {
		return zero, false
	}
	hex64 := key[len(gbciKeyPrefix):]
	if len(hex64) != gbciHashSize*2 {
		return zero, false
	}
	var h [gbciHashSize]byte
	if _, err := hex.Decode(h[:], []byte(hex64)); err != nil {
		return zero, false
	}
	return h, true
}
