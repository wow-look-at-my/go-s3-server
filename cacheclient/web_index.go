package cacheclient

import (
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

// hashSize is the number of bytes in an action ID, and so per entry in the
// index body.
const hashSize = 32

// gbciHashSize is the size the wire header states. It must equal hashSize.
const gbciHashSize = hashSize

// actionHash is an action ID in the form the client indexes by: the raw 32
// bytes, not the 64 hex characters behind a prefix that the wire uses. It is a
// comparable array, so a set of them is one flat allocation rather than one
// string per entry.
type actionHash = [hashSize]byte

// parseActionHash decodes a hex action ID into the bytes the client indexes
// by. A cmd/go action ID is always 32 bytes, so it fills the array exactly.
//
// A shorter hex id lands left-aligned and zero-extended rather than being
// refused. Such an id cannot come from the wire -- the index format is 32
// bytes per entry, and every key the server names is 64 hex characters -- so
// it is always a consumer's own synthetic id, and it only ever has to match
// itself. Refusing it would break that consumer for a strictness the format
// already enforces everywhere it matters.
//
// Anything that is not even-length hex, or is longer than an action ID, is not
// an id at all, and no round trip is owed to it.
func parseActionHash(actionID string) (actionHash, bool) {
	var h actionHash
	if len(actionID) == 0 || len(actionID) > hashSize*2 || len(actionID)%2 != 0 {
		return h, false
	}
	if _, err := hex.Decode(h[:len(actionID)/2], []byte(actionID)); err != nil {
		return actionHash{}, false
	}
	return h, true
}

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
	dir := b.indexDir
	if dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, name)
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
//
// A disk copy younger than the backend's index max age is served as current
// with no request at all. A test suite starts thousands of go commands a
// minute, and the blob is tens of megabytes that the server rebuilds as keys
// arrive, so a copy validated within the last minute is what a revalidation
// would download again.
func (b *WebBackend) loadOrFetchIndex() (*hashSet, bool) {
	path := b.indexCachePath()
	diskBlob, diskKeys, diskETag, diskAge := b.readDiskIndex(path)
	if diskBlob != nil && b.indexMaxAge > 0 && diskAge < b.indexMaxAge {
		return diskKeys, true
	}

	// The absolute ceiling covers the whole load; each fetch also enforces the header and stall budgets above.
	ctx, cancel := context.WithTimeout(context.Background(), indexFetchCeiling)
	defer cancel()

	blob, status, err := b.fetchIndexBlob(ctx, diskETag)
	if err != nil {
		if diskBlob != nil {
			// A failed refresh over a disk copy is routine: the build keeps a
			// key set, and every go command reports it on a busy host.
			logging.Infof("cacheprog: web index refresh: %v", err)
			return diskKeys, false
		}
		logging.Warnf("cacheprog: web index fetch: %v", err)
		return newHashSet(0), false
	}
	if status == http.StatusNotModified {
		if diskBlob != nil {
			// The copy's age is the time since the server last confirmed it.
			now := time.Now()
			_ = os.Chtimes(path, now, now)
			return diskKeys, true
		}
		// No disk copy despite a not-modified answer (likely a cleared /tmp); refetch unconditionally.
		blob, _, err = b.fetchIndexBlob(ctx, "")
		if err != nil {
			logging.Warnf("cacheprog: web index refetch: %v", err)
			return newHashSet(0), false
		}
	}
	keys, _, err := parseIndexBlob(blob)
	if err != nil {
		logging.Warnf("cacheprog: web index parse: %v", err)
		if diskBlob != nil {
			return diskKeys, false
		}
		return newHashSet(0), false
	}
	b.writeIndexBlob(path, blob)
	return keys, true
}

// readDiskIndex returns (raw, parsed, etag, age) or (nil, empty, "", 0) if
// the file is missing or invalid. The age is the time since the copy was
// written or last confirmed current.
func (b *WebBackend) readDiskIndex(path string) ([]byte, *hashSet, string, time.Duration) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, newHashSet(0), "", 0
	}
	keys, etag, err := parseIndexBlob(data)
	if err != nil {
		return nil, newHashSet(0), "", 0
	}
	var age time.Duration
	if st, err := os.Stat(path); err == nil {
		age = time.Since(st.ModTime())
	}
	return data, keys, etag, age
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
func parseIndexBlob(blob []byte) (*hashSet, string, error) {
	if len(blob) < gbciHeaderSize+sha256.Size {
		return nil, "", fmt.Errorf("blob too small (%d bytes)", len(blob))
	}
	if !bytes.Equal(blob[0:4], gbciMagic[:]) {
		return nil, "", fmt.Errorf("bad magic")
	}
	if blob[4] != gbciVersion {
		return nil, "", fmt.Errorf("unsupported version %d", blob[4])
	}
	if blob[5] != gbciHashSize {
		return nil, "", fmt.Errorf("unsupported hash size %d", blob[5])
	}
	count := binary.LittleEndian.Uint64(blob[16:24])
	bodyEnd := gbciHeaderSize + int(count)*gbciHashSize
	if bodyEnd+sha256.Size != len(blob) {
		return nil, "", fmt.Errorf("length %d != header+%d*%d+trailer", len(blob), count, gbciHashSize)
	}
	expected := sha256.Sum256(blob[:bodyEnd])
	if !bytes.Equal(expected[:], blob[bodyEnd:]) {
		return nil, "", fmt.Errorf("trailer hash mismatch")
	}
	// The body IS the hashes, ascending, which is the order the set holds them
	// in. Each entry is copied as it stands, with no hex encode, no per-key
	// allocation and no sort: on a cache of hundreds of thousands of entries
	// that work all lands before the build starts.
	hashes := make([]actionHash, count)
	for i := uint64(0); i < count; i++ {
		off := gbciHeaderSize + int(i)*gbciHashSize
		copy(hashes[i][:], blob[off:off+gbciHashSize])
	}
	keys := newHashSetFromSorted(hashes)
	etag := `"` + hex.EncodeToString(blob[bodyEnd:]) + `"`
	return keys, etag, nil
}

// marshalIndex encodes the given hash set as a GBCI v1 blob. Used by tests.
func marshalIndex(keys *hashSet) []byte {
	hashes := make([]actionHash, 0, keys.Len())
	for h := range keys.All() {
		hashes = append(hashes, h)
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
func decodeActionHash(key string) (actionHash, bool) {
	if !strings.HasPrefix(key, gbciKeyPrefix) {
		return actionHash{}, false
	}
	return parseActionHash(key[len(gbciKeyPrefix):])
}
