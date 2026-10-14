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
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

// Bounded by SILENCE, never by total duration. Exhaustion falls back to a
// non-authoritative key set; batch-probing stays on.
var (
	indexHeaderBudget = 10 * time.Second
	indexStallTimeout = 10 * time.Second
)

const indexFetchRetries = 1

// gbciKeyPrefix leads every cacheprog key; the wire format carries only the raw hash after it.
const gbciKeyPrefix = "go-buildcache/v1"

// hashSize is the number of bytes in an action ID, and so per entry in the
// index body.
const hashSize = 32

// gbciHashSize is the size the wire header states. It must equal hashSize.
const gbciHashSize = hashSize

// It is a comparable array, so a set of them is a single flat allocation
// rather than a single string per entry.
type actionHash = [hashSize]byte

// parseActionHash decodes a hex action ID into the bytes the client indexes
// by.
//
// Refusing it would break that consumer for a strictness the format already
// enforces everywhere it matters.
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

// fetchIndex revalidates the disk copy against the server and returns the set
// of known cache keys and whether that set is AUTHORITATIVE, i.e.
// server-confirmed this run (a parsed blob, or a not-modified answer
// validating the disk copy).
//
// It issues a conditional GET /<bucket>/_index. On not-modified it keeps the
// disk blob and restarts its age; on a fresh body it adopts and persists it.
// Any failure produces a NON-authoritative set (the disk copy, or empty):
// Get/Put still work, and because absences from a non-authoritative set prove
// nothing, cold keys are batch-probed instead of fast-missed (see
// WebBackend.Get).
//
// ctx carries no deadline. Each fetch carries the header and stall budgets
// above, and those are what a hung server trips; ctx is cancelled only when
// the backend closes.
//
// Each outcome logs a single time here. The consumer's stderr carries only
// the Warnf lines unless it asks for the routine ones.
func (b *WebBackend) fetchIndex(ctx context.Context, path string, disk diskIndex) (*hashSet, bool) {
	diskBlob, diskKeys, diskETag := disk.blob, disk.keys, disk.etag

	reply, err := b.doIndexGET(ctx, "", diskETag)
	blob, status := reply.body, reply.status
	if err != nil {
		if ctx.Err() != nil {
			// The backend closed under the fetch. Nothing failed that anyone
			// needs to hear about, and nothing is left to install.
			return nil, false
		}
		if diskBlob != nil {
			// A failed refresh over a disk copy is routine: the build keeps a
			// key set, and every go command reports it on a busy host.
			logging.Infof("cacheprog: web index refresh: %v", err)
			logging.Infof("cacheprog: web index: refresh failed; using %d cached keys", disk.count)
			return diskKeys, false
		}
		logging.Warnf("cacheprog: web index fetch: %v", err)
		logging.Warnf("cacheprog: web index is unavailable")
		return newHashSet(0), false
	}
	if status == http.StatusNotModified {
		if diskBlob != nil {
			// The copy's age is the time since the server last confirmed it.
			now := time.Now()
			_ = os.Chtimes(path, now, now)
			writeIndexGen(path, diskETag, reply.gen)
			b.setLiveGen(reply.gen)
			logging.Infof("cacheprog: web index: %d keys", disk.count)
			return diskKeys, true
		}
		// No disk copy despite a not-modified answer (likely a cleared /tmp); refetch unconditionally.
		reply, err = b.doIndexGET(ctx, "", "")
		blob = reply.body
		if err != nil {
			if ctx.Err() != nil {
				return nil, false
			}
			logging.Warnf("cacheprog: web index refetch: %v", err)
			logging.Warnf("cacheprog: web index is unavailable")
			return newHashSet(0), false
		}
	}
	keys, etag, err := parseIndexBlob(blob)
	if err != nil {
		logging.Warnf("cacheprog: web index parse: %v", err)
		if diskBlob != nil {
			logging.Infof("cacheprog: web index: refresh failed; using %d cached keys", disk.count)
			return diskKeys, false
		}
		logging.Warnf("cacheprog: web index is unavailable")
		return newHashSet(0), false
	}
	b.writeIndexBlob(path, blob)
	writeIndexGen(path, etag, reply.gen)
	b.setLiveGen(reply.gen)
	logging.Infof("cacheprog: web index: %d keys", keys.Len())
	return keys, true
}

// diskIndex is the index's disk copy as a single read found it.
type diskIndex struct {
	blob []byte   // the raw copy; nil when missing or invalid
	keys *hashSet // its parsed keys; empty when blob is nil
	etag string   // its strong ETag; empty when blob is nil
	gen  indexGen // its add-log position; zero when the server named none
	// count is the key count as read. keys may be installed as the live set
	// and gain claims after that, so a report on the copy uses this instead.
	count int
	// mtime is when the copy was written or last confirmed current, and the
	// empty time when there is no file at all. A change in it is how
	// another process's refresh shows up here.
	mtime time.Time
}

// age is the time since the copy was written or last confirmed current.
func (d diskIndex) age() time.Duration {
	return time.Since(d.mtime)
}

// readDiskIndex reads the disk copy at path. A missing or invalid copy comes
// back with a nil blob and an empty key set.
func (b *WebBackend) readDiskIndex(path string) diskIndex {
	d := diskIndex{keys: newHashSet(0)}
	st, err := os.Stat(path)
	if err != nil {
		return d
	}
	d.mtime = st.ModTime()
	data, err := os.ReadFile(path)
	if err != nil {
		return d
	}
	keys, etag, err := parseIndexBlob(data)
	if err != nil {
		return d
	}
	d.blob, d.keys, d.etag, d.count = data, keys, etag, keys.Len()
	d.gen = readIndexGen(path, etag)
	return d
}

// indexReply is a GET /_index answer: the body, the status, the add-log
// position the server named, and whether the body is a delta.
type indexReply struct {
	body   []byte
	status int
	gen    indexGen
	delta  bool
}

// doIndexGET does a conditional GET <endpoint>/<bucket>/_index?rawQuery under
// ctx, with at most indexFetchRetries retries (further capped by the
// configured retry policy). A bad HTTP status comes back with the status kept,
// so the caller can classify it. A transport failure comes back with no status.
func (b *WebBackend) doIndexGET(ctx context.Context, rawQuery, ifNoneMatch string) (indexReply, error) {
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

	// Every request asks for the compact form. An older server answers the 8-byte or the full form, which parse too.
	target := b.endpoint + "/" + b.bucket + "/_index?" + riceQuery
	if rawQuery != "" {
		target += "&" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		return indexReply{}, err
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
		return indexReply{}, wrapErr(err)
	}
	defer closeBody(resp.Body)
	reply := indexReply{
		status: resp.StatusCode,
		gen:    indexGenFromHeader(resp.Header),
		delta:  resp.Header.Get(HeaderIndexDelta) == "1",
	}
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(&stallGuardedReader{r: resp.Body, watchdog: watchdog, window: indexStallTimeout})
		if err != nil {
			return indexReply{}, wrapErr(err)
		}
		// Every process pays this before it can tell a hit from a miss.
		b.indexBytes.Add(uint64(len(body)))
		reply.body = body
		return reply, nil
	case http.StatusNotModified:
		return reply, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return reply, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body))
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

// writeIndexBlob persists a GBCI v1 blob via a temp file private to this
// writer and an atomic rename, so processes sharing the directory never
// interleave into a single file. Best-effort: a stale on-disk cache only
// forces a fresh GET next time.
func (b *WebBackend) writeIndexBlob(path string, blob []byte) {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return
	}
	tmp := f.Name()
	_, werr := f.Write(blob)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Chmod(tmp, 0644); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
	}
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
	if blob[4] == gbciRiceVersion {
		return parseRiceBlob(blob)
	}
	if blob[4] != gbciVersion {
		return nil, "", fmt.Errorf("unsupported version %d", blob[4])
	}
	width := int(blob[5])
	if width != gbciHashSize && width != gbciPrefixSize {
		return nil, "", fmt.Errorf("unsupported hash size %d", blob[5])
	}
	count := binary.LittleEndian.Uint64(blob[16:24])
	bodyEnd := gbciHeaderSize + int(count)*width
	if bodyEnd+sha256.Size != len(blob) {
		return nil, "", fmt.Errorf("length %d != header+%d*%d+trailer", len(blob), count, width)
	}
	expected := sha256.Sum256(blob[:bodyEnd])
	if !bytes.Equal(expected[:], blob[bodyEnd:]) {
		return nil, "", fmt.Errorf("trailer hash mismatch")
	}
	// The body IS the hashes, ascending, which is the order the set holds them in.
	keys := make([]hashKey, count)
	for idx := range keys {
		off := gbciHeaderSize + idx*width
		keys[idx] = binary.BigEndian.Uint64(blob[off : off+gbciPrefixSize])
	}
	set := newHashSetFromSorted(keys)
	etag := `"` + hex.EncodeToString(blob[bodyEnd:]) + `"`
	return set, etag, nil
}

// parseRiceBlob is parseIndexBlob for the compact form.
func parseRiceBlob(blob []byte) (*hashSet, string, error) {
	bodyEnd := len(blob) - sha256.Size
	expected := sha256.Sum256(blob[:bodyEnd])
	if !bytes.Equal(expected[:], blob[bodyEnd:]) {
		return nil, "", fmt.Errorf("trailer hash mismatch")
	}
	count := binary.LittleEndian.Uint64(blob[16:24])
	if count > uint64(bodyEnd-gbciHeaderSize)*8 {
		return nil, "", fmt.Errorf("%d keys in a body of %d bytes", count, bodyEnd-gbciHeaderSize)
	}
	width := int(blob[5])
	keys, err := decodeRice(blob[gbciHeaderSize:bodyEnd], int(count), width, uint(blob[6]))
	if err != nil {
		return nil, "", err
	}
	set := newHashSetFromSorted(keys)
	set.drop = uint(64 - 8*width)
	return set, `"` + hex.EncodeToString(blob[bodyEnd:]) + `"`, nil
}

// marshalIndex encodes the given hash set: the compact form for keys
// narrower than gbciPrefixSize, else a GBCI v1 blob.
func marshalIndex(keys *hashSet) []byte {
	sorted := slices.Sorted(keys.All())
	if width := keys.width(); width < gbciPrefixSize {
		return marshalRiceIndex(sorted, width)
	}
	return marshalSortedIndex(sorted)
}

// marshalSortedIndex encodes ascending, distinct keys as a GBCI v1 blob.
func marshalSortedIndex(sorted []hashKey) []byte {
	blob := make([]byte, gbciHeaderSize+len(sorted)*gbciPrefixSize+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = gbciPrefixSize
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(sorted)))
	off := gbciHeaderSize
	for _, key := range sorted {
		binary.BigEndian.PutUint64(blob[off:], key)
		off += gbciPrefixSize
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
