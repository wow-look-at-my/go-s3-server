package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// batchGetRequest is the JSON body for POST /_batch/get.
type batchGetRequest struct {
	Keys     []string `json:"keys"`
	Prefetch bool     `json:"prefetch"` // include temporally related entries
}

// batchGetManifestEntry describes one entry in the batch response manifest.
type batchGetManifestEntry struct {
	Key      string            `json:"key"`
	Size     int64             `json:"size"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Prefetch bool              `json:"prefetch,omitempty"`
}

// batchGetManifest is the first entry in the tar response.
type batchGetManifest struct {
	Entries []batchGetManifestEntry `json:"entries"`
}

// batchPutManifestEntry describes one object in a /_batch/put upload. metadata
// holds the same values a single PUT carries in X-Cache-Meta-<Name> headers,
// keyed by the lowercased meta name WITHOUT the prefix (e.g. "outputid",
// "compression", "size"); they are stored exactly as handlePutObject stores
// native metadata (user.s3.* xattrs).
type batchPutManifestEntry struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// batchPutManifest is the first member ("manifest.json") of a /_batch/put tar.
type batchPutManifest struct {
	Entries []batchPutManifestEntry `json:"entries"`
}

// batchPutResult is one entry in the JSON response, one per manifest key, in
// manifest order. Status is one of storeStatus* (stored|dropped|conflict|error).
type batchPutResult struct {
	Key     string `json:"key"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// batchPutResponse is the JSON body returned by /_batch/put.
type batchPutResponse struct {
	Results []batchPutResult `json:"results"`
}

// batchEntry identifies a cache entry to include in a batch response. It holds
// only metadata (key, size, mtime, user metadata) — never the body. Bodies are
// streamed straight from disk into the tar at write time, so a batch of hundreds
// of large objects is never materialized in the heap at once.
type batchEntry struct {
	key      string
	meta     *ObjectMeta
	prefetch bool
}

// maxBatchKeys caps how many keys one batch request may ask for. The cacheprog
// client batches in chunks of 128, so this is generous headroom; it bounds the
// per-request manifest/stat work and rejects a pathological request with 400.
const maxBatchKeys = 4096

// prefetchWindow is the time window around requested entries within which
// other entries are considered related and included as prefetch.
const prefetchWindow = 30 * time.Second

// maxPrefetchEntries caps how many extra entries the server will include
// beyond what was explicitly requested.
const maxPrefetchEntries = 200

// prefetchTrackerTTL is how long the server remembers having sent a key to a
// given user. Prefetch entries are suppressed for this duration.
const prefetchTrackerTTL = 5 * time.Minute

// prefetchTracker remembers which keys were recently sent as prefetch to each
// user so that subsequent batch requests from the same user do not receive the
// same bulk data over and over (e.g. the same 200-entry pool on every request).
type prefetchTracker struct {
	mu   sync.Mutex
	sent map[string]map[string]time.Time // user → key → sent_at
}

func newPrefetchTracker() *prefetchTracker {
	return &prefetchTracker{sent: make(map[string]map[string]time.Time)}
}

// filterAndRecord returns the subset of candidates not recently sent to user,
// records those entries as sent, and evicts stale entries for that user.
func (t *prefetchTracker) filterAndRecord(user string, candidates []batchEntry) []batchEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	userSent := t.sent[user]

	var out []batchEntry
	for _, e := range candidates {
		if userSent != nil {
			if sentAt, ok := userSent[e.key]; ok && now.Sub(sentAt) < prefetchTrackerTTL {
				continue
			}
		}
		out = append(out, e)
	}

	if len(out) > 0 {
		if userSent == nil {
			userSent = make(map[string]time.Time)
			t.sent[user] = userSent
		}
		for _, e := range out {
			userSent[e.key] = now
		}
	}

	// Amortised eviction: clean stale keys for this user on every call.
	for k, sentAt := range userSent {
		if now.Sub(sentAt) >= prefetchTrackerTTL {
			delete(userSent, k)
		}
	}
	if len(userSent) == 0 {
		delete(t.sent, user)
	}

	return out
}

// handleBatchGet handles GET /_batch/get requests. The client sends a JSON
// list of keys it needs, and the server responds with a tar stream containing
// the data and metadata for each found entry.
//
// If prefetch is enabled, the server also includes entries whose modification
// time falls within ±30s of the requested entries, capturing entries from the
// same build that the client is likely to need next. The prefetchTracker
// suppresses keys already sent to this user recently, preventing the same
// 200-entry pool from flooding the client on every request.
//
// The tar layout is:
//
//	manifest.json                    — index of all entries with metadata
//	data/<key>                       — raw file content for each entry
func handleBatchGet(w http.ResponseWriter, r *http.Request, storage *Storage, tracker *prefetchTracker) {
	if r.Method != "GET" {
		writeError(w, 405, "method_not_allowed", "method not allowed")
		return
	}

	var req batchGetRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, 400, "invalid_request", fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, 400, "invalid_request", "no keys specified")
		return
	}
	if len(req.Keys) > maxBatchKeys {
		writeError(w, 400, "invalid_request", fmt.Sprintf("too many keys: %d (max %d)", len(req.Keys), maxBatchKeys))
		return
	}

	user := anonymousUser
	if a := auditFromContext(r.Context()); a != nil {
		user = a.Username
	}

	// Phase 1: collect metadata for the requested keys WITHOUT reading bodies.
	// Stat is cheap (os.Stat + xattrs); the bodies are streamed later, one at a
	// time, so the whole batch never sits in memory.
	var entries []batchEntry
	requestedSet := make(map[string]bool, len(req.Keys))
	var minMod, maxMod time.Time

	for _, key := range req.Keys {
		requestedSet[key] = true
		meta, err := storage.Stat(key)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				log.Printf("batch get: %s: %v", key, err)
			}
			continue
		}
		// Module-index guard: refuse + evict a stored Go module-index blob (same
		// rationale as handleGetObject) before it ever enters the manifest. On
		// detection the key is evicted and omitted entirely -- the client treats
		// the missing entry as a miss and recomputes the index locally.
		if evictModuleIndexOnReadByKey(storage, key, meta) {
			continue
		}
		// Self-heal: repair an outputid-less object in place (same rationale as
		// handleGetObject) so it can be served as a hit. If it cannot be repaired,
		// omit it from the manifest -- the client then treats it as a miss -- but
		// leave it on disk (no eviction). ensureOutputID fills meta.Metadata with
		// the reconstructed outputid on success.
		if !ensureOutputID(storage, key, meta) {
			continue
		}
		entries = append(entries, batchEntry{key: key, meta: meta})

		if minMod.IsZero() || meta.ModTime.Before(minMod) {
			minMod = meta.ModTime
		}
		if maxMod.IsZero() || meta.ModTime.After(maxMod) {
			maxMod = meta.ModTime
		}
	}

	// Prefetch: find related entries by modification time proximity, then
	// suppress any the server has already sent to this user recently.
	var nSuppressed int
	if req.Prefetch && len(entries) > 0 && !minMod.IsZero() {
		windowStart := minMod.Add(-prefetchWindow)
		windowEnd := maxMod.Add(prefetchWindow)

		candidates := findByModTime(storage, windowStart, windowEnd, requestedSet)
		prefetched := tracker.filterAndRecord(user, candidates)
		nSuppressed = len(candidates) - len(prefetched)
		entries = append(entries, prefetched...)
	}

	// Build manifest from metadata only — no body bytes are held here.
	manifest := batchGetManifest{}
	for _, e := range entries {
		manifest.Entries = append(manifest.Entries, batchGetManifestEntry{
			Key:      e.key,
			Size:     e.meta.Size,
			Metadata: e.meta.Metadata,
			Prefetch: e.prefetch,
		})
	}

	// Count stats for logging.
	var nPrefetch int
	for _, e := range entries {
		if e.prefetch {
			nPrefetch++
		}
	}

	// Write tar response.
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(200)

	tw := tar.NewWriter(w)
	defer tw.Close()

	// Manifest first.
	manifestData, _ := json.Marshal(manifest)
	if err := writeTarEntry(tw, "manifest.json", int64(len(manifestData)), bytes.NewReader(manifestData)); err != nil {
		log.Printf("batch get: write manifest: %v", err)
		return
	}

	// Phase 2: stream each body straight from disk into the tar. Only one body
	// is in flight at a time (an io.Copy-sized buffer), so a batch of hundreds
	// of large objects no longer materializes hundreds of bodies in the heap —
	// the change that keeps the server within its memory budget under the
	// concurrent CI matrix load that previously OOM-killed it.
	var streamed int
	for _, e := range entries {
		f, meta, err := storage.Open(e.key)
		if err != nil {
			// Vanished between stat and stream (e.g. operator eviction). Skip it:
			// the client matches data entries by name and treats a missing one as
			// a cache miss, so omitting it is safe.
			continue
		}
		err = writeTarEntry(tw, "data/"+e.key, meta.Size, f)
		f.Close()
		if err != nil {
			// A write error here is almost always the client going away mid-stream;
			// stop rather than spin through the rest of the batch.
			log.Printf("batch get: stream %s: %v", e.key, err)
			return
		}
		streamed++
	}

	log.Printf("batch get: requested=%d found=%d prefetched=%d suppressed=%d streamed=%d",
		len(req.Keys), len(entries)-nPrefetch, nPrefetch, nSuppressed, streamed)
}

// handleBatchPut handles PUT /_batch/put. The go-toolchain client issues one
// HTTP PUT per cached object; a CI build produces thousands, each consuming an
// admission-control slot, which saturates the server and sheds uploads with 503.
// This endpoint accepts a tar of many objects in a SINGLE request holding ONE
// admission slot (the whole point), and stores each member through the same path
// as a single PUT (storeOneObject: module-index refusal, write_once, audit
// xattrs, index append), returning a per-object result manifest.
//
// The request tar layout mirrors /_batch/get:
//
//	manifest.json        — JSON {"entries":[{"key":...,"metadata":{...}}]} (FIRST member)
//	data/<key>           — the (already lz4-compressed) body for each entry, in manifest order
//
// A malformed tar, a missing/late manifest, or a key mismatch between the
// manifest and the data members is a whole-request 400 invalid_request. A
// per-object store failure does NOT abort the batch: it is recorded as an
// "error" result and the remaining members are still processed.
func handleBatchPut(w http.ResponseWriter, r *http.Request, storage *Storage, maxObjectBytes int64) {
	if r.Method != "PUT" {
		writeError(w, 405, "method_not_allowed", "method not allowed")
		return
	}

	audit := auditMapFromContext(r)

	// Bound the whole batch so one request cannot exhaust memory/disk. The cap is
	// maxBatchKeys * maxObjectBytes (the most a well-formed maximal batch could
	// legitimately carry); an over-limit body is refused as 413. Each member is
	// additionally bounded to maxObjectBytes below via a per-member LimitReader.
	maxBatchBytes := int64(maxBatchKeys) * maxObjectBytes
	body := http.MaxBytesReader(w, r.Body, maxBatchBytes)
	tr := tar.NewReader(body)

	// First member MUST be manifest.json.
	hdr, err := tr.Next()
	if err != nil {
		if isMaxBytesErr(err) {
			writeError(w, 413, "too_large", fmt.Sprintf("batch exceeds max size of %d bytes", maxBatchBytes))
			return
		}
		writeError(w, 400, "invalid_request", fmt.Sprintf("read tar: %v", err))
		return
	}
	if hdr.Name != "manifest.json" {
		writeError(w, 400, "invalid_request", fmt.Sprintf("first tar member must be manifest.json, got %q", hdr.Name))
		return
	}
	var manifest batchPutManifest
	if err := json.NewDecoder(io.LimitReader(tr, 1<<20)).Decode(&manifest); err != nil {
		writeError(w, 400, "invalid_request", fmt.Sprintf("invalid manifest.json: %v", err))
		return
	}
	if len(manifest.Entries) == 0 {
		writeError(w, 400, "invalid_request", "manifest has no entries")
		return
	}
	if len(manifest.Entries) > maxBatchKeys {
		writeError(w, 413, "too_large", fmt.Sprintf("too many entries: %d (max %d)", len(manifest.Entries), maxBatchKeys))
		return
	}

	// Index the manifest by the data member name we expect for each entry, so we
	// can pair the data members (read in stream order) with their metadata and
	// detect a data member that has no manifest entry (or vice versa).
	type pending struct {
		entry  batchPutManifestEntry
		seen   bool
		result *batchPutResult
	}
	results := make([]batchPutResult, len(manifest.Entries))
	byDataName := make(map[string]*pending, len(manifest.Entries))
	for i, e := range manifest.Entries {
		if e.Key == "" {
			writeError(w, 400, "invalid_request", fmt.Sprintf("manifest entry %d has empty key", i))
			return
		}
		name := "data/" + e.Key
		if _, dup := byDataName[name]; dup {
			writeError(w, 400, "invalid_request", fmt.Sprintf("duplicate key in manifest: %q", e.Key))
			return
		}
		results[i] = batchPutResult{Key: e.Key}
		byDataName[name] = &pending{entry: e, result: &results[i]}
	}

	// Stream the data members. storeOneObject reads each body through a bounded
	// per-member reader (maxObjectBytes) so one oversized member cannot blow the
	// budget, and the tar reader bounds reads to the current member anyway.
	var nStored, nDropped, nConflict, nError int
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isMaxBytesErr(err) {
				writeError(w, 413, "too_large", fmt.Sprintf("batch exceeds max size of %d bytes", maxBatchBytes))
				return
			}
			writeError(w, 400, "invalid_request", fmt.Sprintf("read tar member: %v", err))
			return
		}
		p, ok := byDataName[hdr.Name]
		if !ok {
			writeError(w, 400, "invalid_request", fmt.Sprintf("data member %q has no manifest entry", hdr.Name))
			return
		}
		if p.seen {
			writeError(w, 400, "invalid_request", fmt.Sprintf("duplicate data member %q", hdr.Name))
			return
		}
		p.seen = true

		// auditMapFromContext returns a fresh map each time it is called from the
		// single-PUT path; here the request-level audit (uploader, IP, UA,
		// timestamp) is shared across members, so clone it per member because
		// PutStream mutates the map (it writes content_length).
		memberAudit := cloneAudit(audit)
		member := io.LimitReader(tr, maxObjectBytes)
		status, storeErr := storeOneObject(storage, p.entry.Key, member, p.entry.Metadata, memberAudit)
		p.result.Status = status
		switch status {
		case storeStatusStored:
			nStored++
		case storeStatusDropped:
			nDropped++
		case storeStatusConflict:
			nConflict++
		case storeStatusError:
			nError++
			if storeErr != nil {
				p.result.Message = storeErr.Error()
			}
			log.Printf("batch put: store %s: %v", p.entry.Key, storeErr)
		}
	}

	// Every manifest entry must have had a matching data member.
	for name, p := range byDataName {
		if !p.seen {
			writeError(w, 400, "invalid_request", fmt.Sprintf("manifest entry %q has no data member %q", p.entry.Key, name))
			return
		}
	}

	log.Printf("batch put: entries=%d stored=%d dropped=%d conflict=%d error=%d",
		len(manifest.Entries), nStored, nDropped, nConflict, nError)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(batchPutResponse{Results: results})
}

// cloneAudit returns a shallow copy of the per-request audit map so each batch
// member gets its own map to carry the per-member content_length that PutStream
// writes (PutStream mutates the map in place). A nil audit clones to nil.
func cloneAudit(audit map[string]string) map[string]string {
	if audit == nil {
		return nil
	}
	c := make(map[string]string, len(audit)+1)
	for k, v := range audit {
		c[k] = v
	}
	return c
}

// isMaxBytesErr reports whether err is (or wraps) an http.MaxBytesError, the
// over-limit signal from the batch body's MaxBytesReader.
func isMaxBytesErr(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

// writeTarEntry writes one tar member, copying exactly size bytes from r so the
// bytes written always match the declared header size (a tar invariant).
func writeTarEntry(tw *tar.Writer, name string, size int64, r io.Reader) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Size: size, Mode: 0644}); err != nil {
		return err
	}
	_, err := io.CopyN(tw, r, size)
	return err
}

// findByModTime returns related entries (metadata only) whose modification time
// falls within [start, end], excluding already-requested keys. Bodies are not
// read here — they are streamed later alongside the explicitly requested ones.
func findByModTime(storage *Storage, start, end time.Time, exclude map[string]bool) []batchEntry {
	if storage.Index == nil {
		return nil
	}

	keys := storage.Index.NearbyKeys(start.Unix(), end.Unix(), maxPrefetchEntries, exclude)

	var out []batchEntry
	for _, key := range keys {
		meta, err := storage.Stat(key)
		if err != nil {
			continue
		}
		// Module-index guard here too: never prefetch a stored module-index blob.
		// Detect it, evict it, and skip it -- offering it would just hand the
		// client poison it would refuse anyway.
		if evictModuleIndexOnReadByKey(storage, key, meta) {
			continue
		}
		// Self-heal here too: repair an outputid-less object in place so it is
		// usable prefetch; if it cannot be repaired, skip it (leave it on disk)
		// rather than offer the client bytes it would have to discard.
		if !ensureOutputID(storage, key, meta) {
			continue
		}
		out = append(out, batchEntry{key: key, meta: meta, prefetch: true})
	}
	return out
}
