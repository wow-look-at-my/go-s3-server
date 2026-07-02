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

// filterKeys returns the subset of candidate keys not recently sent to user.
// It records nothing: suppression runs BEFORE the per-key stat/guard/heal
// work, so up to maxPrefetchEntries already-sent candidates cost a map lookup
// each instead of a file open plus an lz4 first-block decode. record is called
// afterwards with only the keys that actually made it into the response.
func (t *prefetchTracker) filterKeys(user string, keys []string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	userSent := t.sent[user]

	var out []string
	for _, k := range keys {
		if userSent != nil {
			if sentAt, ok := userSent[k]; ok && now.Sub(sentAt) < prefetchTrackerTTL {
				continue
			}
		}
		out = append(out, k)
	}
	return out
}

// record marks keys as sent to user now and amortizes eviction of that user's
// stale entries. Only keys that were genuinely included in a response should
// be recorded — a candidate dropped by the guard/heal checks stays eligible.
func (t *prefetchTracker) record(user string, keys []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	userSent := t.sent[user]

	if len(keys) > 0 {
		if userSent == nil {
			userSent = make(map[string]time.Time)
			t.sent[user] = userSent
		}
		for _, k := range keys {
			userSent[k] = now
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
}

// handleBatchGet handles GET and POST /_batch/get requests. The client sends a
// JSON list of keys it needs, and the server responds with a tar stream
// containing the data and metadata for each found entry. POST is the
// semantically sound method (the request carries a body; GET-with-a-body is
// hostile to proxies and caches); GET remains accepted for existing clients.
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
	if r.Method != "GET" && r.Method != "POST" {
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
		// the reconstructed outputid on success. (No open handle here; the repair
		// opens, hashes, and stamps its own fd.)
		if !ensureOutputID(storage, key, meta, nil) {
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

	// Prefetch: find related keys by modification time proximity, suppress the
	// ones already sent to this user recently, and only THEN pay the per-key
	// stat/guard/heal work for the survivors. Running the tracker first matters:
	// the guard peek opens the file and decodes an lz4 block, so inspecting up
	// to maxPrefetchEntries candidates that were about to be thrown away as
	// already-sent wasted that work on every repeat request. Only the keys that
	// actually make it into the response are recorded as sent, so a candidate
	// dropped by the guard stays eligible for a later request.
	var nSuppressed int
	if req.Prefetch && len(entries) > 0 && !minMod.IsZero() && storage.Index != nil {
		windowStart := minMod.Add(-prefetchWindow)
		windowEnd := maxMod.Add(prefetchWindow)

		candidateKeys := storage.Index.NearbyKeys(windowStart.Unix(), windowEnd.Unix(), maxPrefetchEntries, requestedSet)
		freshKeys := tracker.filterKeys(user, candidateKeys)
		nSuppressed = len(candidateKeys) - len(freshKeys)

		prefetched := buildPrefetchEntries(storage, freshKeys)
		sentKeys := make([]string, len(prefetched))
		for i, e := range prefetched {
			sentKeys[i] = e.key
		}
		tracker.record(user, sentKeys)
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

// writeTarEntry writes one tar member, copying exactly size bytes from r so the
// bytes written always match the declared header size (a tar invariant).
func writeTarEntry(tw *tar.Writer, name string, size int64, r io.Reader) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Size: size, Mode: 0644}); err != nil {
		return err
	}
	_, err := io.CopyN(tw, r, size)
	return err
}

// buildPrefetchEntries stats the given prefetch keys and applies the same
// guard/heal gates the requested-key loop uses, returning servable entries
// (metadata only). Bodies are not read here — they are streamed later
// alongside the explicitly requested ones. The caller has already run tracker
// suppression, so every key here is genuinely about to be offered.
func buildPrefetchEntries(storage *Storage, keys []string) []batchEntry {
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
		if !ensureOutputID(storage, key, meta, nil) {
			continue
		}
		out = append(out, batchEntry{key: key, meta: meta, prefetch: true})
	}
	return out
}
