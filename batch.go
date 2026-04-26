package main

import (
	"archive/tar"
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

// batchEntry holds a fetched cache entry for inclusion in a batch response.
type batchEntry struct {
	key      string
	data     []byte
	meta     *ObjectMeta
	prefetch bool
}

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
		writeS3Error(w, 405, "MethodNotAllowed", "Method not allowed")
		return
	}

	var req batchGetRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeS3Error(w, 400, "InvalidRequest", fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Keys) == 0 {
		writeS3Error(w, 400, "InvalidRequest", "no keys specified")
		return
	}

	user := anonymousUser
	if a := auditFromContext(r.Context()); a != nil {
		user = a.Username
	}

	// Fetch all requested entries.
	var entries []batchEntry
	requestedSet := make(map[string]bool, len(req.Keys))
	var minMod, maxMod time.Time

	for _, key := range req.Keys {
		requestedSet[key] = true
		data, meta, err := storage.Get(key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			log.Printf("batch get: %s: %v", key, err)
			continue
		}
		entries = append(entries, batchEntry{key: key, data: data, meta: meta})

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

	// Build manifest.
	manifest := batchGetManifest{}
	for _, e := range entries {
		manifest.Entries = append(manifest.Entries, batchGetManifestEntry{
			Key:      e.key,
			Size:     int64(len(e.data)),
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

	// Write manifest first.
	manifestData, _ := json.Marshal(manifest)
	tw.WriteHeader(&tar.Header{
		Name: "manifest.json",
		Size: int64(len(manifestData)),
		Mode: 0644,
	})
	tw.Write(manifestData)

	// Write data entries.
	for _, e := range entries {
		tw.WriteHeader(&tar.Header{
			Name: "data/" + e.key,
			Size: int64(len(e.data)),
			Mode: 0644,
		})
		tw.Write(e.data)
	}

	log.Printf("batch get: requested=%d found=%d prefetched=%d suppressed=%d",
		len(req.Keys), len(entries)-nPrefetch, nPrefetch, nSuppressed)
}

// findByModTime scans storage for entries with the given prefix whose
// modification time falls within [start, end], excluding already-requested keys.
func findByModTime(storage *Storage, start, end time.Time, exclude map[string]bool) []batchEntry {
	if storage.Index == nil {
		return nil
	}

	keys := storage.Index.NearbyKeys(start.Unix(), end.Unix(), maxPrefetchEntries, exclude)

	var out []batchEntry
	for _, key := range keys {
		data, meta, err := storage.Get(key)
		if err != nil {
			continue
		}
		out = append(out, batchEntry{key: key, data: data, meta: meta, prefetch: true})
	}
	return out
}
