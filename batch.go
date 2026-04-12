package main

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
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

// handleBatchGet handles POST /_batch/get requests. The client sends a JSON
// list of keys it needs, and the server responds with a tar stream containing
// the data and metadata for each found entry.
//
// If prefetch is enabled, the server also includes entries whose modification
// time falls within ±30s of the requested entries, capturing entries from the
// same build that the client is likely to need next.
//
// The tar layout is:
//
//	manifest.json                    — index of all entries with metadata
//	data/<key>                       — raw file content for each entry
func handleBatchGet(w http.ResponseWriter, r *http.Request, storage *Storage) {
	if r.Method != "POST" {
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

	// Prefetch: find related entries by modification time proximity.
	if req.Prefetch && len(entries) > 0 && !minMod.IsZero() {
		windowStart := minMod.Add(-prefetchWindow)
		windowEnd := maxMod.Add(prefetchWindow)

		// Scope the scan to the common key prefix.
		prefix := commonPrefix(req.Keys)
		prefetched := findByModTime(storage, prefix, windowStart, windowEnd, requestedSet)
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

	log.Printf("batch get: requested=%d found=%d prefetched=%d",
		len(req.Keys), len(entries)-nPrefetch, nPrefetch)
}

// findByModTime scans storage for entries with the given prefix whose
// modification time falls within [start, end], excluding already-requested keys.
func findByModTime(storage *Storage, prefix string, start, end time.Time, exclude map[string]bool) []batchEntry {
	result, err := storage.List(prefix, maxPrefetchEntries+len(exclude), "")
	if err != nil {
		return nil
	}

	// Sort by modification time to prioritize entries closest to the request window.
	sort.Slice(result.Objects, func(i, j int) bool {
		return result.Objects[i].LastModified.Before(result.Objects[j].LastModified)
	})

	var out []batchEntry
	for _, obj := range result.Objects {
		if len(out) >= maxPrefetchEntries {
			break
		}
		if exclude[obj.Key] {
			continue
		}
		if obj.LastModified.Before(start) || obj.LastModified.After(end) {
			continue
		}
		data, meta, err := storage.Get(obj.Key)
		if err != nil {
			continue
		}
		out = append(out, batchEntry{key: obj.Key, data: data, meta: meta, prefetch: true})
	}
	return out
}

// commonPrefix finds the longest common prefix among a set of keys,
// truncated to the last slash for a clean directory boundary.
func commonPrefix(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	prefix := keys[0]
	for _, k := range keys[1:] {
		for !strings.HasPrefix(k, prefix) {
			prefix = prefix[:len(prefix)-1]
			if prefix == "" {
				return ""
			}
		}
	}
	if idx := strings.LastIndex(prefix, "/"); idx >= 0 {
		prefix = prefix[:idx+1]
	}
	return prefix
}
