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
	"os"
	"sync"
	"time"

	"github.com/wow-look-at-my/go-containers/set"
)

// batchGetRequest is the JSON body for POST /_batch/get.
type batchGetRequest struct {
	Keys     []string `json:"keys"`
	Prefetch bool     `json:"prefetch"` // include temporally related entries
	// PrefetchOnly answers with the window around Keys and none of the Keys themselves.
	PrefetchOnly bool `json:"prefetch_only"`
	// Have is what the client says it already holds. The window leaves those keys out.
	Have *haveFilter `json:"have"`
}

// batchGetManifestEntry describes a single entry in the batch response manifest.
type batchGetManifestEntry struct {
	Key      string            `json:"key"`
	Size     int64             `json:"size"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Prefetch bool              `json:"prefetch,omitempty"`
}

// batchGetManifest is the earliest entry in the tar response.
type batchGetManifest struct {
	Entries []batchGetManifestEntry `json:"entries"`
}

// batchPutManifestEntry describes a single object in a /_batch/put upload.
type batchPutManifestEntry struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// batchPutManifest is the earliest member ("manifest.json") of a /_batch/put tar.
type batchPutManifest struct {
	Entries []batchPutManifestEntry `json:"entries"`
}

// batchPutResult is a single entry in the JSON response, a single per
// manifest key, in manifest order.
type batchPutResult struct {
	Key     string `json:"key"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// batchPutResponse is the JSON body returned by /_batch/put.
type batchPutResponse struct {
	Results []batchPutResult `json:"results"`
}

// batchEntry identifies a cache entry to include in a batch response.
type batchEntry struct {
	key      string
	meta     *ObjectMeta
	prefetch bool
}

// maxBatchKeys caps how many keys a single batch request may ask for.
const maxBatchKeys = 4096

// prefetchWindow is the time window around requested entries.
const prefetchWindow = 30 * time.Second

// maxPrefetchEntries caps how many extra entries the server will include beyond what was explicitly requested.
const maxPrefetchEntries = 200

// handleBatchGet handles GET and POST /_batch/get requests. The client sends a
// JSON list of keys it needs. The server responds with a tar stream containing
// the data and metadata for each found entry. POST is the semantically sound
// method (the request carries a body; GET-with-a-body is hostile to proxies
// and caches); GET remains accepted for existing clients.
func handleBatchGet(w http.ResponseWriter, r *http.Request, storage *Storage, agg *logAggregator, prefetchEnabled bool) {
	if r.Method != "GET" && r.Method != "POST" {
		writeError(w, 405, "method_not_allowed", "method not allowed")
		return
	}

	if !prefetchEnabled {
		// A look-ahead request to this server gets an empty answer, so the client stops sending them.
		w.Header().Set(headerPrefetch, "off")
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

	prov := provenanceOf(r)

	// lookup is the keys whose bodies this response carries.
	lookup := req.Keys
	// A prefetch_only request asks for no key.
	anchorsOnly := req.PrefetchOnly
	if !prefetchEnabled {
		if req.PrefetchOnly {
			lookup = nil
		}
		req.Prefetch, req.PrefetchOnly = false, false
	}

	requestedSet := set.New[string](len(lookup))
	for _, key := range lookup {
		requestedSet.Add(key)
	}
	entries := parallelFilter(lookup, func(key string) (batchEntry, bool) {
		return servableEntry(storage, key, false)
	})
	var minMod, maxMod time.Time
	for _, e := range entries {
		if minMod.IsZero() || e.meta.ModTime.Before(minMod) {
			minMod = e.meta.ModTime
		}
		if maxMod.IsZero() || e.meta.ModTime.After(maxMod) {
			maxMod = e.meta.ModTime
		}
	}

	// Prefetch.
	var nHeld int
	if req.Prefetch && len(entries) > 0 && !minMod.IsZero() && storage.Index != nil {
		windowStart := minMod.Add(-prefetchWindow)
		windowEnd := maxMod.Add(prefetchWindow)

		freshKeys := storage.Index.NearbyKeys(windowStart.Unix(), windowEnd.Unix(), maxPrefetchEntries, requestedSet,
			func(key string) bool {
				h, ok := extractActionHash(key)
				if !ok || !req.Have.contains(h) {
					return false
				}
				nHeld++
				return true
			})

		prefetched := buildPrefetchEntries(storage, freshKeys)
		if req.PrefetchOnly {
			// The requested keys were the anchor, not the ask.
			entries = prefetched
		} else {
			entries = append(entries, prefetched...)
		}
	}

	// Open every body and check it against its stored digest before anything is written. That is the request's work.
	opened := openVerifiedEntries(storage, entries)
	defer func() {
		for _, o := range opened {
			if o.f != nil {
				o.f.Close()
			}
		}
	}()

	// Build manifest from metadata only — no body bytes are held here.
	manifest := batchGetManifest{}
	var nPrefetch int
	for _, o := range opened {
		manifest.Entries = append(manifest.Entries, batchGetManifestEntry{
			Key:      o.key,
			Size:     o.size,
			Metadata: o.meta.Metadata,
			Prefetch: o.prefetch,
		})
		if o.prefetch {
			nPrefetch++
		}
	}
	manifestData, _ := json.Marshal(manifest)

	// The rest is wire time, which a slow runner stretches to minutes.
	releaseSlot(r)

	// Write tar response.
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(200)

	tw := tar.NewWriter(w)
	defer tw.Close()

	// Manifest earliest.
	if err := writeTarEntry(tw, "manifest.json", int64(len(manifestData)), bytes.NewReader(manifestData)); err != nil {
		log.Printf("batch get: write manifest: %v", err)
		return
	}

	// Only a single body is in flight at a time (an io.Copy-sized buffer).
	var streamed int
	for i := range opened {
		o := &opened[i]
		err := writeTarEntry(tw, "data/"+o.key, o.size, o.f)
		o.f.Close()
		o.f = nil
		if err != nil {
			// A write error here is almost always the client going away mid-stream.
			log.Printf("batch get: stream %s: %v", o.key, err)
			return
		}
		streamed++
		// Counted where the bytes leave.
		recordObject(agg, prov, o.meta.Metadata, o.size, false, true)
		// The bandwidth chart is attributed one entry at a time, for the same
		// reason.
		bandwidthFromContext(r.Context()).record(bandwidthSample{
			module: projectOf(o.meta.Metadata, prov),
			bytes:  o.size,
		})
	}

	requested, anchors := len(req.Keys), 0
	if anchorsOnly {
		requested, anchors = 0, len(req.Keys)
	}
	found := len(opened) - nPrefetch
	batchRequestsTotal.Inc()
	batchKeysTotal.WithLabelValues("requested").Add(float64(requested))
	batchKeysTotal.WithLabelValues("found").Add(float64(found))
	batchKeysTotal.WithLabelValues("anchors").Add(float64(anchors))
	batchKeysTotal.WithLabelValues("prefetched").Add(float64(nPrefetch))
	batchKeysTotal.WithLabelValues("client_held").Add(float64(nHeld))
	batchKeysTotal.WithLabelValues("streamed").Add(float64(streamed))
	// A key this batch asked for that no entry answers is a miss for the project that asked.
	noteProjectMiss(prov, requested-found)
	// Attached to this request's own log line rather than printed as another
	// line about the same request.
	auditFromContext(r.Context()).note("batch_get requested=%d found=%d anchors=%d prefetched=%d client_held=%d streamed=%d",
		requested, found, anchors, nPrefetch, nHeld, streamed)
}

// handleBatchPut handles PUT /_batch/put. This endpoint accepts a tar of many
// objects in a SINGLE request holding a single admission slot (the whole point).
// This endpoint stores each member through the same path as a single PUT
// (storeOneObject: module-index refusal, write_once, audit xattrs, index
// append), returning a per-object result manifest.
//
// The request tar layout mirrors /_batch/get:
//
//	manifest.json — JSON {"entries":[{"key":...,"metadata":{...}}]} (earliest member)
//	data/<key> — the (already lz4-compressed) body for each entry, in manifest order
//
// A per-object store failure does NOT abort the batch: it is recorded as
// an "error" result and the remaining members are still processed.
func handleBatchPut(w http.ResponseWriter, r *http.Request, storage *Storage, maxObjectBytes int64, agg *logAggregator) {
	if r.Method != "PUT" {
		writeError(w, 405, "method_not_allowed", "method not allowed")
		return
	}

	audit := auditMapFromContext(r)

	// Bound the whole batch so a single request cannot exhaust memory/disk.
	maxBatchBytes := int64(maxBatchKeys) * maxObjectBytes
	body := http.MaxBytesReader(w, r.Body, maxBatchBytes)
	tr := tar.NewReader(body)

	// Earliest member MUST be manifest.json.
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
	// can pair the data members (read in stream order) with their metadata.
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

	// Stream the data members.
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

		// auditMapFromContext returns a fresh map each time it is called from the single-PUT path; here the request-level audit (uploader, IP, UA, timestamp) is shared across members.
		memberAudit := cloneAudit(audit)
		member := io.LimitReader(tr, maxObjectBytes)
		status, storeErr := storeOneObject(storage, p.entry.Key, member, p.entry.Metadata, memberAudit)
		p.result.Status = status
		switch status {
		case storeStatusStored:
			nStored++
			recordObject(agg, provenanceOf(r), p.entry.Metadata, hdr.Size, true, true)
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

	auditFromContext(r.Context()).note("batch_put entries=%d stored=%d dropped=%d conflict=%d error=%d",
		len(manifest.Entries), nStored, nDropped, nConflict, nError)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(batchPutResponse{Results: results})
}

// cloneAudit returns a shallow copy of the per-request audit map so each
// batch.
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

// tarCopyBufs supplies the copy buffer each member is streamed through.
var tarCopyBufs = sync.Pool{New: func() any {
	b := make([]byte, 64<<10)
	return &b
}}

// writeTarEntry writes a single tar member, copying exactly size bytes from r
// so the bytes written always match the declared header size (a tar invariant).
func writeTarEntry(tw *tar.Writer, name string, size int64, r io.Reader) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Size: size, Mode: 0644}); err != nil {
		return err
	}
	buf := tarCopyBufs.Get().(*[]byte)
	defer tarCopyBufs.Put(buf)
	n, err := io.CopyBuffer(tw, io.LimitReader(r, size), *buf)
	if err == nil && n != size {
		return io.ErrUnexpectedEOF
	}
	return err
}

// openedEntry is a batch member whose body is open and matches its stored digest.
type openedEntry struct {
	batchEntry
	f    *os.File
	size int64
}

// openVerifiedEntries opens each entry's body and checks it against the digest
// recorded with it, the same check the single-object GET makes. A body that
// vanished since its stat is left out. A body that no longer matches is left
// out and evicted, so the client reads it as a miss and the next upload
// replaces it. The caller owns the returned files.
func openVerifiedEntries(storage *Storage, entries []batchEntry) []openedEntry {
	return parallelFilter(entries, func(e batchEntry) (openedEntry, bool) {
		f, size, err := storage.OpenBody(e.key)
		if err != nil {
			return openedEntry{}, false
		}
		ok, verifyErr := storage.verifyStoredDigestFor(e.key, f, e.meta.Metadata)
		if verifyErr == nil && ok {
			return openedEntry{batchEntry: e, f: f, size: size}, true
		}
		f.Close()
		if verifyErr != nil {
			return openedEntry{}, false
		}
		storedDigestMismatchTotal.WithLabelValues("batch_get").Inc()
		log.Printf("stored digest: %q no longer hashes to the digest stored with it; evicting", e.key)
		if delErr := storage.Delete(e.key); delErr != nil && !errors.Is(delErr, ErrNotFound) {
			log.Printf("stored digest: evicting %q: %v", e.key, delErr)
		}
		return openedEntry{}, false
	})
}

// servableEntry stats key and applies the read guards. It refuses and evicts
// a stored module-index blob, and it repairs an object with no outputid in
// place. Bodies are not read here.
func servableEntry(storage *Storage, key string, prefetch bool) (batchEntry, bool) {
	meta, err := storage.Stat(key)
	if err != nil {
		if !prefetch && !errors.Is(err, ErrNotFound) {
			log.Printf("batch get: %s: %v", key, err)
		}
		return batchEntry{}, false
	}
	if evictModuleIndexOnReadByKey(storage, key, meta) {
		return batchEntry{}, false
	}
	if !ensureOutputID(storage, key, meta, nil) {
		return batchEntry{}, false
	}
	return batchEntry{key: key, meta: meta, prefetch: prefetch}, true
}

// buildPrefetchEntries returns the servable entries for the prefetch keys. The
// caller has already left out the keys the client holds.
func buildPrefetchEntries(storage *Storage, keys []string) []batchEntry {
	return parallelFilter(keys, func(key string) (batchEntry, bool) {
		return servableEntry(storage, key, true)
	})
}
