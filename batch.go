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

	"github.com/wow-look-at-my/go-containers/set"
)

// batchGetRequest is the JSON body for POST /_batch/get.
type batchGetRequest struct {
	Keys     []string `json:"keys"`
	Prefetch bool     `json:"prefetch"` // include temporally related entries
	// PrefetchOnly answers with the window around Keys and none of the Keys
	// themselves. A look-ahead client names keys it already holds purely to say
	// where in the store's time order to look, so streaming those bodies back
	// would spend the whole request re-sending what the caller has.
	PrefetchOnly bool `json:"prefetch_only"`
	// Have is what the client says it already holds. The window leaves those
	// keys out. An absent filter is a client that states nothing, and it is
	// sent the full window. See havefilter.go.
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
// metadata holds the same values a single PUT carries in X-Cache-Meta-<Name>
// headers, keyed by the lowercased meta name WITHOUT the prefix (e.g.
// "outputid", "compression", "size"); they are stored exactly as
// handlePutObject stores native metadata (user.s3.* xattrs).
type batchPutManifestEntry struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// batchPutManifest is the earliest member ("manifest.json") of a /_batch/put tar.
type batchPutManifest struct {
	Entries []batchPutManifestEntry `json:"entries"`
}

// batchPutResult is a single entry in the JSON response, a single per manifest
// key, in manifest order. Status is any of storeStatus* (stored|dropped|conflict|error).
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
// of large objects is never materialized in the heap at the same time.
type batchEntry struct {
	key      string
	meta     *ObjectMeta
	prefetch bool
}

// maxBatchKeys caps how many keys a single batch request may ask for.
const maxBatchKeys = 4096

// prefetchWindow is the time window around requested entries within which
// other entries are considered related and included as prefetch.
const prefetchWindow = 30 * time.Second

// maxPrefetchEntries caps how many extra entries the server will include
// beyond what was explicitly requested.
const maxPrefetchEntries = 200

// handleBatchGet handles GET and POST /_batch/get requests. The client sends a
// JSON list of keys it needs, and the server responds with a tar stream
// containing the data and metadata for each found entry. POST is the
// semantically sound method (the request carries a body; GET-with-a-body is
// hostile to proxies and caches); GET remains accepted for existing clients.
//
// If the server's prefetch config (prefetchEnabled) and the request both ask
// for it, the server also includes entries whose modification time falls
// within 30s either side of the requested entries, capturing entries from the same build
// that the client is likely to need next. The request's own Have filter says
// which of those the client already holds, and those are left out, so the
// window keeps moving instead of the same 200-entry pool arriving on every
// request. The server keeps no record of any of this. With prefetchEnabled
// false the client's prefetch and prefetch_only flags are ignored: a batch
// carries only the requested keys, and a prefetch_only request gets an empty
// manifest.
//
// The tar layout is.
//
//	manifest.json — index of all entries with metadata data/<key> — raw
//	file content for each entry
func handleBatchGet(w http.ResponseWriter, r *http.Request, storage *Storage, agg *logAggregator, prefetchEnabled bool) {
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

	prov := provenanceOf(r)

	// lookup is the keys whose bodies this response carries. With prefetch
	// off, a prefetch_only request wants nothing but the window, so it carries
	// none; every other request carries exactly what it asked for.
	lookup := req.Keys
	// A prefetch_only request asks for no key. Its keys are look-ahead anchors,
	// which the client has already hit, so they count as anchors and never
	// as requested or missed.
	anchorsOnly := req.PrefetchOnly
	if !prefetchEnabled {
		if req.PrefetchOnly {
			lookup = nil
		}
		req.Prefetch, req.PrefetchOnly = false, false
	}

	// Stat is cheap (os.Stat + xattrs); the bodies are streamed later, a single
	// at a time, so the whole batch never sits in memory.
	var entries []batchEntry
	requestedSet := set.New[string](len(lookup))
	var minMod, maxMod time.Time

	for _, key := range lookup {
		requestedSet.Add(key)
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

	// Prefetch: find related keys by modification time proximity, and let the
	// index skip what the request says the client holds AS IT SELECTS.
	// Skipping during selection is what keeps the window moving: filtering the
	// result afterwards handed back the same nearest maxPrefetchEntries
	// candidates on every request, so a single time a client had received them
	// it got prefetched=0 for the rest of its build. The skip is a few bit
	// tests against the request's own filter, and it runs before the per-key
	// stat, guard and heal work, so a rejected candidate never costs a file
	// open or an lz4 block decode.
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
			// The requested keys were the anchor, not the ask. They still had to
			// be stat'ed to find the window, and they still set it, but only the
			// window is sent.
			entries = prefetched
		} else {
			entries = append(entries, prefetched...)
		}
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

	// Manifest earliest.
	manifestData, _ := json.Marshal(manifest)
	if err := writeTarEntry(tw, "manifest.json", int64(len(manifestData)), bytes.NewReader(manifestData)); err != nil {
		log.Printf("batch get: write manifest: %v", err)
		return
	}

	// Only a single body is in flight at a time (an io.Copy-sized buffer), so a
	// batch of hundreds of large objects no longer materializes hundreds of
	// bodies in the heap — the change that keeps the server within its memory
	// budget under the concurrent CI matrix load that previously OOM-killed it.
	var streamed int
	for _, e := range entries {
		// The size still comes from the open fd.
		f, size, err := storage.OpenBody(e.key)
		if err != nil {
			// Vanished between stat and stream (e.g. operator eviction). Skip it:
			// the client matches data entries by name and treats a missing a
			// single as a cache miss, so omitting it is safe.
			continue
		}
		// Same check the single-object GET makes: the digest recorded with the
		// body decides whether these bytes are the ones that were stored.
		if ok, verifyErr := verifyStoredDigest(f, e.meta.Metadata); verifyErr != nil || !ok {
			f.Close()
			if verifyErr == nil {
				storedDigestMismatchTotal.WithLabelValues("batch_get").Inc()
				log.Printf("stored digest: %q no longer hashes to the digest stored with it; evicting", e.key)
				if delErr := storage.Delete(e.key); delErr != nil && !errors.Is(delErr, ErrNotFound) {
					log.Printf("stored digest: evicting %q: %v", e.key, delErr)
				}
			}
			continue
		}
		err = writeTarEntry(tw, "data/"+e.key, size, f)
		f.Close()
		if err != nil {
			// A write error here is almost always the client going away mid-stream;
			// stop rather than spin through the rest of the batch.
			log.Printf("batch get: stream %s: %v", e.key, err)
			return
		}
		streamed++
		// Counted where the bytes actually leave: an entry that vanished or
		// failed above never reached the client and must not appear in the
		// rate.
		recordObject(agg, prov, e.meta.Metadata, size, false, true)
		// The bandwidth chart is attributed one entry at a time, for the same
		// reason: a batch carries entries from as many modules as the build
		// touches, so the response as a whole has no single module to charge.
		bandwidthFromContext(r.Context()).record(bandwidthSample{
			module: projectOf(e.meta.Metadata, prov),
			bytes:  size,
		})
	}

	requested, anchors := len(req.Keys), 0
	if anchorsOnly {
		requested, anchors = 0, len(req.Keys)
	}
	found := len(entries) - nPrefetch
	batchRequestsTotal.Inc()
	batchKeysTotal.WithLabelValues("requested").Add(float64(requested))
	batchKeysTotal.WithLabelValues("found").Add(float64(found))
	batchKeysTotal.WithLabelValues("anchors").Add(float64(anchors))
	batchKeysTotal.WithLabelValues("prefetched").Add(float64(nPrefetch))
	batchKeysTotal.WithLabelValues("client_held").Add(float64(nHeld))
	batchKeysTotal.WithLabelValues("streamed").Add(float64(streamed))
	// A key this batch asked for that no entry answers is a miss for the
	// project that asked. Prefetched entries answer nothing that was asked
	// for, so they are excluded from the found count here as they are above.
	noteProjectMiss(prov, requested-found)
	// Attached to this request's own log line rather than printed as another
	// line about the same request.
	auditFromContext(r.Context()).note("batch_get requested=%d found=%d anchors=%d prefetched=%d client_held=%d streamed=%d",
		requested, found, anchors, nPrefetch, nHeld, streamed)
}

// handleBatchPut handles PUT /_batch/put. This endpoint accepts a tar of many
// objects in a SINGLE request holding a single admission slot (the whole point),
// and stores each member through the same path as a single PUT (storeOneObject:
// module-index refusal, write_once, audit xattrs, index append), returning a
// per-object result manifest.
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

	// Bound the whole batch so a single request cannot exhaust memory/disk. Each
	// member is additionally bounded to maxObjectBytes below via a per-member
	// LimitReader.
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
	// per-member reader (maxObjectBytes) so a single oversized member cannot
	// blow the budget, and the tar reader bounds reads to the current member anyway.
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

// tarCopyBufs supplies the copy buffer each member is streamed through. The
// tar writer implements neither ReaderFrom nor WriterTo, so an explicit
// buffer is what gets used.
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
