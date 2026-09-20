package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// writeError sends a native plain-text error response. The body is
// "<code>: <message>" and the short machine-readable code is repeated in the
// X-Cache-Error-Code header. This replaces the old S3-style XML <Error> body:
// the cache protocol is no longer S3-compatible, and the only client
// (go-toolchain) reads just the status code and logs the body, so the XML
// envelope bought nothing.
func writeError(w http.ResponseWriter, httpStatus int, code, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Cache-Error-Code", code)
	w.WriteHeader(httpStatus)
	fmt.Fprintf(w, "%s: %s\n", code, message)
}

func absentKeyOutcome(storage *Storage, key string) string {
	if h, ok := extractActionHash(key); ok && storage.Index != nil && storage.Index.Contains(h) {
		return "miss_advertised_unservable"
	}
	return "miss_not_found"
}

func handleGetObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string, agg *logAggregator) {
	// Open + stream rather than ReadFile + Write: the body is copied straight
	// from disk to the socket with a fixed-size buffer, so a large object never
	// becomes a large heap allocation. This is what keeps memory flat when many
	// GETs run concurrently.
	f, meta, err := storage.Open(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			getRequestsTotal.WithLabelValues(absentKeyOutcome(storage, key)).Inc()
			writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	defer f.Close()

	// Refuse + evict a stored Go module-index blob on read. This is orthogonal to
	// the outputid self-heal -- an index carries an outputid, so ensureOutputID
	// would happily pass it -- and must run earliest.
	switch evictModuleIndexOnRead(storage, key, f, meta) {
	case guardEvicted:
		getRequestsTotal.WithLabelValues("miss_module_index_evicted").Inc()
		writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
		return
	case guardPeekError:
		getRequestsTotal.WithLabelValues("miss_peek_error").Inc()
		writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
		return
	}

	// Self-heal: an object with no outputid metadata can never be a cache hit --
	// the client needs the outputid (the content address) to verify the body, and
	// without it discards the download and rebuilds -- yet its key stays in
	// /_index, so clients skip re-uploading it and every build that needs the
	// action takes a forced miss. These are leftovers from earlier cache-data
	// iterations or a data-dir move that stripped xattrs. Repair it in place:
	// reconstruct the outputid from the body (it IS sha256 of the decompressed
	// body) and persist it, so the object keeps its bytes + audit trail, stays in
	// /_index, and serves as a hit. If the body cannot be decompressed (and is
	// thus unusable by the client anyway), report a clean miss without deleting
	// anything -- the object is left for the normal eviction policy.
	if !ensureOutputID(storage, key, meta, f) {
		getRequestsTotal.WithLabelValues("miss_selfheal_failed").Inc()
		writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
		return
	}

	// A cacheprog key is verified by whoever reads it: the client hashes the
	// body against the outputid before it consumes it, so rot there costs a
	// single refused fetch. Any other key has no such reader, and this is the
	// only place its bytes are ever checked. The cost is a hash of the whole
	// body, paid on a path that serves the occasional arbitrary object rather
	// than a build's worth of them.
	if _, indexed := extractActionHash(key); !indexed {
		ok, verifyErr := verifyStoredDigest(f, meta.Metadata)
		switch {
		case verifyErr != nil:
			log.Printf("stored digest: cannot verify %q, so not serving it: %v", key, verifyErr)
			getRequestsTotal.WithLabelValues("miss_stored_digest").Inc()
			writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
			return
		case !ok:
			// The bytes changed after they were written. Nothing here can say
			// what they should be, so the object goes and the next uploader
			// replaces it.
			storedDigestMismatchTotal.WithLabelValues("get").Inc()
			log.Printf("stored digest: %q no longer hashes to the digest stored with it; evicting", key)
			if delErr := storage.Delete(key); delErr != nil && !errors.Is(delErr, ErrNotFound) {
				log.Printf("stored digest: evicting %q: %v", key, delErr)
			}
			getRequestsTotal.WithLabelValues("miss_stored_digest").Inc()
			writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
			return
		}
	}

	if a := auditFromContext(r.Context()); a != nil {
		a.Label = objectLabel(meta.Metadata)
	}
	getRequestsTotal.WithLabelValues("hit").Inc()
	prov := provenanceOf(r)
	recordObject(agg, prov, meta.Metadata, meta.Size, false, false)

	emitObjectHeaders(w, meta)
	w.WriteHeader(200)
	// The guards are done. What is left is a copy from the open file through a
	// fixed buffer, which holds no per-request memory, so it runs without an
	// admission slot.
	releaseSlot(r)
	// Stream the body, logging DISK-side failures. The status is already
	// written, so an error here truncates the response; the client's hash
	// check refuses the partial body, but without a log the server would be
	// silently serving from a failing disk. Read errors from f surface as
	// *fs.PathError; anything else is the peer going away mid-download, which
	// is normal and not logged. (f stays the direct copy source so the
	// ResponseWriter's ReadFrom/sendfile fast path remains available.)
	n, err := io.Copy(w, f)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			log.Printf("get %q: body read failed mid-copy (truncated response; check storage health): %v", key, err)
		}
	}
	// The bandwidth chart is a record of what crossed the wire, so it is
	// counted from the copy and not from the object's size on disk: a download a
	// client abandons half way sent half the bytes, and the build that fetches
	// the object again pays for the rest.
	bandwidthFromContext(r.Context()).record(bandwidthSample{
		module: projectOf(meta.Metadata, prov),
		bytes:  n,
	})
}

// emitObjectHeaders writes an object's user metadata under both the native
// and deprecated header prefixes, plus Last-Modified and Content-Length —
// the shared header surface of GET and HEAD responses.
func emitObjectHeaders(w http.ResponseWriter, meta *ObjectMeta) {
	for k, v := range meta.Metadata {
		// Capitalize earliest letter of metadata key
		name := k
		if len(name) > 0 {
			name = strings.ToUpper(name[:1]) + name[1:]
		}
		// Emit the native header, plus the deprecated S3-style header so that
		// not-yet-upgraded clients (which read X-Amz-Meta-*) still get the
		// outputid and keep hitting the cache. The legacy header is dropped at
		// the repository rename.
		w.Header().Set("X-Cache-Meta-"+name, v)
		w.Header().Set("X-Amz-Meta-"+name, v)
	}
	w.Header().Set("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
}

// handleHeadObject answers HEAD for a single object: the exact header surface
// a GET would emit (both metadata prefixes, Last-Modified, Content-Length)
// with no body. It is the cheap inspection endpoint — "does this key exist,
// how big is it, what metadata does it carry" — backed by Stat only: no body
// read, no module-index probe, no self-heal, and no last-access stamp, so
// inspecting a key never mutates cache state or extends its LRU lifetime.
func handleHeadObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	meta, err := storage.Stat(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	emitObjectHeaders(w, meta)
	w.WriteHeader(200)
}

func handlePutObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string, maxObjectBytes int64, agg *logAggregator) {
	meta := make(map[string]string)
	// Native metadata headers earliest.
	for k, vals := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, nativeMetaPrefix) {
			meta[strings.TrimPrefix(lk, nativeMetaPrefix)] = vals[0]
		}
	}
	// Deprecated S3-style headers, filling only keys the native headers did not
	// supply (native wins). Their presence flags a not-yet-upgraded client.
	usedLegacyMeta := false
	for k, vals := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, legacyMetaPrefix) {
			usedLegacyMeta = true
			metaKey := strings.TrimPrefix(lk, legacyMetaPrefix)
			if _, ok := meta[metaKey]; !ok {
				meta[metaKey] = vals[0]
			}
		}
	}
	if usedLegacyMeta {
		noteDeprecatedS3Meta(r)
	}

	audit := auditMapFromContext(r)

	// Cap a single upload and stream it straight to disk.
	body := http.MaxBytesReader(w, r.Body, maxObjectBytes)

	// storeOneObject does the peek-refuse + write_once + store work shared with
	// the batch path (see its doc); map its status/error to the single-PUT HTTP
	// contract here.
	status, err := storeOneObject(storage, key, body, meta, audit)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, 413, "too_large", fmt.Sprintf("object exceeds max size of %d bytes", maxObjectBytes))
			return
		}
		// The uploader's own digest says these are not the bytes it meant to
		// send. Naming it as the client's request, not this server's fault, is
		// what makes a retry the obvious answer.
		if errors.Is(err, ErrStoredDigestMismatch) {
			writeError(w, 400, "invalid_request", err.Error())
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	switch status {
	case storeStatusConflict:
		// A write_once conflict.
		writeError(w, 409, "conflict", "object already exists with different content")
		return
	case storeStatusStored:
		if a := auditFromContext(r.Context()); a != nil {
			a.Label = objectLabel(meta)
		}
		// A chunked upload declares no length.
		recordObject(agg, provenanceOf(r), meta, max(r.ContentLength, 0), true, false)
	}
	w.WriteHeader(200)
}

// store status values reported by storeOneObject, mirrored in the batch
// response manifest. They classify the outcome WITHOUT deciding an HTTP status
// -- the single-PUT caller maps them to a status code, the batch caller records
// them per object.
const (
	storeStatusStored   = "stored"   // written to disk + index
	storeStatusDropped  = "dropped"  // a Go module index: accepted but not stored
	storeStatusConflict = "conflict" // write_once conflict: accepted, not overwritten
	storeStatusError    = "error"    // an I/O or store failure for this object
)

// storeOneObject is the shared store path used by BOTH handlePutObject (a single
// object per request) and handleBatchPut (many objects in a single tar).
// Factoring it out keeps the module-index refusal, write_once handling, and
// audit/index bookkeeping identical across both endpoints so they cannot drift.
//
// It peeks a bounded but block-sized prefix -- enough to cover a real index's
// earliest lz4 block, since the magic only decodes a single time the whole
// earliest block is present (a fixed 512-byte peek truncated the single-block
// bodies the client sends and missed every real index; see modindex.go). The
// peeked bytes are stitched back in front of the unread rest so a non-index body
// is still stored intact and large bodies keep streaming. A dropped index is a
// no-op for the client (it recomputes the index locally on the resulting miss),
// so it is reported as a clean "dropped", not an error.
//
// The peek read SELF-SIZES to the bytes actually present rather than
// pre-allocating the full indexPutPeekBytes cap on every call. LimitReader
// keeps the detection bytes identical to the old cap; the cap stays generous so
// a real index's earliest block is always covered.
//
// Returns (status, err). On a write_once conflict it returns (conflict, nil) --
// an accepted, non-error outcome the caller classifies. On any other store
// failure it returns (error, err) so the caller can surface the error (single
// PUT) or record it per object and continue (batch).
func storeOneObject(storage *Storage, key string, body io.Reader, meta, audit map[string]string) (string, error) {
	peek, peekErr := io.ReadAll(io.LimitReader(body, int64(indexPutPeekBytes)))
	if peekErr != nil {
		return storeStatusError, peekErr
	}
	if looksLikeGoModuleIndex(peek, meta["compression"]) {
		// Count + log the refusal. Clients build module indexes all the time,
		// so an occasionally-nonzero counter during CI activity is the live
		// proof the guard works; refusals are rare enough (the client-side
		// guard blocks most uploads earliest) that a per-event log line is
		// cheap and names the offending key for forensics. Living here, the
		// counter covers BOTH the single-PUT and the batch-put refusal paths.
		putRefusalsTotal.WithLabelValues("module_index").Inc()
		log.Printf("put guard: refused module-index upload for %q (accepted, stored nothing; client recomputes locally)", key)
		// Drain any remaining bytes so a streaming writer (the single-PUT client)
		// completes cleanly; the batch caller passes a bounded per-member reader,
		// for which this is a cheap no-op a single time the member is consumed.
		io.Copy(io.Discard, body)
		return storeStatusDropped, nil
	}

	// Not an index: stitch the peeked prefix back in front of the unread rest.
	full := io.MultiReader(bytes.NewReader(peek), body)
	if err := storage.PutStream(key, full, meta, audit); err != nil {
		if errors.Is(err, ErrWriteOnceConflict) || errors.Is(err, ErrWriteOnceDuplicate) {
			return storeStatusConflict, nil
		}
		return storeStatusError, err
	}
	return storeStatusStored, nil
}

// handleDeleteObject removes a single object. It is the surgical eviction lever
// for a poisoned build-cache entry: delete the bad key and the next build
// recomputes and re-uploads the correct object. Auth is enforced upstream in
// ServeHTTP, the same gate PUT goes through.
func handleDeleteObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	w.WriteHeader(204)
}

// handleGetIndex serves the precomputed GBCI v1 binary cache-key index.
// The body is a fixed 24-byte header + sorted action-ID hashes + 32-byte
// SHA-256 trailer.
func handleGetIndex(w http.ResponseWriter, r *http.Request, idx *Index) {
	if idx == nil {
		writeError(w, 500, "internal_error", "index unavailable")
		return
	}
	blob, etag := idx.Blob()
	// The blob is shared by every request and already built, so the transfer
	// holds no memory of its own. It is tens of megabytes, and a CI runner can
	// take minutes to pull it, so it goes out without an admission slot.
	releaseSlot(r)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etag)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(blob))
	// An index fetch is a series of its own: the blob belongs to no module (a
	// whole fleet reads the same one), so its bytes are accounted for on their
	// own and never against the module of the key that asked for it. A
	// conditional request answered 304 puts no body on the wire, so there is
	// nothing to count for it.
	bandwidthFromContext(r.Context()).record(bandwidthSample{
		index: true,
		bytes: servedBodyBytes(w, int64(len(blob))),
	})
}

// servedBodyBytes is what the response body actually carried, when the writer
// kept count of it: the server's own statusRecorder always does, which is what
// tells a full index download apart from a 304 that carried none. A writer that
// keeps no count (a handler driven directly in a test) is taken to have sent the
// body it was handed.
func servedBodyBytes(w http.ResponseWriter, body int64) int64 {
	if rec, ok := w.(*statusRecorder); ok {
		return rec.bytesWritten.Load()
	}
	return body
}

// objectLabel builds a short human-readable description of a cache entry from its stored
// metadata, e.g.
func objectLabel(meta map[string]string) string {
	objType := meta["object-type"]
	if objType == "" {
		return ""
	}
	pkg := meta["pkg"]
	src := meta["src"]
	goVer := meta["go-version"]
	target := meta["target"]

	label := objType
	if pkg != "" {
		label += " " + pkg
	}
	if src != "" {
		label += " (" + src + ")"
	}
	if goVer != "" && target != "" {
		label += " " + goVer + " " + target
	}
	return label
}

// auditMapFromContext converts per-request audit info into a flat map that
// storage.Put can persist as extended attributes on the uploaded object.
// These fields answer: who uploaded this, when, from where, and with what
// client — the data needed to investigate a suspected compromise.
// content_length is filled in by storage.PutStream from the actual number of
// bytes streamed to disk, since the size is not known up front when the body is
// not buffered.
func auditMapFromContext(r *http.Request) map[string]string {
	a := auditFromContext(r.Context())
	if a == nil {
		return nil
	}
	m := map[string]string{
		"uploader":    a.Username,
		"uploaded_at": a.Timestamp.UTC().Format(time.RFC3339Nano),
		"client_ip":   a.ClientIP,
		"user_agent":  a.UserAgent,
	}
	return m
}
