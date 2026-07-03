package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

func handleGetObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	// Open + stream rather than ReadFile + Write: the body is copied straight
	// from disk to the socket with a fixed-size buffer, so a large object never
	// becomes a large heap allocation. This is what keeps memory flat when many
	// GETs run concurrently.
	f, meta, err := storage.Open(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	defer f.Close()

	// Refuse + evict a stored Go module-index blob on read. The PutObject guard
	// only blocks NEW indexes; one already on disk (uploaded before that guard, or
	// surviving because the one-time v3 startup purge already ran) would otherwise
	// be served verbatim here and re-advertised in /_index forever, and the client
	// -- which never re-uploads an index and has no remote DELETE -- refuses and
	// re-fetches it on every build. evictModuleIndexOnRead detects it, evicts it
	// (dropping the file and the /_index entry), and signals a miss; the peek is
	// non-destructive (it rewinds f), so the non-index serve path below reads the
	// body from byte 0 unchanged. This is orthogonal to the outputid self-heal --
	// an index carries an outputid, so ensureOutputID would happily pass it -- and
	// must run first. A miss recomputes the index locally on the client, so 404
	// (the normal not-found path) is exactly right.
	if evictModuleIndexOnRead(storage, key, f, meta) {
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
	if !ensureOutputID(storage, key, meta) {
		writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
		return
	}

	if a := auditFromContext(r.Context()); a != nil {
		a.Label = objectLabel(meta.Metadata)
	}

	for k, v := range meta.Metadata {
		// Capitalize first letter of metadata key
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
	w.WriteHeader(200)
	io.Copy(w, f)
}

func handlePutObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string, maxObjectBytes int64) {
	meta := make(map[string]string)
	// Native metadata headers first.
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

	// Cap a single upload and stream it straight to disk. PutStream never
	// buffers the whole body, so concurrent large PUTs no longer multiply into
	// the heap; MaxBytesReader bounds a runaway/oversized upload (413).
	body := http.MaxBytesReader(w, r.Body, maxObjectBytes)

	// storeOneObject does the peek-refuse + write_once + store work shared with
	// the batch path (see its doc); map its status/error to the single-PUT HTTP
	// contract here.
	status, err := storeOneObject(storage, key, body, meta, audit)
	if err != nil {
		// A MaxBytesReader overflow surfaces as a *http.MaxBytesError from either
		// the peek read or the streamed copy: report it as 413, the single-PUT
		// over-limit contract.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, 413, "too_large", fmt.Sprintf("object exceeds max size of %d bytes", maxObjectBytes))
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	switch status {
	case storeStatusConflict:
		// A write_once conflict. Report it as 409 (the single-PUT contract); the
		// batch path instead records "conflict" as an accepted per-object result.
		writeError(w, 409, "conflict", "object already exists with different content")
		return
	case storeStatusStored:
		if a := auditFromContext(r.Context()); a != nil {
			a.Label = objectLabel(meta)
		}
	}
	// stored and dropped (module index) are both 200/no-error to the client: a
	// dropped index is a no-op (the client recomputes it locally on the miss).
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

// storeOneObject is the shared store path used by BOTH handlePutObject (one
// object per request) and handleBatchPut (many objects in one tar). Factoring it
// out keeps the module-index refusal, write_once handling, and audit/index
// bookkeeping identical across the two endpoints so they cannot drift.
//
// It refuses Go module-index blobs (see looksLikeGoModuleIndex): they cannot be
// verified against their key and a mis-keyed one poisons every consumer's build,
// so this shared cache must never hold one. It peeks a bounded but block-sized
// prefix -- enough to cover a real index's first lz4 block, since the magic only
// decodes once the whole first block is present (a fixed 512-byte peek truncated
// the single-block bodies the client sends and missed every real index; see
// modindex.go). The peeked bytes are stitched back in front of the unread rest
// so a non-index body is still stored intact and large bodies keep streaming. A
// dropped index is a no-op for the client (it recomputes the index locally on
// the resulting miss), so it is reported as a clean "dropped", not an error.
//
// The peek read SELF-SIZES to the bytes actually present rather than
// pre-allocating the full indexPutPeekBytes cap on every call. io.ReadAll grows
// its buffer from the body's real size (a typical ~8 KiB object allocates ~8-16
// KiB), while io.LimitReader caps the worst case (a large body) at exactly
// indexPutPeekBytes -- identical detection input, but without the per-PUT 1 MiB
// allocation. That fixed cap-sized allocation was a measured regression: ~1 MiB
// churned per PUT (vs ~body size) made a CI burst of ~7000 PUTs thrash GC and
// saturate the admission-control sem, shedding PUTs with 503 so nothing got
// stored/indexed and the next build saw an empty /_index (hits=0). LimitReader
// keeps the detection bytes identical to the old cap; the cap stays generous so
// a real index's first block is always covered.
//
// Returns (status, err). On a write_once conflict it returns (conflict, nil) --
// an accepted, non-error outcome the caller classifies. On any other store
// failure it returns (error, err) so the caller can surface the error (single
// PUT) or record it per object and continue (batch). A returned err may wrap a
// *http.MaxBytesError when body is a bounded reader; the caller decides whether
// that maps to 413.
func storeOneObject(storage *Storage, key string, body io.Reader, meta, audit map[string]string) (string, error) {
	peek, peekErr := io.ReadAll(io.LimitReader(body, int64(indexPutPeekBytes)))
	if peekErr != nil {
		return storeStatusError, peekErr
	}
	if looksLikeGoModuleIndex(peek, meta["compression"]) {
		// Drain any remaining bytes so a streaming writer (the single-PUT client)
		// completes cleanly; the batch caller passes a bounded per-member reader,
		// for which this is a cheap no-op once the member is consumed.
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
// recomputes and re-uploads the correct object. DELETE is idempotent --
// removing a missing key still reports success (204), so retries and races are
// harmless. Auth is enforced upstream in ServeHTTP, the same gate PUT goes
// through.
func handleDeleteObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		writeError(w, 500, "internal_error", err.Error())
		return
	}
	w.WriteHeader(204)
}

// handleGetIndex serves the precomputed GBCI v1 binary cache-key index.
// The body is a fixed 24-byte header + sorted action-ID hashes + 32-byte
// SHA-256 trailer. The strong ETag is the hex-encoded trailer; conditional
// GETs (If-None-Match) are handled by http.ServeContent and return 304.
func handleGetIndex(w http.ResponseWriter, r *http.Request, idx *Index) {
	if idx == nil {
		writeError(w, 500, "internal_error", "index unavailable")
		return
	}
	blob, etag := idx.Blob()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etag)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(blob))
}

// objectLabel builds a short human-readable description of a cache entry
// from its stored metadata, e.g. "go-archive github.com/foo/bar (bar.go) go1.24.0 linux/amd64".
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
