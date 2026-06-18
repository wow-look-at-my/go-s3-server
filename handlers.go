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

	// Self-heal: an object with no outputid metadata can never be a cache hit --
	// the client needs the outputid (the content address) to verify the body, and
	// without it discards the download and rebuilds -- yet its key stays in
	// /_index, so clients skip re-uploading it and every build that needs the
	// action takes a forced miss. These are leftovers from earlier cache-data
	// iterations or a data-dir move that stripped xattrs. Evict it (which also
	// drops the key from /_index) and return a clean 404, so the next PUT
	// repopulates a correct object instead of the client tripping over a 200 it
	// cannot use. Close the handle first so Delete can unlink on Windows too.
	if missingOutputID(meta) {
		f.Close()
		selfHeal(storage, key)
		writeError(w, 404, "not_found", fmt.Sprintf("the specified key does not exist: %s", key))
		return
	}
	defer f.Close()

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

	// Refuse Go module-index blobs (see looksLikeGoModuleIndex): they cannot be
	// verified against their key and a mis-keyed one poisons every consumer's
	// build, so this shared cache must never hold one. Peek only the leading
	// bytes (cheap; the magic is at the very start), then stream the rest. A
	// dropped index is a no-op for the client -- it recomputes the index locally
	// on the resulting miss -- so we report success rather than an error.
	prefix := make([]byte, indexPeekBytes)
	n, peekErr := io.ReadFull(body, prefix)
	prefix = prefix[:n]
	if peekErr != nil && peekErr != io.EOF && peekErr != io.ErrUnexpectedEOF {
		// A MaxBytesReader overflow (or other read error) on the very first read.
		var maxErr *http.MaxBytesError
		if errors.As(peekErr, &maxErr) {
			writeError(w, 413, "too_large", fmt.Sprintf("object exceeds max size of %d bytes", maxObjectBytes))
			return
		}
		writeError(w, 500, "internal_error", peekErr.Error())
		return
	}
	if looksLikeGoModuleIndex(prefix, meta["compression"]) {
		io.Copy(io.Discard, body) // drain so the client's write completes cleanly
		w.WriteHeader(200)
		return
	}

	// Not an index: stitch the peeked prefix back in front of the unread rest.
	full := io.MultiReader(bytes.NewReader(prefix), body)
	if err := storage.PutStream(key, full, meta, audit); err != nil {
		if errors.Is(err, ErrWriteOnceConflict) || errors.Is(err, ErrWriteOnceDuplicate) {
			writeError(w, 409, "conflict", err.Error())
			return
		}
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, 413, "too_large", fmt.Sprintf("object exceeds max size of %d bytes", maxObjectBytes))
			return
		}
		writeError(w, 500, "internal_error", err.Error())
		return
	}

	if a := auditFromContext(r.Context()); a != nil {
		a.Label = objectLabel(meta)
	}
	w.WriteHeader(200)
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
