package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type S3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeS3Error(w http.ResponseWriter, httpStatus int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(httpStatus)
	xml.NewEncoder(w).Encode(S3Error{Code: code, Message: message})
}

func handleGetObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	// Open + stream rather than ReadFile + Write: the body is copied straight
	// from disk to the socket with a fixed-size buffer, so a large object never
	// becomes a large heap allocation. This is what keeps memory flat when many
	// GETs run concurrently.
	f, meta, err := storage.Open(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeS3Error(w, 404, "NoSuchKey", fmt.Sprintf("The specified key does not exist: %s", key))
			return
		}
		writeS3Error(w, 500, "InternalError", err.Error())
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
		w.Header().Set("X-Amz-Meta-"+name, v)
	}
	w.Header().Set("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(200)
	io.Copy(w, f)
}

func handlePutObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string, maxObjectBytes int64) {
	meta := make(map[string]string)
	for k, vals := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") {
			metaKey := strings.TrimPrefix(lk, "x-amz-meta-")
			meta[metaKey] = vals[0]
		}
	}

	audit := auditMapFromContext(r)

	// Cap a single upload and stream it straight to disk. PutStream never
	// buffers the whole body, so concurrent large PUTs no longer multiply into
	// the heap; MaxBytesReader bounds a runaway/oversized upload (413).
	body := http.MaxBytesReader(w, r.Body, maxObjectBytes)
	if err := storage.PutStream(key, body, meta, audit); err != nil {
		if errors.Is(err, ErrWriteOnceConflict) || errors.Is(err, ErrWriteOnceDuplicate) {
			writeS3Error(w, 409, "ConflictException", err.Error())
			return
		}
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeS3Error(w, 413, "EntityTooLarge", fmt.Sprintf("object exceeds max size of %d bytes", maxObjectBytes))
			return
		}
		writeS3Error(w, 500, "InternalError", err.Error())
		return
	}

	if a := auditFromContext(r.Context()); a != nil {
		a.Label = objectLabel(meta)
	}
	w.WriteHeader(200)
}

// handleDeleteObject removes a single object (S3 DeleteObject). It is the
// surgical eviction lever for a poisoned build-cache entry: delete the bad key
// and the next build recomputes and re-uploads the correct object. Like S3,
// DELETE is idempotent -- removing a missing key still reports success (204), so
// retries and races are harmless. Auth is enforced upstream in ServeHTTP, the
// same gate PUT goes through.
func handleDeleteObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		writeS3Error(w, 500, "InternalError", err.Error())
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
		writeS3Error(w, 500, "InternalError", "index unavailable")
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
