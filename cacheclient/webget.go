package cacheclient

import (
	"bytes"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// getIndividual fetches a single object stored under an individual cache key.
// It is the fallback sendBatch uses against a server with no batch endpoint.
func (b *WebBackend) getIndividual(actionID, key string) (string, io.ReadCloser, int64, time.Time, bool, error) {
	r := b.getIndividualEntry(actionID, key)
	if r.miss {
		return "", nil, 0, time.Time{}, true, nil
	}
	return r.outputID, r.body, r.size, r.t, false, r.err
}

// getObjectResult is one object fetched from the remote: the verified body
// plus the metadata the server stored with it, or miss=true with the rest
// zeroed. An error that made the object unusable is reported as a miss with
// err set (the caller treats a miss as a local recompute either way).
type getObjectResult struct {
	outputID string
	body     io.ReadCloser
	size     int64
	t        time.Time
	meta     map[string]string
	miss     bool
	err      error
}

// getIndividualEntry is getIndividual returning the object's metadata map as
// well, for the caller that has to know whether the body is an executable
// (see GetExecutable, exeNameFromMeta).
func (b *WebBackend) getIndividualEntry(actionID, key string) getObjectResult {

	miss := func() getObjectResult { return getObjectResult{miss: true} }

	req, err := http.NewRequest("GET", b.url(key), nil)
	if err != nil {
		return miss()
	}
	b.signRequest(req)

	b.Pool.Acquire()
	httpStart := time.Now()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		b.MissNetwork.Increment()
		logging.Warnf("cacheprog: web get %s: %v", ShortID(actionID), err)
		return miss()
	}

	if resp.StatusCode == 404 {
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTP404.Increment()
		// Drop the stale index claim so the PUT path re-uploads; otherwise the key 404s forever.
		b.reclaimAbsent(key)
		return miss()
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTPError.Increment()
		b.errLog.Record("web get", resp.StatusCode, actionID, string(respBody))
		return miss()
	}

	// Fall back to the deprecated S3-style header for a cache server that predates X-Cache-Meta-Outputid.
	outputID := resp.Header.Get("X-Cache-Meta-Outputid")
	if outputID == "" {
		outputID = resp.Header.Get("X-Amz-Meta-Outputid")
	}
	if outputID == "" {
		resp.Body.Close()
		b.Pool.Release()
		b.MissNoOutputID.Increment()
		logging.Warnf("cacheprog: web get %s: missing outputid metadata", ShortID(actionID))
		return miss()
	}

	compressed, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	b.Pool.Release()
	if b.Latency != nil {
		b.Latency.HTTPGet.Record(time.Since(httpStart))
	}
	if err != nil {
		b.MissReadBody.Increment()
		logging.Warnf("cacheprog: web get %s: read body: %v", ShortID(actionID), err)
		return miss()
	}

	decompressStart := time.Now()
	decompressed, err := Decompress(compressed)
	if b.Latency != nil {
		b.Latency.Decompress.Record(time.Since(decompressStart))
	}
	if err != nil {
		b.MissDecompress.Increment()
		logging.Warnf("cacheprog: web get %s: decompress: %v", ShortID(actionID), err)
		return miss()
	}

	// Integrity check: the body must hash to its advertised outputID. A mismatch means the remote object is
	// corrupt (truncated, poisoned, or rotted), which would feed cmd/go a damaged object. Refuse to serve and
	// evict the key so the next recompute re-uploads it clean.
	if got, ok := OutputIDMatches(outputID, decompressed); !ok {
		b.MissChecksum.Increment()
		b.Stats.Corrupt.Increment()
		b.removeClaimed(key)
		logging.Warnf("cacheprog: web get %s: body checksum mismatch (want outputid=%s, got sha256=%s, len=%d); evicting and treating as miss",
			ShortID(actionID), ShortID(outputID), ShortID(got), len(decompressed))
		return miss()
	}

	// Cross-contamination guard: a compiled package self-certifies its action key in its build id. A body
	// whose build id belongs to a different action is a poisoned mapping the hash check cannot catch (e.g.
	// reflectlite served for `runtime`, surfacing as "imported as reflectlite"). Refuse it and evict the key.
	if act, ok := BuildIDMatchesAction(actionID, decompressed); !ok {
		b.MissBuildID.Increment()
		b.Stats.Corrupt.Increment()
		b.removeClaimed(key)
		logging.Warnf("cacheprog: web get %s: build-id action mismatch (want action=%s, got action=%s, len=%d); evicting and treating as miss",
			ShortID(actionID), ExpectedBuildIDAction(actionID), act, len(decompressed))
		return miss()
	}

	// Module-index guard: a Go module index blob self-certifies neither its outputID nor its build id, so a
	// wrong index is silently fatal at package load ("corrupt index"). Refuse it and let cmd/go recompute
	// locally; evict the claim so the recompute is free to re-Put.
	if IsGoModuleIndex(decompressed) {
		b.MissModuleIndex.Increment()
		b.removeClaimed(key)
		logging.Warnf("cacheprog: web get %s: refusing module-index blob (unverifiable under this key, len=%d); treating as miss",
			ShortID(actionID), len(decompressed))
		return miss()
	}

	t := time.Now()
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if parsed, parseErr := time.Parse(http.TimeFormat, lm); parseErr == nil {
			t = parsed
		}
	}

	// The server round-trips user metadata on the single-object path as
	// X-Cache-Meta-* headers (see metadataHeaders, the inverse mapping).
	meta := map[string]string{}
	for name, vals := range resp.Header {
		const prefix = "X-Cache-Meta-"
		if len(vals) == 0 || len(name) < len(prefix) || !strings.EqualFold(name[:len(prefix)], prefix) {
			continue
		}
		meta[strings.ToLower(name[len(prefix):])] = vals[0]
	}

	b.Stats.Hits.Increment()
	return getObjectResult{
		outputID: outputID,
		body:     io.NopCloser(bytes.NewReader(decompressed)),
		size:     int64(len(decompressed)),
		t:        t,
		meta:     meta,
	}
}

// exeNameFromMeta reads the exe-name metadata an uploader stamped via
// PutExecutable, sanitizing it for use as a file name: the cache server is a
// trust boundary, and cmd/go turns this into the final path component of a
// file it will fork/exec. The name is absent for ordinary objects -- "" is the
// answer then, and the caller stores the body as plain data.
func exeNameFromMeta(meta map[string]string) string {
	name := meta["exe-name"]
	if name == "" {
		return ""
	}
	// Base() also strips Windows separators, which filepath.Base alone does not.
	name = path.Base(filepath.FromSlash(name))
	if name == "." || name == ".." || name == "/" || name == `\` {
		return ""
	}
	return name
}

// getBatchWithMeta enqueues this key on the coalescer and waits for the result.
// Multiple concurrent callers funnel into the same outgoing HTTP request
// instead of each making their own — see batchCoalescer / sendBatch. The
// reply carries the object's metadata so the caller can tell an executable
// from ordinary data (GetExecutable).
func (b *WebBackend) getBatchWithMeta(actionID, key string) (string, io.ReadCloser, int64, time.Time, map[string]string, bool, error) {
	respCh := make(chan batchResp, 1)
	select {
	case b.batchReqCh <- batchReq{actionID: actionID, key: key, resp: respCh}:
	case <-b.batchStop:
		// Backend is closing — return miss so the caller can fall back.
		return "", nil, 0, time.Time{}, nil, true, nil
	}
	select {
	case r := <-respCh:
		return r.outputID, r.body, r.size, r.t, r.meta, r.miss, nil
	case <-b.batchDone:
		// Shutdown raced the enqueue: use the buffered reply if sendBatch already produced it, else degrade to a miss.
		select {
		case r := <-respCh:
			return r.outputID, r.body, r.size, r.t, r.meta, r.miss, nil
		default:
			return "", nil, 0, time.Time{}, nil, true, nil
		}
	}
}

// removeClaimed removes a key that was optimistically added to the index
// when the upload fails, so it can be retried on the next attempt.
func (b *WebBackend) removeClaimed(key string) {
	b.keysMu.Lock()
	b.keys.Remove(key)
	b.keysMu.Unlock()
}
