package cacheclient

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

// getIndividual fetches a single object stored under an individual cache key.
// It is the fallback sendBatch uses against a server with no batch endpoint.
func (b *WebBackend) getIndividual(actionID, key string) (string, io.ReadCloser, int64, time.Time, bool, error) {

	req, err := http.NewRequest("GET", b.url(key), nil)
	if err != nil {
		return "", nil, 0, time.Time{}, true, nil
	}
	b.signRequest(req)

	b.Pool.Acquire()
	httpStart := time.Now()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		b.MissNetwork.Increment()
		logging.Warnf("cacheprog: web get %s: %v", ShortID(actionID), err)
		return "", nil, 0, time.Time{}, true, nil
	}

	if resp.StatusCode == 404 {
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTP404.Increment()
		// Drop the stale index claim so the PUT path re-uploads; otherwise the key 404s forever.
		b.reclaimAbsent(key)
		return "", nil, 0, time.Time{}, true, nil
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTPError.Increment()
		b.errLog.Record("web get", resp.StatusCode, actionID, string(respBody))
		return "", nil, 0, time.Time{}, true, nil
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
		return "", nil, 0, time.Time{}, true, nil
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
		return "", nil, 0, time.Time{}, true, nil
	}

	decompressStart := time.Now()
	decompressed, err := Decompress(compressed)
	if b.Latency != nil {
		b.Latency.Decompress.Record(time.Since(decompressStart))
	}
	if err != nil {
		b.MissDecompress.Increment()
		logging.Warnf("cacheprog: web get %s: decompress: %v", ShortID(actionID), err)
		return "", nil, 0, time.Time{}, true, nil
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
		return "", nil, 0, time.Time{}, true, nil
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
		return "", nil, 0, time.Time{}, true, nil
	}

	// Module-index guard: a Go module index blob self-certifies neither its outputID nor its build id, so a
	// wrong index is silently fatal at package load ("corrupt index"). Refuse it and let cmd/go recompute
	// locally; evict the claim so the recompute is free to re-Put.
	if IsGoModuleIndex(decompressed) {
		b.MissModuleIndex.Increment()
		b.removeClaimed(key)
		logging.Warnf("cacheprog: web get %s: refusing module-index blob (unverifiable under this key, len=%d); treating as miss",
			ShortID(actionID), len(decompressed))
		return "", nil, 0, time.Time{}, true, nil
	}

	t := time.Now()
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if parsed, parseErr := time.Parse(http.TimeFormat, lm); parseErr == nil {
			t = parsed
		}
	}

	b.Stats.Hits.Increment()
	return outputID, io.NopCloser(bytes.NewReader(decompressed)), int64(len(decompressed)), t, false, nil
}

// getBatch enqueues this key on the coalescer and waits for the result.
// Multiple concurrent callers funnel into the same outgoing HTTP request
// instead of each making their own — see batchCoalescer / sendBatch.
func (b *WebBackend) getBatch(actionID, key string) (string, io.ReadCloser, int64, time.Time, bool, error) {
	respCh := make(chan batchResp, 1)
	select {
	case b.batchReqCh <- batchReq{actionID: actionID, key: key, resp: respCh}:
	case <-b.batchStop:
		// Backend is closing — return miss so the caller can fall back.
		return "", nil, 0, time.Time{}, true, nil
	}
	select {
	case r := <-respCh:
		return r.outputID, r.body, r.size, r.t, r.miss, nil
	case <-b.batchDone:
		// Shutdown raced the enqueue: use the buffered reply if sendBatch already produced it, else degrade to a miss.
		select {
		case r := <-respCh:
			return r.outputID, r.body, r.size, r.t, r.miss, nil
		default:
			return "", nil, 0, time.Time{}, true, nil
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
