package cacheclient

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// batchPutManifest: the leading tar member sent to /_batch/put; entries carry the same metadata as X-Cache-Meta-* headers.
type batchPutManifest struct {
	Entries []batchPutManifestEntry `json:"entries"`
}

type batchPutManifestEntry struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// batchPutResponse is the JSON body the server returns from /_batch/put: a
// result per requested key, in any order (matched back by key).
type batchPutResponse struct {
	Results []batchPutResult `json:"results"`
}

type batchPutResult struct {
	Key     string `json:"key"`
	Status  string `json:"status"` // stored | dropped | conflict | error
	Message string `json:"message,omitempty"`
}

// batchPutWindow: PUT coalescing window, wider than GETs since PUTs are fire-and-forget and off the critical path.
const batchPutWindow = 50 * time.Millisecond

// putWindow returns the effective coalescing window, honoring a per-process
// override so tests can widen it for deterministic single-batch coalescing.
func (b *WebBackend) putWindow() time.Duration {
	if ms := envInt("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", 0); ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return batchPutWindow
}

// batchPutCoalescer collects prepped putReqs on a short coalescing window and
// ships each batch as a single /_batch/put tar. It mirrors batchCoalescer: up to
// batchMaxKeys objects per request, flushed on count, on the time window, or on
// drain/Close. On drain it waits for all in-flight HTTP batches before exiting,
// so Close does not return until every buffered upload has been attempted.
func (b *WebBackend) batchPutCoalescer() {
	defer close(b.putBatchDone)

	var pending []putReq
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.putBatchHTTPWG.Add(1)
		go func() {
			defer b.putBatchHTTPWG.Done()
			b.sendBatchPut(batch)
		}()
	}

	for {
		select {
		case req, ok := <-b.putBatchReqCh:
			if !ok {
				flush()
				b.putBatchHTTPWG.Wait()
				return
			}
			if len(pending) == 0 {
				timer.Reset(b.putWindow())
			}
			pending = append(pending, req)
			if len(pending) >= batchMaxKeys {
				flush()
			}
		case <-timer.C:
			flush()
		case <-b.putBatchStop:
			// Drain anything already enqueued before stopping so a Close does not
			// lose buffered uploads, then wait for in-flight batches.
			for {
				select {
				case req := <-b.putBatchReqCh:
					pending = append(pending, req)
					if len(pending) >= batchMaxKeys {
						flush()
					}
					continue
				default:
				}
				break
			}
			flush()
			b.putBatchHTTPWG.Wait()
			return
		}
	}
}

// buildPutTar assembles the /_batch/put request body: a tar whose leading member
// is manifest.json and whose subsequent data/<key> members carry each object's
// lz4-compressed bytes, in manifest order.
func buildPutTar(reqs []putReq) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	manifest := batchPutManifest{Entries: make([]batchPutManifestEntry, len(reqs))}
	for i, r := range reqs {
		manifest.Entries[i] = batchPutManifestEntry{Key: r.key, Metadata: r.metadata}
	}
	mdata, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(mdata); err != nil {
		return nil, err
	}
	for _, r := range reqs {
		if err := tw.WriteHeader(&tar.Header{Name: "data/" + r.key, Size: int64(len(r.compressed)), Mode: 0644}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(r.compressed); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// sendBatchPut ships a single /_batch/put tar for all objects in reqs. The whole tar
// is retried as a unit on a transient failure (an admission shed, a server error,
// network) via doRetryPUT, honoring Retry-After and rewinding the in-memory tar
// bytes on each attempt — exactly like a single PUT keeps its compressed body.
//
// Outcomes:
//   - per-object "stored"/"conflict": success, keep the optimistic claim, count Puts.
//   - per-object "dropped": server refused (module index); keep the claim, no retry.
//   - per-object "error": roll back ONLY that object's claim so a later run re-uploads.
//   - not-found or method-not-allowed: server has no batch endpoint — set the sticky batchPutUnsupported
//     flag and re-issue every object in this batch on the single-PUT fallback path.
//   - whole-request final failure (network / non-2xx after retries): roll back ALL
//     claims in the batch so a later healthy run re-uploads them.
func (b *WebBackend) sendBatchPut(reqs []putReq) {
	if len(reqs) == 0 {
		return
	}

	tarBytes, err := buildPutTar(reqs)
	if err != nil {
		logging.Warnf("cacheprog: web batch put: build tar: %v", err)
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}

	batchURL := b.endpoint + "/" + b.bucket + "/_batch/put"
	httpReq, err := http.NewRequest("PUT", batchURL, bytes.NewReader(tarBytes))
	if err != nil {
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}
	httpReq.Header.Set("Content-Type", "application/x-tar")
	b.signRequest(httpReq)

	b.Pool.Acquire()
	// Retry the WHOLE tar on a transient failure (honors Retry-After); doRetryPUT rebuilds the body reader each attempt.
	resp, err := b.doRetryPUT(httpReq, tarBytes)
	if err != nil {
		b.Pool.Release()
		logging.Warnf("cacheprog: web batch put: %v", err)
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}

	// Not-found or method-not-allowed → server does not support the batch endpoint. Set the sticky flag
	// so all subsequent PUTs this process take the single-PUT path, and re-issue
	// every object in THIS batch on that path now.
	if resp.StatusCode == 404 || resp.StatusCode == 405 {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		b.Pool.Release()
		b.batchPutUnsupported.Store(true)
		for _, r := range reqs {
			if perr := b.putSingle(r); perr != nil && !isLoggedErr(perr) {
				logging.Warnf("cacheprog: web put %s: %v", ShortID(r.actionID), perr)
			}
		}
		return
	}

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		b.Pool.Release()
		b.errLog.Record("web batch put", resp.StatusCode, reqs[0].actionID, string(respBody))
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	b.Pool.Release()
	if err != nil {
		logging.Warnf("cacheprog: web batch put: read response: %v", err)
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}

	var parsed batchPutResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		logging.Warnf("cacheprog: web batch put: parse response: %v", err)
		for _, r := range reqs {
			b.removeClaimed(r.key)
		}
		return
	}

	// Index results by key; an object the server omitted is treated as an error and its claim rolled back.
	resultByKey := make(map[string]batchPutResult, len(parsed.Results))
	for _, res := range parsed.Results {
		resultByKey[res.Key] = res
	}

	var stored int
	for _, r := range reqs {
		res, ok := resultByKey[r.key]
		switch {
		case ok && (res.Status == "stored" || res.Status == "conflict"):
			// Success: keep the optimistic claim.
			b.Stats.Puts.Increment()
			stored++
		case ok && res.Status == "dropped":
			// Server refused (module index); keep the claim, do NOT retry — the
			// client already filters indexes so this is rare.
		default:
			// "error" or an absent/unknown result: roll back the claim so a later run re-uploads it.
			b.removeClaimed(r.key)
			if res.Message != "" {
				logging.Warnf("cacheprog: web batch put %s: server error: %s", ShortID(r.actionID), res.Message)
			}
		}
	}

}

// putSingle uploads a prepped object via the per-object single-PUT path
// (doRetryPUT). It is the fallback when the server has no /_batch/put endpoint
// (batchPutUnsupported) and the re-issue path for an unsupported batch response. The
// optimistic index claim is already held by the caller; putSingle keeps it on
// success and rolls it back on any failure, mirroring the original Put body.
func (b *WebBackend) putSingle(pr putReq) error {
	req, err := http.NewRequest("PUT", b.url(pr.key), bytes.NewReader(pr.compressed))
	if err != nil {
		b.removeClaimed(pr.key)
		return fmt.Errorf("web put request: %w", err)
	}
	req.ContentLength = int64(len(pr.compressed))
	for name, vals := range metadataHeaders(pr.metadata) {
		req.Header[name] = vals
	}
	b.signRequest(req)

	b.Pool.Acquire()
	httpStart := time.Now()
	// doRetryPUT rebuilds the body reader each attempt, honoring an admission shed's Retry-After.
	resp, err := b.doRetryPUT(req, pr.compressed)
	b.Pool.Release()
	if b.Latency != nil && err == nil {
		b.Latency.HTTPPut.Record(time.Since(httpStart))
	}
	if err != nil {
		b.removeClaimed(pr.key)
		return fmt.Errorf("web put %s: %w", ShortID(pr.actionID), err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		b.removeClaimed(pr.key)
		b.errLog.Record("web put", resp.StatusCode, pr.actionID, string(respBody))
		return fmt.Errorf("web put: HTTP %d: %w", resp.StatusCode, ErrLogged)
	}

	b.Stats.Puts.Increment()
	return nil
}

// isLoggedErr reports whether err is the already-logged sentinel, so callers
// don't double-report a web-layer error to stderr.
func isLoggedErr(err error) bool {
	return errors.Is(err, ErrLogged)
}
