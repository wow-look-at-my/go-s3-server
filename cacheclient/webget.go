package cacheclient

import (
	"io"
	"net/http"
	"time"
)

// getIndividual fetches a single object stored under an individual cache key.
// It is the fallback sendBatch uses against a server with no batch endpoint.
func (b *WebBackend) getIndividual(actionID, key string, h actionHash) batchResp {
	req, err := http.NewRequest("GET", b.url(key), nil)
	if err != nil {
		return batchResp{miss: true}
	}
	b.signRequest(req)

	b.Pool.Acquire()
	httpStart := time.Now()
	resp, err := b.doRetryGET(req)
	if err != nil {
		b.Pool.Release()
		b.MissNetwork.Increment()
		logging.Warnf("cacheprog: web get %s: %v", ShortID(actionID), err)
		return batchResp{miss: true}
	}

	if resp.StatusCode == 404 {
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTP404.Increment()
		// Drop the stale index claim so the PUT path re-uploads; otherwise the key 404s forever.
		b.reclaimAbsent(h)
		return batchResp{miss: true}
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		b.Pool.Release()
		b.MissHTTPError.Increment()
		b.errLog.Record("web get", resp.StatusCode, actionID, string(respBody))
		return batchResp{miss: true}
	}

	// Fall back to the deprecated S3-style header for a cache server that predates X-Cache-Meta-Outputid.
	outputID := resp.Header.Get("X-Cache-Meta-Outputid")
	if outputID == "" {
		outputID = resp.Header.Get("X-Amz-Meta-Outputid")
	}

	// The header states the length, so the body lands in one exactly-sized
	// allocation instead of io.ReadAll's doubling.
	var compressed []byte
	if resp.ContentLength >= 0 {
		compressed = make([]byte, resp.ContentLength)
		_, err = io.ReadFull(resp.Body, compressed)
	} else {
		compressed, err = io.ReadAll(resp.Body)
	}
	resp.Body.Close()
	b.Pool.Release()
	if b.Latency != nil {
		b.Latency.HTTPGet.Record(time.Since(httpStart))
	}
	if err != nil {
		b.MissReadBody.Increment()
		logging.Warnf("cacheprog: web get %s: read body: %v", ShortID(actionID), err)
		return batchResp{miss: true}
	}

	decompressStart := time.Now()
	data, ok := b.verify("web get", actionID, outputID, compressed)
	if b.Latency != nil {
		b.Latency.Decompress.Record(time.Since(decompressStart))
	}
	if !ok {
		// This path holds the key, so a refused body also loses its index
		// claim: the recompute that follows is then free to re-upload it.
		b.removeClaimed(h)
		return batchResp{miss: true}
	}

	t := time.Now()
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if parsed, parseErr := time.Parse(http.TimeFormat, lm); parseErr == nil {
			t = parsed
		}
	}

	b.Stats.Hits.Increment()
	return batchResp{outputID: outputID, data: data, t: t}
}

// getBatch enqueues this key on the coalescer and waits for the result.
// Multiple concurrent callers funnel into the same outgoing HTTP request
// instead of each making their own — see batchCoalescer / sendBatch.
func (b *WebBackend) getBatch(actionID, key string, h actionHash) batchResp {
	respCh := make(chan batchResp, 1)
	select {
	case b.batchReqCh <- batchReq{actionID: actionID, key: key, hash: h, resp: respCh}:
	case <-b.batchStop:
		// Backend is closing — return miss so the caller can fall back.
		return batchResp{miss: true}
	}
	select {
	case r := <-respCh:
		return r
	case <-b.batchDone:
		// Shutdown raced the enqueue: use the buffered reply if sendBatch already produced it, else degrade to a miss.
		select {
		case r := <-respCh:
			return r
		default:
			return batchResp{miss: true}
		}
	}
}

// removeClaimed removes a key that was optimistically added to the index
// when the upload fails, so it can be retried on the next attempt.
func (b *WebBackend) removeClaimed(h actionHash) {
	b.keysMu.Lock()
	b.keys.Remove(h)
	b.keysMu.Unlock()
}
