package cacheclient

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// batchGetRequest is the JSON body sent to the server's /_batch/get endpoint.
type batchGetRequest struct {
	Keys     []string `json:"keys"`
	Prefetch bool     `json:"prefetch"`
	// PrefetchOnly asks for the window around Keys without the bodies of Keys
	// themselves. The look-ahead pool uses it: it names keys the build already
	// has in order to say WHERE to look, and re-sending those bodies would
	// throw away the point of the request.
	PrefetchOnly bool `json:"prefetch_only,omitempty"`
}

// batchGetManifest is the manifest entry in the server's tar response.
type batchGetManifest struct {
	Entries []batchGetManifestEntry `json:"entries"`
}

type batchGetManifestEntry struct {
	Key      string            `json:"key"`
	Size     int64             `json:"size"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Prefetch bool              `json:"prefetch,omitempty"`
}

// BatchEntry holds a single cache entry from a batch GET response. Data is the
// stored (compressed) body: a consumer that wants only some of a look-ahead's
// entries must not pay to decompress the rest.
type BatchEntry struct {
	Key      string
	OutputID string
	Data     []byte
	Prefetch bool
	// RawSize is the body's uncompressed length, as the uploader recorded it.
	// It lets a reader allocate once instead of growing a buffer. Zero when the
	// object predates the metadata or carries a value that will not parse.
	RawSize int64
}

// streamBatchResponse reads a tar stream from the server's /_batch/get endpoint
// and hands each entry to fn as it arrives.
//
// It streams because the alternative was the client's largest resident cost. A
// response carries up to batchMaxKeys requested bodies plus the server's whole
// prefetch window, several requests are in flight at once, and every `go`
// process in a `dist test` run builds its own. Nothing bounded the BYTES: the
// caps upstream and downstream both count ENTRIES, and a count is not a size.
// That is what put a windows runner into ERROR_COMMITMENT_LIMIT.
//
// The server writes manifest.json as the tar's FIRST member, so an entry's
// metadata is always known by the time its body arrives. Holding one body
// rather than the whole tar is what that ordering buys, and it also unblocks a
// waiting caller as its own body lands instead of after the last one.
func streamBatchResponse(r io.Reader, fn func(BatchEntry)) error {
	tr := tar.NewReader(r)

	var manifest batchGetManifest
	meta := map[string]*batchGetManifestEntry{}

	readMember := func(tr *tar.Reader, hdr *tar.Header) ([]byte, error) {
		// The header states the size, so the body lands in one exactly-sized
		// allocation rather than io.ReadAll's doubling.
		raw := make([]byte, hdr.Size)
		n, err := io.ReadFull(tr, raw)
		if err != nil {
			// Say how far it got. A cut response and a server that stopped
			// after one member both surface as "unexpected EOF", and the
			// counts are what tell them apart. Entries handed over before this
			// point are already the caller's -- streaming means a truncated
			// response costs the tail, not the batch.
			return nil, fmt.Errorf("read entry %s: %d of %d bytes: %w", hdr.Name, n, hdr.Size, err)
		}
		return raw, nil
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}

		if hdr.Name == "manifest.json" {
			// The header states the size, so the body lands in one exactly-sized
			// allocation rather than io.ReadAll's doubling.
			raw, err := readMember(tr, hdr)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &manifest); err != nil {
				return fmt.Errorf("parse manifest: %w", err)
			}
			for i := range manifest.Entries {
				meta[manifest.Entries[i].Key] = &manifest.Entries[i]
			}
			continue
		}

		if len(hdr.Name) <= 5 || hdr.Name[:5] != "data/" {
			continue
		}
		me, ok := meta[hdr.Name[5:]]
		if !ok {
			// A body the manifest never named. The old code dropped it too, by
			// walking the manifest rather than the members.
			continue
		}
		raw, err := readMember(tr, hdr)
		if err != nil {
			return err
		}
		rawSize, _ := strconv.ParseInt(me.Metadata["body-size"], 10, 64)
		fn(BatchEntry{
			Key:      me.Key,
			OutputID: me.Metadata["outputid"],
			Data:     raw,
			Prefetch: me.Prefetch,
			RawSize:  rawSize,
		})
	}
	return nil
}

// parseBatchResponse collects a whole response. It is the shape the tests were
// written against; the paths that carry real traffic stream instead.
func parseBatchResponse(r io.Reader) ([]BatchEntry, error) {
	var entries []BatchEntry
	if err := streamBatchResponse(r, func(e BatchEntry) { entries = append(entries, e) }); err != nil {
		return nil, err
	}
	return entries, nil
}

// batchCoalescer collects incoming batchReqs and dispatches each batch as a
// single HTTP request to the server's batch endpoint.
//
// The window is Nagle's rule, not a fixed wait. A caller blocks on its own key,
// and the build's parallelism decides how many keys are outstanding at once, so
// sitting a fixed window out bought nothing: a four-way build never offered
// more than four keys, and all four paid the wait for an answer the server
// produces in a millisecond. Here the first batch leaves at once, and only what
// arrives while a request is already in flight rides the next one. Load alone
// widens a batch, which is the only condition under which a wider batch is
// worth its latency.
func (b *WebBackend) batchCoalescer() {
	defer close(b.batchDone)

	var pending []batchReq
	// When the batch's first key arrived. Every caller in the batch has been
	// blocked since at least this instant, so it is what the window costs.
	var firstQueued time.Time
	var inFlight atomic.Int64
	sent := make(chan struct{}, 1)
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
		b.batchTiming.recordWait(len(batch), time.Since(firstQueued))
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.batchHTTPWG.Add(1)
		inFlight.Add(1)
		go func() {
			defer func() {
				inFlight.Add(-1)
				select {
				case sent <- struct{}{}:
				default:
				}
				b.batchHTTPWG.Done()
			}()
			b.sendBatch(batch)
		}()
	}

	for {
		select {
		case req, ok := <-b.batchReqCh:
			if !ok {
				flush()
				b.batchHTTPWG.Wait()
				return
			}
			if len(pending) == 0 {
				firstQueued = time.Now()
				timer.Reset(batchCoalesceWait)
			}
			pending = append(pending, req)
			if len(pending) >= batchMaxKeys || inFlight.Load() == 0 {
				flush()
			}
		case <-sent:
			// A request finished and freed the line. Whatever queued behind it
			// goes now.
			flush()
		case <-timer.C:
			// The safety net for a key that arrived behind a slow request, not
			// the thing that forms a batch.
			flush()
		case <-b.batchStop:
			flush()
			b.batchHTTPWG.Wait()
			return
		}
	}
}

// sendBatch issues a single HTTP request to /_batch/get for all keys in reqs
// and distributes the entries back to the waiting callers via their reply
// channels.
//
// It asks for the requested keys and nothing else. Every caller in this batch
// is a build goroutine that cannot proceed until this answers, so a megabyte of
// speculative bodies queued in front of theirs is latency charged straight to
// the critical path. Look-ahead runs on its own pool over its own requests;
// this request stays as small as the build made it.
func (b *WebBackend) sendBatch(reqs []batchReq) {
	start := time.Now()
	keys := make([]string, len(reqs))
	for i, r := range reqs {
		keys[i] = r.key
	}

	// Transient failure only; never marks knownMiss, since only an
	// authoritative OK-without-key response proves absence.
	respondAllMiss := func(reason *AtomicCounter) {
		for _, r := range reqs {
			if reason != nil && b.keyKnown(r.hash) {
				reason.Increment()
			}
			r.resp <- batchResp{miss: true}
		}
	}

	reqBody, _ := json.Marshal(batchGetRequest{Keys: keys})
	batchURL := b.endpoint + "/" + b.bucket + "/_batch/get"
	// POST, not GET-with-body, since a body-carrying GET is proxy-hostile.
	httpReq, err := http.NewRequest("POST", batchURL, bytes.NewReader(reqBody))
	if err != nil {
		respondAllMiss(nil)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	b.signRequest(httpReq)
	httpReq.Header.Set(HeaderKind, KindCritical)

	b.Pool.Acquire()
	resp, err := b.doRetryGET(httpReq)
	if err != nil {
		b.Pool.Release()
		logging.Warnf("cacheprog: web batch get: %v", err)
		respondAllMiss(&b.MissNetwork)
		return
	}

	if resp.StatusCode != 200 {
		resp.Body.Close()
		b.Pool.Release()
		// Not-found or method-not-allowed → server has no batch endpoint; fall back to individual
		// GETs for every caller in this batch.
		if resp.StatusCode == 404 || resp.StatusCode == 405 {
			for _, r := range reqs {
				r.resp <- b.getIndividual(r.actionID, r.key, r.hash)
			}
			return
		}
		// 5xx etc. — coalesced via errLog; its group total covers the request count.
		for _, r := range reqs {
			b.errLog.Record("web batch get", resp.StatusCode, r.actionID, "")
		}
		respondAllMiss(&b.MissHTTPError)
		return
	}

	// Each body is answered as it arrives rather than after the last one, so the
	// response is never resident as a whole and a blocked caller waits only for
	// its own object.
	reqByKey := make(map[string]batchReq, len(reqs))
	for _, r := range reqs {
		reqByKey[r.key] = r
	}
	hit := make([]string, 0, len(reqs))
	count := 0

	err = streamBatchResponse(resp.Body, func(e BatchEntry) {
		count++
		r, ok := reqByKey[e.Key]
		if !ok {
			return // a prefetched body nobody in this batch asked for
		}
		delete(reqByKey, e.Key)
		data, ok := b.verify("web batch get", r.actionID, e.OutputID, e.Data, e.RawSize)
		if !ok {
			r.resp <- batchResp{miss: true}
			return
		}
		b.Stats.Hits.Increment()
		hit = append(hit, r.key)
		r.resp <- batchResp{outputID: e.OutputID, data: data, t: time.Now()}
	})
	resp.Body.Close()
	b.Pool.Release()
	if err != nil {
		logging.Warnf("cacheprog: web batch get: parse: %v", err)
		// Only the callers still waiting: one already answered must not be sent
		// a second reply.
		for _, r := range reqByKey {
			if b.keyKnown(r.hash) {
				b.MissReadBody.Increment()
			}
			r.resp <- batchResp{miss: true}
		}
		return
	}

	// Whatever the stream never named is authoritatively absent. Drop the stale
	// index claim (reclaimAbsent) so the PUT path re-uploads it.
	for _, r := range reqByKey {
		if b.reclaimAbsent(r.hash) {
			b.MissHTTP404.Increment()
		}
		r.resp <- batchResp{miss: true}
	}

	// A run of empty batches stops probing after a threshold; any non-empty batch resets it.
	b.noteBatchEntries(count)

	trip := time.Since(start)
	b.batchTiming.recordTrip(trip)
	b.errLog.RecordBatchHTTP(len(reqs), count, trip)

	// The keys that answered are where this build's objects sit in the store's
	// time order, and that order is the only anchor the look-ahead has. A key
	// that missed says nothing about where to look.
	b.lookAhead.Seed(hit)
}

// verify decompresses a stored body and puts it through every gate before any
// caller can see it. A body that fails one is a miss: the recompute that
// follows re-uploads it clean.
//
// It is the one place the gates live, so a body reaching the build through the
// look-ahead pool is checked exactly as hard as one the build asked for by
// name. op names the path for the log line.
func (b *WebBackend) verify(op, actionID, outputID string, stored []byte, rawSize int64) ([]byte, bool) {
	// A missing outputid is a metadata gap, not a corrupt body — count it as
	// such rather than as the checksum mismatch it would become below.
	if outputID == "" {
		b.MissNoOutputID.Increment()
		logging.Warnf("cacheprog: %s %s: missing outputid metadata", op, ShortID(actionID))
		return nil, false
	}
	data, err := DecompressSized(stored, rawSize)
	if err != nil {
		b.MissDecompress.Increment()
		logging.Warnf("cacheprog: %s %s: decompress: %v", op, ShortID(actionID), err)
		return nil, false
	}
	// The body must hash to its advertised outputID. A mismatch means the
	// remote object is corrupt, and serving it would feed the compiler a
	// damaged object under a key it trusts.
	if got, ok := OutputIDMatches(outputID, data); !ok {
		b.MissChecksum.Increment()
		b.Stats.Corrupt.Increment()
		logging.Warnf("cacheprog: %s %s: body checksum mismatch (want outputid=%s, got sha256=%s, len=%d); treating as miss",
			op, ShortID(actionID), ShortID(outputID), ShortID(got), len(data))
		return nil, false
	}
	// A compiled object self-certifies its action key in its build id. A body
	// whose build id names another action is a poisoned mapping the hash check
	// cannot catch, because both halves of the pair are internally consistent.
	if act, ok := BuildIDMatchesAction(actionID, data); !ok {
		b.MissBuildID.Increment()
		b.Stats.Corrupt.Increment()
		logging.Warnf("cacheprog: %s %s: build-id action mismatch (want action=%s, got action=%s, len=%d); treating as miss",
			op, ShortID(actionID), ExpectedBuildIDAction(actionID), act, len(data))
		return nil, false
	}
	// A module index certifies neither its outputID nor its build id, so a
	// wrong one is silently fatal at package load. Recomputing it locally
	// costs nothing.
	if IsGoModuleIndex(data) {
		b.MissModuleIndex.Increment()
		logging.Warnf("cacheprog: %s %s: refusing module-index blob (unverifiable under this key, len=%d); treating as miss",
			op, ShortID(actionID), len(data))
		return nil, false
	}
	return data, true
}
