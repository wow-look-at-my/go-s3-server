// The client answers many reads and writes from its own copy of the index and sends nothing.

package cacheclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The client answers many reads and writes from its own copy of the index and sends nothing.

// SkipReportPath is the endpoint, under the bucket, that takes a skip report.
const SkipReportPath = "_skips"

// skipReportInterval is how often a running backend sends what it skipped. Close sends the rest.
const skipReportInterval = 30 * time.Second

// skipReportTimeout bounds a single report.
const skipReportTimeout = 10 * time.Second

// SkipReport is the wire body of a skip report. Each field is a count since
// the report that the server accepted.
type SkipReport struct {
	// Gets the client answered as a miss and never sent, by reason.
	GetNotInIndex   uint64 `json:"get_not_in_index"`
	GetEmptyIndex   uint64 `json:"get_empty_index"`
	GetBatchBackoff uint64 `json:"get_batch_backoff"`
	GetKnownMiss    uint64 `json:"get_known_miss"`
	// Puts the client did not send because the index already holds the key.
	PutKnown uint64 `json:"put_known"`
	// Uploads that never reached the store.
	PutFailed          uint64 `json:"put_failed"`
	PutRefusedBuildID  uint64 `json:"put_refused_buildid"`
	PutRefusedModIndex uint64 `json:"put_refused_modindex"`
	PutAbandoned       uint64 `json:"put_abandoned"`
	// SkippedKeys is a sample of the keys behind GetNotInIndex, as hex action ids.
	SkippedKeys []string `json:"skipped_keys,omitempty"`
}

// skipSampleMax bounds the keys one report carries.
const skipSampleMax = 64

// empty reports whether the report carries nothing: every count is zero and the sample is empty.
func (r SkipReport) empty() bool {
	return len(r.SkippedKeys) == 0 &&
		r.GetNotInIndex+r.GetEmptyIndex+r.GetBatchBackoff+r.GetKnownMiss+r.PutKnown+
			r.PutFailed+r.PutRefusedBuildID+r.PutRefusedModIndex+r.PutAbandoned == 0
}

func (r SkipReport) minus(o SkipReport) SkipReport {
	return SkipReport{
		GetNotInIndex:      r.GetNotInIndex - o.GetNotInIndex,
		GetEmptyIndex:      r.GetEmptyIndex - o.GetEmptyIndex,
		GetBatchBackoff:    r.GetBatchBackoff - o.GetBatchBackoff,
		GetKnownMiss:       r.GetKnownMiss - o.GetKnownMiss,
		PutKnown:           r.PutKnown - o.PutKnown,
		PutFailed:          r.PutFailed - o.PutFailed,
		PutRefusedBuildID:  r.PutRefusedBuildID - o.PutRefusedBuildID,
		PutRefusedModIndex: r.PutRefusedModIndex - o.PutRefusedModIndex,
		PutAbandoned:       r.PutAbandoned - o.PutAbandoned,
	}
}

// skipReporter holds what the server has already accepted. A failed send
// leaves it unchanged, so the next send carries the counts again.
type skipReporter struct {
	mu       sync.Mutex
	sent     SkipReport
	disabled atomic.Bool // the server has no skip endpoint
	// lastSend is the UnixNano time of the last send that started.
	lastSend atomic.Int64
	inFlight atomic.Bool
	wg       sync.WaitGroup
	// sample holds the first skipSampleMax keys skipped since the last accepted report.
	sampleMu sync.Mutex
	sample   []actionHash
}

// sampleSkippedKey keeps h for the next report while the sample has room.
func (b *WebBackend) sampleSkippedKey(h actionHash) {
	b.skips.sampleMu.Lock()
	if len(b.skips.sample) < skipSampleMax {
		b.skips.sample = append(b.skips.sample, h)
	}
	b.skips.sampleMu.Unlock()
}

// takeSample answers the sample as hex, and the length to clear once the server accepts it.
func (b *WebBackend) takeSample() ([]string, int) {
	b.skips.sampleMu.Lock()
	defer b.skips.sampleMu.Unlock()
	keys := make([]string, len(b.skips.sample))
	for i, h := range b.skips.sample {
		keys[i] = hex.EncodeToString(h[:])
	}
	return keys, len(keys)
}

// clearSample drops the first n keys of the sample: the ones a report carried.
func (b *WebBackend) clearSample(n int) {
	b.skips.sampleMu.Lock()
	b.skips.sample = append(b.skips.sample[:0], b.skips.sample[n:]...)
	b.skips.sampleMu.Unlock()
}

// skipTotals reads the client's own skip counters.
func (b *WebBackend) skipTotals() SkipReport {
	return SkipReport{
		GetNotInIndex:      uint64(b.SkippedNotInIndex.Load()),
		GetEmptyIndex:      uint64(b.SkippedEmptyIndex.Load()),
		GetBatchBackoff:    uint64(b.SkippedBatchBackoff.Load()),
		GetKnownMiss:       uint64(b.SkippedKnownMiss.Load()),
		PutKnown:           uint64(b.PutSkippedKnown.Load()),
		PutFailed:          uint64(b.PutFailed.Load()),
		PutRefusedBuildID:  uint64(b.PutRefusedBuildID.Load()),
		PutRefusedModIndex: uint64(b.PutRefusedModIndex.Load()),
		PutAbandoned:       uint64(b.PutAbandoned.Load()),
	}
}

// noteSkip starts a send in the background when the last one started at
// least skipReportInterval ago. The skip itself never waits on the network.
func (b *WebBackend) noteSkip() {
	now := time.Now().UnixNano()
	last := b.skips.lastSend.Load()
	if last == 0 {
		// The interval counts from the first skip, so a short run sends one report at Close.
		b.skips.lastSend.CompareAndSwap(0, now)
		return
	}
	if now-last < int64(skipReportInterval) || !b.skips.inFlight.CompareAndSwap(false, true) {
		return
	}
	b.skips.lastSend.Store(now)
	b.skips.wg.Add(1)
	go func() {
		defer b.skips.wg.Done()
		defer b.skips.inFlight.Store(false)
		b.FlushSkips()
	}()
}

// stopSkipReporter waits for a send in flight, then sends what is left. It is
// the last network call of a Close.
func (b *WebBackend) stopSkipReporter() {
	b.skips.wg.Wait()
	b.FlushSkips()
}

// FlushSkips sends the skips counted since the last accepted report. It
// reports whether the server now has every skip this backend counted. The
// request runs on its own context, not the one a hand-off cancels, so the
// report goes out after the uploads were handed off.
func (b *WebBackend) FlushSkips() bool {
	if b.endpoint == "" || b.skips.disabled.Load() {
		return false
	}
	b.skips.mu.Lock()
	defer b.skips.mu.Unlock()
	total := b.skipTotals()
	delta := total.minus(b.skips.sent)
	var sampled int
	delta.SkippedKeys, sampled = b.takeSample()
	if delta.empty() {
		return true
	}
	body, err := json.Marshal(delta)
	if err != nil {
		logging.Warnf("cacheprog: skip report: %v", err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), skipReportTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", b.url(SkipReportPath), bytes.NewReader(body))
	if err != nil {
		logging.Warnf("cacheprog: skip report: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	b.signRequest(req)
	resp, err := b.client.Do(req)
	if err != nil {
		logging.Warnf("cacheprog: skip report: %v", err)
		return false
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	closeBody(resp.Body)
	switch {
	case resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK:
		b.skips.sent = total
		b.clearSample(sampled)
		return true
	case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotFound:
		// The server is older than the skip endpoint. Its hit rate stays too high.
		b.skips.disabled.Store(true)
		logging.Warnf("cacheprog: skip report: the server has no skip endpoint (HTTP %d)", resp.StatusCode)
		return false
	default:
		logging.Warnf("cacheprog: skip report: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		return false
	}
}
