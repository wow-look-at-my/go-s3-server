package cacheclient

import (
	"bytes"
	"context"
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
}

func (r SkipReport) empty() bool { return r == SkipReport{} }

func (r SkipReport) minus(o SkipReport) SkipReport {
	return SkipReport{
		GetNotInIndex:   r.GetNotInIndex - o.GetNotInIndex,
		GetEmptyIndex:   r.GetEmptyIndex - o.GetEmptyIndex,
		GetBatchBackoff: r.GetBatchBackoff - o.GetBatchBackoff,
		GetKnownMiss:    r.GetKnownMiss - o.GetKnownMiss,
		PutKnown:        r.PutKnown - o.PutKnown,
	}
}

// skipReporter holds what the server has already accepted. A failed send
// leaves it unchanged, so the next send carries the counts again.
type skipReporter struct {
	mu       sync.Mutex
	sent     SkipReport
	disabled atomic.Bool // the server has no skip endpoint
	stop     chan struct{}
	done     chan struct{}
}

// skipTotals reads the client's own skip counters.
func (b *WebBackend) skipTotals() SkipReport {
	return SkipReport{
		GetNotInIndex:   uint64(b.SkippedNotInIndex.Load()),
		GetEmptyIndex:   uint64(b.SkippedEmptyIndex.Load()),
		GetBatchBackoff: uint64(b.SkippedBatchBackoff.Load()),
		GetKnownMiss:    uint64(b.SkippedKnownMiss.Load()),
		PutKnown:        uint64(b.PutSkippedKnown.Load()),
	}
}

// startSkipReporter sends the skip counts on a timer until Close.
func (b *WebBackend) startSkipReporter() {
	b.skips.stop = make(chan struct{})
	b.skips.done = make(chan struct{})
	go func() {
		defer close(b.skips.done)
		t := time.NewTicker(skipReportInterval)
		defer t.Stop()
		for {
			select {
			case <-b.skips.stop:
				return
			case <-t.C:
				b.FlushSkips()
			}
		}
	}()
}

// stopSkipReporter stops the timer and sends what is left.
func (b *WebBackend) stopSkipReporter() {
	if b.skips.stop == nil {
		return
	}
	close(b.skips.stop)
	<-b.skips.done
	b.FlushSkips()
}

// FlushSkips sends the skips counted since the last accepted report. It
// reports whether the server now has every skip this backend counted.
func (b *WebBackend) FlushSkips() bool {
	if b.endpoint == "" || b.skips.disabled.Load() {
		return false
	}
	b.skips.mu.Lock()
	defer b.skips.mu.Unlock()
	total := b.skipTotals()
	delta := total.minus(b.skips.sent)
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
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK:
		b.skips.sent = total
		return true
	case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotFound:
		// The server is older than the skip endpoint. Its hit rate stays too high.
		b.skips.disabled.Store(true)
		logging.Warnf("cacheprog: server has no skip endpoint (HTTP %d); its hit rate omits %d skipped gets", resp.StatusCode, delta.GetNotInIndex+delta.GetEmptyIndex+delta.GetBatchBackoff+delta.GetKnownMiss)
		return false
	default:
		logging.Warnf("cacheprog: skip report: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		return false
	}
}
