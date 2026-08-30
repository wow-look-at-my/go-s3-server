package cacheclient

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	httpErrFlushInterval = 30 * time.Second
	httpErrMaxNamed      = 3
	httpErrBodyKeyLen    = 128
	httpErrShortIDLen    = 8
)

// httpErrLogger coalesces repetitive HTTP error messages so a failing remote
// does not flood stderr with a line per request. Each error also exports
// does not flood stderr with a line per request.
type httpErrLogger struct {
	mu        sync.Mutex
	w         io.Writer
	interval  time.Duration
	maxNamed  int
	groups    map[httpErrKey]*httpErrGroup
	batchHTTP map[batchHTTPKey]*batchHTTPGroup

	stop   chan struct{}
	done   chan struct{}
	closed bool
}

type httpErrKey struct {
	op       string // "web put" | "web get" | "web batch get"
	status   int
	bodyNorm string // normalized body for dedup stability
}

type httpErrGroup struct {
	named   []string // the leading maxNamed short IDs, in order seen
	total   int      // total records matching this key
	bodyRaw string   // last-observed raw body, for display
}

// batchHTTPKey buckets batch-GET HTTP requests so all-miss requests are
// reported separately from requests that hit something.
type batchHTTPKey struct {
	allMiss bool
}

type batchHTTPGroup struct {
	total      int           // number of HTTP requests in this bucket
	sumKeys    int           // sum of keys requested across all requests
	sumEntries int           // sum of entries returned
	sumPref    int           // sum of prefetched entries
	sumDur     time.Duration // sum of HTTP durations
}

// newHTTPErrLogger returns a logger that writes aggregated stderr
// summaries to w on every interval tick (and again on Close).

func newHTTPErrLogger(w io.Writer, interval time.Duration) *httpErrLogger {
	l := &httpErrLogger{
		w:         w,
		interval:  interval,
		maxNamed:  httpErrMaxNamed,
		groups:    map[httpErrKey]*httpErrGroup{},
		batchHTTP: map[batchHTTPKey]*batchHTTPGroup{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go l.loop()
	return l
}

// Record reports an HTTP error and
// queues the error for the next stderr summary flush. Safe for nil receiver
// so call sites that may have a partially-constructed WebBackend don't panic.
func (l *httpErrLogger) Record(op string, status int, id, body string) {
	if l == nil {
		msg := fmt.Sprintf("cacheprog: %s %s: HTTP %d", op, ShortID(id), status)
		if body != "" {
			msg += fmt.Sprintf(": %s", body)
		}
		logging.Warnf("%s", msg)
		return
	}
	key := httpErrKey{
		op:       op,
		status:   status,
		bodyNorm: normalizeBody(body),
	}
	l.mu.Lock()
	g, ok := l.groups[key]
	if !ok {
		g = &httpErrGroup{}
		l.groups[key] = g
	}
	g.total++
	if len(g.named) < l.maxNamed {
		g.named = append(g.named, ShortID(id))
	}
	g.bodyRaw = body
	l.mu.Unlock()
}

// RecordBatchHTTP coalesces stats from a batch-GET HTTP request
// (which may carry many keys after client-side coalescing). Buckets are
// split by hit/all-miss so a flaky cold cache stays distinguishable from
// a working remote. Nil-safe.
func (l *httpErrLogger) RecordBatchHTTP(keysRequested, entriesReturned, prefetched int, dur time.Duration) {
	if l == nil {
		logging.Infof("cacheprog: batch GET: %d keys → %d entries (%d prefetched) in %v",
			keysRequested, entriesReturned, prefetched, dur.Round(time.Millisecond))
		return
	}
	key := batchHTTPKey{allMiss: entriesReturned == 0}
	l.mu.Lock()
	g, ok := l.batchHTTP[key]
	if !ok {
		g = &batchHTTPGroup{}
		l.batchHTTP[key] = g
	}
	g.total++
	g.sumKeys += keysRequested
	g.sumEntries += entriesReturned
	g.sumPref += prefetched
	g.sumDur += dur
	l.mu.Unlock()
}

func (l *httpErrLogger) loop() {
	defer close(l.done)
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.flush()
		case <-l.stop:
			l.flush()
			return
		}
	}
}

func (l *httpErrLogger) flush() {
	l.mu.Lock()
	if len(l.groups) == 0 && len(l.batchHTTP) == 0 {
		l.mu.Unlock()
		return
	}
	groups := l.groups
	batchHTTP := l.batchHTTP
	l.groups = map[httpErrKey]*httpErrGroup{}
	l.batchHTTP = map[batchHTTPKey]*batchHTTPGroup{}
	l.mu.Unlock()
	for k, g := range groups {
		fmt.Fprintln(l.w, formatGroup(k, g))
	}
	for k, g := range batchHTTP {
		fmt.Fprintln(l.w, formatBatchHTTPGroup(k, g))
	}
}

// Close stops the ticker and flushes pending groups. Idempotent. It does not

func (l *httpErrLogger) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	close(l.stop)
	<-l.done
	l.flush()
	return nil
}

func formatGroup(k httpErrKey, g *httpErrGroup) string {
	ids := formatIDList(g.named, g.total)
	if g.bodyRaw == "" {
		return fmt.Sprintf("cacheprog: %s %s: HTTP %d", k.op, ids, k.status)
	}
	return fmt.Sprintf("cacheprog: %s %s: HTTP %d: %s", k.op, ids, k.status, g.bodyRaw)
}

func formatBatchHTTPGroup(k batchHTTPKey, g *batchHTTPGroup) string {
	durMs := g.sumDur.Round(time.Millisecond).Milliseconds()

	if g.total == 1 {
		if k.allMiss {
			return fmt.Sprintf("cacheprog: batch GET: %d keys → 0 entries (server has no entries for any of them) in %dms",
				g.sumKeys, durMs)
		}
		return fmt.Sprintf("cacheprog: batch GET: %d keys → %d entries (%d prefetched) in %dms",
			g.sumKeys, g.sumEntries, g.sumPref, durMs)
	}

	if k.allMiss {
		return fmt.Sprintf("cacheprog: batch GET ×%d: %d keys → 0 entries (server has no entries), %dms total",
			g.total, g.sumKeys, durMs)
	}
	return fmt.Sprintf("cacheprog: batch GET ×%d: %d keys → %d entries (%d prefetched), %dms total",
		g.total, g.sumKeys, g.sumEntries, g.sumPref, durMs)
}

// formatIDList renders the named IDs + "and N more" tail (or a bare
// bare ID when the group holds a lone entry). Shared by formatGroup and formatBatchGroup.
func formatIDList(named []string, total int) string {
	if total == 1 && len(named) == 1 {
		return named[0]
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, n := range named {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(n)
	}
	if total > len(named) {
		fmt.Fprintf(&b, ", and %d more", total-len(named))
	}
	b.WriteByte(']')
	return b.String()
}

func normalizeBody(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > httpErrBodyKeyLen {
		s = s[:httpErrBodyKeyLen]
	}
	return s
}

func ShortID(id string) string {
	if len(id) > httpErrShortIDLen {
		return id[:httpErrShortIDLen]
	}
	return id
}
