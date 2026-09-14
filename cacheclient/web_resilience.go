package cacheclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Failure-handling defaults for the remote cache backend. They are deliberately
// conservative: the cache is an optimization, never a correctness dependency, so
// the priority under any backend trouble is to get out of the way of the build
// fast and quietly rather than to keep trying.
const (
	// defaultMaxRetries caps extra attempts after a transient failure, to limit load on a struggling backend.
	defaultMaxRetries = 2

	// defaultEmptyBatchBackoff: consecutive empty /_batch/get responses before probing turns off; an unset value disables it.
	defaultEmptyBatchBackoff = 24

	// retryBaseDelay / retryMaxDelay bound the exponential backoff between retries; full jitter on top (see sleepBackoff).
	retryBaseDelay = 100 * time.Millisecond
	retryMaxDelay  = 2 * time.Second
)

// stallTimeout bounds SILENCE, never total duration. Responses here are bulk: a
// batch get runs to tens of megabytes and the key index further still. One that
// keeps delivering bytes is healthy however long it takes, and one that stops
// delivering is not, so the clock measures the gap between reads instead of the
// whole transfer. A var, so a test can shorten it like the index budgets.
var stallTimeout = 30 * time.Second

// guardedBody re-arms a watchdog on every read, so a body that keeps flowing
// never expires while one that goes quiet is cancelled. Close stops the
// watchdog and releases the request context that carries it.
type guardedBody struct {
	io.ReadCloser
	watchdog *time.Timer
	cancel   context.CancelFunc
	stalled  *atomic.Bool
}

func (g *guardedBody) Read(p []byte) (int, error) {
	g.watchdog.Reset(stallTimeout)
	n, err := g.ReadCloser.Read(p)
	if err != nil && err != io.EOF && g.stalled.Load() {
		return n, fmt.Errorf("no progress for %v: %w", stallTimeout, err)
	}
	return n, err
}

func (g *guardedBody) Close() error {
	g.watchdog.Stop()
	err := g.ReadCloser.Close()
	g.cancel()
	return err
}

// noteBatchEntries feeds the entry count of a served /_batch/get response to the
// consecutive-empty-batch backoff. An empty batch is a healthy remote that
// holds none of this build's keys; after enough of them stack up, the remote
// has nothing useful for this run, so we disable further batch probing
// (logged a single time). Any non-empty batch resets the streak — the remote IS serving.
// An empty-but-healthy response is not a backend failure: the backoff is purely a
// "nothing here to fetch" optimization, orthogonal to the per-op retry path.
//
// The notice is routine, so it is Info. A build with new code misses on every
// one of its own packages, and a disk copy of the index served as
// non-authoritative probes each of them: the threshold trips on most builds.
// A consumer whose stderr is compared, as go test does, must not see it.
func (b *WebBackend) noteBatchEntries(n int) {
	if b.emptyBatchBackoffThreshold <= 0 || b.batchProbingDisabled.Load() {
		return
	}
	if n > 0 {
		b.consecutiveEmptyBatches.Store(0)
		return
	}
	if b.consecutiveEmptyBatches.Add(1) >= int64(b.emptyBatchBackoffThreshold) {
		if b.batchProbingDisabled.CompareAndSwap(false, true) {
			b.batchBackoffLogOnce.Do(func() {
				logging.Infof("cacheprog: remote returned %d empty batches; "+
					"disabling further batch probes for this run (endpoint=%s)",
					b.emptyBatchBackoffThreshold, b.endpoint)
			})
		}
	}
}

// batchProbingOff reports whether the empty-batch backoff tripped, so cold keys miss without a round-trip.
func (b *WebBackend) batchProbingOff() bool {
	return b.batchProbingDisabled.Load()
}

// envInt reads an integer environment variable, falling back to def when unset
// or unparseable. A negative value is clamped to nothing (feature disabled).
func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < 0 {
		return 0
	}
	return n
}

// transientStatus reports whether a status is transient (a server error or a rate limit); another client error is definitive.
func transientStatus(code int) bool {
	return code >= 500 || code == http.StatusTooManyRequests
}

// parseRetryAfter extracts a backoff hint from a response's Retry-After header.
// It handles the delta-seconds form (an integer number of seconds) and the
// HTTP-date form, returning no delay when the header is absent or unparseable. The
// result is capped at retryMaxDelay so a server cannot pin a retry far into the
// future.
func parseRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
		if d <= 0 {
			return 0
		}
	} else {
		return 0
	}
	if d > retryMaxDelay {
		d = retryMaxDelay
	}
	return d
}

// doRetryGET issues an idempotent GET with the configured bounded-retry policy on transient failures.
func (b *WebBackend) doRetryGET(req *http.Request) (*http.Response, error) {
	return b.doRetry(req, b.maxRetries)
}

// doRetryGETN issues a GET with up to maxRetries retries; the index fetch caps this lower so it can't stall startup.
func (b *WebBackend) doRetryGETN(req *http.Request, maxRetries int) (*http.Response, error) {
	return b.doRetry(req, maxRetries)
}

// doRetryPUT issues an upload with doRetryGET's bounded-retry policy; PUT is idempotent here (key = content address).
// The body is []byte so each retry rebuilds a fresh reader via req.GetBody, or an admission shed silently drops it.
func (b *WebBackend) doRetryPUT(req *http.Request, body []byte) (*http.Response, error) {
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return b.doRetry(req, b.maxRetries)
}

// heldBackBody is the body of the answer doRetry gives an operation whose
// every attempt fell in the server's quiet period, so none was sent.
const heldBackBody = "overloaded: not sent; the server asked for quiet with Retry-After"

// heldBackResponse stands in for the shed the server would have answered.
// It is a 503 like the real one, so every caller accounts for it the way it
// accounts for a shed: through its status handling and the coalesced error
// log, never a per-operation warning.
func heldBackResponse(req *http.Request) *http.Response {
	return &http.Response{
		Status:        "503 Service Unavailable",
		StatusCode:    http.StatusServiceUnavailable,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"X-Cache-Error-Code": {"overloaded"}},
		Body:          io.NopCloser(strings.NewReader(heldBackBody)),
		ContentLength: int64(len(heldBackBody)),
		Request:       req,
	}
}

// noteShed records a server's request for quiet. A later deadline wins, and
// it is capped at retryMaxDelay like the per-request hint.
func (b *WebBackend) noteShed(retryAfter time.Duration) {
	until := time.Now().Add(retryAfter).UnixNano()
	for {
		cur := b.shedUntil.Load()
		if cur >= until || b.shedUntil.CompareAndSwap(cur, until) {
			return
		}
	}
}

// shedRemaining reports how much of the server's quiet period is left.
func (b *WebBackend) shedRemaining() time.Duration {
	return time.Until(time.Unix(0, b.shedUntil.Load()))
}

// doRetry is the shared retry loop behind doRetryGET, doRetryGETN, and
// doRetryPUT. It retries a transient response (transientStatus) up to
// maxRetries times, sleeping max(exponential-jittered backoff, server
// Retry-After) capped at retryMaxDelay between attempts, and rewinds the
// request body from req.GetBody on each retry. It returns the final
// (resp, err) exactly as http.Client.Do would, so callers handle status codes
// and bodies unchanged; it never retries a definitive client-error
// response. An admission shed is transient and so is retried and backed
// off (honoring Retry-After); if the retry budget is exhausted the caller
// falls back to a local miss for that operation alone.
//
// A Retry-After on a transient answer is a request for quiet to the whole
// process, not only to the operation that got it. Every concurrent operation
// here talks to the same server, so each one retrying on its own schedule
// multiplied the load on a server that had just said it was full. An attempt
// that falls in the quiet period is not sent: it waits the period out (plus
// jitter, so the waiters do not return together) and counts against the
// retry budget like an attempt that was shed. An operation left with no
// attempt sent gets heldBackResponse.
func (b *WebBackend) doRetry(req *http.Request, maxRetries int) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)
	for attempt := 0; ; attempt++ {
		if quiet := b.shedRemaining(); quiet > 0 {
			b.ShedWaits.Increment()
			if attempt >= maxRetries {
				if resp == nil && err == nil {
					resp = heldBackResponse(req)
				}
				return resp, err
			}
			b.sleepQuiet(attempt, quiet)
			continue
		}
		// Rewind the body for a retry (batch get carries a small JSON body; a
		// PUT carries the compressed object).
		if attempt > 0 && req.GetBody != nil {
			if body, gerr := req.GetBody(); gerr == nil {
				req.Body = body
			}
		}
		// One watchdog per attempt, armed on the context the attempt runs
		// under. It survives past this function only on the success path, where
		// the returned body owns it and the reads re-arm it.
		ctx, cancel := context.WithCancel(req.Context())
		stalled := &atomic.Bool{}
		watchdog := time.AfterFunc(stallTimeout, func() {
			stalled.Store(true)
			cancel()
		})
		resp, err = b.client.Do(req.WithContext(ctx))
		if err == nil && !transientStatus(resp.StatusCode) {
			resp.Body = &guardedBody{ReadCloser: resp.Body, watchdog: watchdog, cancel: cancel, stalled: stalled}
			return resp, nil
		}
		watchdog.Stop()
		cancel()
		var retryAfter time.Duration
		if err == nil {
			retryAfter = parseRetryAfter(resp)
			if retryAfter > 0 {
				b.noteShed(retryAfter)
			}
		}
		if attempt >= maxRetries {
			return resp, err
		}
		// Honor a server Retry-After (e.g. an admission shed), but never sleep less than the jittered backoff.
		if err == nil {
			// Drain and close so the connection returns to the pool. What was
			// read is kept, so a response returned later still has its body.
			drained, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(drained))
		}
		b.sleepBackoff(attempt, retryAfter)
	}
}

// sleepQuiet waits out the server's quiet period plus full jitter over the
// attempt's backoff, so the operations held back by one shed do not all
// return in the same instant. It returns early on shutdown.
func (b *WebBackend) sleepQuiet(attempt int, quiet time.Duration) {
	jitter := retryBaseDelay << attempt
	if jitter > retryMaxDelay || jitter <= 0 {
		jitter = retryMaxDelay
	}
	timer := time.NewTimer(quiet + time.Duration(rand.Int64N(int64(jitter)+1)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-b.batchStop:
	}
}

// sleepBackoff waits before a retry (full jitter, so parallel builds don't sync into a thundering herd), returning
// early on shutdown. A set atLeast (a server Retry-After hint) raises the floor, still capped at retryMaxDelay.
func (b *WebBackend) sleepBackoff(attempt int, atLeast time.Duration) {
	d := retryBaseDelay << attempt
	if d > retryMaxDelay || d <= 0 {
		d = retryMaxDelay
	}
	d = time.Duration(rand.Int64N(int64(d) + 1))
	if atLeast > 0 {
		if atLeast > retryMaxDelay {
			atLeast = retryMaxDelay
		}
		if atLeast > d {
			d = atLeast
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-b.batchStop:
	}
}
