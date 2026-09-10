package cacheclient

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestLogger returns a logger that flushes only via Close.
// tests needing spans build their own.
func newTestLogger(buf *bytes.Buffer) *httpErrLogger {
	return newHTTPErrLogger(buf, time.Hour)
}

// captureLogger installs a Logger for one test and returns what it collected.
// A batch summary goes there rather than to the writer, so the consumer can
// decide whether its build's output carries it.
type captureLogger struct {
	mu   sync.Mutex
	sb   strings.Builder
	prev Logger
}

// captureMu serializes the capture. logging is one package variable, so two
// tests that install a capture at once share whichever one landed last: the
// second test's lines go to the first test's buffer, and the first test reads
// an empty one. Both then fail on somebody else's output. A mutex held for the
// whole test gives each one the variable to itself.
//
// It is a mutex rather than t.Serial because this module is consumed by trees
// built with a stock toolchain, which has no such method. It is also narrower:
// a test that never captures has no reason to wait.
var captureMu sync.Mutex

func newCaptureLogger(t *testing.T) *captureLogger {
	t.Helper()
	t.Serial() // the logger is package state
	captureMu.Lock()
	c := &captureLogger{prev: logging}
	SetLogger(c)
	t.Cleanup(func() {
		logging = c.prev
		captureMu.Unlock()
	})
	return c
}

func (c *captureLogger) Infof(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(&c.sb, format+"\n", args...)
}

func (c *captureLogger) Warnf(format string, args ...any)  { c.Infof(format, args...) }
func (c *captureLogger) Debugf(format string, args ...any) { c.Infof(format, args...) }

func (c *captureLogger) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sb.String()
}

// syncBuffer is a mutex-guarded bytes.Buffer, safe for a test to read while
// the logger's ticker goroutine may still be flushing into it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestHTTPErrLogger_SingleRecordFormat(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Record("web put", 502, "8187a9f3deadbeef", "error code: 502")
	require.NoError(t, l.Close())

	require.Equal(t, "cacheprog: web put 8187a9f3: HTTP 502: error code: 502\n", buf.String())
}

func TestHTTPErrLogger_CoalesceSameKey(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	ids := []string{
		"hash0001abcdef00", "hash0002abcdef00", "hash0003abcdef00",
		"hash0004abcdef00", "hash0005abcdef00",
	}
	for _, id := range ids {
		l.Record("web put", 502, id, "error code: 502")
	}
	require.NoError(t, l.Close())

	out := buf.String()
	require.Equal(t, 1, strings.Count(out, "\n"), "expected exactly one line, got: %q", out)
	require.Contains(t, out, "[hash0001, hash0002, hash0003, and 2 more]")
	require.Contains(t, out, "HTTP 502: error code: 502")
}

func TestHTTPErrLogger_CoalesceUnderMaxNamed(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Record("web put", 502, "aaaa1111", "boom")
	l.Record("web put", 502, "bbbb2222", "boom")
	require.NoError(t, l.Close())

	require.Equal(t, "cacheprog: web put [aaaa1111, bbbb2222]: HTTP 502: boom\n", buf.String())
}

func TestHTTPErrLogger_DifferentKeysStayDistinct(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Record("web put", 502, "aaaaaaaaaaaa", "error code: 502")
	l.Record("web put", 503, "bbbbbbbbbbbb", "error code: 502")
	l.Record("web get", 502, "ccccccccccccc", "error code: 502")
	l.Record("web put", 502, "ddddddddddddd", "different body")
	require.NoError(t, l.Close())

	out := buf.String()
	require.Equal(t, 4, strings.Count(out, "\n"), "expected 4 lines, got: %q", out)
}

func TestHTTPErrLogger_EmptyBodyOmitsTrailer(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	l.Record("web batch get", 502, "c5061394aabbccdd", "")
	require.NoError(t, l.Close())

	require.Equal(t, "cacheprog: web batch get c5061394: HTTP 502\n", buf.String())
}

func TestHTTPErrLogger_BodyNormalization(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	// Several body variations normalize to the same key; past maxNamed this gives an "and N more" tail.
	l.Record("web put", 502, "aaaa1111", "error code: 502")
	l.Record("web put", 502, "bbbb2222", "  error code: 502  ")
	l.Record("web put", 502, "cccc3333", "\nerror code: 502\n")
	l.Record("web put", 502, "dddd4444", "error code: 502")
	l.Record("web put", 502, "eeee5555", "error code: 502")
	require.NoError(t, l.Close())

	out := buf.String()
	require.Equal(t, 1, strings.Count(out, "\n"), "expected coalesced output, got: %q", out)
	require.Contains(t, out, "and 2 more")
}

func TestHTTPErrLogger_CloseFlushesPending(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	l.Record("web put", 502, "aabbccdd", "boom")
	require.Empty(t, buf.String(), "no flush should have happened before Close")
	require.NoError(t, l.Close())
	require.NotEmpty(t, buf.String(), "Close should flush pending records")
}

func TestHTTPErrLogger_CloseIdempotent(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	l.Record("web put", 502, "aabbccdd", "boom")
	require.NoError(t, l.Close())
	require.NotPanics(t, func() { _ = l.Close() })
}

func TestHTTPErrLogger_TickerFlush(t *testing.T) {
	// buf must be synchronized: the ticker goroutine writes while Eventually polls Len().
	var buf syncBuffer
	l := newHTTPErrLogger(&buf, 10*time.Millisecond)

	l.Record("web put", 502, "aabbccdd", "boom")

	require.Eventually(t, func() bool {
		return buf.Len() > 0
	}, time.Second, 5*time.Millisecond, "ticker should have flushed")

	require.NoError(t, l.Close())
}

func TestHTTPErrLogger_ConcurrentRecord(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)

	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				l.Record("web put", 502, "aabbccdd", "error code: 502")
			}
		}()
	}
	wg.Wait()
	require.NoError(t, l.Close())

	out := buf.String()
	require.Equal(t, 1, strings.Count(out, "\n"), "expected one coalesced line, got: %q", out)
	expected := goroutines*perGoroutine - httpErrMaxNamed
	require.Contains(t, out, "and "+strconv.Itoa(expected)+" more")
}

func TestHTTPErrLogger_NilReceiver(t *testing.T) {
	var l *httpErrLogger
	// A nil receiver has no writer, so it reports straight to the package
	// logger. Capturing that is what keeps these two lines out of whatever
	// buffer a test running beside this one installed.
	cap := newCaptureLogger(t)
	require.NotPanics(t, func() {
		l.Record("web put", 502, "aabbccdd", "boom")
		l.Record("web batch get", 502, "ccddeeff", "")
	})
	require.Contains(t, cap.String(), "web put aabbccdd: HTTP 502: boom")
	require.Contains(t, cap.String(), "web batch get ccddeeff: HTTP 502")
}

// The aggregated summaries must reach the installed Logger, not a stream of
// the client's own choosing. A consumer whose stdout is a protocol channel,
// or whose stderr is read by its own tests, gets silence until it asks --
// which is the package contract, and which the summaries used to sidestep by
// holding os.Stderr directly.
func TestLoggerWriter_SummariesGoToTheInstalledLogger(t *testing.T) {
	t.Serial() // the logger is package state
	var got []string
	SetLogger(recordingLogger{&got})
	t.Cleanup(func() { SetLogger(nil) })

	l := newHTTPErrLogger(loggerWriter{}, time.Hour)
	l.Record("web put", 404, "aabbccdd", "404 page not found")
	require.NoError(t, l.Close())

	require.Len(t, got, 1)
	require.Contains(t, got[0], "web put")
	require.Contains(t, got[0], "404")
	require.NotContains(t, got[0], "\n", "each summary line is one message")
}

// With no Logger installed the same summaries go nowhere at all.
func TestLoggerWriter_SilentWithNoLogger(t *testing.T) {
	t.Serial() // the logger is package state
	SetLogger(nil)
	l := newHTTPErrLogger(loggerWriter{}, time.Hour)
	require.NotPanics(t, func() {
		l.Record("web put", 404, "aabbccdd", "404 page not found")
		require.NoError(t, l.Close())
	})
}

// recordingLogger collects Warnf messages so a test can assert on them.
type recordingLogger struct{ msgs *[]string }

func (r recordingLogger) Infof(format string, args ...any) {
	*r.msgs = append(*r.msgs, fmt.Sprintf(format, args...))
}

func (r recordingLogger) Warnf(format string, args ...any) {
	*r.msgs = append(*r.msgs, fmt.Sprintf(format, args...))
}

func (recordingLogger) Debugf(string, ...any) {}

func TestHTTPErrLogger_ShortIDSafe(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	require.NotPanics(t, func() {
		l.Record("web put", 502, "ab12", "boom")
	})
	require.NoError(t, l.Close())
	require.Contains(t, buf.String(), "ab12")
}

// A batch summary reaches the Logger, never the captured writer. It says the
// cache is working, once per flush for the whole process, and a consumer whose
// own output is data somebody parses has to be able to quiet it.
func TestHTTPErrLogger_BatchHTTPSingleHit(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	l.RecordBatchHTTP(100, 25, 47*time.Millisecond)
	require.NoError(t, l.Close())

	require.Equal(t, "cacheprog: batch GET: 100 keys → 25 entries in 47ms\n", cap.String())
	require.Empty(t, buf.String(), "a summary is not a failure and must not reach the writer")
}

func TestHTTPErrLogger_BatchHTTPSingleMiss(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	l.RecordBatchHTTP(100, 0, 47*time.Millisecond)
	require.NoError(t, l.Close())

	require.Equal(t,
		"cacheprog: batch GET: 100 keys → 0 entries (server has no entries for any of them) in 47ms\n",
		cap.String())
}

func TestHTTPErrLogger_BatchHTTPCoalescedMisses(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	// Several all-miss batch HTTP requests, whose keys and durations the summary totals.
	l.RecordBatchHTTP(100, 0, 30*time.Millisecond)
	l.RecordBatchHTTP(80, 0, 50*time.Millisecond)
	l.RecordBatchHTTP(120, 0, 40*time.Millisecond)
	require.NoError(t, l.Close())

	require.Equal(t,
		"cacheprog: batch GET ×3: 300 keys → 0 entries (server has no entries), 120ms total\n",
		cap.String())
}

func TestHTTPErrLogger_BatchHTTPCoalescedHits(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	// The summary totals the keys, entries and duration.
	l.RecordBatchHTTP(100, 25, 47*time.Millisecond)
	l.RecordBatchHTTP(100, 30, 50*time.Millisecond)
	require.NoError(t, l.Close())

	require.Equal(t,
		"cacheprog: batch GET ×2: 200 keys → 55 entries, 97ms total\n",
		cap.String())
}

func TestHTTPErrLogger_BatchHTTPHitsAndMissesStayDistinct(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	l.RecordBatchHTTP(100, 0, 30*time.Millisecond)
	l.RecordBatchHTTP(50, 25, 40*time.Millisecond)
	require.NoError(t, l.Close())

	out := cap.String()
	require.Equal(t, 2, strings.Count(out, "\n"), "expected hit and miss buckets to stay separate, got: %q", out)
}

func TestHTTPErrLogger_BatchHTTPNilReceiver(t *testing.T) {
	var l *httpErrLogger
	// As above: no receiver means no writer, so the line goes to the package
	// logger and must be captured rather than left loose in the package.
	newCaptureLogger(t)
	require.NotPanics(t, func() {
		l.RecordBatchHTTP(100, 0, 30*time.Millisecond)
	})
}

// One flush carries both kinds, and they part company by destination: the
// failure to the writer, the summary to the Logger. That split is the whole
// point -- a consumer can quiet the second without losing the first.
func TestHTTPErrLogger_MixedHTTPErrAndBatchHTTP(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf)
	cap := newCaptureLogger(t)

	l.Record("web put", 502, "aaaaaaaa", "error code: 502")
	l.RecordBatchHTTP(100, 0, 30*time.Millisecond)
	require.NoError(t, l.Close())

	require.Contains(t, buf.String(), "HTTP 502")
	require.NotContains(t, buf.String(), "0 entries")
	require.Contains(t, cap.String(), "0 entries (server has no entries for any of them)")
	require.NotContains(t, cap.String(), "HTTP 502")
}

func TestHTTPErrLogger_NoFlushWhenEmpty(t *testing.T) {
	var buf syncBuffer // read below while the ticker goroutine is still live
	l := newHTTPErrLogger(&buf, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, buf.String(), "ticker must not emit when there are no records")
	require.NoError(t, l.Close())
}
