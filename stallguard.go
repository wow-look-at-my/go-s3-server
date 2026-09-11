package main

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// stallWindow is how long a request may go without moving a byte in either
// direction before the server hangs up on it.
//
// It replaces the whole-request read and write deadlines this server used to
// set. Those measured SIZE, not health: an object is capped at a gigabyte and
// a batch response streams thousands of them, so at the bandwidth a remote CI
// runner actually gets, the biggest healthy transfers could not finish inside
// any cap short enough to be useful against a wedged peer. Silence is what a
// wedged peer shows, and a transfer that keeps delivering bytes is fine
// however long it takes.
//
// The window must exceed the longest legitimate gap between bytes, which is
// the work a handler does before its first write: an index blob rebuild, or
// the stat and guard pass over the keys of a batch. It is still far under the
// five-minute total cap it replaces, so a genuinely stuck connection is now
// reaped sooner than before, not later.
// A var, so a test can shorten it rather than sleep out the real one.
var stallWindow = 60 * time.Second

// countingReader counts what a request body delivers, so an upload that is
// making progress reads as progress.
type countingReader struct {
	rc io.ReadCloser
	n  atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func (c *countingReader) Close() error { return c.rc.Close() }

// guardStall arms an inactivity watchdog over one request and returns the stop
// func the caller defers. The watchdog samples the bytes the response has
// written and the bytes the body has delivered. Movement re-arms it. Silence
// past stallWindow sets both connection deadlines to now, which fails the
// blocked read or write and lets the handler unwind.
//
// It takes the raw ResponseWriter, not the recorder wrapping it, because the
// controller has to reach the connection underneath.
//
// A deadline reaches an I/O operation, and nothing else. A handler wedged in
// its own work, rather than in a read or a write, keeps its slot until it
// returns. Admission control is what bounds that.
func guardStall(w http.ResponseWriter, r *http.Request, rec *statusRecorder) func() {
	body := &countingReader{}
	if r.Body != nil {
		body.rc = r.Body
		r.Body = body
	}

	rc := http.NewResponseController(w)
	var last int64
	var timer *time.Timer
	timer = time.AfterFunc(stallWindow, func() {
		moved := rec.progress.Load() + body.n.Load()
		if moved != last {
			last = moved
			timer.Reset(stallWindow)
			return
		}
		now := time.Now()
		_ = rc.SetReadDeadline(now)
		_ = rc.SetWriteDeadline(now)
	})
	return func() { timer.Stop() }
}
