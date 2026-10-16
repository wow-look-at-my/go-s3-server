package cacheclient

import (
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
)

// drainLimit bounds what closeBody reads to save a connection.
const drainLimit = 256 << 10

// closeBody reads the rest of a response body, then closes it. An HTTP/1.1
// connection goes back to the pool only when its body was read to EOF.
func closeBody(body io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(body, drainLimit))
	body.Close()
}

// firstDial makes the first request of a process dial alone, so the others
// share its connection.
type firstDial struct {
	next    *http.Transport
	started atomic.Bool
	ready   chan struct{}
	once    sync.Once
}

func (gate *firstDial) RoundTrip(req *http.Request) (*http.Response, error) {
	if gate.started.CompareAndSwap(false, true) {
		release := func() { gate.once.Do(func() { close(gate.ready) }) }
		defer release()
		trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { release() }}
		return gate.next.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	}
	select {
	case <-gate.ready:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	return gate.next.RoundTrip(req)
}

func (gate *firstDial) CloseIdleConnections() {
	gate.next.CloseIdleConnections()
}
