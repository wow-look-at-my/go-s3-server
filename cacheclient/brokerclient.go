package cacheclient

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// brokerLink is a child's side of the socket. Every call blocks in a read on
// that socket, which parks the thread in the kernel until the answer arrives.
// There is no poll and no retry loop.
type brokerLink struct {
	client *http.Client
	path   string
}

// brokerIdleConns is how many sockets a child keeps warm. A go command runs its
// compiles in parallel, so its cache reads are parallel too, and a pool of one
// would serialize them behind each other.
const brokerIdleConns = 64

// newBrokerLink builds a link over a unix socket. The transport dials that one
// path whatever host a request names, so the URLs below carry a placeholder
// host and mean nothing to DNS.
func newBrokerLink(path string) *brokerLink {
	dialer := &net.Dialer{Timeout: brokerDialWait}
	return &brokerLink{
		path: path,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, "unix", path)
				},
				MaxIdleConns:        brokerIdleConns,
				MaxIdleConnsPerHost: brokerIdleConns,
				MaxConnsPerHost:     brokerIdleConns,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
	}
}

// brokerDialWait bounds the connect. A live broker on this machine accepts at
// once, so a wait past this is a socket whose owner is gone.
const brokerDialWait = 2 * time.Second

// alive reports whether the broker answers. A socket file outlives the process
// that made it, so its presence proves nothing and this asks instead.
func (link *brokerLink) alive() bool {
	resp, err := link.client.Get("http://broker" + brokerPing)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// get asks the broker for an object. A 204 is a miss, which is what the caller
// would have got from the store.
func (link *brokerLink) get(actionID string) (outputID string, data []byte, stamp time.Time, miss bool) {
	resp, err := link.client.Get("http://broker" + brokerGet + "?id=" + url.QueryEscape(actionID))
	if err != nil {
		return "", nil, time.Time{}, true
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return "", nil, time.Time{}, true
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, time.Time{}, true
	}
	if nanos, err := strconv.ParseInt(resp.Header.Get(mtimeHeader), 10, 64); err == nil {
		stamp = time.Unix(0, nanos)
	}
	return resp.Header.Get(outputHeader), body, stamp, false
}

// putFile hands a body to the broker by naming the file it sits in. The broker
// reads it before it answers, so this returns once the bytes are safely another
// process's problem.
func (link *brokerLink) putFile(actionID, outputID, path string) error {
	query := url.Values{"id": {actionID}, "out": {outputID}, "path": {path}}
	resp, err := link.client.Post("http://broker"+brokerPut+"?"+query.Encode(), "application/octet-stream", nil)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

// close releases the idle sockets. The uploads belong to the broker, so there
// is nothing here to drain.
func (link *brokerLink) close() {
	link.client.CloseIdleConnections()
}

// brokerFromEnv answers a live link to the broker this process was told about,
// or nil. A socket that answers nothing leaves the caller to dial the store.
func brokerFromEnv() *brokerLink {
	path := os.Getenv(BrokerEnv)
	if path == "" || os.Getenv(BrokerOffEnv) != "" {
		return nil
	}
	link := newBrokerLink(path)
	if !link.alive() {
		link.close()
		return nil
	}
	return link
}
