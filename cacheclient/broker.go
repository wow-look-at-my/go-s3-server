package cacheclient

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// A build is not one process. A test suite starts thousands of go commands a
// minute. Each one stands up its own connection pool, parses its own copy of
// the key index, and holds its own exit open while it drains its own uploads.
// The index is tens of megabytes. The store is one machine away from all of
// them.
//
// So the first process to reach the store keeps it. It serves the others over a
// unix socket and names that socket in the environment, which every process it
// starts inherits. A child asks the broker and never dials the store. One
// process holds the index, one holds the connection pool, and an upload runs
// after the process that produced the body has exited.
//
// A child that cannot reach the broker dials the store itself. A build with a
// slower tier is the cost of that; a build that stops is not.

// BrokerEnv names the socket a broker listens on. A process that finds it set
// and dialable is a child. A process that finds it unset becomes the broker.
const BrokerEnv = "GO_BUILDCACHE_BROKER"

// BrokerOffEnv turns the broker off when it is set to a non-empty value. Every
// process then reaches the store on its own, which is what a bisect wants.
const BrokerOffEnv = "GO_BUILDCACHE_BROKER_OFF"

// liveBroker is the broker this process serves, if it serves one.
var liveBroker atomic.Pointer[broker]

// BrokerAddr is the socket this process serves the cache on, or "" when it
// serves none. A consumer hands its children a curated environment rather than
// this process's own, and this is what such an environment must carry:
//
//	env = append(env, cacheclient.BrokerEnv+"="+cacheclient.BrokerAddr())
//
// Without it each child dials the store and loads an index of its own, which
// is the cost this whole file exists to remove.
func BrokerAddr() string {
	if bkr := liveBroker.Load(); bkr != nil {
		return bkr.path
	}
	return ""
}

const (
	brokerPing = "/v1/ping"
	brokerGet  = "/v1/get"
	brokerPut  = "/v1/put"

	// outputHeader carries the output ID of a served body. mtimeHeader carries
	// the store's own timestamp for it, which a caller reports as the hit's age.
	outputHeader = "Cache-Output-Id"
	mtimeHeader  = "Cache-Mtime"
)

// broker serves one backend to the processes below it.
type broker struct {
	back *WebBackend
	dir  string
	path string
	srv  *http.Server
	done chan struct{}
	once sync.Once
	// fly collapses concurrent asks for one key into one piece of work. Two
	// children compiling the same package reach the store once.
	fly *flights
}

// startBroker listens on a socket of its own and exports it. It answers nil
// when this process must not become a broker, which is never an error: the
// backend then works exactly as it did before.
func startBroker(back *WebBackend) *broker {
	if os.Getenv(BrokerOffEnv) != "" || os.Getenv(BrokerEnv) != "" {
		return nil
	}
	// The socket lives beside the index copy when there is one, so a sandbox
	// that mounts the cache reaches the broker too. A unix path is bounded at
	// about 100 characters, and a cache directory is not, so a long one falls
	// back to a private temporary directory.
	dir, err := brokerDir(back.indexDir)
	if err != nil {
		return nil
	}
	path := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		os.RemoveAll(dir)
		return nil
	}
	bkr := &broker{back: back, dir: dir, path: path, done: make(chan struct{}), fly: newFlights()}
	mux := http.NewServeMux()
	mux.HandleFunc(brokerPing, bkr.handlePing)
	mux.HandleFunc(brokerGet, bkr.handleGet)
	mux.HandleFunc(brokerPut, bkr.handlePut)
	bkr.srv = &http.Server{Handler: mux}
	go func() {
		defer close(bkr.done)
		bkr.srv.Serve(listener)
	}()
	// This process's own environment carries the socket. That covers a child
	// started with os.Environ. A child that never receives the socket dials
	// the store and loads an index of its own.
	os.Setenv(BrokerEnv, path)
	liveBroker.Store(bkr)
	logging.Infof("cacheprog: build cache broker at %s", path)
	return bkr
}

// brokerDir makes the directory the socket lives in. It prefers a directory
// beside the index copy, because a build that can read the cache can reach a
// socket there.
func brokerDir(indexDir string) (string, error) {
	if indexDir != "" {
		dir, err := os.MkdirTemp(indexDir, "broker-")
		if err == nil && len(filepath.Join(dir, "broker.sock")) < 100 {
			return dir, nil
		}
		if err == nil {
			os.RemoveAll(dir)
		}
	}
	return os.MkdirTemp("", "gobuildcache-")
}

// stop closes the socket and removes it. The uploads the children handed over
// are drained by the backend's own Close, which runs after this.
func (bkr *broker) stop() {
	if bkr == nil {
		return
	}
	bkr.once.Do(func() {
		bkr.srv.Close()
		<-bkr.done
		os.RemoveAll(bkr.dir)
		os.Unsetenv(BrokerEnv)
	})
}

func (bkr *broker) handlePing(wri http.ResponseWriter, req *http.Request) {
	wri.WriteHeader(http.StatusOK)
}

// handleGet answers one child's read from the backend this process owns. A miss
// is 204, which the child reports as a miss of its own.
//
// Concurrent asks for one key share the first one's fetch. The others block on
// its channel, which parks them until the close wakes them.
func (bkr *broker) handleGet(wri http.ResponseWriter, req *http.Request) {
	actionID := req.URL.Query().Get("id")
	flight, mine := bkr.fly.startGet(actionID)
	if mine {
		flight.outputID, flight.data, flight.stampNS, flight.miss = bkr.fetch(actionID)
		bkr.fly.finishGet(actionID, flight)
	} else {
		select {
		case <-flight.done:
		case <-req.Context().Done():
			// The child gave up. Its fetch is still somebody else's to finish.
			wri.WriteHeader(http.StatusNoContent)
			return
		}
	}
	outputID, data, miss := flight.outputID, flight.data, flight.miss
	stamp := time.Unix(0, flight.stampNS)
	if miss || data == nil {
		wri.WriteHeader(http.StatusNoContent)
		return
	}
	wri.Header().Set(outputHeader, outputID)
	wri.Header().Set(mtimeHeader, strconv.FormatInt(stamp.UnixNano(), 10))
	wri.Header().Set("Content-Length", strconv.Itoa(len(data)))
	wri.WriteHeader(http.StatusOK)
	wri.Write(data)
}

// handlePut takes over a child's upload. The body stays on disk and the request
// names it: both processes see the same cache directory, so sending the bytes
// over the socket would copy them for nothing.
//
// The read happens HERE, before the answer. The child is then free to exit, and
// free to let its own cache trim the file, because the bytes are already this
// process's. The compression and the upload run after the answer, on this
// process, and no child waits for either.
func (bkr *broker) handlePut(wri http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	actionID, outputID, path := query.Get("id"), query.Get("out"), query.Get("path")
	if actionID == "" || outputID == "" || path == "" {
		http.Error(wri, "put needs id, out and path", http.StatusBadRequest)
		return
	}
	// Two children that built the same object offer it at the same time. The
	// first one's hand-over is the one that happens: the rest wait on its
	// channel and then answer, so one body is read once and uploaded once.
	flight, mine := bkr.fly.startPut(actionID)
	if !mine {
		select {
		case <-flight.done:
		case <-req.Context().Done():
		}
		wri.WriteHeader(http.StatusNoContent)
		return
	}
	defer bkr.fly.finishPut(actionID, flight)
	data, err := os.ReadFile(path)
	if err != nil {
		// The body is gone. That is this build's object to re-make, not an error
		// the child can act on, so the answer stays 204.
		wri.WriteHeader(http.StatusNoContent)
		return
	}
	if err := bkr.back.Put(actionID, outputID, data); err != nil {
		logging.Infof("cacheprog: broker put %s: %v", ShortID(actionID), err)
	}
	wri.WriteHeader(http.StatusNoContent)
}

// fetch reads one object from the store for a flight's owner.
func (bkr *broker) fetch(actionID string) (outputID string, data []byte, stampNS int64, miss bool) {
	outputID, data, stamp, miss := bkr.back.Get(actionID)
	return outputID, data, stamp.UnixNano(), miss
}
