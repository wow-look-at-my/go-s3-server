// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"encoding/hex"
	"fmt"
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
// minute. Each one opens this directory for itself: its own index writes, its
// own trim, its own connection to the store, and its own exit held open while
// it drains its own uploads.
//
// One process owns the cache instead. It is the first go command of the build,
// and it serves the others over a unix socket. A child holds no DiskCache at
// all: it asks for an action and gets back the identity of a file this process
// has already written, which it then opens.
//
// That leaves one writer for the directory, so the trim has a single owner and
// the invariant Cache.Close describes is enforceable rather than hoped for.

// brokerEnv names the socket the owner listens on. A process that finds it set
// and answering is a child. A process that finds it unset becomes the owner.
const brokerEnv = "GO_BUILDCACHE_BROKER"

// brokerOffEnv makes every process open the cache for itself, which is what a
// bisect of a broker-shaped problem wants.
const brokerOffEnv = "GO_BUILDCACHE_BROKER_OFF"

const (
	brokerPing  = "/v1/ping"
	brokerEntry = "/v1/entry"
	brokerPut   = "/v1/put"

	outputHeader = "Cache-Output-Id"
	sizeHeader   = "Cache-Size"
	mtimeHeader  = "Cache-Mtime"
	dirHeader    = "Cache-Dir"
	tierHeader   = "Cache-Tier"
)

// brokerServer serves the owner's cache to the processes below it.
type brokerServer struct {
	owner Cache
	dir   string
	sock  string
	tmp   string
	srv   *http.Server
	done  chan struct{}
	once  sync.Once
	fly   *flights
}

// liveBroker is the server this process runs, if it runs one.
var liveBroker atomic.Pointer[brokerServer]

// startBroker serves owner on a socket of its own and names that socket in the
// environment. It answers nil when this process must not serve, which is not
// an error: the cache then works as it did.
func serveBroker(owner Cache, dir string) {
	if os.Getenv(brokerOffEnv) != "" || os.Getenv(brokerEnv) != "" {
		return
	}
	// The socket sits in the cache directory, so a process that can reach the
	// cache can reach its owner. A unix path is bounded near 100 characters
	// and a cache directory is not, so a long one falls back to a private
	// temporary directory.
	sockDir, err := brokerDir(dir)
	if err != nil {
		return
	}
	sock := filepath.Join(sockDir, "b.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		os.RemoveAll(sockDir)
		return
	}
	bkr := &brokerServer{owner: owner, dir: dir, sock: sock, tmp: sockDir, done: make(chan struct{}), fly: newFlights()}
	mux := http.NewServeMux()
	mux.HandleFunc(brokerPing, bkr.handlePing)
	mux.HandleFunc(brokerEntry, bkr.handleEntry)
	mux.HandleFunc(brokerPut, bkr.handlePut)
	bkr.srv = &http.Server{Handler: mux}
	go func() {
		defer close(bkr.done)
		bkr.srv.Serve(listener)
	}()
	os.Setenv(brokerEnv, sock)
	liveBroker.Store(bkr)
	brokerNotice("cache: serving the build cache at %s", sock)
}

// BrokerEnviron is what a child needs in its environment to find this
// process's cache, or nothing when this process serves none.
//
// A consumer that starts its children with os.Environ needs none of this: the
// socket is already there. One that builds a curated environment, as the go
// command does for a test binary, adds these entries to it. A child without
// them opens the cache directory for itself, which is what this exists to
// stop.
func BrokerEnviron() []string {
	if bkr := liveBroker.Load(); bkr != nil {
		return []string{brokerEnv + "=" + bkr.sock}
	}
	return nil
}

// StopBroker closes the socket. The consumer calls it as the process exits,
// after every child it started has gone.
func StopBroker() {
	if bkr := liveBroker.Swap(nil); bkr != nil {
		bkr.stop()
		// The socket is gone, so nothing may be sent to it. A child handed
		// this process's environment after the stop would dial a name nobody
		// answers.
		os.Unsetenv(brokerEnv)
	}
}

// brokerDir makes the directory the socket lives in.
func brokerDir(cacheDir string) (string, error) {
	dir, err := os.MkdirTemp(cacheDir, "broker-")
	if err == nil && len(filepath.Join(dir, "b.sock")) < 100 {
		return dir, nil
	}
	if err == nil {
		os.RemoveAll(dir)
	}
	return os.MkdirTemp("", "gobuildcache-")
}

// stop closes the socket. Every child was started by this process and has
// exited, so nothing below is left to ask.
func (bkr *brokerServer) stop() {
	if bkr == nil {
		return
	}
	bkr.once.Do(func() {
		bkr.srv.Close()
		<-bkr.done
		os.RemoveAll(bkr.tmp)
	})
}

func (bkr *brokerServer) handlePing(wri http.ResponseWriter, req *http.Request) {
	wri.Header().Set(dirHeader, bkr.dir)
	wri.WriteHeader(http.StatusOK)
}

// handleEntry answers where an action's output is. The body never travels: the
// answer names a file in the cache directory both processes share, and the
// owner has written it before it answers.
//
// Concurrent asks for one action share the first one's work. The rest park on
// its channel until the close wakes them.
func (bkr *brokerServer) handleEntry(wri http.ResponseWriter, req *http.Request) {
	id, ok := parseActionID(req.URL.Query().Get("id"))
	if !ok {
		http.Error(wri, "entry needs a hex action id", http.StatusBadRequest)
		return
	}
	key := req.URL.Query().Get("id")
	flight, mine := bkr.fly.startGet(key)
	if mine {
		func() {
			defer bkr.fly.finishGet(key, flight)
			entry, tier, err := bkr.ownerGet(id)
			flight.entry, flight.tier, flight.miss = entry, tier, err != nil
		}()
	} else {
		select {
		case <-flight.done:
		case <-req.Context().Done():
			wri.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if flight.miss {
		wri.WriteHeader(http.StatusNoContent)
		return
	}
	wri.Header().Set(tierHeader, flight.tier)
	writeEntry(wri, flight.entry)
}

// ownerGet looks an action up in the owner's cache, naming the tier that
// answered when the owner can report one.
func (bkr *brokerServer) ownerGet(id ActionID) (Entry, string, error) {
	if tiered, ok := bkr.owner.(Tiered); ok {
		return tiered.GetTiered(id)
	}
	entry, err := bkr.owner.Get(id)
	return entry, TierDisk, err
}

// handlePut stores a body the child names by path. The owner reads it, so the
// child may exit as soon as this answers.
func (bkr *brokerServer) handlePut(wri http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	id, ok := parseActionID(query.Get("id"))
	path := query.Get("path")
	if !ok || path == "" {
		http.Error(wri, "put needs a hex action id and a path", http.StatusBadRequest)
		return
	}
	// Two children that built the same object offer it at once. The first
	// one's store is the one that happens: the rest park on its channel, then
	// take the entry it produced, so one body is read and stored once.
	key := query.Get("id")
	flight, mine := bkr.fly.startPut(key)
	if !mine {
		select {
		case <-flight.done:
		case <-req.Context().Done():
			wri.WriteHeader(http.StatusNoContent)
			return
		}
		if entry, err := bkr.owner.Get(id); err == nil {
			writeEntry(wri, entry)
			return
		}
	} else {
		defer bkr.fly.finishPut(key, flight)
	}
	file, err := os.Open(path)
	if err != nil {
		http.Error(wri, "put: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	out, size, err := bkr.owner.Put(id, file)
	if err != nil {
		http.Error(wri, "put: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeEntry(wri, Entry{OutputID: out, Size: size, Time: time.Now()})
}

// writeEntry answers an entry as headers. There is no body: the bytes are in
// the cache directory, under the name OutputID gives them.
func writeEntry(wri http.ResponseWriter, entry Entry) {
	wri.Header().Set(outputHeader, hex.EncodeToString(entry.OutputID[:]))
	wri.Header().Set(sizeHeader, strconv.FormatInt(entry.Size, 10))
	wri.Header().Set(mtimeHeader, strconv.FormatInt(entry.Time.UnixNano(), 10))
	wri.WriteHeader(http.StatusOK)
}

// parseActionID reads a hex action ID.
func parseActionID(text string) (ActionID, bool) {
	var id ActionID
	raw, err := hex.DecodeString(text)
	if err != nil || len(raw) != len(id) {
		return id, false
	}
	copy(id[:], raw)
	return id, true
}

// brokerNotice reports what the broker did, under the variable that makes the
// cache report itself.
func brokerNotice(format string, args ...any) {
	if !cacheDebug() {
		return
	}
	fmt.Fprintf(os.Stderr, "go: "+format+"\n", args...)
}
