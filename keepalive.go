package main

import (
	"net/http"
	"sync"
	"time"
)

// batchKeepAlive is how long a batch put may store without a byte back to the client.
var batchKeepAlive = 10 * time.Second

type keepAlive struct {
	mu        sync.Mutex
	w         http.ResponseWriter
	rc        *http.ResponseController
	timer     *time.Timer
	committed bool
	done      bool
}

func startKeepAlive(w http.ResponseWriter) *keepAlive {
	k := &keepAlive{w: w, rc: http.NewResponseController(w)}
	// An HTTP/1 server closes the request body at the first write of the answer. The tar still has to be read after it.
	_ = k.rc.EnableFullDuplex()
	k.timer = time.AfterFunc(batchKeepAlive, k.tick)
	return k
}

func (k *keepAlive) tick() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.done {
		return
	}
	if !k.committed {
		k.w.Header().Set("Content-Type", "application/json")
		k.w.WriteHeader(http.StatusOK)
		k.committed = true
	}
	_, _ = k.w.Write([]byte(" "))
	_ = k.rc.Flush()
	k.timer.Reset(batchKeepAlive)
}

// stop ends the ticks.
func (k *keepAlive) stop() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.done = true
	k.timer.Stop()
	return k.committed
}
