// All rights reserved. Use of this source code is
// governed by a BSD-style license that can be found
// in the LICENSE file.

package cacheclient

import "sync"

// Processes of a single build ask for the same action all the time: a test
// binary and the go command that starts it link the same package, and script
// tests compile the same fixture. Without this each ask is its own trip to the
// store, and each stored body is written by of them.
//
// A flight holds the earliest ask. Nothing polls: a waiter wakes on the
// close, and the close happens a single time.

// getFlight is a single in-progress lookup, shared by everyone who asked for it.
type getFlight struct {
	done  chan struct{}
	entry Entry
	tier  string
	miss  bool
}

// putFlight is a single in-progress store, shared the same way. It
// carries no result: a caller needs to know the bytes are taken, and nothing else.
type putFlight struct {
	done chan struct{}
}

// flights holds what is in progress, keyed by action ID.
type flights struct {
	mutex sync.Mutex
	gets  map[string]*getFlight
	puts  map[string]*putFlight
}

func newFlights() *flights {
	return &flights{gets: make(map[string]*getFlight), puts: make(map[string]*putFlight)}
}

// startGet answers the flight for this action and whether this caller owns it.
// An owner does the work and calls finishGet. Everybody else waits.
func (fli *flights) startGet(key string) (*getFlight, bool) {
	fli.mutex.Lock()
	defer fli.mutex.Unlock()
	if have, ok := fli.gets[key]; ok {
		return have, false
	}
	fresh := &getFlight{done: make(chan struct{})}
	fli.gets[key] = fresh
	return fresh, true
}

// finishGet publishes the result and wakes every waiter. The key leaves the
// map so the next ask starts a flight of its own rather than reading a result
// that is already spent.
func (fli *flights) finishGet(key string, flight *getFlight) {
	fli.mutex.Lock()
	delete(fli.gets, key)
	fli.mutex.Unlock()
	close(flight.done)
}

// startPut answers the flight for this action and whether this caller owns it.
func (fli *flights) startPut(key string) (*putFlight, bool) {
	fli.mutex.Lock()
	defer fli.mutex.Unlock()
	if have, ok := fli.puts[key]; ok {
		return have, false
	}
	fresh := &putFlight{done: make(chan struct{})}
	fli.puts[key] = fresh
	return fresh, true
}

// finishPut releases the key and wakes every waiter.
func (fli *flights) finishPut(key string, flight *putFlight) {
	fli.mutex.Lock()
	delete(fli.puts, key)
	fli.mutex.Unlock()
	close(flight.done)
}
