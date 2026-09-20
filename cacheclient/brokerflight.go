package cacheclient

import "sync"

// Two processes of one build ask for the same object all the time: a test
// binary and the go command that starts it link the same package, and two
// script tests compile the same fixture. Without this each ask is its own
// request to the store, and each upload is its own body over the wire.
//
// A flight holds the first ask for a key. Every other ask for that key blocks
// on the first one's channel, which parks the goroutine in the runtime's
// scheduler. Nothing polls and nothing sleeps: a waiter is woken by the close,
// and the close happens once.

// getFlight is one in-progress read, shared by everyone who asked for its key.
type getFlight struct {
	done     chan struct{}
	outputID string
	data     []byte
	stampNS  int64
	miss     bool
}

// putFlight is one in-progress hand-over of a body, shared the same way. It
// carries no result: a caller needs to know the bytes are taken, and nothing
// else.
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
	return &flights{
		gets: make(map[string]*getFlight),
		puts: make(map[string]*putFlight),
	}
}

// startGet answers the flight for this key and whether this caller owns it. An
// owner runs the read and calls finishGet. Everybody else waits on the channel.
func (fli *flights) startGet(actionID string) (*getFlight, bool) {
	fli.mutex.Lock()
	defer fli.mutex.Unlock()
	if have, ok := fli.gets[actionID]; ok {
		return have, false
	}
	fresh := &getFlight{done: make(chan struct{})}
	fli.gets[actionID] = fresh
	return fresh, true
}

// finishGet publishes the result and wakes every waiter. The key leaves the map
// first, so the next ask for it starts a new flight rather than reading a
// result that is already spent.
func (fli *flights) finishGet(actionID string, flight *getFlight) {
	fli.mutex.Lock()
	delete(fli.gets, actionID)
	fli.mutex.Unlock()
	close(flight.done)
}

// startPut answers the flight for this key and whether this caller owns it.
func (fli *flights) startPut(actionID string) (*putFlight, bool) {
	fli.mutex.Lock()
	defer fli.mutex.Unlock()
	if have, ok := fli.puts[actionID]; ok {
		return have, false
	}
	fresh := &putFlight{done: make(chan struct{})}
	fli.puts[actionID] = fresh
	return fresh, true
}

// finishPut releases the key and wakes every waiter.
func (fli *flights) finishPut(actionID string, flight *putFlight) {
	fli.mutex.Lock()
	delete(fli.puts, actionID)
	fli.mutex.Unlock()
	close(flight.done)
}
