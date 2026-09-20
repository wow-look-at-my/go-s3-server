package cacheclient

import (
	"sync"
	"testing"
	"time"
)

// A flight is what makes one ask out of many. The owner runs the work; every
// other caller parks on the channel until the owner closes it.
func TestFlightsCollapseConcurrentGets(test *testing.T) {
	fly := newFlights()
	const askers = 16

	owner, mine := fly.startGet("abc")
	if !mine {
		test.Fatal("the first ask does not own its flight")
	}

	var owners int
	var count sync.Mutex
	var waiting sync.WaitGroup
	for range askers {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			flight, mine := fly.startGet("abc")
			if mine {
				count.Lock()
				owners++
				count.Unlock()
				return
			}
			<-flight.done
			if string(flight.data) != "body" {
				count.Lock()
				owners++ // a waiter that woke on an unfinished flight
				count.Unlock()
			}
		}()
	}

	owner.data = []byte("body")
	fly.finishGet("abc", owner)
	waiting.Wait()
	if owners != 0 {
		test.Errorf("%d of %d askers did not share the first flight", owners, askers)
	}

	// The key is free again once its flight is done, so the next ask is a new
	// one rather than a reader of a spent result.
	if _, mine := fly.startGet("abc"); !mine {
		test.Error("the ask after a finished flight joined it instead of starting one")
	}
}

// A put flight carries no result. It exists so one body is read and uploaded
// once, however many children offer it.
func TestFlightsCollapseConcurrentPuts(test *testing.T) {
	fly := newFlights()
	owner, mine := fly.startPut("def")
	if !mine {
		test.Fatal("the first offer does not own its flight")
	}
	joined, mine := fly.startPut("def")
	if mine {
		test.Fatal("the second offer started its own flight")
	}
	if joined != owner {
		test.Fatal("the second offer joined a different flight")
	}

	woke := make(chan struct{})
	go func() {
		<-joined.done
		close(woke)
	}()
	select {
	case <-woke:
		test.Fatal("a waiter woke before the owner finished")
	case <-time.After(10 * time.Millisecond):
	}

	fly.finishPut("def", owner)
	select {
	case <-woke:
	case <-time.After(time.Second):
		test.Fatal("the waiter did not wake when the flight finished")
	}
}
