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

	var missed int
	var count sync.Mutex
	var waiting sync.WaitGroup
	// Every asker joins before the owner finishes. Dedup is about asks that
	// overlap, and an ask that arrives after the answer is a new one by
	// design, so a test that let them race would be testing the scheduler.
	var joined sync.WaitGroup
	joined.Add(askers)
	for range askers {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			flight, mine := fly.startGet("abc")
			joined.Done()
			if mine {
				// A second flight for a key somebody else is already fetching.
				// It still has to finish: an owner that walks away leaves every
				// later asker parked on a channel nothing closes.
				count.Lock()
				missed++
				count.Unlock()
				fly.finishGet("abc", flight)
				return
			}
			<-flight.done
			if string(flight.data) != "body" {
				count.Lock()
				missed++ // woke on a flight that carried nothing
				count.Unlock()
			}
		}()
	}

	joined.Wait()
	owner.data = []byte("body")
	fly.finishGet("abc", owner)
	waiting.Wait()
	if missed != 0 {
		test.Errorf("%d of %d askers did not share the first flight", missed, askers)
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
