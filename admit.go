// slotQueue hands out the server's work slots.

package main

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// slotQueue hands out the server's work slots.
type slotQueue struct {
	mu       sync.Mutex
	free     int
	demand   list.List // *slotWaiter, in arrival order
	prefetch list.List // *slotWaiter, in arrival order
}

type slotWaiter struct {
	ready   chan struct{}
	granted bool
}

func newSlotQueue(slots int) *slotQueue {
	return &slotQueue{free: slots}
}

// tryTake takes a free slot without waiting.
func (q *slotQueue) tryTake() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.free == 0 {
		return false
	}
	q.free--
	return true
}

// waiting is the number of requests in both classes that wait for a slot.
func (q *slotQueue) waiting() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.demand.Len() + q.prefetch.Len()
}

// take waits for a slot until wait passes or ctx ends. It refuses at once when maxQueued requests already wait.
func (q *slotQueue) take(ctx context.Context, prefetch bool, wait time.Duration, maxQueued int) bool {
	q.mu.Lock()
	if q.free > 0 {
		q.free--
		q.mu.Unlock()
		return true
	}
	if q.demand.Len()+q.prefetch.Len() >= maxQueued {
		q.mu.Unlock()
		return false
	}
	waiter := &slotWaiter{ready: make(chan struct{})}
	class := &q.demand
	if prefetch {
		class = &q.prefetch
	}
	elem := class.PushBack(waiter)
	q.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-waiter.ready:
		return true
	case <-timer.C:
	case <-ctx.Done():
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if !waiter.granted {
		class.Remove(elem)
		return false
	}
	// The slot came in the same instant the wait ended. A client that went away cannot use it.
	if ctx.Err() != nil {
		q.releaseLocked()
		return false
	}
	return true
}

// release hands a slot to the earliest demand waiter, else the earliest look-ahead waiter, else back to the free count.
func (q *slotQueue) release() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.releaseLocked()
}

func (q *slotQueue) releaseLocked() {
	for _, class := range []*list.List{&q.demand, &q.prefetch} {
		if front := class.Front(); front != nil {
			waiter := class.Remove(front).(*slotWaiter)
			waiter.granted = true
			close(waiter.ready)
			return
		}
	}
	q.free++
}
