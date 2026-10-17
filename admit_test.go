package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitFor starts a take of a single slot and answers the channel that reports it.
func waitFor(q *slotQueue, prefetch bool) chan bool {
	got := make(chan bool, 1)
	go func() { got <- q.take(context.Background(), prefetch, 10*time.Second, 64) }()
	return got
}

// A demand request that waits behind look-ahead requests gets the next slot.
func TestDemandRequestGoesBeforeWaitingLookAhead(t *testing.T) {
	q := newSlotQueue(1)
	require.True(t, q.tryTake())
	var prefetch []chan bool
	for range 3 {
		prefetch = append(prefetch, waitFor(q, true))
	}
	require.Eventually(t, func() bool { return q.waiting() == 3 }, 5*time.Second, time.Millisecond)
	demand := waitFor(q, false)
	require.Eventually(t, func() bool { return q.waiting() == 4 }, 5*time.Second, time.Millisecond)

	q.release()
	require.True(t, <-demand, "the freed slot goes to the demand request")
	for _, got := range prefetch {
		select {
		case <-got:
			t.Fatal("a look-ahead request took a slot that a demand request waited for")
		default:
		}
	}

	// With no demand waiting, the look-ahead requests go in arrival order.
	for idx, got := range prefetch {
		q.release()
		require.True(t, <-got, "look-ahead request %d", idx)
		for _, later := range prefetch[idx+1:] {
			select {
			case <-later:
				t.Fatal("a later look-ahead request went first")
			default:
			}
		}
	}
}

// A request that waits out its time leaves the queue, and the slot it never got is not lost.
func TestTimedOutWaiterLeavesNoSlotBehind(t *testing.T) {
	q := newSlotQueue(1)
	require.True(t, q.tryTake())
	require.False(t, q.take(context.Background(), false, 20*time.Millisecond, 64))
	require.Zero(t, q.waiting())
	q.release()
	require.True(t, q.tryTake(), "the released slot is free again")
}

// A full queue refuses at once.
func TestFullQueueRefusesAtOnce(t *testing.T) {
	q := newSlotQueue(1)
	require.True(t, q.tryTake())
	first := waitFor(q, true)
	require.Eventually(t, func() bool { return q.waiting() == 1 }, 5*time.Second, time.Millisecond)
	start := time.Now()
	require.False(t, q.take(context.Background(), false, 10*time.Second, 1))
	require.Less(t, time.Since(start), time.Second)
	q.release()
	require.True(t, <-first)
}
