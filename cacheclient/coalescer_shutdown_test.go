package cacheclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetBatch_ShutdownDoesNotHangQueuedWaiters: a getBatch waiter whose
// request was still buffered in batchReqCh when the coalescer shut down must
// degrade to a miss, never block forever on a reply that will never come.
func TestGetBatch_ShutdownDoesNotHangQueuedWaiters(t *testing.T) {
	// Bare backend with NO coalescer goroutine: simulates the request
	// sitting in the buffered channel when shutdown lands.
	b := &WebBackend{
		batchReqCh: make(chan batchReq, 4),
		batchStop:  make(chan struct{}),
		batchDone:  make(chan struct{}),
	}

	type result struct {
		miss bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		_, _, _, _, miss, err := b.getBatch("aabbccdd", "go-buildcache/v1aabbccdd")
		done <- result{miss: miss, err: err}
	}()

	// Wait until the request is enqueued, then shut down without draining.
	require.Eventually(t, func() bool { return len(b.batchReqCh) == 1 }, time.Second, time.Millisecond)
	close(b.batchStop)
	close(b.batchDone)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.True(t, r.miss, "an undrained queued request must miss cleanly on shutdown")
	case <-time.After(2 * time.Second):
		t.Fatal("getBatch waiter hung after coalescer shutdown")
	}
}
