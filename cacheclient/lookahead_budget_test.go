package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The pool reads a whole batch response into memory before the populator sees
// any of it, so what it may hold has to be a size. The server caps a window at
// maxPrefetchEntries, which counts entries, and it cannot see how many pools
// exist: `dist test` runs many go processes at the same time and each builds
// its own. Without a byte budget the machine's peak is that count times a body
// size times the number of builds, which is how a windows runner reached "Out
// of memory" with an empty log.
func TestLookAheadGoesOverBudgetOnlyWhenItHoldsIt(t *testing.T) {
	la := &lookAhead{budget: 1024}
	assert.False(t, la.overBudget(), "an empty pool holds nothing back")

	release := la.charge([]BatchEntry{{Data: make([]byte, 1000)}})
	assert.False(t, la.overBudget(), "under the budget the next seed is still served")

	release2 := la.charge([]BatchEntry{{Data: make([]byte, 24)}})
	assert.True(t, la.overBudget(), "at the budget the next seed must be dropped")

	release2()
	assert.False(t, la.overBudget(), "a returned window admits the next seed")
	release()
	assert.Zero(t, la.held.Load(), "every charge must be released exactly once")
}

// A pool built with no budget must not refuse every seed. empty means
// unbounded, which is what the accounting reads as "nothing held back".
func TestLookAheadWithNoBudgetHoldsNothingBack(t *testing.T) {
	la := &lookAhead{}
	la.charge([]BatchEntry{{Data: make([]byte, 1<<20)}})
	assert.False(t, la.overBudget(), "a zero budget is unbounded, never permanently full")
}

// A positive count is honored up to maxLookAheadWorkers.
func TestLookAheadWorkerCount(t *testing.T) {
	for _, tc := range []struct {
		env     string
		set     bool
		workers int
	}{
		{set: false, workers: 0},
		{env: "0", set: true, workers: 0},
		{env: "-3", set: true, workers: 0},
		{env: "1", set: true, workers: 1},
		{env: "5", set: true, workers: 5},
		{env: "64", set: true, workers: 64},
		{env: "1000", set: true, workers: maxLookAheadWorkers},
	} {
		t.Run(tc.env, func(t *testing.T) {
			if tc.set {
				t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", tc.env)
			} else {
				t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "")
			}
			workers, depth := lookAheadDefaults()
			assert.Equal(t, tc.workers, workers)
			assert.Equal(t, tc.workers*8, depth)

			la := newLookAhead(&WebBackend{})
			if tc.workers == 0 {
				assert.Nil(t, la, "no workers means no pool")
				return
			}
			assert.NotNil(t, la)
			la.Close()
		})
	}
}

// The budget is a real number, not a name in a comment: an earlier revision
// described "prefetchBudget bytes" that no code defined.
func TestLookAheadBudgetIsPositiveByDefault(t *testing.T) {
	assert.Positive(t, lookAheadBudget())
}
