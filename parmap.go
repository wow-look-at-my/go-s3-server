// batchKeyWorkers bounds the goroutines one batch request uses for its per-key work: stat, open and the digest hash.

package main

import (
	"runtime"
	"sync"
)

// batchKeyWorkers bounds the goroutines one batch request uses for its per-key work: stat, open and the digest hash.
var batchKeyWorkers = max(4, runtime.GOMAXPROCS(0))

// parallelFilter runs fn on each item with up to batchKeyWorkers goroutines.
// It returns the results that fn kept, in the order of items.
func parallelFilter[T, R any](items []T, fn func(T) (R, bool)) []R {
	if len(items) == 0 {
		return nil
	}
	results := make([]R, len(items))
	kept := make([]bool, len(items))
	workers := min(batchKeyWorkers, len(items))
	if workers == 1 {
		for idx, item := range items {
			results[idx], kept[idx] = fn(item)
		}
	} else {
		var next sync.Mutex
		pos := 0
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					next.Lock()
					idx := pos
					pos++
					next.Unlock()
					if idx >= len(items) {
						return
					}
					results[idx], kept[idx] = fn(items[idx])
				}
			}()
		}
		wg.Wait()
	}
	out := results[:0]
	for idx := range results {
		if kept[idx] {
			out = append(out, results[idx])
		}
	}
	return out
}
