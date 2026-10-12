package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Results keep the input order, and the dropped items leave no gap.
func TestParallelFilterKeepsOrder(t *testing.T) {
	items := make([]int, 1000)
	for idx := range items {
		items[idx] = idx
	}
	got := parallelFilter(items, func(num int) (int, bool) { return num * 2, num%3 != 0 })
	var want []int
	for _, num := range items {
		if num%3 != 0 {
			want = append(want, num*2)
		}
	}
	assert.Equal(t, want, got)
	assert.Empty(t, parallelFilter([]int(nil), func(num int) (int, bool) { return num, true }))
	assert.Equal(t, []int{7}, parallelFilter([]int{7}, func(num int) (int, bool) { return num, true }))
}
