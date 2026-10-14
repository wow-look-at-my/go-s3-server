package cacheclient

import (
	"math/rand"
	"slices"
	"testing"
)

// BenchmarkHashSetDelta merges a delta the size a go command sees after a
// couple of hours into an index the size of the shared cache's.
func BenchmarkHashSetDelta(b *testing.B) {
	rng := rand.New(rand.NewSource(4))
	base := make([]hashKey, 2_600_000)
	for idx := range base {
		base[idx] = rng.Uint64()
	}
	slices.Sort(base)
	delta := newHashSet(0)
	for range 133_000 {
		delta.addKey(rng.Uint64())
	}
	delta.compact()

	b.Run("bykey", func(b *testing.B) {
		for b.Loop() {
			set := newHashSetFromSorted(slices.Clone(base))
			for key := range delta.All() {
				set.addKey(key)
			}
		}
	})
	b.Run("addAll", func(b *testing.B) {
		for b.Loop() {
			set := newHashSetFromSorted(slices.Clone(base))
			set.addAll(delta)
		}
	})
}
